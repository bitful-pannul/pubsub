use anyhow::Result;
use kinode_process_lib::{
    await_message, call_init, get_blob, get_state, kinode::process::standard::OnExit, println,
    save_capabilities, set_on_exit, Address, Capability, Message, ProcessId, Request, Response,
};
use kinode_pubsub::{
    InitPubRequest, MessageHistory, PubConfig, PubRequest, PublishRequest, SubscribeResponse,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, str::FromStr};

wit_bindgen::generate!({
    path: "target/wit",
    world: "process-v0",
    generate_unused_types: true,
    additional_derives: [PartialEq, serde::Deserialize, serde::Serialize],
});

const TIMER_PROCESS: &str = "timer:distro:sys";

// todo: figure out restart/state situation!
#[derive(Debug, Serialize, Deserialize)]
pub struct PublisherState {
    topic: String,
    last_sequence: u64,
    subscribers: HashSet<Address>,
    offline_subscribers: HashSet<(Address, u64)>, // (address, retry_count)
    config: PubConfig,
    parent: Address,
    message_history: MessageHistory,
}

impl PublisherState {
    pub fn new(config: PubConfig, parent: &Address, topic: String) -> Result<Self> {
        let message_history = MessageHistory::new(parent.clone(), config.default_persistence)?;

        Ok(PublisherState {
            topic,
            config,
            last_sequence: 0,
            subscribers: HashSet::new(), // what about an initial subscription list?
            offline_subscribers: HashSet::new(), // then it's more similar to gossip
            parent: parent.clone(),
            message_history,
        })
    }
    // todo: implement save state at the right moments.
    pub fn load(our: &Address) -> Result<Self> {
        if let Some(state) = get_state() {
            if let Ok(state) = serde_json::from_slice::<PublisherState>(&state) {
                return Ok(state);
            }
        }
        // if not found/successfully deserialized, wait for init message.
        let message = await_message()?;
        if message.source().node() != our.node()
            && message.source().package_id() != our.package_id()
        {
            return Err(anyhow::anyhow!(
                "publisher got init message from wrong source!"
            ));
        }

        let req: InitPubRequest = serde_json::from_slice(&message.body())?;
        Self::new(req.config, message.source(), req.topic)
    }
}

fn handle_message(message: Message, state: &mut PublisherState) -> Result<()> {
    let timer_addrress = Address::new("our", ProcessId::from_str(TIMER_PROCESS).unwrap());

    if message.source() == &timer_addrress {
        // we should have an automatic loop fire every X seconds with help of the timer.
        // check heartbeats, retry messages if applicable.
        return Ok(());
    }
    if message.is_request() {
        let req: PubRequest = serde_json::from_slice(&message.body())?;
        handle_request(req, message.source(), state, message.capabilities())?;
    } else {
        // maybe as a PONG response...
        // let res: PubSubResponse = serde_json::from_slice(&message.body())?;
        // handle_response(res, message.source(), state)?;
    }

    Ok(())
}

fn handle_request(
    req: PubRequest,
    source: &Address,
    state: &mut PublisherState,
    caps: &Vec<Capability>,
) -> Result<()> {
    match req {
        PubRequest::Subscribe(sub_req) => {
            let (success, error) = if state.topic == sub_req.topic {
                state.subscribers.insert(source.clone());
                // save messaging cap!
                save_capabilities(caps.as_slice());
                (true, None)
            } else {
                (
                    false,
                    Some(format!(
                        "error: publisher does not have requested topic: {}, has: {}",
                        sub_req.topic, state.topic
                    )),
                )
            };
            let res = SubscribeResponse {
                success,
                topic: sub_req.topic,
                error,
            };
            Response::new().body(res).send()?;

            // send historical messages too if requested.
            if success && sub_req.from_sequence.is_some() {
                let messages = state
                    .message_history
                    .get_messages_from(sub_req.from_sequence.unwrap())?;
                for message in messages {
                    // Send historical messages to the new subscriber
                    let historical_pub_req = PubRequest::Publish(PublishRequest {
                        topic: state.topic.clone(),
                        sequence: message.sequence,
                    });
                    Request::to(source)
                        .body(&historical_pub_req)
                        .blob_bytes(message.content)
                        .send()?;
                }
            }
        }
        PubRequest::Unsubscribe(unsub_req) => {
            let (success, error) = if state.topic == unsub_req.topic {
                state.subscribers.remove(source);
                (true, None)
            } else {
                (
                    false,
                    Some(format!(
                        "error: publisher does not have requested topic: {}, has: {}",
                        unsub_req.topic, state.topic
                    )),
                )
            };
            let res = SubscribeResponse {
                success,
                topic: unsub_req.topic,
                error,
            };
            Response::new().body(&res).send()?;
        }
        PubRequest::Publish(mut pub_msg) => {
            if source == &state.parent {
                // 1. Fetch and increment sequence number
                state.last_sequence += 1;
                let new_seq = state.last_sequence;

                let bytes = if let Some(blob) = get_blob() {
                    blob.bytes
                } else {
                    vec![]
                };

                // store message (if persistence is enabled)
                // doublecheck blob behaviour/persistence here (if none, no need to bring in and clone...)
                state.message_history.add_message(new_seq, bytes.clone())?;

                // distribute to subscribers!
                pub_msg.sequence = new_seq;
                let req = PubRequest::Publish(pub_msg);

                for subscriber in &state.subscribers {
                    Request::to(subscriber)
                        .body(&req)
                        .blob_bytes(bytes.clone())
                        .send()?;
                }
            }
        }
        PubRequest::Kill => {
            set_on_exit(&OnExit::None);
            // maybe clear state too? and kv store?
            panic!("publisher got kill request, exiting and not restarting");
        }
        _ => {}
    }
    Ok(())
}

fn handle_response(_res: PubRequest, _source: &Address, _state: &mut PublisherState) -> Result<()> {
    Ok(())
}

call_init!(init);
fn init(our: Address) {
    println!("publisher init");

    // upon init, publisher checks for a saved state+config,
    // if not found, it'll wait for a init message from the publisher including config.
    // if something fails in the init flow, it'll exit.
    let mut state: PublisherState = match PublisherState::load(&our) {
        Ok(state) => state,
        Err(e) => {
            println!("publisher: failure of loading state: {e} exiting");
            set_on_exit(&OnExit::None);
            return;
        }
    };

    loop {
        match await_message() {
            Err(send_error) => println!("publisher: got SendError: {send_error}"),
            Ok(message) => {
                if let Err(e) = handle_message(message, &mut state) {
                    println!("publisher: got error: {e}");
                }
            }
        }
    }
}
