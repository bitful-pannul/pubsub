use anyhow::Result;
use kinode_process_lib::{
    await_message, call_init, get_state, println, Address, Message, ProcessId, Request, Response,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, str::FromStr};

use kinode::process::{
    common::SubscribeResponse,
    pub_::{InitPubRequest, PubConfig, PubRequest, PublishRequest},
    standard::get_blob,
};

mod history;
mod pubsub;

use history::MessageHistory;
wit_bindgen::generate!({
    path: "target/wit",
    world: "pubsub-template-os-v0",
    generate_unused_types: true,
    additional_derives: [PartialEq, serde::Deserialize, serde::Serialize],
});

// s123123123123132:callat:publisher.os

const TIMER_PROCESS: &str = "timer:distro:sys";

// todo: figure out restart/state situation! especially if exit situation! :)
//       also, do try out exit:: with restart functionality especially here.
//       in the subscriber, probably not possible.

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
        // todo: it's by package_id anyway, we can use parent as "our" here.
        let message_history = MessageHistory::new(parent.clone(), config.default_persistence)?;
        // wat do about errors...
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
    // todo: implement save state at the right moments... recovery too.
    pub fn load() -> Result<Self> {
        if let Some(state) = get_state() {
            if let Ok(state) = serde_json::from_slice::<PublisherState>(&state) {
                return Ok(state);
            }
        }
        // if not found/successfully deserialized, wait for init message.
        // todo: add check of our_package = this_package
        loop {
            if let Ok(message) = await_message() {
                if let Ok(req) = serde_json::from_slice::<InitPubRequest>(&message.body()) {
                    return Self::new(req.config, message.source(), req.topic);
                }
            }
        }
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
        handle_request(req, message.source(), state)?;
    } else {
        // not sure if we need this.
        // maybe as a PONG response...
        // let res: PubSubResponse = serde_json::from_slice(&message.body())?;
        // handle_response(res, message.source(), state)?;
    }

    Ok(())
}

fn handle_request(req: PubRequest, source: &Address, state: &mut PublisherState) -> Result<()> {
    match req {
        PubRequest::Subscribe(sub_req) => {
            let (success, error) = if state.topic == sub_req.topic {
                state.subscribers.insert(source.clone());
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
            Response::new().body(serde_json::to_vec(&res)?).send()?;

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
                        .body(serde_json::to_vec(&historical_pub_req)?)
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
            Response::new().body(serde_json::to_vec(&res)?).send()?;
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
                        .body(serde_json::to_vec(&req)?)
                        .blob_bytes(bytes.clone())
                        .send()?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

// might not need at all.
fn handle_response(res: PubRequest, source: &Address, state: &mut PublisherState) -> Result<()> {
    Ok(())
}

call_init!(init);
fn init(_our: Address) {
    println!("publisher init");

    // upon init, publisher checks for a saved state+config,
    // if not found, it'll wait for a init message from the publisher including config.
    let mut state: PublisherState = match PublisherState::load() {
        Ok(state) => state,
        Err(e) => {
            println!("publisher: BRUTAL failure of loading state: {e} EXITING");
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
