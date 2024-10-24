use anyhow::Result;
use kinode::process::standard::clear_state;
use kinode_process_lib::{
    await_message, call_init, get_capability, get_state, kinode::process::standard::OnExit,
    println, set_on_exit, set_state, Address, Message, ProcessId, Request, Response,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, str::FromStr};

use kinode_pubsub::{InitSubRequest, SubRequest, SubResponse, SubscribeRequest, SubscribeResponse};

const TIMER_PROCESS: &str = "timer:distro:sys";

wit_bindgen::generate!({
    path: "target/wit",
    world: "process-v0",
    generate_unused_types: true,
    additional_derives: [PartialEq, serde::Deserialize, serde::Serialize],
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriberState {
    subscription: Subscription,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub publisher: Address,
    pub topic: String,
    pub last_received_seq: u64,
    pub parent: Address,
    pub forward_to: HashSet<Address>,
}

impl SubscriberState {
    pub fn new(sub: Subscription) -> Self {
        SubscriberState { subscription: sub }
    }

    pub fn save(&self) -> Result<()> {
        set_state(&serde_json::to_vec(&self)?);
        Ok(())
    }

    pub fn clear(&self) {
        clear_state();
    }

    pub fn load(our: &Address) -> Result<Self> {
        if let Some(state) = get_state().and_then(|s| serde_json::from_slice(&s).ok()) {
            return Ok(state);
        }

        Self::process_init_message(our)
    }

    fn process_init_message(our: &Address) -> Result<Self> {
        let message = await_message()?;

        let req: InitSubRequest = serde_json::from_slice(&message.body())?;

        let parent = Address::from_str(&req.parent)?;
        let publisher: Address = Address::from_str(&req.publisher)?;
        let forward_to: HashSet<Address> = req
            .forward_to
            .into_iter()
            .map(|addr_str| Address::from_str(&addr_str))
            .collect::<Result<_, _>>()?;

        let subscribe_request = SubRequest::Subscribe(SubscribeRequest {
            topic: req.topic.clone(),
            from_sequence: req.from_sequence,
        });

        let messaging_cap = get_capability(our, "\"messaging\"").ok_or(anyhow::anyhow!(
            "Subscriber failed to get messaging capability"
        ))?;

        let response = Request::to(&publisher)
            .body(&subscribe_request)
            .capabilities(vec![messaging_cap])
            .send_and_await_response(10)??;

        let resp: SubscribeResponse = serde_json::from_slice(&response.body())?;

        // send response back to parent.
        Response::new().body(&resp).send()?;

        if !resp.success {
            return Err(anyhow::anyhow!("Subscription failed"));
        }

        Ok(SubscriberState::new(Subscription {
            parent,
            publisher,
            topic: resp.topic,
            last_received_seq: req.from_sequence.unwrap_or(0),
            forward_to,
        }))
    }
}

fn handle_message(our: &Address, message: Message, state: &mut SubscriberState) -> Result<()> {
    let timer_addrress = Address::new("our", ProcessId::from_str(TIMER_PROCESS).unwrap());

    if message.source() == &timer_addrress {
        // we should have an automatic loop fire every X seconds with help of the timer.
        // check heartbeats, retry messages if applicable.
        return Ok(());
    }

    if message.is_request() {
        let req: SubRequest = serde_json::from_slice(&message.body())?;
        handle_request(&our, req, message.source(), state)?;
    } else {
        let res: SubResponse = serde_json::from_slice(&message.body())?;
        handle_response(res, message.source(), state)?;
    }

    Ok(())
}

fn handle_request(
    our: &Address,
    req: SubRequest,
    source: &Address,
    state: &mut SubscriberState,
) -> Result<()> {
    match &req {
        SubRequest::Unsubscribe(unsub) => {
            if source == &state.subscription.parent {
                if state.subscription.topic == unsub.topic {
                    // return error too?
                    println!(
                        "parent tried to unsubscribe from unknown topic: {}, have topic {}",
                        unsub.topic, state.subscription.topic
                    );
                }
                Request::to(&state.subscription.publisher)
                    .body(&req)
                    .send()?;

                set_on_exit(&OnExit::None);
                state.clear();
                panic!("unsubscribed, exiting!");

                // also note.. it'll restart upon boot. figure that out.
                // perhaps need some state in the lib struct that'll manage this
                // but we need that anyway I feel like.
            }
        }
        SubRequest::Publish(pub_msg) => {
            if state.subscription.topic == pub_msg.topic {
                state.subscription.last_received_seq = pub_msg.sequence;
                // println!("sub: got message. seq: {}", pub_msg.sequence);

                // Forward to parent
                Request::to(&state.subscription.parent)
                    .body(&req)
                    .inherit(true)
                    .send()?;

                // Forward to other subscribers
                for forward_to in &state.subscription.forward_to {
                    Request::to(forward_to).body(&req).inherit(true).send()?;
                }
            }
        }
        SubRequest::Subscribe(_sub_req) => {
            // TODO: no send_and_await in this resubscribe
            // currently just shooting it away, ignoring response
            if source == &state.subscription.parent {
                let messaging_cap = get_capability(our, "\"messaging\"").ok_or(anyhow::anyhow!(
                    "Subscriber failed to get messaging capability"
                ))?;

                Request::to(&state.subscription.publisher)
                    .body(&req)
                    .capabilities(vec![messaging_cap])
                    .send()?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn handle_response(
    _res: SubResponse,
    _source: &Address,
    _state: &mut SubscriberState,
) -> Result<()> {
    // might need ping and pongs.
    Ok(())
}

call_init!(init);
fn init(our: Address) {
    println!("subscriber init");

    let mut state = match SubscriberState::load(&our) {
        Ok(state) => state,
        Err(e) => {
            println!("subscriber got error loading state: {}, exiting.", e);
            set_on_exit(&OnExit::None);
            return;
        }
    };

    let _ = state.save();
    loop {
        match await_message() {
            Err(send_error) => println!("subscriber: got SendError: {send_error}"),
            Ok(message) => {
                if let Err(e) = handle_message(&our, message, &mut state) {
                    println!("subscriber: error handling message: {e}");
                }
            }
        }
    }
}
