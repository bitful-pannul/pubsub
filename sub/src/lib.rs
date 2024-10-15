use anyhow::Result;
use kinode_process_lib::{
    await_message, call_init, get_state, println, Address, Message, ProcessId, Request, Response,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, str::FromStr};

use kinode::process::sub::{InitSubRequest, SubRequest, SubResponse};

const TIMER_PROCESS: &str = "timer:distro:sys";

wit_bindgen::generate!({
    path: "target/wit",
    world: "pubsub-template-os-v0",
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

    pub fn load() -> Self {
        if let Some(state) = get_state() {
            if let Ok(state) = serde_json::from_slice(&state) {
                return state;
            }
        }
        // if not found/successfully deserialized, wait for init message.
        loop {
            if let Ok(message) = await_message() {
                if let Ok(req) = serde_json::from_slice::<InitSubRequest>(&message.body()) {
                    if let Ok(parent) = Address::from_str(&req.parent) {
                        if let Ok(publisher) = Address::from_str(&req.publisher) {
                            let forward_to: Result<HashSet<Address>, _> = req
                                .forward_to
                                .into_iter()
                                .map(|addr_str| Address::from_str(&addr_str))
                                .collect();
                            if let Ok(forward_to) = forward_to {
                                return SubscriberState::new(Subscription {
                                    publisher,
                                    topic: req.topic,
                                    last_received_seq: req.from_sequence.unwrap_or(0),
                                    parent,
                                    forward_to,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
}

fn handle_message(message: Message, state: &mut SubscriberState) -> Result<()> {
    let timer_addrress = Address::new("our", ProcessId::from_str(TIMER_PROCESS).unwrap());

    if message.source() == &timer_addrress {
        // we should have an automatic loop fire every X seconds with help of the timer.
        // check heartbeats, retry messages if applicable.
        return Ok(());
    }

    if message.is_request() {
        let req: SubRequest = serde_json::from_slice(&message.body())?;
        handle_request(req, message.source(), state)?;
    } else {
        // MIght not even need this.
        let res: SubResponse = serde_json::from_slice(&message.body())?;
        handle_response(res, message.source(), state)?;
    }

    Ok(())
}

fn handle_request(req: SubRequest, source: &Address, state: &mut SubscriberState) -> Result<()> {
    // this only handles unsubscribe requests for now, which also shut down the worker.
    // subscribe requests are handled in the initialization.
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
                    .body(serde_json::to_vec(&req)?)
                    .send()?;

                // Note! also return boolean, so that this process can exit!
                // also note.. it'll restart upon boot. figure that out.
                // perhaps need some state in the lib struct that'll manage this
                // but we need that anyway I feel like.
            }
        }
        _ => {}
    }
    Ok(())
}

fn handle_response(res: SubResponse, source: &Address, state: &mut SubscriberState) -> Result<()> {
    // ping/pongs here?
    // needed? maybe just online checks..
    // but that's for the entire node? from the publisher side or no?
    // might need ping and pongs.
    Ok(())
}

call_init!(init);
fn init(our: Address) {
    println!("sub init");

    // need init message to set the parent address.

    // caps can be specific.
    // networking yes. but publisher can be public.
    // so this just needs the messaging cap back to parent process.
    let mut state = SubscriberState::load();

    // loaded state (got init config and subscription)
    // now, send subscribe request to publisher.

    // wait and then confirm, fire into loop?

    loop {
        match await_message() {
            Err(send_error) => println!("got SendError: {send_error}"),
            Ok(message) => {
                if let Err(e) = handle_message(message, &mut state) {
                    println!("error handling message: {e}");
                }
            }
        }
    }
}
