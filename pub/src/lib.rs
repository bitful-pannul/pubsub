use anyhow::Result;
use kinode_process_lib::{
    await_message, call_init, get_state, println, Address, Message, ProcessId, Request, Response,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, str::FromStr};

use kinode::process::{
    common::SubscribeResponse,
    pub_::{InitPubRequest, Persistence, PubConfig, PubRequest, PublishRequest},
};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublisherState {
    topic: String,
    last_sequence: u64,
    subscribers: HashSet<Address>,
    offline_subscribers: HashSet<(Address, u64)>, // (address, retry_count)
    config: PubConfig,
    parent: Address,
    // add vector of messages (or last_sequence numbers!)
    // if in memory, just store the values (veqdeque or hashmap?)
    // if disk, do the same but the values are pointers to keys?
}

impl PublisherState {
    pub fn new(config: PubConfig, parent: &Address, topic: String) -> Self {
        PublisherState {
            topic,
            config,
            last_sequence: 0,
            subscribers: HashSet::new(), // what about an initial subscription list?
            offline_subscribers: HashSet::new(), // then it's more similar to gossip
            parent: parent.clone(),
        }
    }
    // todo: implement save state, ++ kv strategy + vecdeque stuff.
    pub fn load() -> Self {
        if let Some(state) = get_state() {
            if let Ok(state) = serde_json::from_slice(&state) {
                return state;
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

                // store message (if persistence is enabled)
                // TODO: implement message storage based on persistence configuration

                // distribute to subscribers!
                pub_msg.sequence = new_seq;
                let req = PubRequest::Publish(pub_msg);

                for subscriber in &state.subscribers {
                    Request::to(subscriber)
                        .body(serde_json::to_vec(&req)?)
                        .inherit(true)
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
    println!("pub init");

    // upon init, publisher checks for a saved state+config,
    // if not found, it'll wait for a init message from the publisher including config.
    let mut state: PublisherState = PublisherState::load();

    loop {
        match await_message() {
            Err(send_error) => println!("got SendError: {send_error}"),
            Ok(message) => {
                if let Err(e) = handle_message(message, &mut state) {
                    println!("got error: {e}");
                }
            }
        }
    }
}
