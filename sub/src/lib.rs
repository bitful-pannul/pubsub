use anyhow::Result;
use kinode_process_lib::{
    await_message, call_init, get_state, println, Address, Message, ProcessId, Request, Response,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};

mod pubsub;
use pubsub::{HeartbeatResponse, HeartbeatStatus, InitSubRequest, PubSubRequest, PubSubResponse};

const TIMER_PROCESS: &str = "timer:distro:sys";

wit_bindgen::generate!({
    path: "target/wit",
    world: "pubsub-template-os-v0",
    generate_unused_types: true,
    additional_derives: [PartialEq, serde::Deserialize, serde::Serialize],
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriberState {
    subscriptions: HashMap<(Address, String), SubscriptionInfo>,
    parent: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionInfo {
    last_received_seq: u64,
    // retain policy/config/similar stuff here?
}

impl SubscriberState {
    pub fn new(parent: Address) -> Self {
        SubscriberState {
            subscriptions: HashMap::new(),
            parent,
        }
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
                    return SubscriberState::new(req.config.parent);
                }
            }
        }
    }

    pub fn update_sequence(
        &mut self,
        publisher: &Address,
        topic: &str,
        sequence: u64,
    ) -> Result<()> {
        let subscription = self
            .subscriptions
            .get_mut(&(publisher.clone(), topic.to_string()))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Not subscribed to topic '{}' from publisher {:?}",
                    topic,
                    publisher
                )
            })?;

        if sequence > subscription.last_received_seq {
            subscription.last_received_seq = sequence;
        }
        Ok(())
    }

    pub fn acknowledge(&mut self, publisher: &Address, topic: &str, sequence: u64) -> Result<()> {
        let subscription = self
            .subscriptions
            .get_mut(&(publisher.clone(), topic.to_string()))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Not subscribed to topic '{}' from publisher {:?}",
                    topic,
                    publisher
                )
            })?;

        if sequence > subscription.last_received_seq {
            subscription.last_received_seq = sequence;
        }
        Ok(())
    }

    // todo: implement save, resubscribe
}

fn handle_message(message: Message, state: &mut SubscriberState) -> Result<()> {
    let timer_addrress = Address::new("our", ProcessId::from_str(TIMER_PROCESS).unwrap());

    if message.source() == &timer_addrress {
        // we should have an automatic loop fire every X seconds with help of the timer.
        // check heartbeats, retry messages if applicable.
        return Ok(());
    }

    if message.is_request() {
        let req: PubSubRequest = serde_json::from_slice(&message.body())?;
        handle_request(req, message.source(), state)?;
    } else {
        let res: PubSubResponse = serde_json::from_slice(&message.body())?;
        handle_response(res, message.source(), state)?;
    }

    Ok(())
}

fn handle_request(req: PubSubRequest, source: &Address, state: &mut SubscriberState) -> Result<()> {
    match req {
        PubSubRequest::Subscribe(sub_req) => {
            if source == &state.parent {
                for topic in &sub_req.topics {
                    state.subscriptions.insert(
                        (sub_req.target.clone(), topic.clone()),
                        SubscriptionInfo {
                            last_received_seq: 0,
                        },
                    );
                }

                Request::to(&sub_req.target)
                    .body(serde_json::to_vec(&sub_req)?)
                    .send()?;
            }
        }
        PubSubRequest::Unsubscribe(unsub_req) => {
            if source == &state.parent {
                for topic in &unsub_req.topics {
                    state
                        .subscriptions
                        .remove(&(unsub_req.target.clone(), topic.clone()));
                }
                Request::to(&unsub_req.target)
                    .body(serde_json::to_vec(&unsub_req)?)
                    .send()?;
            }
        }
        PubSubRequest::Publish(pub_msg) => {
            // check if we have an active subscription to this publisher/topic
            if let Some(_subscription) = state
                .subscriptions
                .get_mut(&(source.clone(), pub_msg.topic.clone()))
            {
                // inherit the published content from the message blob
                state.update_sequence(source, &pub_msg.topic, pub_msg.sequence)?;

                // note: this should also send a message response.
                state.acknowledge(source, &pub_msg.topic, pub_msg.sequence)?;
                // todo: inherit bytes and send to parent
            }
        }
        // NOTE: heartbeat should include topic?
        PubSubRequest::Heartbeat(_heartbeat) => {
            let heartbeat_resp = PubSubResponse::Heartbeat(HeartbeatResponse {
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs(),
                status: HeartbeatStatus::Ok,
            });
            Response::new()
                .body(serde_json::to_vec(&heartbeat_resp)?)
                .send()?;
        }
        _ => {}
    }
    Ok(())
}

fn handle_response(
    res: PubSubResponse,
    source: &Address,
    state: &mut SubscriberState,
) -> Result<()> {
    match res {
        PubSubResponse::Subscribe(sub_resp) => {
            // might not need this if we send_and_await the original sub req.
        }
        PubSubResponse::Unsubscribe(unsub_resp) => {
            // might not need this if we send_and_await the original unsub req.
        }
        _ => {}
    }

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
