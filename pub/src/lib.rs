use anyhow::Result;
use kinode_process_lib::{
    await_message, call_init, get_state, println, Address, Message, ProcessId, Request, Response,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::Duration,
};

mod pubsub;
use pubsub::{
    InitPubRequest, Persistence, PubSubRequest, PubSubResponse, PublisherConfig, SubscribeResponse,
};

wit_bindgen::generate!({
    path: "target/wit",
    world: "process-v0",
});

const TIMER_PROCESS: &str = "timer:distro:sys";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublisherState {
    topics: HashMap<String, TopicState>,
    config: PublisherConfig,
    parent: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicState {
    persistence: Persistence, // note: could be in config too, making it topic-wide.
    last_sequence: u64,
    subscribers: HashMap<Address, SubscriberState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriberState {
    last_acked_sequence: u64,
    pending_acks: HashMap<u64, RetryInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryInfo {
    retry_count: u32,
    last_sent: u64, // unix timestamp?
}

impl Default for SubscriberState {
    fn default() -> Self {
        SubscriberState {
            last_acked_sequence: 0,
            pending_acks: HashMap::new(),
        }
    }
}

impl PublisherState {
    pub fn new(config: PublisherConfig, parent: &Address) -> Self {
        PublisherState {
            topics: HashMap::new(),
            config,
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
        loop {
            if let Ok(message) = await_message() {
                if let Ok(req) = serde_json::from_slice::<InitPubRequest>(&message.body()) {
                    return Self::new(req.config, message.source());
                }
            }
        }
    }
}

impl Default for PublisherConfig {
    fn default() -> Self {
        PublisherConfig {
            max_retry_attempts: 3,
            heartbeat_interval: Duration::from_secs(30),
            default_persistence: Persistence::Memory { max_length: 1000 },
            retry_interval: Duration::from_secs(120),
        }
    }
}

impl PublisherConfig {
    pub fn new(
        max_retry_attempts: u32,
        heartbeat_interval: Duration,
        default_persistence: Persistence,
        retry_interval: Duration,
    ) -> Self {
        PublisherConfig {
            max_retry_attempts,
            heartbeat_interval,
            default_persistence,
            retry_interval,
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
        let req: PubSubRequest = serde_json::from_slice(&message.body())?;
        handle_request(req, message.source(), state)?;
    } else {
        let res: PubSubResponse = serde_json::from_slice(&message.body())?;
        handle_response(res, message.source(), state)?;
    }

    Ok(())
}

fn handle_request(req: PubSubRequest, source: &Address, state: &mut PublisherState) -> Result<()> {
    match req {
        PubSubRequest::Subscribe(sub_req) => {
            for topic in &sub_req.topics {
                if let Some(topic_state) = state.topics.get_mut(topic) {
                    topic_state
                        .subscribers
                        .insert(source.clone(), SubscriberState::default());
                }
                // todo:
                // if we have stored some old message, and sub is not up to date, send those.
            }
            let res = SubscribeResponse {
                success: true,
                topics: sub_req.topics,
                error: None,
            };
            Response::new().body(serde_json::to_vec(&res)?).send()?;
        }
        PubSubRequest::Unsubscribe(unsub_req) => {
            for topic in &unsub_req.topics {
                if let Some(topic_state) = state.topics.get_mut(topic) {
                    topic_state.subscribers.remove(&source.clone());
                }
            }
            // TODO: add UnsubscribeResponse
            let res = SubscribeResponse {
                success: true,
                topics: unsub_req.topics,
                error: None,
            };
            Response::new().body(serde_json::to_vec(&res)?).send()?;
        }
        PubSubRequest::Publish(pub_msg) => {
            if source == &state.parent {
                // 1. Fetch and increment sequence number
                let new_seq = {
                    let topic_state =
                        state
                            .topics
                            .entry(pub_msg.topic.clone())
                            .or_insert_with(|| TopicState {
                                persistence: state.config.default_persistence.clone(),
                                last_sequence: 0,
                                subscribers: HashMap::new(),
                            });
                    topic_state.last_sequence += 1;
                    topic_state.last_sequence
                };

                // store message (if persistence is enabled)
                // TODO: implement message storage based on persistence configuration

                // distribute to subscribers!
                if let Some(topic_state) = state.topics.get_mut(&pub_msg.topic) {
                    let current_time = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();

                    for (subscriber, sub_state) in &mut topic_state.subscribers {
                        // update pending_acks
                        sub_state.pending_acks.insert(
                            new_seq,
                            RetryInfo {
                                retry_count: 0,
                                last_sent: current_time,
                            },
                        );

                        // Create a new PubSubRequest for each subscriber
                        let req = PubSubRequest::Publish(pub_msg.clone());

                        Request::to(subscriber)
                            .body(serde_json::to_vec(&req)?)
                            .inherit(true)
                            .send()?;
                    }
                }
            }
        }
        PubSubRequest::Acknowledge(ack) => {
            if let Some(topic_state) = state.topics.get_mut(&ack.topic) {
                if let Some(subscriber_state) = topic_state.subscribers.get_mut(&source) {
                    subscriber_state.last_acked_sequence = ack.sequence;
                    subscriber_state.pending_acks.remove(&ack.sequence);
                }
            }
        }
        PubSubRequest::Heartbeat(heartbeat) => {
            // 1. Update subscriber's last heartbeat
            // 2. Return HeartbeatResponse
        }
        _ => {}
    }
    Ok(())
}

fn handle_response(
    res: PubSubResponse,
    source: &Address,
    state: &mut PublisherState,
) -> Result<()> {
    Ok(())
}

call_init!(init);
fn init(our: Address) {
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
