use kinode_process_lib::Address;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

wit_bindgen::generate!({
    path: "target/wit",
    world: "process-v0",
});

// main library helper struct

// let fly = PubSub::new(None);

// fly.pub("topic1", b"hello there");
//

// fly.sub("topic1", "doria.kino".into());
//
//

// req.metadata() .metadata() .context()
//

// req => {
//   // !!!notSend_and_await
// req::to().send()
// }
//
//

// handle_incoming_sub() -> {
// }

#[allow(unused)]
pub struct PubSub {
    subscriptions: HashMap<(Address, String), u64>, // (publisher, topic) -> sequence (do we need this)
}

#[allow(unused)]
impl PubSub {
    pub fn new() -> Self {
        // spawn pub + sub process. (read from predictable)
        PubSub {
            subscriptions: HashMap::new(),
        }
    }

    pub fn get_subscribed_topics(&self) -> HashSet<(Address, String)> {
        self.subscriptions.keys().cloned().collect()
    }

    pub fn get_last_sequence(&self, publisher: &Address, topic: &str) -> Option<u64> {
        self.subscriptions
            .get(&(publisher.clone(), topic.to_string()))
            .cloned()
    }
}

/// Publisher Config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublisherConfig {
    pub max_retry_attempts: u32,          // default 3
    pub retry_interval: Duration,         // default 2 minutes
    pub heartbeat_interval: Duration,     // default 30 seconds,
    pub default_persistence: Persistence, // default Memory { max_length: 1000 }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Persistence {
    None,                         // retains no messages, fire and forget.
    Memory { max_length: usize }, // retains max_length of messages in memory.
    Disk { max_length: usize },   // retains max_length of messages on disk in key value store.
                                  // pub:package_id:publisher_id
                                  // sub:package_id:subscriber_id
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PubSubRequest {
    InitPub(InitPubRequest),
    InitSub(InitSubRequest),
    Subscribe(SubscribeRequest),
    Unsubscribe(UnsubscribeRequest),
    Publish(PublishMessage),
    //
    //
    // maybe these are unnecesary
    Acknowledge(Ack),
    FetchMessages(FetchMessagesRequest), // maybe remove.
    Heartbeat(Heartbeat),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PubSubResponse {
    Subscribe(SubscribeResponse),
    Unsubscribe(UnsubscribeResponse),
    FetchMessages(FetchMessagesResponse),
    Heartbeat(HeartbeatResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitPubRequest {
    pub config: PublisherConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitSubRequest {
    pub config: SubscriberConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriberConfig {
    pub parent: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeRequest {
    pub target: Address, // note see if target is publisher or a subscriber, tailor
    pub topics: Vec<String>,
    pub from_sequence: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeResponse {
    pub success: bool,
    pub topics: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
    pub target: Address, // note see if target is publisher or a subscriber, tailor
    pub topics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubscribeResponse {
    pub success: bool,
    pub topics: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishMessage {
    pub topic: String,
    pub sequence: u64, // [1,2, 4, 6] - > [3, 5]
                       // payload: as blob
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ack {
    pub topic: String,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchMessagesRequest {
    // maybe remove
    pub subscriber: Address,
    pub publisher: Address,
    pub topic: String,
    pub from_sequence: u64,
    pub max_messages: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchMessagesResponse {
    // maybe remove
    pub messages: Vec<PublishMessage>,
    pub more_available: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub timestamp: u64,
    pub status: HeartbeatStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HeartbeatStatus {
    Ok,
    Busy,
    Error(String),
}
