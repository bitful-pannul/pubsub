use kinode_process_lib::Address;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

/// Publisher Config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublisherConfig {
    pub max_retry_attempts: u32,          // default 3
    pub retry_interval: Duration,         // default 2 minutes
    pub heartbeat_interval: Duration,     // default 30 seconds
    pub default_persistence: Persistence, // default Memory { max_length: 1000 }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Persistence {
    None,                         // retains no messages, fire and forget.
    Memory { max_length: usize }, // retains max_length of messages in memory.
    Disk { max_length: usize },   // retains max_length of messages on disk in key value store.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PubSubRequest {
    InitPub(InitPubRequest),
    InitSub(InitSubRequest),
    Subscribe(SubscribeRequest),
    Unsubscribe(UnsubscribeRequest),
    Publish(PublishMessage),
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
    pub sequence: u64,
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

// New API struct
pub struct PubSub {
    subscriptions: HashMap<String, u64>,
    // pending_messages: Vec<PubSubRequest>, <- this should be the persistence vecdeque
    config: PubSubConfig,
}

// Configuration options
pub struct PubSubConfig {
    pub max_message_size: usize, // necessary or no? 10mb limit on messages enforced on kinode level already
    pub persistence_path: Option<String>,
    pub max_retry_attempts: u32,
    pub heartbeat_interval: Duration,
}

impl Default for PubSubConfig {
    fn default() -> Self {
        PubSubConfig {
            max_message_size: 7_000_000, // 7MB
            persistence_path: None,
            max_retry_attempts: 3,
            heartbeat_interval: Duration::from_secs(30),
        }
    }
}

impl PubSub {
    pub fn new(config: PubSubConfig) -> Self {
        PubSub {
            subscriptions: HashMap::new(),
            config,
        }
    }

    pub fn get_subscribed_topics(&self) -> HashSet<String> {
        self.subscriptions.keys().cloned().collect()
    }

    pub fn get_last_sequence(&self, topic: &str) -> Option<u64> {
        self.subscriptions.get(topic).cloned()
    }

    // Persistence methods (placeholder implementations)
    pub fn save_state(&self) -> Result<(), &'static str> {
        if self.config.persistence_path.is_some() {
            // Implement state saving logic here
            Ok(())
        } else {
            Err("Persistence not configured")
        }
    }

    pub fn load_state(&mut self) -> Result<(), &'static str> {
        if self.config.persistence_path.is_some() {
            // Implement state loading logic here
            Ok(())
        } else {
            Err("Persistence not configured")
        }
    }
}
