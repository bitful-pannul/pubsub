use anyhow::Result;
use kinode_process_lib::kv::Kv;
use kinode_process_lib::{
    kv, our_capabilities, spawn, Address, OnExit, PackageId, ProcessId, Request,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::binary_helpers::{populate_wasm, WasmType};

use crate::kinode::process::common::UnsubscribeRequest;
use crate::kinode::process::pub_::{
    InitPubRequest, Persistence, PubConfig, PubRequest, PublishRequest,
};
use crate::kinode::process::sub::{
    InitSubRequest, SubRequest, SubscribeRequest, SubscribeResponse,
};

/// Represents a publisher in the pub-sub system.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[allow(unused)]
pub struct Pub {
    publishers: HashMap<String, Publisher>,
    our: Address,
    kv: Kv<String, Vec<u8>>,
    default_config: PubConfig,
}

/// Metadata for a specific publisher.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Publisher {
    pub address: Address,
    pub config: PubConfig,
}

#[allow(unused)]
impl Pub {
    /// Creates a new `Pub` instance with the given address and default configuration.
    ///
    /// # Arguments
    ///
    /// * `our` - The address of the current process.
    /// * `default_config` - The default configuration for new publishers.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `Pub` instance or an error.
    pub fn new(our: &Address, default_config: PubConfig) -> Result<Self> {
        let db_name = format!("pub-{}", &our.process);

        let kv: Kv<String, Vec<u8>> = kv::open(our.package_id(), &db_name, None)?;
        populate_wasm(our, WasmType::Pub)?;

        // load state
        let pub_instance = match Self::load_state(&kv) {
            Ok(loaded_state) => loaded_state,
            Err(_) => {
                let new_state = Self {
                    publishers: HashMap::new(),
                    our: our.clone(),
                    kv: kv.clone(),
                    default_config,
                };
                new_state.save_state()?;
                new_state
            }
        };

        Ok(pub_instance)
    }

    /// Loads the state of the `Pub` instance from the key-value store.
    ///
    /// # Arguments
    ///
    /// * `kv` - The key-value store to load from.
    ///
    /// # Returns
    ///
    /// A `Result` containing the loaded `Pub` instance or an error.
    fn load_state(kv: &Kv<String, Vec<u8>>) -> Result<Self> {
        let state = kv.get(&"state".to_string())?;
        let pubsub: Self = serde_json::from_slice(&state)?;
        Ok(pubsub)
    }

    /// Saves the current state of the `Pub` instance to the key-value store.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an error.
    fn save_state(&self) -> Result<()> {
        let state = serde_json::to_vec(&self)?;
        self.kv.set(&"state".to_string(), &state, None)?;
        Ok(())
    }

    /// Creates a new topic with the given configuration or uses the default.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the new topic.
    /// * `config` - An optional configuration for the new topic. If None, uses the default.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or a `PubError`.
    pub fn new_topic(&mut self, topic: &str, config: Option<PubConfig>) -> Result<(), PubError> {
        // spawn new publisher process

        // TODO: implement more granular capabilities, not just passing all from parent.
        let our_caps = our_capabilities();
        let process_name = format!("pub-{}", topic);
        let wasm_path = format!("{}/pkg/pub.wasm", self.our.package_id());
        let process = spawn(
            Some(&process_name),
            &wasm_path,
            OnExit::Restart,
            our_caps,
            vec![],
            true,
        )
        .map_err(|e| PubError::SpawningError(e.to_string()))?;
        let publisher_address = Address::new(self.our.node.clone(), process);

        let config = config.unwrap_or(self.default_config.clone());

        // send pub info to new process
        let init_pub_request = InitPubRequest {
            topic: topic.to_string(),
            config: config,
        };
        Request::to(&publisher_address)
            .body(&init_pub_request)
            .send()
            .unwrap();

        let publisher = Publisher {
            address: publisher_address,
            config: config,
        };

        self.publishers.insert(topic.to_string(), publisher);
        Ok(())
    }

    /// Retrieves the publisher for a given topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic.
    ///
    /// # Returns
    ///
    /// An `Option` containing a reference to the `Publisher` if found, or `None`.
    pub fn get_topic(&self, topic: &str) -> Option<&Publisher> {
        self.publishers.get(topic)
    }

    /// Publishes a message to a specific topic, creating the topic if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to publish to.
    /// * `message` - The message to publish.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or a `PubError`.
    pub fn publish(&mut self, topic: &str, message: &[u8]) -> Result<(), PubError> {
        if let Some(publisher) = self.publishers.get(topic) {
            let publish_message = PubRequest::Publish(PublishRequest {
                topic: topic.to_string(),
                sequence: 0,
            });

            Request::to(&publisher.address)
                .body(&publish_message)
                .blob_bytes(message)
                .send()
                .unwrap();
            Ok(())
        } else {
            // this is config specific almost.
            // if you already don't have a publisher, do we spawn one?
            // leads to default config... which might not be what you want.
            // default config could also be stored and set in the api!
            self.new_topic(topic, None)?;
            self.publish(topic, message)?;
            // NOTE: this could be a topic.publish instead! to avoid infinite loops or something..?
            Ok(())
        }
    }

    /// Removes a topic and its associated publisher.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to remove.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or a `PubError`.
    pub fn remove_topic(&mut self, topic: &str) -> Result<(), PubError> {
        if let Some(publisher) = self.publishers.get(topic) {
            let req = PubRequest::Kill;
            Request::to(&publisher.address).body(&req).send().unwrap();
        }
        Ok(())
    }
}

/// Default implementation for PubConfig.
impl Default for PubConfig {
    fn default() -> Self {
        PubConfig {
            max_retry_attempts: 3,
            retry_interval: 120,
            heartbeat_interval: 60,
            default_persistence: Persistence::Memory(1000),
        }
    }
}

/// Represents a subscriber in the pub-sub system.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[allow(unused)]
pub struct Sub {
    subscriptions: HashMap<Subscription, Subscriber>,
    our: Address,
    kv: Kv<String, Vec<u8>>,
}

/// Represents a unique subscription identified by publisher and topic.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct Subscription {
    publisher: Address,
    topic: String,
}

/// Metadata for a specific subscriber.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscriber {
    address: Address,
    latest_sequence: u64,
}

#[allow(unused)]
impl Sub {
    /// Creates a new `Sub` instance.
    ///
    /// # Arguments
    ///
    /// * `our` - The address of the current process.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `Sub` instance or an error.
    pub fn new(our: &Address) -> Result<Self> {
        let db_name = format!("sub-{}", &our.process);
        let kv: Kv<String, Vec<u8>> = kv::open(our.package_id(), &db_name, None)?;

        populate_wasm(our, WasmType::Sub)?;

        // try loading state
        let sub_instance = match Self::load_state(&kv) {
            Ok(loaded_state) => loaded_state,
            Err(_) => {
                let new_state = Self {
                    subscriptions: HashMap::new(),
                    our: our.clone(),
                    kv: kv.clone(),
                };
                new_state.save_state()?;
                new_state
            }
        };

        Ok(sub_instance)
    }

    /// Loads the state of the `Sub` instance from the key-value store.
    ///
    /// # Arguments
    ///
    /// * `kv` - The key-value store to load from.
    ///
    /// # Returns
    ///
    /// A `Result` containing the loaded `Sub` instance or an error.
    fn load_state(kv: &Kv<String, Vec<u8>>) -> Result<Self> {
        let state = kv.get(&"state".to_string())?;
        let sub: Self = serde_json::from_slice(&state)?;
        Ok(sub)
    }

    /// Saves the current state of the `Sub` instance to the key-value store.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an error.
    fn save_state(&self) -> Result<()> {
        let state = serde_json::to_vec(&self)?;
        self.kv.set(&"state".to_string(), &state, None)?;
        Ok(())
    }

    /// Subscribes to a topic from a specific sequence number.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to subscribe to.
    /// * `publisher_pkg` - The package ID of the publisher.
    /// * `node` - The node of the publisher.
    /// * `sequence` - The optional sequence number to start from.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or a `SubError`.
    pub fn subscribe_from<T: Into<PackageId>>(
        &mut self,
        topic: &str,
        publisher_pkg: T,
        node: &str,
        sequence: Option<u64>,
    ) -> Result<(), SubError> {
        let publisher_pkg = publisher_pkg.into();
        let publisher_process = ProcessId::from((
            format!("pub-{}", topic).as_str(),
            publisher_pkg.package_name.as_str(),
            publisher_pkg.publisher_node.as_str(),
        ));

        let publisher = Address::new(node.to_string(), publisher_process);

        let subscription = Subscription {
            publisher: publisher.clone(),
            topic: topic.to_string(),
        };

        if let Some(subscriber) = self.subscriptions.get(&subscription) {
            // TODO: resubscribe, sending a normal subscribe request
            let req = SubRequest::Subscribe(SubscribeRequest {
                topic: topic.to_string(),
                from_sequence: sequence,
            });

            Request::to(&subscriber.address).body(&req).send().unwrap();

            return Ok(());
        }

        // TODO: more granular caps.
        let our_caps = our_capabilities();

        // we spawn a subscriber.
        let wasm_path = format!("{}/pkg/sub.wasm", self.our.package_id());

        let process = spawn(None, &wasm_path, OnExit::Restart, our_caps, vec![], false)
            .map_err(|e| SubError::SpawningError(e.to_string()))?;

        let subscriber_address = Address::new(self.our.node.clone(), process);

        let sub_init = InitSubRequest {
            topic: topic.to_string(),
            parent: self.our.to_string(),
            forward_to: vec![],
            publisher: publisher.to_string(),
            from_sequence: sequence,
        };

        let res = Request::to(&subscriber_address)
            .body(&sub_init)
            .send_and_await_response(10)
            .unwrap()
            .map_err(|e| SubError::SubInitError(e.to_string()))?;

        let sub_response = serde_json::from_slice::<SubscribeResponse>(&res.body())
            .map_err(|e| SubError::SerializeError(e.to_string()))?;

        if !sub_response.success {
            return Err(SubError::SubInitError(
                sub_response.error.unwrap_or_default(),
            ));
        }

        let subscriber = Subscriber {
            address: subscriber_address,
            latest_sequence: sequence.unwrap_or(0),
        };

        self.subscriptions.insert(subscription, subscriber);

        Ok(())
    }

    /// Subscribes to a topic from the latest available message.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to subscribe to.
    /// * `publisher_pkg` - The package ID of the publisher.
    /// * `node` - The node of the publisher.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or a `SubError`.
    pub fn subscribe<T: Into<PackageId>>(
        &mut self,
        topic: &str,
        publisher_pkg: T,
        node: &str,
    ) -> Result<(), SubError> {
        self.subscribe_from(topic, publisher_pkg, node, None)
    }

    /// Unsubscribes from a topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to unsubscribe from.
    /// * `publisher_pkg` - The package ID of the publisher.
    /// * `node` - The node of the publisher.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or a `SubError`.
    pub fn unsubscribe<T: Into<PackageId>>(
        &mut self,
        topic: &str,
        publisher_pkg: T,
        node: &str,
    ) -> Result<(), SubError> {
        let publisher_pkg = publisher_pkg.into();
        let publisher_process = ProcessId::from((
            format!("pub-{}", topic).as_str(),
            publisher_pkg.package_name.as_str(),
            publisher_pkg.publisher_node.as_str(),
        ));

        let publisher = Address::new(node.to_string(), publisher_process);

        let subscription = Subscription {
            publisher,
            topic: topic.to_string(),
        };

        if let Some(subscriber) = self.subscriptions.remove(&subscription) {
            // Send an unsubscribe request to the subscriber process
            let unsub_request = SubRequest::Unsubscribe(UnsubscribeRequest {
                topic: topic.to_string(),
            });
            Request::to(&subscriber.address)
                .body(&unsub_request)
                .send()
                .map_err(|e| SubError::UnsubscribeError(e.to_string()))?;

            Ok(())
        } else {
            Err(SubError::SubscriptionNotFound)
        }
    }
}

/// Errors that can occur in the subscriber operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubError {
    SpawningError(String),
    SerializeError(String),
    InvalidAddress,
    SubscriptionNotFound,
    SubInitError(String),
    UnsubscribeError(String),
}

/// Errors that can occur in the publisher operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PubError {
    TopicNotFound,
    SpawningError(String),
    NoPublisherProcessFound(String),
}

impl std::fmt::Display for PubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PubError::TopicNotFound => write!(f, "Topic not found"),
            PubError::SpawningError(s) => write!(f, "Error spawning process: {}", s),
            PubError::NoPublisherProcessFound(s) => {
                write!(f, "No publisher process found for: {}", s)
            }
        }
    }
}

impl std::fmt::Display for SubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubError::SpawningError(s) => write!(f, "Error spawning process: {}", s),
            SubError::SerializeError(s) => write!(f, "Serialization error: {}", s),
            SubError::InvalidAddress => write!(f, "Invalid address"),
            SubError::SubscriptionNotFound => write!(f, "Subscription not found"),
            SubError::SubInitError(s) => write!(f, "Subscriber initialization error: {}", s),
            SubError::UnsubscribeError(s) => write!(f, "Unsubscribe error: {}", s),
        }
    }
}

impl std::error::Error for SubError {}
impl std::error::Error for PubError {}
