use anyhow::Result;
use kinode_process_lib::{our_capabilities, spawn, Address, OnExit, PackageId, ProcessId, Request};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::kinode::process::common::UnsubscribeRequest;
use crate::kinode::process::pub_::{
    InitPubRequest, Persistence, PubConfig, PubRequest, PublishRequest,
};
use crate::kinode::process::sub::{InitSubRequest, SubRequest, SubResponse};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(unused)]
pub struct Pub {
    publishers: HashMap<String, Publisher>, // topic, metadata
    our: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Publisher {
    pub address: Address,
    pub config: PubConfig,
}

#[allow(unused)]
impl Pub {
    pub fn new(our: &Address) -> Self {
        // load in state of previous publishers here?
        // save state to kv? because it's being used as a library...
        Pub {
            publishers: HashMap::new(),
            our: our.clone(),
        }
    }

    pub fn new_topic(&mut self, topic: &str, config: PubConfig) -> Result<(), PubError> {
        // spawn new publisher process

        // TODO: implement more granular capabilities, not just passing all from parent.
        //
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

        // send pub info to new process
        let init_pub_request = InitPubRequest {
            topic: topic.to_string(),
            config: config,
        };
        Request::to(&publisher_address)
            .body(serde_json::to_vec(&init_pub_request).unwrap())
            .send()
            .unwrap();

        let publisher = Publisher {
            address: publisher_address,
            config: config,
        };

        self.publishers.insert(topic.to_string(), publisher);
        Ok(())
    }

    pub fn get_topic(&self, topic: &str) -> Option<&Publisher> {
        self.publishers.get(topic)
    }

    pub fn publish(&mut self, topic: &str, message: &[u8]) -> Result<(), PubError> {
        if let Some(publisher) = self.publishers.get(topic) {
            let publish_message = PubRequest::Publish(PublishRequest {
                topic: topic.to_string(),
                sequence: 0,
            });

            Request::to(&publisher.address)
                .body(serde_json::to_vec(&publish_message).unwrap())
                .blob_bytes(message)
                .send()
                .unwrap();
            Ok(())
        } else {
            // this is config specific almost.
            // if you already don't have a publisher, do we spawn one?
            // leads to default config... which might not be what you want.
            // default config could also be stored and set in the api!
            self.new_topic(topic, PubConfig::default())?;
            self.publish(topic, message)?;
            // NOTE: this could be a topic.publish instead! to avoid infinite loops or something..?
            Ok(())
        }
    }
    /// todo:: implement killing of the process.
    pub fn remove_topic(&mut self, topic: &str) -> Result<(), PubError> {
        if let Some(publisher) = self.publishers.get(topic) {
            Request::to(&publisher.address)
                .body(b"kill")
                .send()
                .unwrap();
        }
        Ok(())
    }
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(unused)]
pub struct Sub {
    subscriptions: HashMap<Subscription, Subscriber>, // (publisher, topic) -> sequence (do we need this)
    our: Address,
    // could also have topic -> (publisher, sequence)...
    // different lib or optimization here?
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct Subscription {
    publisher: Address, // publisher worker address
    topic: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscriber {
    address: Address,     // subscriber workers address
    latest_sequence: u64, // latest sequence number received (pain to keep up to date?)
}

#[allow(unused)]
impl Sub {
    pub fn new(our: &Address) -> Self {
        Sub {
            subscriptions: HashMap::new(),
            our: our.clone(),
        }
    }

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

        if self.subscriptions.contains_key(&subscription) {
            // we have a sequence already.. resubscribe?
            return Ok(());
        }

        // TODO: more granular caps.
        let our_caps = our_capabilities();

        // we spawn a subscriber.
        let wasm_path = format!("{}/pkg/sub.wasm", self.our.package_id());
        // TODO: needs ability to message its parent? //networking?
        let process = spawn(None, &wasm_path, OnExit::Restart, our_caps, vec![], false)
            .map_err(|e| SubError::SpawningError(e.to_string()))?;

        let subscriber_address = Address::new(self.our.node.clone(), process);

        let sub_init = SubRequest::InitSub(InitSubRequest {
            topic: topic.to_string(),
            parent: self.our.to_string(),
            forward_to: vec![],
            publisher: publisher.to_string(),
            from_sequence: sequence,
        });

        let res = Request::to(&subscriber_address)
            .body(serde_json::to_vec(&sub_init).unwrap())
            .send_and_await_response(5)
            .unwrap()
            .map_err(|e| SubError::SubInitError(e.to_string()))?;

        let sub_response = serde_json::from_slice::<SubResponse>(&res.body())
            .map_err(|e| SubError::SerializeError(e.to_string()))?;

        if let SubResponse::Subscribe(sub) = sub_response {
            println!("init_sub response: {:?}", sub);

            if !sub.success {
                return Err(SubError::SubInitError(sub.error.unwrap_or_default()));
            }

            let subscriber = Subscriber {
                address: subscriber_address,
                latest_sequence: sequence.unwrap_or(0),
            };

            self.subscriptions.insert(subscription, subscriber);
        } else {
            return Err(SubError::SerializeError(
                "Not a SubscribeResponse".to_string(),
            ));
        }

        Ok(())
    }

    pub fn subscribe<T: Into<PackageId>>(
        &mut self,
        topic: &str,
        publisher_pkg: T,
        node: &str,
    ) -> Result<(), SubError> {
        self.subscribe_from(topic, publisher_pkg, node, None)
    }

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
                .body(serde_json::to_vec(&unsub_request).unwrap())
                .send()
                .map_err(|e| SubError::UnsubscribeError(e.to_string()))?;

            Ok(())
        } else {
            Err(SubError::SubscriptionNotFound)
        }
    }
}

// Error types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PubError {
    TopicNotFound,
    SpawningError(String),
    NoPublisherProcessFound(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubError {
    SpawningError(String),
    SerializeError(String),
    InvalidAddress,
    SubscriptionNotFound,
    SubInitError(String),
    UnsubscribeError(String),
}
