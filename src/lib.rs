wit_bindgen::generate!({
    path: "wit-api",
    world: "pubsub-lib",
    generate_unused_types: true,
    additional_derives: [PartialEq, serde::Deserialize, serde::Serialize],
});

pub use history::MessageHistory;
pub use kinode::process::common::UnsubscribeRequest;
pub use kinode::process::pub_::{
    InitPubRequest, Persistence, PubConfig, PubRequest, PublishRequest,
};
pub use kinode::process::sub::{InitSubRequest, SubRequest, SubResponse, SubscribeResponse};

pub mod history;
pub mod pubsub;
