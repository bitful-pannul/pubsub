wit_bindgen::generate!({
    path: "wit-api",
    world: "pubsub-v0",
    generate_unused_types: true,
    additional_derives: [PartialEq, serde::Deserialize, serde::Serialize, process_macros::SerdeJsonInto],
});

mod binary_helpers;
pub mod history;
pub mod pubsub;

// re-export main api helper structs and common types.
pub use pubsub::{Pub, Sub};

pub use history::MessageHistory;
pub use kinode::process::common::UnsubscribeRequest;
pub use kinode::process::pub_::{
    InitPubRequest, Persistence, PubConfig, PubRequest, PublishRequest,
};
pub use kinode::process::sub::{
    InitSubRequest, SubRequest, SubResponse, SubscribeRequest, SubscribeResponse,
};
