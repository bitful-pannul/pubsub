wit_bindgen::generate!({
    path: "wit-api",
    world: "pubsub-lib",
    generate_unused_types: true,
    additional_derives: [PartialEq, serde::Deserialize, serde::Serialize],
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
pub use kinode::process::sub::{InitSubRequest, SubRequest, SubResponse, SubscribeResponse};
