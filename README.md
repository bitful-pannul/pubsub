# Kinode PubSub Library

A simple publish-subscribe library for Kinode processes.

## Overview

You can use the `kinode_pubsub::{Sub, Pub};` structs directly in your kinode process. When publishing to a new topic or subscribing to a new one, the library will spawn a child process, either with the process name `pub-{topic}` in the case of a publisher or a random u64 in the case of a subscriber.

Currently, you'll have to copy in the `pub.wasm` and `sub.wasm` from /processes/pkg/ into your own /pkg if you want to run this (also you'll need caps to message vfs:distro:sys and kv:distro:sys).

In the process of streamlining this however!

## Usage

### Publishing

```rust
use kinode_pubsub::{Pub, PubConfig};

fn init(our: Address) {
    let mut pubb = Pub::new(&our);
    
    // Create a new topic
    pubb.new_topic("my-topic", PubConfig::default()).unwrap();
    
    // Publish a message
    pubb.publish("my-topic", b"Hello, World!").unwrap();

    // Can also be done in one (creates the topic and spawns the publisher if you already haven't)
    pubb.publish("new-topic", b"hello again!").unwrap();
}
```

### Subscribing

```rust
use kinode_pubsub::{Sub, SubRequest};

fn init(our: Address) {
    let mut sub = Sub::new(&our);
    
    // Subscribe to a topic, on a node with some specific package_id
    subb.subscribe("my-topic", our.package_id(), "incredible.kino").unwrap();


    // Incoming subscriptions will come in as PublishMessages, content is in the blob!
    loop {
        match await_message() {
            Ok(message) => {
                let body = message.body();
                match body.try_into() {
                    Ok(SubRequest::Publish(pub_msg)) => {
                        println!(
                            "got a message on topic: {} with sequence: {}",
                            pub_msg.topic, pub_msg.sequence
                        );
                        let msg_bytes = get_blob().unwrap();
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
}
```

## Example Applications

[todo] list apps

For more detailed information, please refer to the API documentation and example applications.
