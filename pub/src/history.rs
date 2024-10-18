use crate::kinode::process::pub_::Persistence;
use anyhow::Result;
use kinode_process_lib::{
    kv::{self, Kv},
    println, Address,
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub sequence: u64,
    pub content: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StorageEntry {
    Full(Message),
    SequenceOnly(u64),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageHistory {
    our: Address,
    entries: VecDeque<StorageEntry>,
    persistence: Persistence,
    kv: Kv<u64, Vec<u8>>,
}

impl MessageHistory {
    pub fn new(our: Address, persistence: Persistence) -> Result<Self> {
        let kv: Kv<u64, Vec<u8>> = kv::open(our.package_id(), "message-history", Some(5))?;
        Ok(MessageHistory {
            our,
            entries: VecDeque::new(),
            persistence,
            kv,
        })
    }

    pub fn add_message(&mut self, sequence: u64, content: Vec<u8>) -> Result<()> {
        let message = Message { sequence, content };

        match &self.persistence {
            Persistence::None => {
                // for ephemeral, we don't need to store anything
            }
            Persistence::Memory(max_size) => {
                if self.entries.len() >= *max_size as usize {
                    self.entries.pop_front();
                }
                self.entries.push_back(StorageEntry::Full(message));
            }
            Persistence::Disk(max_size) => {
                if self.entries.len() >= *max_size as usize {
                    if let Some(StorageEntry::SequenceOnly(oldest_seq)) = self.entries.pop_front() {
                        // delete oldest message
                        // NOTE: test if this works with a non-existent value!
                        self.kv.delete(&oldest_seq, None)?;
                    }
                }
                self.kv.set(&sequence, &message.content, None)?;
                self.entries.push_back(StorageEntry::SequenceOnly(sequence));
            }
        }

        Ok(())
    }

    pub fn get_messages_from(&self, start_sequence: u64) -> Result<Vec<Message>> {
        match &self.persistence {
            Persistence::None => Ok(vec![]),
            Persistence::Memory(_) => Ok(self
                .entries
                .iter()
                .filter_map(|entry| {
                    if let StorageEntry::Full(msg) = entry {
                        if msg.sequence >= start_sequence {
                            Some(msg.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect()),
            Persistence::Disk(_) => {
                let mut result = Vec::new();
                for entry in &self.entries {
                    if let StorageEntry::SequenceOnly(seq) = entry {
                        if *seq >= start_sequence {
                            if let Ok(stored_message) = self.kv.get(seq) {
                                result.push(serde_json::from_slice(&stored_message)?);
                            }
                        }
                    }
                }
                Ok(result)
            }
        }
    }

    pub fn get_latest_sequence(&self) -> Option<u64> {
        self.entries.back().map(|entry| match entry {
            StorageEntry::Full(msg) => msg.sequence,
            StorageEntry::SequenceOnly(seq) => *seq,
        })
    }

    pub fn clear(&mut self) -> Result<()> {
        self.entries.clear();
        if let Persistence::Disk(_) = self.persistence {
            // Clear all stored messages in KV store
            kv::remove_db(self.our.package_id(), "message-history", None)?;
            // see if this works with a non-existent db
            let kv: Kv<u64, Vec<u8>> = kv::open(self.our.package_id(), "message-history", None)?;
            self.kv = kv;
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn persistence_type(&self) -> &Persistence {
        &self.persistence
    }
}
