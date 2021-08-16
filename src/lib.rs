//! This crate uses the [Redis](https://redis.io) Key-Value-Store to work as a persistent store for
//! everything stored in the local store of a libp2p-kad. The advantage over the [HashMap](std::collections::HashMap) used by
//! libp2p is the ability that the data stored will still be there even after a node shuts down.
//!
//! The performance is not ideal. This is partly because of constraints given by the implemented trait
//! and on the other hand the code written. Please feel free to contribute any improvements.
//!
//! The actual data store happens with internal structs which are then en- and decoded for storage in
//! Redis.
//!
//! ## Example
//! ```rust
//! # use libp2p::kad::store::RecordStore;
//! # use libp2p::kad::record::Key;
//! # use libp2p::kad::Record;
//! # use libp2p_kad_record_store_redis::{RedisStore, conn_from_env};
//! # let addr = conn_from_env();
//! let mut store = RedisStore::new(addr).unwrap(); /// 'addr' is just a normal redis address like 'redis://localhost:6379'
//! let key = Key::from(b"Hello".to_vec());
//! let record = Record::new(key.clone(), b"World".to_vec());
//! store.put(record.clone()).unwrap();
//! let ret_record = store.get(&key).unwrap().into_owned();
//! assert_eq!(ret_record, record);
//! ```

use redis::{Client, Commands, IntoConnectionInfo};
use libp2p::kad::store::RecordStore;
use libp2p::kad::record::Key;
use std::borrow::{Cow, Borrow};
use libp2p::kad::{Record, ProviderRecord};
use libp2p::{PeerId, Multiaddr};
use std::time::Instant;
use serde::{Serialize, Deserialize};
use std::vec::IntoIter;

/// For debugging only
pub fn conn_from_env() -> String {
    let host = std::env::var("REDIS_HOST")
        .expect("Missing environment variable REDIS_HOST");
    let port = std::env::var("REDIS_PORT")
        .expect("Missing environment variable REDIS_PORT");
    format!("redis://{}:{}", host, port)
}

#[derive(Serialize, Deserialize, PartialEq)]
enum RecordType {
    Record(InnerRecord),
    Provider(Vec<InnerProvider>),
}

#[derive(Serialize, Deserialize, PartialEq)]
struct InnerRecord {
    #[serde(with = "serde_bytes")]
    key: Vec<u8>,
    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
    #[serde(with = "serde_bytes")]
    publisher: Option<Vec<u8>>,
    #[serde(with = "serde_millis")]
    expires: Option<Instant>,
}

#[derive(Serialize, Deserialize, PartialEq)]
struct InnerProvider {
    #[serde(with = "serde_bytes")]
    key: Vec<u8>,
    #[serde(with = "serde_bytes")]
    provider: Vec<u8>,
    #[serde(with = "serde_millis")]
    expires: Option<Instant>,
    addresses: Vec<Multiaddr>,
}

impl From<&Record> for RecordType {
    fn from(record: &Record) -> Self {
        let key = record.key.to_vec();
        let value = record.value.to_vec();
        let publisher = record.publisher.map(|e| e.to_bytes());
        let inner = InnerRecord {
            key,
            value,
            publisher,
            expires: record.expires,
        };
        RecordType::Record(inner)
    }
}

impl From<RecordType> for Record {
    fn from(record_type: RecordType) -> Self {
        match record_type {
            RecordType::Record(inner) => {
                Record {
                    key: inner.key.into(),
                    value: inner.value,
                    publisher: inner.publisher.map(|e| PeerId::from_bytes(&e).unwrap()),
                    expires: inner.expires,
                }
            }
            RecordType::Provider(_) => {
                panic!("Provided wrong type (Provider)")
            }
        }
    }
}

impl From<&[ProviderRecord]> for RecordType {
    fn from(provider_records: &[ProviderRecord]) -> Self {
        let inner: Vec<InnerProvider> = provider_records.iter()
            .map(|e| {
                let key = e.key.to_vec();
                let provider = e.provider.to_bytes();
                InnerProvider {
                    key,
                    provider,
                    expires: e.expires,
                    addresses: e.addresses.clone()
                }
            })
            .collect();
        RecordType::Provider(inner)
    }
}

impl From<RecordType> for Vec<ProviderRecord> {
    fn from(record_type: RecordType) -> Self {
        match record_type {
            RecordType::Record(_) => {
                panic!("Provided wrong type (Record)")
            }
            RecordType::Provider(inner) => {
                inner.iter()
                    .map(|e| {
                        ProviderRecord {
                            key: Key::from(e.key.clone()),
                            provider: PeerId::from_bytes(&e.provider).unwrap(),
                            expires: e.expires,
                            addresses: e.addresses.clone(),
                        }
                    })
                    .collect()
            }
        }
    }
}

/// Implementation of [RecordStore](libp2p_kad::record::store::RecordStore) using Redis.
///
/// For usagae, just replace it in the Swarm-init and in the type specification.
/// ```ignore
/// #[derive(NetworkBehaviour)]
/// struct MyBehaviour {
///     kademlia: Kademlia<MemoryStore>,
///     mdns: Mdns,
/// }
///
/// let mut swarm = {
///         // Create a Kademlia behaviour.
///         let store = RedisStore::new("localhost:6479");
///         let kademlia = Kademlia::new(local_peer_id, store);
///         let mdns = task::block_on(Mdns::new(MdnsConfig::default()))?;
///         let behaviour = MyBehaviour { kademlia, mdns };
///         Swarm::new(transport, behaviour, local_peer_id)
///     };
/// ```
pub struct RedisStore {
    client: Client,
}

impl RedisStore {
    pub fn new<U: IntoConnectionInfo>(addr: U) -> anyhow::Result<Self> {
        let client = Client::open(addr)?;
        Ok(Self {
            client,
        })
    }
}

impl<'a> RecordStore<'a> for RedisStore {
    type RecordsIter = Box<dyn Iterator<Item = Cow<'a, Record>>>;
    type ProvidedIter = Box<dyn Iterator<Item = Cow<'a, ProviderRecord>>>;

    fn get(&'a self, k: &Key) -> Option<Cow<'_, Record>> {
        let mut conn = self.client.get_connection().unwrap();
        let key = k.to_vec();
        let value: Vec<u8> = conn.get(&key).ok()?;
        let inner_record: RecordType = bincode::deserialize(&value).ok()?;
        let record: Record = inner_record.into();
        Some(Cow::Owned(record))
    }

    fn put(&'a mut self, r: Record) -> libp2p::kad::record::store::Result<()> {
        let mut conn = match self.client.get_connection() {
            Ok(d) => d,
            Err(_) => {
                return Err(libp2p::kad::store::Error::MaxProvidedKeys)
            }
        };
        let record: RecordType = r.borrow().into();
        let key = r.key.to_vec();
        let record: Vec<u8> = bincode::serialize(&record).unwrap();
        let _: () = conn.set(&key, &record).expect("Something went wrong while setting");
        Ok(())
    }

    fn remove(&'a mut self, k: &Key) {
        let mut conn = self.client.get_connection().unwrap();
        let key = k.to_vec();
        let _: () = conn.del(&key).expect("Error deleting key");
    }

    fn records(&'a self) -> Self::RecordsIter {
        let mut conn = self.client.get_connection().unwrap();
        let iterator: IntoIter<Vec<u8>> = conn.scan().unwrap().collect::<Vec<Vec<u8>>>().into_iter();
        Box::new(iterator
            .map(move |e| conn.get(&e).unwrap())
            .map(|a: Vec<u8>|
            {
                println!("{:?}", &a);
                bincode::deserialize::<RecordType>(&a).unwrap()
            })
            .filter(|b| match b {
                RecordType::Record(_) => true,
                RecordType::Provider(_) => false,
            })
            .map(|c| Cow::Owned(c.into())))
    }

    fn add_provider(&'a mut self, record: ProviderRecord) -> libp2p::kad::record::store::Result<()> {
        let mut conn = self.client.get_connection().unwrap();
        let mut records = vec![record.clone()];
        let key = record.key.to_vec();
        if conn.exists(&key).unwrap() {
            let mut already_set: Vec<ProviderRecord> = bincode::deserialize::<RecordType>(&conn.get::<&Vec<u8>, Vec<u8>>(&key).unwrap()).unwrap()
                .into();
            records.append(&mut already_set);
        }
        let inner_record: RecordType = records.as_slice().into();
        let bytes = bincode::serialize(&inner_record).unwrap();
        let _: () = conn.set(key, bytes).unwrap();
        Ok(())
    }

    fn providers(&'a self, key: &Key) -> Vec<ProviderRecord> {
        let mut conn = self.client.get_connection().unwrap();
        let key = key.to_vec();
        let res: Vec<u8> = conn.get(key).unwrap();
        let record_type = bincode::deserialize::<RecordType>(&res).unwrap();
        Vec::from(record_type)
    }

    fn provided(&'a self) -> Self::ProvidedIter {
        let mut conn = self.client.get_connection().unwrap();
        let iterator: IntoIter<Vec<u8>> = conn.scan().unwrap().collect::<Vec<Vec<u8>>>().into_iter();
        Box::new(iterator
            .map(move |a| conn.get(a).unwrap())
            .map(|a: Vec<u8>| bincode::deserialize(&a).unwrap())
            .filter(|b| match b {
                RecordType::Record(_) => false,
                RecordType::Provider(_) => true,
            })
            .map(Vec::from)
            .flatten()
            .map(Cow::Owned)
        )
    }

    fn remove_provider(&'a mut self, k: &Key, p: &PeerId) {
        let key = k.to_vec();
        let mut conn = self.client.get_connection().unwrap();
        let all: Vec<u8> = conn.get(&key).unwrap();
        let all: RecordType = bincode::deserialize(&all).unwrap();
        let all = Vec::from(all);
        let new: Vec<ProviderRecord> = all.iter()
            .filter(|e| &e.provider != p)
            .map(|e| e.to_owned())
            .collect();
        let new: Vec<u8> = bincode::serialize(&RecordType::from(new.as_slice())).unwrap();
        let _: () = conn.set(key, new).unwrap();
    }
}

#[cfg(test)]
mod records {
    use crate::{RedisStore, conn_from_env};
    use libp2p::kad::record::Key;
    use libp2p::kad::Record;
    use libp2p::kad::store::RecordStore;

    #[test]
    fn put_get_remove() {
        let addr = conn_from_env();
        let mut store = RedisStore::new(addr).unwrap();
        let key = Key::from(b"Hello".to_vec());
        let record = Record::new(key.to_vec(), b"World".to_vec());
        store.put(record.clone()).unwrap();
        let ret = store.get(&key).unwrap().into_owned();
        store.remove(&key);
        assert_eq!(ret, record);
    }

    #[test]
    fn put_iter() {
        let addr = conn_from_env();
        let mut store = RedisStore::new(addr).unwrap();
        let key_values: Vec<(&[u8], &[u8])> = vec![(b"one", b"one_value"), (b"two", b"two_value"), (b"three", b"three_value")];
        let mut records: Vec<Record> = key_values
            .iter()
            .map(|e| Record::new(Key::from(e.0.to_vec()), e.1.to_vec()))
            .collect();
        records.iter()
            .for_each(|e| store.put(e.clone()).unwrap());
        let mut records_iterator: Vec<Record> = store.records().map(|e| e.into_owned()).collect();
        records.sort_by_key(|e| e.key.to_vec());
        records_iterator.sort_by_key(|e| e.key.to_vec());
        assert_eq!(records, records_iterator);
        records_iterator.iter()
            .for_each(|e| store.remove(&e.key));
    }
}

#[cfg(test)]
mod providers {
    use crate::{RedisStore, conn_from_env};
    use libp2p::kad::record::Key;
    use libp2p::kad::ProviderRecord;
    use libp2p::kad::store::RecordStore;
    use libp2p::{PeerId, Multiaddr};

    #[test]
    fn put_get_remove() {
        let addr = conn_from_env();
        let mut store = RedisStore::new(addr).unwrap();
        let key = Key::from(b"Hello-PROV".to_vec());
        let peer_id = PeerId::random();
        let record = ProviderRecord::new(key.clone(), peer_id.clone(), vec![Multiaddr::empty()]);
        store.add_provider(record.clone()).unwrap();
        store.add_provider(record.clone()).unwrap();
        let ret = store.providers(&key).get(0).unwrap().to_owned();
        store.remove_provider(&key, &peer_id);
        assert_eq!(ret, record)
    }

    #[test]
    fn put_iter() {
        let addr = conn_from_env();
        let mut store = RedisStore::new(addr).unwrap();
        let peers = vec![PeerId::random(), PeerId::random(), PeerId::random()];
        let addresses = vec![Multiaddr::empty(), Multiaddr::empty(), Multiaddr::empty()];
        let keys = vec![Key::from(b"one-rec".to_vec()), Key::from(b"two-rec".to_vec()), Key::from(b"three-rec".to_vec())];
        let mut records: Vec<ProviderRecord> = keys.iter().zip(peers.iter()).zip(addresses.iter())
            .map(|e| ProviderRecord::new(e.0.0.to_owned(), e.0.1.to_owned(), vec![e.1.to_owned()]))
            .collect();
        records.iter()
            .for_each(|e| store.add_provider(e.to_owned()).unwrap());
        let mut records_iterator: Vec<ProviderRecord> = store.provided()
            .map(|e| e.into_owned())
            .filter(|e| e.key != Key::from(b"Hello-PROV".to_vec()))
            .collect();
        records.sort_by_key(|e| e.key.to_vec());
        records_iterator.sort_by_key(|e| e.key.to_vec());
        assert_eq!(records_iterator, records);
        records_iterator.iter()
            .for_each(|e| store.remove_provider(&e.key, &e.provider));
    }
}