# libp2p-kad-record-store-redis

This crate uses the [Redis](https://redis.io) Key-Value-Store to work as a persistent store for
everything stored in the local store of a libp2p-kad. The advantage over the [HashMap](std::collections::HashMap) used by
libp2p is the ability that the data stored will still be there even after a node shuts down.

The performance is not ideal. This is partly because of constraints given by the implemented trait
and on the other hand the code written. Please feel free to contribute any improvements.

The actual data store happens with internal structs which are then en- and decoded for storage in
Redis.

### Example
```rust
let mut store = RedisStore::new(addr).unwrap(); /// 'addr' is just a normal redis address like 'redis://localhost:6379'
let key = Key::from(b"Hello".to_vec());
let record = Record::new(key.clone(), b"World".to_vec());
store.put(record.clone()).unwrap();
let ret_record = store.get(&key).unwrap().into_owned();
assert_eq!(ret_record, record);
```
