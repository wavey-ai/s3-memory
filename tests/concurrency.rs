use bytes::Bytes;
use s3_memory::{MemoryS3, StoreError};
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn readers_see_complete_versions_during_replacement() {
    let store = MemoryS3::new();
    assert!(store.create_bucket("bench"));
    let old = Bytes::from(vec![b'a'; 5_760]);
    let new = Bytes::from(vec![b'b'; 5_760]);
    store.put_object("bench", "hot", old.clone()).unwrap();
    let barrier = Arc::new(Barrier::new(9));
    thread::scope(|scope| {
        for _ in 0..8 {
            let store = store.clone();
            let barrier = barrier.clone();
            let old = old.clone();
            let new = new.clone();
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..20_000 {
                    let value = store.get_object("bench", "hot").unwrap();
                    assert!(value == old || value == new);
                }
            });
        }
        barrier.wait();
        for index in 0..20_000 {
            let value = if index % 2 == 0 { &new } else { &old };
            store.put_object("bench", "hot", value.clone()).unwrap();
        }
    });
}

#[test]
fn deleted_objects_leave_existing_snapshots_valid() {
    let store = MemoryS3::new();
    store.create_bucket("bench");
    store
        .put_object("bench", "hot", Bytes::from_static(b"complete"))
        .unwrap();
    let snapshot = store.get_object("bench", "hot").unwrap();
    let handle = store.object_handle("bench", "hot").unwrap();
    store
        .put_object("bench", "hot", Bytes::from_static(b"new"))
        .unwrap();
    assert_eq!(handle.get().unwrap(), "new");
    assert_eq!(store.delete_object("bench", "hot"), Ok(true));
    assert!(store.get_object("bench", "hot").is_none());
    assert!(handle.get().is_none());
    store
        .put_object("bench", "hot", Bytes::from_static(b"third"))
        .unwrap();
    assert!(handle.get().is_none());
    assert_eq!(
        store.object_handle("bench", "hot").unwrap().get().unwrap(),
        "third"
    );
    assert_eq!(snapshot, "complete");
}

#[test]
fn capacity_rejects_growth_and_preserves_existing_object() {
    let store = MemoryS3::new().with_max_stored_bytes(8);
    store.create_bucket("bench");
    store
        .put_object("bench", "hot", Bytes::from_static(b"123456"))
        .unwrap();
    assert_eq!(store.stored_bytes(), 6);
    assert_eq!(
        store.put_object("bench", "hot", Bytes::from_static(b"123456789")),
        Err(StoreError::CapacityExceeded)
    );
    assert_eq!(store.get_object("bench", "hot").unwrap(), "123456");
    store
        .put_object("bench", "hot", Bytes::from_static(b"12"))
        .unwrap();
    assert_eq!(store.stored_bytes(), 2);
    store
        .put_object("bench", "other", Bytes::from_static(b"123456"))
        .unwrap();
    assert_eq!(store.stored_bytes(), 8);
    assert_eq!(store.delete_object("bench", "hot"), Ok(true));
    assert_eq!(store.stored_bytes(), 6);
}
