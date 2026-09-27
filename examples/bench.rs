use bytes::Bytes;
use s3_memory::MemoryS3;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

fn measure_read(
    store: &MemoryS3,
    workers: usize,
    operations: usize,
    use_handle: bool,
    independent: bool,
) {
    let barrier = Arc::new(Barrier::new(workers + 1));
    let per_worker = operations / workers;
    let seconds = thread::scope(|scope| {
        let mut handles = Vec::new();
        for worker in 0..workers {
            let store = store.clone();
            let barrier = barrier.clone();
            let key = if independent {
                format!("hot-{worker}")
            } else {
                "hot".to_owned()
            };
            let object_handle = use_handle.then(|| store.object_handle("bench", &key).unwrap());
            handles.push(scope.spawn(move || {
                barrier.wait();
                for _ in 0..per_worker {
                    if let Some(object_handle) = &object_handle {
                        black_box(object_handle.get().unwrap());
                    } else {
                        black_box(store.get_object("bench", &key).unwrap());
                    }
                }
            }));
        }
        barrier.wait();
        let start = Instant::now();
        for handle in handles {
            handle.join().unwrap();
        }
        start.elapsed().as_secs_f64()
    });
    let mode = if use_handle { "handle" } else { "lookup" };
    let keys = if independent { "independent" } else { "shared" };
    println!(
        "{mode} keys={keys} workers={workers} operations={} seconds={seconds:.3} ops_per_second={:.0}",
        per_worker * workers,
        per_worker as f64 * workers as f64 / seconds
    );
}

fn main() {
    let store = MemoryS3::new();
    store.create_bucket("bench");
    store
        .put_object("bench", "hot", Bytes::from(vec![0; 5_760]))
        .unwrap();
    for worker in 0..8 {
        store
            .put_object(
                "bench",
                format!("hot-{worker}"),
                Bytes::from(vec![0; 5_760]),
            )
            .unwrap();
    }
    for workers in [1, 4, 8] {
        for use_handle in [false, true] {
            for independent in [false, true] {
                measure_read(&store, workers, 100_000, use_handle, independent);
                measure_read(&store, workers, 2_000_000, use_handle, independent);
            }
        }
    }
}
