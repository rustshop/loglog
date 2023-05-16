#[path = "../tests/common.rs"]
mod common;

use common::TestLoglogd;
use criterion::{criterion_group, criterion_main, Criterion};
use loglog::Client;

fn criterion_benchmark(c: &mut Criterion) {
    let server = TestLoglogd::new().unwrap();

    let small_entry = &[1u8, 2u8, 3u8];
    let big_entry = &[1u8; 10000];

    let mut client = server.new_client().unwrap();

    c.bench_function("small write nocommit", |b| {
        b.iter(|| client.append_nocommit(small_entry))
    });
    c.bench_function("big write nocommit", |b| {
        b.iter(|| client.append_nocommit(big_entry))
    });
    c.bench_function("small write commit", |b| {
        b.iter(|| client.append(small_entry))
    });
    c.bench_function("big write commit", |b| b.iter(|| client.append(big_entry)));

    c.bench_function("small roundtrip", |b| {
        b.iter(|| {
            client.append_nocommit(small_entry).unwrap();
            client.read().unwrap();
        })
    });
    c.bench_function("big roundtrip", |b| {
        b.iter(|| {
            client.append_nocommit(big_entry).unwrap();
            client.read().unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
