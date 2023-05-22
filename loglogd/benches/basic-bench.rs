#[path = "../tests/common.rs"]
mod common;

use common::TestLoglogd;
use convi::ExpectFrom;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use loglog::std::Client;

fn criterion_benchmark(c: &mut Criterion) {
    let entries = [
        [1u8].as_slice(),
        [2u8; 1000].as_slice(),
        [3u8; 100000].as_slice(),
    ];

    {
        let mut write_nocommit = c.benchmark_group("write nocommit");
        for entry in entries {
            let server = TestLoglogd::new().unwrap();
            let mut client = server.new_client().unwrap();
            write_nocommit.throughput(criterion::Throughput::Bytes(u64::expect_from(entry.len())));
            write_nocommit.bench_with_input(
                BenchmarkId::from_parameter(entry.len()),
                entry,
                |b, entry| b.iter(|| client.append_nocommit(entry)),
            );
        }
        write_nocommit.finish();
    }
    {
        let mut write_commit = c.benchmark_group("write commit");
        for entry in entries {
            let server = TestLoglogd::new().unwrap();
            let mut client = server.new_client().unwrap();
            write_commit.throughput(criterion::Throughput::Bytes(u64::expect_from(entry.len())));
            write_commit.bench_with_input(
                BenchmarkId::from_parameter(entry.len()),
                entry,
                |b, entry| b.iter(|| client.append(entry)),
            );
        }
        write_commit.finish();
    }
    {
        let mut roundtrip = c.benchmark_group("roundtrip");
        for entry in entries {
            let server = TestLoglogd::new().unwrap();
            let mut client = server.new_client().unwrap();
            roundtrip.throughput(criterion::Throughput::Bytes(u64::expect_from(entry.len())));
            roundtrip.bench_with_input(
                BenchmarkId::from_parameter(entry.len()),
                entry,
                |b, entry| {
                    b.iter(|| {
                        client.append_nocommit(entry).unwrap();
                        let read = client.read().unwrap();
                        assert_eq!(entry.len(), read.len());
                    })
                },
            );
        }
        roundtrip.finish();
    }
    {
        // new client to ignore the previous entries
        let mut roundtrip = c.benchmark_group("roundtrip-behind");

        for entry in entries {
            let server = TestLoglogd::new().unwrap();
            let mut client = server.new_client().unwrap();
            for _ in 0..1000 {
                client.append_nocommit(entry).unwrap();
            }
            roundtrip.throughput(criterion::Throughput::Bytes(u64::expect_from(entry.len())));
            roundtrip.bench_with_input(
                BenchmarkId::from_parameter(entry.len()),
                entry,
                |b, entry| {
                    b.iter(|| {
                        client.append_nocommit(entry).unwrap();
                        let read = client.read().unwrap();
                        assert_eq!(entry.len(), read.len());
                    })
                },
            );
        }
        roundtrip.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
