#[path = "../tests/common.rs"]
mod common;

use common::TestLoglogd;
use convi::ExpectFrom;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use loglog::Client;

fn criterion_benchmark(c: &mut Criterion) {
    let server = TestLoglogd::new().unwrap();

    let entries = [[1u8, 2u8, 3u8].as_slice(), [1u8; 10000].as_slice()];

    let mut client = server.new_client().unwrap();
    {
        let mut write_nocommit = c.benchmark_group("write nocommit");
        for entry in entries {
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
            roundtrip.throughput(criterion::Throughput::Bytes(u64::expect_from(entry.len())));
            roundtrip.bench_with_input(
                BenchmarkId::from_parameter(entry.len()),
                entry,
                |b, entry| {
                    b.iter(|| {
                        client.append_nocommit(entry).unwrap();
                        client.read().unwrap();
                    })
                },
            );
        }
        roundtrip.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
