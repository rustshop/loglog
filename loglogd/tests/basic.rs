mod common;

use loglog::std::Client;
use std::thread;

use common::*;

// fn init_logging() {
// use std::io;
// use tracing_subscriber::prelude::*;
//     tracing_subscriber::registry()
//         .with(tracing_subscriber::EnvFilter::new(
//             std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
//         ))
//         .with(
//             tracing_subscriber::fmt::layer()
//                 .with_ansi(atty::is(atty::Stream::Stderr))
//                 .with_writer(io::stderr),
//         )
//         .init();
// }

#[test]
fn basic_sanity() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut client = server.new_client()?;

    client.append(&[1, 2, 3])?;
    client.append(&[4, 3, 2])?;
    assert_eq!(client.read()?, [1, 2, 3]);
    assert_eq!(client.read()?, [4, 3, 2]);

    Ok(())
}

#[test]
fn basic_serial_nocommit() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut writer_client = server.new_client()?;
    let mut reader_client = server.new_client()?;

    for b in 0u8..100 {
        let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

        writer_client.append_nocommit(&msg).unwrap();
    }

    for b in 0u8..100 {
        let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

        assert_eq!(reader_client.read().unwrap(), &msg);
    }

    Ok(())
}

#[test]
fn basic_serial() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut writer_client = server.new_client()?;
    let mut reader_client = server.new_client()?;

    for b in 0u8..100 {
        let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

        writer_client.append(&msg).unwrap();
    }

    for b in 0u8..100 {
        let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

        assert_eq!(reader_client.read().unwrap(), &msg);
    }

    Ok(())
}

#[test]
fn basic_concurrent() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut writer_client = server.new_client()?;
    let mut reader_client = server.new_client()?;

    let writer_task = thread::spawn(move || {
        for b in 0u8..100 {
            let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

            writer_client.append(&msg).unwrap();
        }
    });

    let reader_task = thread::spawn(move || {
        for b in 0u8..100 {
            let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

            assert_eq!(reader_client.read().unwrap(), &msg);
        }
    });

    writer_task.join().unwrap();
    reader_task.join().unwrap();

    Ok(())
}

#[test]
fn basic_concurrent_nocommit() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut writer_client = server.new_client()?;
    let mut reader_client = server.new_client()?;

    let writer_task = thread::spawn(move || {
        for b in 0u8..100 {
            let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

            writer_client.append_nocommit(&msg).unwrap();
        }
    });

    let reader_task = thread::spawn(move || {
        for b in 0u8..100 {
            let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

            assert_eq!(reader_client.read().unwrap(), &msg);
        }
    });

    writer_task.join().unwrap();
    reader_task.join().unwrap();

    Ok(())
}
#[test]
fn basic_start_none() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut client1 = server.new_client()?;

    client1.append(&[1, 2, 3])?;

    let mut client2 = server.new_client()?;

    client2.append(&[4, 3, 2])?;

    assert_eq!(client1.read()?, [1, 2, 3]);
    assert_eq!(client1.read()?, [4, 3, 2]);
    assert_eq!(client2.read()?, [4, 3, 2]);

    Ok(())
}
