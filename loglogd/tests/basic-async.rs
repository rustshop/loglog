mod common;

use common::*;
use loglog_tokio::Client;
use tokio::test;

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
async fn basic_sanity() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut client = server.new_client_async().await?;

    client.append(&[1, 2, 3]).await?;
    client.append(&[4, 3, 2]).await?;
    assert_eq!(client.read().await?, [1, 2, 3]);
    assert_eq!(client.read().await?, [4, 3, 2]);

    Ok(())
}

#[test]
async fn basic_serial_nocommit() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut writer_client = server.new_client_async().await?;
    let mut reader_client = server.new_client_async().await?;

    for b in 0u8..100 {
        let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

        writer_client.append_nocommit(&msg).await.unwrap();
    }

    for b in 0u8..100 {
        let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

        assert_eq!(reader_client.read().await.unwrap(), &msg);
    }

    Ok(())
}

#[test]
async fn basic_serial() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut writer_client = server.new_client_async().await?;
    let mut reader_client = server.new_client_async().await?;

    for b in 0u8..100 {
        let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

        writer_client.append(&msg).await.unwrap();
    }

    for b in 0u8..100 {
        let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

        assert_eq!(reader_client.read().await.unwrap(), &msg);
    }

    Ok(())
}

#[test]
async fn basic_concurrent() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut writer_client = server.new_client_async().await?;
    let mut reader_client = server.new_client_async().await?;

    let writer_task = tokio::spawn(async move {
        for b in 0u8..100 {
            let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

            writer_client.append(&msg).await.unwrap();
        }
    });

    let reader_task = tokio::spawn(async move {
        for b in 0u8..100 {
            let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

            assert_eq!(reader_client.read().await.unwrap(), &msg);
        }
    });

    writer_task.await?;
    reader_task.await?;

    Ok(())
}

#[test]
async fn basic_concurrent_nocommit() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut writer_client = server.new_client_async().await?;
    let mut reader_client = server.new_client_async().await?;

    let writer_task = tokio::spawn(async move {
        for b in 0u8..100 {
            let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

            writer_client.append_nocommit(&msg).await.unwrap();
        }
    });

    let reader_task = tokio::spawn(async move {
        for b in 0u8..100 {
            let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

            assert_eq!(reader_client.read().await.unwrap(), &msg);
        }
    });

    writer_task.await?;
    reader_task.await?;

    Ok(())
}
#[test]
async fn basic_start_none() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut client1 = server.new_client_async().await?;

    client1.append(&[1, 2, 3]).await?;

    let mut client2 = server.new_client_async().await?;

    client2.append(&[4, 3, 2]).await?;

    assert_eq!(client1.read().await?, [1, 2, 3]);
    assert_eq!(client1.read().await?, [4, 3, 2]);
    assert_eq!(client2.read().await?, [4, 3, 2]);

    Ok(())
}
