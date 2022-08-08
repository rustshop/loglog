use std::io;

use tokio::sync::mpsc;
use tracing::info;

use crate::segment::{OpenSegment, SegmentFileMeta};

use super::Parameters;

pub struct SegmentPreloading {
    pub params: Parameters,
    pub tx: mpsc::Sender<OpenSegment>,
}

impl SegmentPreloading {
    pub async fn run_segment_preloading_loop(self, start_id: u64) {
        let _guard = scopeguard::guard((), |_| {
            info!("segment preloading loop is done");
        });

        let mut id = start_id;
        loop {
            let segment = self
                .preload_segment_file(id)
                .await
                .expect("Could not preload next segment file");

            id += 1;

            if let Err(_e) = self.tx.send(segment).await {
                // on disconnect, just finish
                return;
            }
        }
    }

    async fn preload_segment_file(&self, id: u64) -> io::Result<OpenSegment> {
        let file_path =
            self.params
                .db_path
                .join(format!("{:016x}{}", id, SegmentFileMeta::FILE_SUFFIX));

        OpenSegment::create_and_fallocate(&file_path, id, self.params.base_segment_file_size).await
    }
}
