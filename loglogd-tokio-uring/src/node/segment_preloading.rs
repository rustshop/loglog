use std::io;

use tokio::sync::mpsc;
use tracing::info;

use crate::segment::{OpenSegment, SegmentFileMeta, SegmentId};

use super::Parameters;

pub struct SegmentPreloading {
    pub params: Parameters,
    pub tx: mpsc::Sender<OpenSegment>,
}

impl SegmentPreloading {
    pub async fn run_segment_preloading_loop(self, start_id: SegmentId) {
        let _guard = scopeguard::guard((), |_| {
            info!("segment preloading loop is done");
        });

        let mut id = start_id;
        loop {
            let segment = self
                .preload_segment_file(id)
                .await
                .expect("Could not preload next segment file");

            id = id.next();

            if let Err(_e) = self.tx.send(segment).await {
                // on disconnect, just finish
                return;
            }
        }
    }

    async fn preload_segment_file(&self, id: SegmentId) -> io::Result<OpenSegment> {
        let file_path = self.params.data_dir.join(format!(
            "{:016x}{}",
            id.as_u64(),
            SegmentFileMeta::FILE_SUFFIX
        ));

        OpenSegment::create_and_fallocate(&file_path, id, self.params.base_segment_file_size).await
    }
}
