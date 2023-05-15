use std::{io, sync::Arc};

use tracing::info;

use crate::{
    segment::{PreallocatedSegment, SegmentFileMeta, SegmentId},
    task::AutoJoinHandle,
};

pub struct SegmentPreallocatorInner {
    params: super::Parameters,
    tx: flume::Sender<PreallocatedSegment>,
}

pub struct SegmentPreallocator {
    #[allow(unused)]
    join_handle: AutoJoinHandle,
}

impl SegmentPreallocator {
    pub fn new(
        start_id: SegmentId,
        params: super::Parameters,
        tx: flume::Sender<PreallocatedSegment>,
    ) -> Self {
        let inner = Arc::new(SegmentPreallocatorInner { params, tx });

        let mut id = start_id;
        Self {
            join_handle: AutoJoinHandle::spawn(move || {
                let _guard = scopeguard::guard((), |_| {
                    info!("SegmentPreallocator is done");
                });
                loop {
                    let segment = inner
                        .preload_segment_file(id)
                        .expect("Could not preload next segment file");

                    id = id.next();

                    if let Err(_e) = inner.tx.send(segment) {
                        // on disconnect, just finish
                        break;
                    }
                }
            }),
        }
    }
}

impl SegmentPreallocatorInner {
    fn preload_segment_file(&self, id: SegmentId) -> io::Result<PreallocatedSegment> {
        let file_path = self.params.data_dir.join(format!(
            "{:016x}{}",
            id.as_u64(),
            SegmentFileMeta::FILE_SUFFIX
        ));

        PreallocatedSegment::create_and_fallocate(
            &file_path,
            id,
            self.params.base_segment_file_size,
        )
    }
}
