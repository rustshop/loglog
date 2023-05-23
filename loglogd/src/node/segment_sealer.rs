use std::{
    os::fd::AsRawFd,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use crate::{
    segment::{self, SegmentFileHeader, SegmentFileMeta, SegmentMeta},
    task::AutoJoinHandle,
};
use loglogd_api::LogOffset;
use tracing::{debug, info, warn};

use super::NodeShared;

/// Thread responsible for calling `fsync` and "sealing" (marking as complete) segment files
pub struct SegmentSealer {
    #[allow(unused)]
    join_handle: AutoJoinHandle,
}

impl SegmentSealer {
    pub fn new(
        shared: Arc<NodeShared>,
        // Updates with each entry written to the log (don't have to come in order)
        mut last_written_entry_log_offset_rx: watch::WatchReceiver<LogOffset>,
    ) -> Self {
        Self {
            join_handle: AutoJoinHandle::spawn(move || {
                let _guard = scopeguard::guard((), |_| {
                    info!("SegmentSealer is done");
                });
                let mut fsynced_log_offset = shared.fsynced_log_offset();
                loop {
                    // we actually don't use the value, only block for updates
                    let _last_written_entry_log_offset =
                        last_written_entry_log_offset_rx.wait_timeout(Duration::from_secs(1));

                    let first_unwritten_log_offset = shared.get_first_unwritten_log_offset();

                    // Termination condition. We sealed all the pending data and...
                    if fsynced_log_offset == first_unwritten_log_offset {
                        // if we're done with all the pending work, and the writting loop
                        // is done too, we can finish.
                        if shared.is_segment_writer_done.load(Ordering::SeqCst) {
                            return;
                        }
                        continue;
                    }

                    Self::seal_segments_up_to(&shared, first_unwritten_log_offset);

                    // Only after successfully looping and fsyncing and/or closing all matching open segments
                    // we update `last_fsynced_log_offset`
                    fsynced_log_offset = first_unwritten_log_offset;
                    shared.update_fsynced_log_offset(first_unwritten_log_offset);
                }
            }),
        }
    }

    fn seal_segments_up_to(shared: &Arc<NodeShared>, first_unwritten_log_offset: LogOffset) {
        'inner: loop {
            let (first_segment, second_segment_start) = {
                let read_open_segments = shared.open_segments.read().expect("Locking failed");

                let mut segments_iter = read_open_segments.inner.iter();

                let first_segment = segments_iter
                    .next()
                    .map(|(offset, segment)| (*offset, Arc::clone(segment)));
                let second_segment_start = segments_iter.next().map(|s| s.0).cloned();

                (first_segment, second_segment_start)
            };

            let Some((open_segment_start, open_segment)) = first_segment else {
                break 'inner;
            };

            debug!(
                segment_id = %open_segment.id,
                segment_start_log_offset = %open_segment_start,
                first_unwritten_log_offset = %first_unwritten_log_offset,
                "fsync"
            );
            if let Err(e) = nix::unistd::fsync(open_segment.fd.as_raw_fd()) {
                warn!(error = %e, "Could not fsync opened segment file");
                break 'inner;
            }

            let Some(open_segment_end ) = second_segment_start else {
                // Until we have at lest two open segments, we won't close the first one.
                // It's not a big deal, and avoids having to calculate the end of first
                // segment.
                break 'inner
            };

            // We can't seal a segment if there are still any pending writes to it.
            if first_unwritten_log_offset < open_segment_end {
                break 'inner;
            }

            let open_segment_offset_size = open_segment_end - open_segment_start;

            // Note: we append to sealed segments first, and remove from open segments second. This way
            // any readers looking for their data can't miss it between removal and addition.
            {
                let sealed_segment = SegmentMeta {
                    file_meta: SegmentFileMeta::new(
                        open_segment.id,
                        open_segment_offset_size + SegmentFileHeader::BYTE_SIZE_U64,
                        SegmentFileMeta::get_path(&shared.params.data_dir, open_segment.id),
                    ),
                    content_meta: segment::SegmentContentMeta {
                        start_log_offset: open_segment_start,
                        end_log_offset: open_segment_end,
                    },
                };
                shared
                    .sealed_segments
                    .write()
                    .expect("Locking failed")
                    .insert(open_segment_start, sealed_segment);
            }
            {
                let mut write_open_segments = shared.open_segments.write().expect("Locking failed");
                let removed = write_open_segments.inner.remove(&open_segment_start);
                debug_assert!(removed.is_some());
            }

            // continue 'inner - there might be more
        }
    }
}
