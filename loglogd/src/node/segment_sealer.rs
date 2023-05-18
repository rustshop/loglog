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
                loop {
                    // we actually don't use the value, and blocking for updates

                    let _last_written_entry_log_offset =
                        last_written_entry_log_offset_rx.wait_timeout(Duration::from_secs(1));

                    let first_unwritten_log_offset = shared.get_first_unwritten_log_offset();
                    let fsynced_log_offset = shared.fsynced_log_offset();

                    if fsynced_log_offset == first_unwritten_log_offset {
                        // if we're done with all the pending work, and the writting loop
                        // is done too, we can finish
                        if shared.is_segment_writer_done.load(Ordering::SeqCst) {
                            return;
                        }
                        continue;
                    }

                    'inner: loop {
                        let read_open_segments =
                            shared.open_segments.read().expect("Locking failed");

                        let mut segments_iter = read_open_segments.inner.iter();
                        let first_segment = segments_iter
                            .next()
                            .map(|(offset, segment)| (*offset, Arc::clone(segment)));
                        let second_segment_start = segments_iter.next().map(|s| s.0).cloned();
                        drop(segments_iter);
                        drop(read_open_segments);

                        if let Some((first_segment_start_log_offset, first_segment)) = first_segment
                        {
                            debug!(
                                segment_id = %first_segment.id,
                                segment_start_log_offset = first_segment_start_log_offset.0,
                                first_unwritten_log_offset = first_unwritten_log_offset.0,
                                "fsync"
                            );
                            if let Err(e) = nix::unistd::fsync(first_segment.fd.as_raw_fd()) {
                                warn!(error = %e, "Could not fsync opened segment file");
                                break 'inner;
                            }

                            let first_segment_end_log_offset = match second_segment_start {
                                Some(v) => v,
                                // Until we have at lest two open segments, we won't close the first one.
                                // It's not a big deal, and avoids having to calculate the end of first
                                // segment.
                                None => break 'inner,
                            };

                            // Are all the pending writes to the first open segment complete? If so,
                            // we can close and seal it.
                            if first_segment_end_log_offset <= first_unwritten_log_offset {
                                let first_segment_offset_size = first_segment_end_log_offset.0
                                    - first_segment_start_log_offset.0;
                                // Note: we append to sealed segments first, and remove from open segments second. This way
                                // any readers looking for their data can't miss it between removal and addition.
                                {
                                    let mut write_sealed_segments =
                                        shared.sealed_segments.write().expect("Locking failed");
                                    write_sealed_segments.insert(
                                        first_segment_start_log_offset,
                                        SegmentMeta {
                                            file_meta: SegmentFileMeta::new(
                                                first_segment.id,
                                                first_segment_offset_size
                                                    + SegmentFileHeader::BYTE_SIZE_U64,
                                                SegmentFileMeta::get_path(
                                                    &shared.params.data_dir,
                                                    first_segment.id,
                                                ),
                                            ),
                                            content_meta: segment::SegmentContentMeta {
                                                start_log_offset: first_segment_start_log_offset,
                                                end_log_offset: first_segment_end_log_offset,
                                            },
                                        },
                                    );
                                    drop(write_sealed_segments);
                                }
                                {
                                    let mut write_open_segments =
                                        shared.open_segments.write().expect("Locking failed");
                                    let removed = write_open_segments
                                        .inner
                                        .remove(&first_segment_start_log_offset);
                                    debug_assert!(removed.is_some());
                                }
                            } else {
                                break 'inner;
                            }

                            // continue 'inner - there might be more
                        } else {
                            break 'inner;
                        }
                    }

                    // Only after successfully looping and fsyncing and/or closing all matching segments
                    // we update `last_fsynced_log_offset`
                    shared
                        .fsynced_log_offset
                        .store(first_unwritten_log_offset.0, Ordering::SeqCst);
                    // we don't care if everyone disconnected
                    let _ = shared
                        .last_fsynced_log_offset_tx
                        .send(first_unwritten_log_offset);
                }
            }),
        }
    }
}
