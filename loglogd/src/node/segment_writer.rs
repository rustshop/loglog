use loglogd_api::LogOffset;
use std::{
    collections::BTreeMap,
    os::fd::AsRawFd,
    sync::{atomic::Ordering, Arc},
};
use tracing::{debug, info, trace};

use crate::{
    ioutil::pwrite_all,
    segment::{EntryWrite, OpenSegment, SegmentFileHeader},
    task::AutoJoinHandle,
};

use super::NodeShared;

pub struct SegmentWriter {
    #[allow(unused)]
    join_handles: Vec<AutoJoinHandle>,
}

impl SegmentWriter {
    pub fn new(
        shared: Arc<NodeShared>,
        new_entry_rx: flume::Receiver<EntryWrite>,
        last_written_entry_log_offset_tx: watch::WatchSender<LogOffset>,
    ) -> Self {
        let inner = Arc::new(WriteLoopInner {
            shared,
            rx: new_entry_rx,
            last_written_entry_log_offset_tx,
        });

        Self {
            join_handles: (0..8)
                .map(|i| {
                    AutoJoinHandle::spawn({
                        let inner = inner.clone();
                        move || {
                            let _guard = scopeguard::guard((), {
                                |_| {
                                    info!("SegmentWriter[{i}] is done");
                                    inner
                                        .shared
                                        .is_segment_writer_done
                                        .store(true, Ordering::SeqCst);
                                    // final update, just to make sure the `SegmentSealer` finishes
                                    inner.last_written_entry_log_offset_tx.update(|_v| {})
                                }
                            });

                            while let Ok(entry) = inner.rx.recv() {
                                inner
                                    .handle_entry_write(entry)
                                    .expect("Error while writting entry to segment file");
                            }
                        }
                    })
                })
                .collect(),
        }
    }
}

struct WriteLoopInner {
    shared: Arc<NodeShared>,
    rx: flume::Receiver<EntryWrite>,
    last_written_entry_log_offset_tx: watch::WatchSender<LogOffset>,
}

impl WriteLoopInner {
    /// Write an entry to the log
    fn handle_entry_write(
        self: &Arc<Self>,
        EntryWrite { offset, entry }: EntryWrite,
    ) -> anyhow::Result<()> {
        debug!(
            offset = %offset,
            size = entry.len(),
            "Received new entry to write"
        );

        let segment = self.get_segment_for_write(offset);

        // TODO: check if we didn't already have this chunk, and if the offset seems valid (keep track in memory)
        let file_offset = offset - segment.start_log_offset + SegmentFileHeader::BYTE_SIZE_U64;

        debug!(
            offset = %offset,
            size = entry.len(),
            segment_id = %segment.id,
            segment_offset = file_offset,
            "Writing entry to segment"
        );
        let res = pwrite_all(segment.fd.as_raw_fd(), file_offset, &entry);

        if let Err(e) = res {
            panic!("IO Error when writing log: {}, crashing immediately", e);
        }

        self.mark_entry_written(offset);

        self.shared.put_entry_buffer(entry);

        Ok(())
    }

    /// Find an open segment to write an entry at `entry_log_offset`
    ///
    /// Returns
    fn get_segment_for_write<'a>(
        self: &'a Arc<Self>,
        entry_log_offset: LogOffset,
    ) -> Arc<OpenSegment> {
        fn get_segment_for_offset(
            open_segments: &BTreeMap<LogOffset, Arc<OpenSegment>>,
            log_offset: LogOffset,
        ) -> Option<(LogOffset, Arc<OpenSegment>)> {
            let mut iter = open_segments.range(..=log_offset);

            if let Some(segment) = iter.next_back() {
                // It's one of the not-last ones, we can return right away
                if segment.0
                    != open_segments
                        .last_key_value()
                        .expect("has at least one element")
                        .0
                {
                    return Some((*segment.0, segment.1.clone()));
                }
            }

            // It's the last one or we need a new one
            if let Some((last_start_log_offset, last)) = open_segments.last_key_value() {
                debug_assert!(*last_start_log_offset <= log_offset);
                let write_offset = log_offset - *last_start_log_offset;
                if write_offset + SegmentFileHeader::BYTE_SIZE_U64 < last.allocated_size {
                    return Some((*last_start_log_offset, last.clone()));
                }
            }
            None
        }

        #[derive(Copy, Clone)]
        struct LastSegmentInfo {
            start_log_offset: LogOffset,
            end_of_allocation_log_offset: LogOffset,
        }

        loop {
            let last_segment_info = {
                // Usually the segment to use should be one of the existing one,
                // so lock the list for reading and clone the match if so.
                let read_open_segments = self.shared.open_segments.read().expect("Locking failed");

                if let Some(segment) =
                    get_segment_for_offset(&read_open_segments.inner, entry_log_offset)
                {
                    trace!(%entry_log_offset, ?segment, "found segment for entry write");
                    assert_eq!(segment.0, segment.1.start_log_offset);
                    return segment.1;
                }
                // Well.. seems like we need a new one... . Record the ending offset
                let last_segment_info =
                    read_open_segments
                        .inner
                        .last_key_value()
                        .map(|(offset, segment)| LastSegmentInfo {
                            start_log_offset: *offset,
                            end_of_allocation_log_offset: *offset
                                + (segment.allocated_size - SegmentFileHeader::BYTE_SIZE_U64),
                        });
                drop(read_open_segments);
                last_segment_info
            };

            let next_segment_start_log_offset = if let Some(last_segment_info) = last_segment_info {
                // It would be a mistake to consider current `entry_log_offset` as a beginning of
                // next segment as we might have arrived here out of order. Instead - we can use
                // `pending` to find first not yet written entry for new segment, and thus its starting
                // byte.
                self.shared
                    .get_first_pending_entry_offset_ge(
                        last_segment_info.end_of_allocation_log_offset,
                    )
                    .expect("at very least this entry should be in `pending`")
            } else {
                self.shared
                    .get_sealed_segments_end_log_offset()
                    .unwrap_or_default()
            };

            // It wasn't one of the existing open segments, so we drop the lock fo reading
            // so potentially other threads can make write their stuff, and try to switch
            // to writes.
            let (new_segment_start_log_offset, new_segment) = {
                let mut write_open_segments =
                    self.shared.open_segments.write().expect("Locking failed");

                // Check again if we still need to create new open segment. Things might have changed between unlock & write lock - some
                // other thread might have done it before us. In such a case the last_segment we recorded, will be different now.
                if write_open_segments.inner.last_key_value().map(|s| *s.0)
                    != last_segment_info.map(|s| s.start_log_offset)
                {
                    // Some other thread(s) must have appened new open segment(s) - we can posibly just use it
                    //  but we need to check again.
                    continue;
                }

                // This recv while holding a write lock looks a bit meh, but
                // for this thread to ever have to wait here, would
                // mean that pre-allocating segments is slower than filling
                // them with actual data.
                let new_segment = Arc::new(
                    write_open_segments
                        .preallocated_segments_rx
                        .recv()
                        .expect("segment pre-writing thread must never disconnect")
                        .finalize(next_segment_start_log_offset),
                );
                let prev = write_open_segments
                    .inner
                    .insert(next_segment_start_log_offset, new_segment.clone());

                debug_assert!(prev.is_none());
                // we can drop the write lock already, since the segment is already usable
                // However, we are still responsible for writing the segment file header,
                // which pre-writer couldn't do, as it didn't know the starting log offset
                drop(write_open_segments);
                debug!(
                    %next_segment_start_log_offset,
                    ?new_segment, "opened new segment"
                );
                (next_segment_start_log_offset, new_segment)
            };

            new_segment.write_file_header(new_segment_start_log_offset);

            // It would be tempting to just return the `new_segment` as the segment we need. But that would be
            // a mistake - this entry might theoretically be so far ahead, that it needs even more segments opened.
            // Because of this we just let the loop run again.
        }
    }

    /// Mark the entry as written to disk (thought possibly not yet fsynced)
    pub fn mark_entry_written_no_notify(self: &Arc<Self>, log_offset: LogOffset) {
        let mut pending_entries_write =
            self.shared.pending_entries.write().expect("Locking failed");
        let was_removed = pending_entries_write.entries.remove(&log_offset);
        debug_assert!(was_removed);
    }

    pub fn mark_entry_written(self: &Arc<Self>, log_offset: LogOffset) {
        self.mark_entry_written_no_notify(log_offset);
        self.last_written_entry_log_offset_tx.send(log_offset);
    }
}
