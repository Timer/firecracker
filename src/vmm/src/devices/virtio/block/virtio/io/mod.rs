// Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

pub mod async_io;
pub mod format;
pub mod sync_io;

use std::fmt::Debug;
use std::fs::File;

use vm_memory::{GuestMemoryBackend, GuestMemoryError};

pub use self::async_io::{AsyncFileEngine, AsyncIoError};
pub use self::format::{DiskImageFormat, VmdkFileEngine, VmdkIoError};
pub use self::sync_io::{SyncFileEngine, SyncIoError};
use crate::devices::virtio::block::virtio::PendingRequest;
use crate::devices::virtio::block::virtio::device::FileEngineType;
use crate::vstate::memory::{GuestAddress, GuestMemoryMmap};

#[derive(Debug)]
pub struct RequestOk {
    pub req: PendingRequest,
    pub count: u32,
}

#[derive(Debug)]
pub enum FileEngineOk {
    Submitted,
    Executed(RequestOk),
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum BlockIoError {
    /// Sync error: {0}
    Sync(SyncIoError),
    /// Async error: {0}
    Async(AsyncIoError),
    /// VMDK error: {0}
    Vmdk(VmdkIoError),
}

impl BlockIoError {
    pub fn is_throttling_err(&self) -> bool {
        match self {
            BlockIoError::Async(AsyncIoError::IoUring(err)) => err.is_throttling_err(),
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct RequestError<E> {
    pub req: PendingRequest,
    pub error: E,
}

/// Checks that each of the `segments` is guest memory, and returns their total length.
///
/// The engines that transfer one segment at a time call this before the first transfer. A
/// request with a segment outside guest memory then fails as a whole, as it does on the Async
/// engine, rather than after some of it has reached the disk or the guest.
pub(super) fn check_segments(
    mem: &GuestMemoryMmap,
    segments: &[(GuestAddress, u32)],
) -> Result<u32, GuestMemoryError> {
    let total = segments
        .iter()
        .try_fold(0u32, |total, &(_, len)| total.checked_add(len))
        .ok_or_else(|| {
            GuestMemoryError::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "the data segments are longer than a request can be",
            ))
        })?;
    for &(addr, len) in segments {
        mem.get_slice(addr, len as usize)?;
    }
    Ok(total)
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum FileEngine {
    #[allow(unused)]
    Async(AsyncFileEngine),
    Sync(SyncFileEngine),
    Vmdk(VmdkFileEngine),
}

impl From<&FileEngine> for DiskImageFormat {
    fn from(engine: &FileEngine) -> Self {
        match engine {
            FileEngine::Vmdk(_) => DiskImageFormat::Vmdk,
            FileEngine::Async(_) | FileEngine::Sync(_) => DiskImageFormat::Raw,
        }
    }
}

impl FileEngine {
    pub fn from_file(file: File, engine_type: FileEngineType) -> Result<FileEngine, BlockIoError> {
        match engine_type {
            FileEngineType::Async => Ok(FileEngine::Async(
                AsyncFileEngine::from_file(file).map_err(BlockIoError::Async)?,
            )),
            FileEngineType::Sync => Ok(FileEngine::Sync(SyncFileEngine::from_file(file))),
        }
    }

    pub fn update_file_path(&mut self, file: File) -> Result<(), BlockIoError> {
        match self {
            FileEngine::Async(engine) => engine.update_file(file).map_err(BlockIoError::Async)?,
            FileEngine::Sync(engine) => engine.update_file(file),
            FileEngine::Vmdk(_) => {
                return Err(BlockIoError::Vmdk(VmdkIoError::UnsupportedOperation));
            }
        };

        Ok(())
    }

    pub fn file(&self) -> &File {
        match self {
            FileEngine::Async(engine) => engine.file(),
            FileEngine::Sync(engine) => engine.file(),
            FileEngine::Vmdk(_) => {
                panic!("file() is not supported for VMDK engine")
            }
        }
    }

    pub fn read(
        &mut self,
        offset: u64,
        mem: &GuestMemoryMmap,
        addr: GuestAddress,
        count: u32,
        req: PendingRequest,
    ) -> Result<FileEngineOk, RequestError<BlockIoError>> {
        match self {
            FileEngine::Async(engine) => match engine.push_read(offset, mem, addr, count, req) {
                Ok(_) => Ok(FileEngineOk::Submitted),
                Err(err) => Err(RequestError {
                    req: err.req,
                    error: BlockIoError::Async(err.error),
                }),
            },
            FileEngine::Sync(engine) => match engine.read(offset, mem, addr, count) {
                Ok(count) => Ok(FileEngineOk::Executed(RequestOk { req, count })),
                Err(err) => Err(RequestError {
                    req,
                    error: BlockIoError::Sync(err),
                }),
            },
            FileEngine::Vmdk(engine) => match engine.read(offset, mem, addr, count) {
                Ok(count) => Ok(FileEngineOk::Executed(RequestOk { req, count })),
                Err(err) => Err(RequestError {
                    req,
                    error: BlockIoError::Vmdk(err),
                }),
            },
        }
    }

    pub fn write(
        &mut self,
        offset: u64,
        mem: &GuestMemoryMmap,
        addr: GuestAddress,
        count: u32,
        req: PendingRequest,
    ) -> Result<FileEngineOk, RequestError<BlockIoError>> {
        match self {
            FileEngine::Async(engine) => match engine.push_write(offset, mem, addr, count, req) {
                Ok(_) => Ok(FileEngineOk::Submitted),
                Err(err) => Err(RequestError {
                    req: err.req,
                    error: BlockIoError::Async(err.error),
                }),
            },
            FileEngine::Sync(engine) => match engine.write(offset, mem, addr, count) {
                Ok(count) => Ok(FileEngineOk::Executed(RequestOk { req, count })),
                Err(err) => Err(RequestError {
                    req,
                    error: BlockIoError::Sync(err),
                }),
            },
            FileEngine::Vmdk(engine) => match engine.write(offset, mem, addr, count) {
                Ok(count) => Ok(FileEngineOk::Executed(RequestOk { req, count })),
                Err(err) => Err(RequestError {
                    req,
                    error: BlockIoError::Vmdk(err),
                }),
            },
        }
    }

    /// Reads into the guest memory `segments` in order, from the file starting at `offset`.
    pub fn readv(
        &mut self,
        offset: u64,
        mem: &GuestMemoryMmap,
        segments: &[(GuestAddress, u32)],
        req: PendingRequest,
    ) -> Result<FileEngineOk, RequestError<BlockIoError>> {
        match self {
            FileEngine::Async(engine) => match engine.push_readv(offset, mem, segments, req) {
                Ok(_) => Ok(FileEngineOk::Submitted),
                Err(err) => Err(RequestError {
                    req: err.req,
                    error: BlockIoError::Async(err.error),
                }),
            },
            FileEngine::Sync(engine) => match engine.readv(offset, mem, segments) {
                Ok(count) => Ok(FileEngineOk::Executed(RequestOk { req, count })),
                Err(err) => Err(RequestError {
                    req,
                    error: BlockIoError::Sync(err),
                }),
            },
            FileEngine::Vmdk(engine) => match engine.readv(offset, mem, segments) {
                Ok(count) => Ok(FileEngineOk::Executed(RequestOk { req, count })),
                Err(err) => Err(RequestError {
                    req,
                    error: BlockIoError::Vmdk(err),
                }),
            },
        }
    }

    /// Writes the guest memory `segments` in order, to the file starting at `offset`.
    pub fn writev(
        &mut self,
        offset: u64,
        mem: &GuestMemoryMmap,
        segments: &[(GuestAddress, u32)],
        req: PendingRequest,
    ) -> Result<FileEngineOk, RequestError<BlockIoError>> {
        match self {
            FileEngine::Async(engine) => match engine.push_writev(offset, mem, segments, req) {
                Ok(_) => Ok(FileEngineOk::Submitted),
                Err(err) => Err(RequestError {
                    req: err.req,
                    error: BlockIoError::Async(err.error),
                }),
            },
            FileEngine::Sync(engine) => match engine.writev(offset, mem, segments) {
                Ok(count) => Ok(FileEngineOk::Executed(RequestOk { req, count })),
                Err(err) => Err(RequestError {
                    req,
                    error: BlockIoError::Sync(err),
                }),
            },
            // Read-only, as for a write from a single buffer.
            FileEngine::Vmdk(_) => Err(RequestError {
                req,
                error: BlockIoError::Vmdk(VmdkIoError::WriteNotSupported),
            }),
        }
    }

    pub fn flush(
        &mut self,
        req: PendingRequest,
    ) -> Result<FileEngineOk, RequestError<BlockIoError>> {
        match self {
            FileEngine::Async(engine) => match engine.push_flush(req) {
                Ok(_) => Ok(FileEngineOk::Submitted),
                Err(err) => Err(RequestError {
                    req: err.req,
                    error: BlockIoError::Async(err.error),
                }),
            },
            FileEngine::Sync(engine) => match engine.flush() {
                Ok(_) => Ok(FileEngineOk::Executed(RequestOk { req, count: 0 })),
                Err(err) => Err(RequestError {
                    req,
                    error: BlockIoError::Sync(err),
                }),
            },
            FileEngine::Vmdk(engine) => match engine.flush() {
                Ok(_) => Ok(FileEngineOk::Executed(RequestOk { req, count: 0 })),
                Err(err) => Err(RequestError {
                    req,
                    error: BlockIoError::Vmdk(err),
                }),
            },
        }
    }

    pub fn discard(
        &mut self,
        range: (u64, u32),
        req: PendingRequest,
    ) -> Result<FileEngineOk, RequestError<BlockIoError>> {
        match self {
            FileEngine::Async(engine) => match engine.discard(range) {
                Ok(count) => Ok(FileEngineOk::Executed(RequestOk { req, count })),
                Err(err) => Err(RequestError {
                    req,
                    error: BlockIoError::Async(err),
                }),
            },
            FileEngine::Sync(engine) => match engine.discard(range) {
                Ok(count) => Ok(FileEngineOk::Executed(RequestOk { req, count })),
                Err(err) => Err(RequestError {
                    req,
                    error: BlockIoError::Sync(err),
                }),
            },
            FileEngine::Vmdk(_) => Err(RequestError {
                req,
                error: BlockIoError::Vmdk(VmdkIoError::WriteNotSupported),
            }),
        }
    }

    pub fn drain(&mut self, discard: bool) -> Result<(), BlockIoError> {
        match self {
            FileEngine::Async(engine) => engine.drain(discard).map_err(BlockIoError::Async),
            FileEngine::Sync(_engine) => Ok(()),
            FileEngine::Vmdk(_engine) => Ok(()),
        }
    }

    pub fn drain_and_flush(&mut self, discard: bool) -> Result<(), BlockIoError> {
        match self {
            FileEngine::Async(engine) => {
                engine.drain_and_flush(discard).map_err(BlockIoError::Async)
            }
            FileEngine::Sync(engine) => engine.flush().map_err(BlockIoError::Sync),
            FileEngine::Vmdk(engine) => engine.flush().map_err(BlockIoError::Vmdk),
        }
    }
}

#[cfg(test)]
pub mod tests {
    #![allow(clippy::undocumented_unsafe_blocks)]
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::FileExt;

    use proptest::prelude::*;
    use vm_memory::{GuestMemoryBackend, GuestMemoryError, GuestMemoryRegion};
    use vmm_sys_util::tempfile::TempFile;

    use super::*;
    use crate::arch::host_page_size;
    use crate::devices::virtio::block::virtio::IO_URING_NUM_ENTRIES;
    use crate::devices::virtio::block::virtio::device::FileEngineType;
    use crate::devices::virtio::block::virtio::io::format::vmdk::tests::create_test_vmdk;
    use crate::utils::u64_to_usize;
    use crate::vmm_config::machine_config::HugePageConfig;
    use crate::vstate::memory;
    use crate::vstate::memory::{Bitmap, Bytes, GuestRegionMmapExt};

    const FILE_LEN: u32 = 1024;
    // 2 pages of memory should be enough to test read/write ops and also dirty tracking.
    const MEM_LEN: usize = 8192;
    // Guest buffers that split a transfer of FILE_LEN bytes unevenly and out of order over both
    // pages of memory, with gaps between them.
    const SEGMENTS: [(GuestAddress, u32); 3] = [
        (GuestAddress(4096), 100),
        (GuestAddress(0), 512),
        (GuestAddress(1024), FILE_LEN - 612),
    ];

    macro_rules! assert_sync_execution {
        ($expression:expr, $count:expr) => {
            match $expression {
                Ok(FileEngineOk::Executed(RequestOk { req: _, count })) => {
                    assert_eq!(count, $count)
                }
                other => panic!(
                    "Expected: Ok(FileEngineOk::Executed(UserDataOk {{ user_data: _, count: {} \
                     }})), got: {:?}",
                    $count, other
                ),
            }
        };
    }

    macro_rules! assert_queued {
        ($expression:expr) => {
            assert!(matches!($expression, Ok(FileEngineOk::Submitted)))
        };
    }

    fn assert_async_execution(mem: &GuestMemoryMmap, engine: &mut FileEngine, count: u32) {
        if let FileEngine::Async(engine) = engine {
            engine.drain(false).unwrap();
            assert_eq!(engine.pop(mem).unwrap().unwrap().result().unwrap(), count);
        }
    }

    // Checks that the engine has no operation left, in flight or completed.
    fn assert_no_async_execution(mem: &GuestMemoryMmap, engine: &mut FileEngine) {
        if let FileEngine::Async(engine) = engine {
            engine.drain(false).unwrap();
            assert!(engine.pop(mem).unwrap().is_none());
        }
    }

    fn create_mem() -> GuestMemoryMmap {
        GuestMemoryMmap::from_regions(
            memory::anonymous(&[(GuestAddress(0), MEM_LEN)], true, HugePageConfig::None)
                .unwrap()
                .into_iter()
                .map(|region| GuestRegionMmapExt::dram_from_mmap_region(region, 0))
                .collect(),
        )
        .unwrap()
    }

    fn check_dirty_mem(mem: &GuestMemoryMmap, addr: GuestAddress, len: u32) {
        let bitmap = mem.find_region(addr).unwrap().bitmap();
        for offset in addr.0..addr.0 + u64::from(len) {
            assert!(bitmap.dirty_at(u64_to_usize(offset)));
        }
    }

    fn check_clean_mem(mem: &GuestMemoryMmap, addr: GuestAddress, len: u32) {
        let bitmap = mem.find_region(addr).unwrap().bitmap();
        for offset in addr.0..addr.0 + u64::from(len) {
            assert!(!bitmap.dirty_at(u64_to_usize(offset)));
        }
    }

    // Writes `data` into the SEGMENTS, in order.
    fn write_segments(mem: &GuestMemoryMmap, mut data: &[u8]) {
        for (addr, len) in SEGMENTS {
            let (chunk, rest) = data.split_at(len as usize);
            mem.write_slice(chunk, addr).unwrap();
            data = rest;
        }
    }

    // Checks that the SEGMENTS hold `data`, in order, and are marked dirty, and that nothing
    // else in memory was written to.
    fn check_read_segments(mem: &GuestMemoryMmap, mut data: &[u8]) {
        let mut expected = vec![0u8; MEM_LEN];
        for (addr, len) in SEGMENTS {
            let (chunk, rest) = data.split_at(len as usize);
            expected[u64_to_usize(addr.0)..][..chunk.len()].copy_from_slice(chunk);
            check_dirty_mem(mem, addr, len);
            data = rest;
        }
        let mut buf = vec![0u8; MEM_LEN];
        mem.read_slice(&mut buf, GuestAddress(0)).unwrap();
        assert_eq!(buf, expected);
    }

    // Checks that nothing was written to memory, which starts out as zeroes, and that none of
    // it is marked dirty.
    fn check_untouched_mem(mem: &GuestMemoryMmap) {
        let mut buf = vec![0u8; MEM_LEN];
        mem.read_slice(&mut buf, GuestAddress(0)).unwrap();
        assert_eq!(buf, vec![0u8; MEM_LEN]);
        check_clean_mem(mem, GuestAddress(0), u32::try_from(MEM_LEN).unwrap());
    }

    // The segments of a transfer of FILE_LEN bytes of which one is not guest memory: the first,
    // one in the middle or the last.
    fn segments_with_one_outside() -> [[(GuestAddress, u32); 3]; 3] {
        let outside = GuestAddress(MEM_LEN as u64);
        [0, 1, 2].map(|bad| {
            let mut segments = SEGMENTS;
            segments[bad].0 = outside;
            segments
        })
    }

    #[test]
    fn test_check_segments() {
        let mem = create_mem();
        assert_eq!(check_segments(&mem, &[]).unwrap(), 0);
        assert_eq!(check_segments(&mem, &SEGMENTS).unwrap(), FILE_LEN);
        // An empty segment adds nothing.
        let segments = [(GuestAddress(4096), 0), (GuestAddress(0), 512)];
        assert_eq!(check_segments(&mem, &segments).unwrap(), 512);

        // Each segment has to be guest memory, to its last byte.
        for segments in segments_with_one_outside() {
            assert!(matches!(
                check_segments(&mem, &segments),
                Err(GuestMemoryError::InvalidGuestAddress(_))
            ));
        }
        let segments = [
            (GuestAddress(0), 512),
            (GuestAddress(MEM_LEN as u64 - 1), 2),
        ];
        check_segments(&mem, &segments).unwrap_err();

        // Lengths that do not add up to a `u32` are rejected before anything else.
        let segments = [(GuestAddress(0), u32::MAX), (GuestAddress(0), 1)];
        match check_segments(&mem, &segments) {
            Err(GuestMemoryError::IOError(err)) => {
                assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput)
            }
            other => panic!("expected an overflow error, got {other:?}"),
        }
    }

    #[test]
    fn test_sync() {
        let mem = create_mem();
        // Create backing file.
        let file = TempFile::new().unwrap().into_file();
        let mut engine = FileEngine::from_file(file, FileEngineType::Sync).unwrap();

        let data = vmm_sys_util::rand::rand_alphanumerics(FILE_LEN as usize)
            .as_bytes()
            .to_vec();

        // Partial write
        let partial_len = 50;
        let addr = GuestAddress(MEM_LEN as u64 - u64::from(partial_len));
        mem.write(&data, addr).unwrap();
        assert_sync_execution!(
            engine.write(0, &mem, addr, partial_len, PendingRequest::default()),
            partial_len
        );
        // Partial read
        let mem = create_mem();
        assert_sync_execution!(
            engine.read(0, &mem, addr, partial_len, PendingRequest::default()),
            partial_len
        );
        // Check data
        let mut buf = vec![0u8; partial_len as usize];
        mem.read_slice(&mut buf, addr).unwrap();
        assert_eq!(buf, data[..partial_len as usize]);

        // Offset write
        let offset = 100;
        let partial_len = 50;
        let addr = GuestAddress(0);
        mem.write(&data, addr).unwrap();
        assert_sync_execution!(
            engine.write(offset, &mem, addr, partial_len, PendingRequest::default()),
            partial_len
        );
        // Offset read
        let mem = create_mem();
        assert_sync_execution!(
            engine.read(offset, &mem, addr, partial_len, PendingRequest::default()),
            partial_len
        );
        // Check data
        let mut buf = vec![0u8; partial_len as usize];
        mem.read_slice(&mut buf, addr).unwrap();
        assert_eq!(buf, data[..partial_len as usize]);

        // Full write
        mem.write(&data, GuestAddress(0)).unwrap();
        assert_sync_execution!(
            engine.write(
                0,
                &mem,
                GuestAddress(0),
                FILE_LEN,
                PendingRequest::default()
            ),
            FILE_LEN
        );
        // Full read
        let mem = create_mem();
        assert_sync_execution!(
            engine.read(
                0,
                &mem,
                GuestAddress(0),
                FILE_LEN,
                PendingRequest::default()
            ),
            FILE_LEN
        );
        // Check data
        let mut buf = vec![0u8; FILE_LEN as usize];
        mem.read_slice(&mut buf, GuestAddress(0)).unwrap();
        assert_eq!(buf, data.as_slice());

        // Check other ops
        engine.flush(PendingRequest::default()).unwrap();
        engine.drain(true).unwrap();
        engine.drain_and_flush(true).unwrap();
    }

    #[test]
    fn test_sync_dirty_tracking() {
        let temp_file = TempFile::new().unwrap();
        // A second, write-only handle on the same file, for the failing read below.
        let write_only = std::fs::OpenOptions::new()
            .write(true)
            .open(temp_file.as_path())
            .unwrap();
        let mut engine =
            FileEngine::from_file(temp_file.into_file(), FileEngineType::Sync).unwrap();
        let data = vmm_sys_util::rand::rand_alphanumerics(FILE_LEN as usize)
            .as_bytes()
            .to_vec();
        let mem = create_mem();
        mem.write(&data, GuestAddress(0)).unwrap();
        assert_sync_execution!(
            engine.write(
                0,
                &mem,
                GuestAddress(0),
                FILE_LEN,
                PendingRequest::default()
            ),
            FILE_LEN
        );

        // A read into the second page marks that page and not the first.
        let mem = create_mem();
        let addr = GuestAddress(4096);
        assert_sync_execution!(
            engine.read(0, &mem, addr, FILE_LEN, PendingRequest::default()),
            FILE_LEN
        );
        check_clean_mem(&mem, GuestAddress(0), 4096);
        check_dirty_mem(&mem, addr, FILE_LEN);

        // A read past the end of the file fails after reading what there is, and marks only
        // that: the file ends in the first page, so the second page stays clean.
        let mem = create_mem();
        let too_long = FILE_LEN + 4096;
        match engine.read(
            0,
            &mem,
            GuestAddress(0),
            too_long,
            PendingRequest::default(),
        ) {
            Err(err) => assert!(matches!(
                err.error,
                BlockIoError::Sync(SyncIoError::Transfer(GuestMemoryError::IOError(_)))
            )),
            Ok(_) => panic!("read past the end of the file succeeded"),
        }
        let mut buf = vec![0u8; FILE_LEN as usize];
        mem.read_slice(&mut buf, GuestAddress(0)).unwrap();
        assert_eq!(buf, data.as_slice());
        check_dirty_mem(&mem, GuestAddress(0), FILE_LEN);
        check_clean_mem(&mem, GuestAddress(4096), 4096);

        // A `read` that fails outright writes nothing, so nothing is marked: on the write-only
        // handle `read` fails with EBADF on the first call.
        let mut engine = FileEngine::from_file(write_only, FileEngineType::Sync).unwrap();
        let mem = create_mem();
        match engine.read(
            0,
            &mem,
            GuestAddress(0),
            2 * 4096,
            PendingRequest::default(),
        ) {
            Err(err) => assert!(matches!(
                err.error,
                BlockIoError::Sync(SyncIoError::Transfer(GuestMemoryError::IOError(_)))
            )),
            Ok(_) => panic!("read from a write-only file succeeded"),
        }
        check_clean_mem(&mem, GuestAddress(0), 2 * 4096);
    }

    #[test]
    fn test_async() {
        // Create backing file.
        let file = TempFile::new().unwrap().into_file();
        let mut engine = FileEngine::from_file(file, FileEngineType::Async).unwrap();

        let data = vmm_sys_util::rand::rand_alphanumerics(FILE_LEN as usize)
            .as_bytes()
            .to_vec();

        // Partial reads and writes cannot really be tested because io_uring will return an error
        // code for trying to write to unmapped memory.

        // Offset write
        let mem = create_mem();
        let offset = 100;
        let partial_len = 50;
        let addr = GuestAddress(0);
        mem.write(&data, addr).unwrap();
        assert_queued!(engine.write(offset, &mem, addr, partial_len, PendingRequest::default()));
        assert_async_execution(&mem, &mut engine, partial_len);
        // Offset read
        let mem = create_mem();
        assert_queued!(engine.read(offset, &mem, addr, partial_len, PendingRequest::default()));
        assert_async_execution(&mem, &mut engine, partial_len);
        // Check data
        let mut buf = vec![0u8; partial_len as usize];
        mem.read_slice(&mut buf, addr).unwrap();
        assert_eq!(buf, data[..partial_len as usize]);
        // check dirty mem
        check_dirty_mem(&mem, addr, partial_len);
        check_clean_mem(&mem, GuestAddress(4096), 4096);

        // Full write
        mem.write(&data, GuestAddress(0)).unwrap();
        assert_queued!(engine.write(0, &mem, addr, FILE_LEN, PendingRequest::default()));
        assert_async_execution(&mem, &mut engine, FILE_LEN);

        // Full read
        let mem = create_mem();
        assert_queued!(engine.read(0, &mem, addr, FILE_LEN, PendingRequest::default()));
        assert_async_execution(&mem, &mut engine, FILE_LEN);
        // Check data
        let mut buf = vec![0u8; FILE_LEN as usize];
        mem.read_slice(&mut buf, GuestAddress(0)).unwrap();
        assert_eq!(buf, data.as_slice());
        // check dirty mem
        check_dirty_mem(&mem, addr, FILE_LEN);
        check_clean_mem(&mem, GuestAddress(4096), 4096);

        // Check other ops
        assert_queued!(engine.flush(PendingRequest::default()));
        assert_async_execution(&mem, &mut engine, 0);

        engine.drain(true).unwrap();
        engine.drain_and_flush(true).unwrap();
    }

    #[test]
    fn test_vmdk() {
        let (_dir, descriptor) = create_test_vmdk("extent", 0);
        let mut engine = FileEngine::Vmdk(VmdkFileEngine::from_path(&descriptor).unwrap());
        let mem = create_mem();

        assert_sync_execution!(
            engine.read(0, &mem, GuestAddress(0), 512, PendingRequest::default()),
            512
        );
        assert!(matches!(
            engine
                .read(
                    0,
                    &mem,
                    GuestAddress(MEM_LEN as u64),
                    512,
                    PendingRequest::default()
                )
                .unwrap_err()
                .error,
            BlockIoError::Vmdk(VmdkIoError::GuestMemory(_))
        ));
        assert!(matches!(
            engine
                .write(0, &mem, GuestAddress(0), 512, PendingRequest::default())
                .unwrap_err()
                .error,
            BlockIoError::Vmdk(VmdkIoError::WriteNotSupported)
        ));

        // A read into several segments fills them in order.
        let text = b"Hello VMDK from Firecracker!";
        let segments = [(GuestAddress(4096), 10), (GuestAddress(1024), 18)];
        let mem = create_mem();
        assert_sync_execution!(
            engine.readv(0, &mem, &segments, PendingRequest::default()),
            28
        );
        let mut buf = [0u8; 28];
        mem.read_slice(&mut buf[..10], GuestAddress(4096)).unwrap();
        mem.read_slice(&mut buf[10..], GuestAddress(1024)).unwrap();
        assert_eq!(&buf, text);
        // With a segment outside guest memory it reads nothing, not even the segments before.
        let segments = [(GuestAddress(4096), 10), (GuestAddress(MEM_LEN as u64), 18)];
        let mem = create_mem();
        assert!(matches!(
            engine
                .readv(0, &mem, &segments, PendingRequest::default())
                .unwrap_err()
                .error,
            BlockIoError::Vmdk(VmdkIoError::GuestMemory(_))
        ));
        check_untouched_mem(&mem);
        // And the image stays read-only.
        assert!(matches!(
            engine
                .writev(0, &mem, &SEGMENTS, PendingRequest::default())
                .unwrap_err()
                .error,
            BlockIoError::Vmdk(VmdkIoError::WriteNotSupported)
        ));

        assert_sync_execution!(engine.flush(PendingRequest::default()), 0);
        assert!(matches!(
            engine
                .discard((0, 512), PendingRequest::default())
                .unwrap_err()
                .error,
            BlockIoError::Vmdk(VmdkIoError::WriteNotSupported)
        ));
        engine.drain(true).unwrap();
        engine.drain_and_flush(true).unwrap();
        assert!(matches!(
            engine.update_file_path(File::open(descriptor).unwrap()),
            Err(BlockIoError::Vmdk(VmdkIoError::UnsupportedOperation))
        ));
    }

    #[test]
    fn test_sync_vectored() {
        let file = TempFile::new().unwrap().into_file();
        let mut engine = FileEngine::from_file(file, FileEngineType::Sync).unwrap();
        let data = vmm_sys_util::rand::rand_alphanumerics(FILE_LEN as usize)
            .as_bytes()
            .to_vec();
        let offset = 512;

        // The segments are written to consecutive bytes of the file.
        let mem = create_mem();
        write_segments(&mem, &data);
        assert_sync_execution!(
            engine.writev(offset, &mem, &SEGMENTS, PendingRequest::default()),
            FILE_LEN
        );
        let mut buf = vec![0u8; FILE_LEN as usize];
        engine.file().read_exact_at(&mut buf, offset).unwrap();
        assert_eq!(buf, data);

        // And read back from them.
        let mem = create_mem();
        assert_sync_execution!(
            engine.readv(offset, &mem, &SEGMENTS, PendingRequest::default()),
            FILE_LEN
        );
        check_read_segments(&mem, &data);

        // A read past the end of the file fails, after filling and marking dirty only the
        // first segment, in the second page.
        let mem = create_mem();
        let short_offset = offset + u64::from(FILE_LEN) - u64::from(SEGMENTS[0].1);
        match engine.readv(short_offset, &mem, &SEGMENTS, PendingRequest::default()) {
            Err(err) => assert!(matches!(
                err.error,
                BlockIoError::Sync(SyncIoError::Transfer(GuestMemoryError::IOError(_)))
            )),
            Ok(_) => panic!("read past the end of the file succeeded"),
        }
        check_dirty_mem(&mem, SEGMENTS[0].0, SEGMENTS[0].1);
        check_clean_mem(&mem, GuestAddress(0), 4096);

        // With a segment outside guest memory, whichever it is, a read fills no segment
        for segments in segments_with_one_outside() {
            let mem = create_mem();
            match engine.readv(offset, &mem, &segments, PendingRequest::default()) {
                Err(err) => assert!(matches!(
                    err.error,
                    BlockIoError::Sync(SyncIoError::Transfer(
                        GuestMemoryError::InvalidGuestAddress(_)
                    ))
                )),
                Ok(_) => panic!("read into memory outside the guest succeeded"),
            }
            check_untouched_mem(&mem);
        }
        // and a write leaves the file as it was.
        for segments in segments_with_one_outside() {
            let mem = create_mem();
            write_segments(&mem, &vec![0xa5; FILE_LEN as usize]);
            match engine.writev(offset, &mem, &segments, PendingRequest::default()) {
                Err(err) => assert!(matches!(
                    err.error,
                    BlockIoError::Sync(SyncIoError::Transfer(
                        GuestMemoryError::InvalidGuestAddress(_)
                    ))
                )),
                Ok(_) => panic!("write from memory outside the guest succeeded"),
            }
            let mut buf = vec![0u8; FILE_LEN as usize];
            engine.file().read_exact_at(&mut buf, offset).unwrap();
            assert_eq!(buf, data);
        }

        // Empty segments transfer nothing and do not move the file offset.
        let segments = [
            (GuestAddress(4096), 0),
            (GuestAddress(0), 512),
            (GuestAddress(2048), 0),
            (GuestAddress(4096), 512),
        ];
        let mem = create_mem();
        assert_sync_execution!(
            engine.readv(offset, &mem, &segments, PendingRequest::default()),
            FILE_LEN
        );
        let mut buf = vec![0u8; FILE_LEN as usize];
        mem.read_slice(&mut buf[..512], GuestAddress(0)).unwrap();
        mem.read_slice(&mut buf[512..], GuestAddress(4096)).unwrap();
        assert_eq!(buf, data);
    }

    #[test]
    fn test_async_vectored() {
        let file = TempFile::new().unwrap().into_file();
        let mut engine = FileEngine::from_file(file, FileEngineType::Async).unwrap();
        let data = vmm_sys_util::rand::rand_alphanumerics(FILE_LEN as usize)
            .as_bytes()
            .to_vec();
        let offset = 512;

        // The segments are written to consecutive bytes of the file.
        let mem = create_mem();
        write_segments(&mem, &data);
        assert_queued!(engine.writev(offset, &mem, &SEGMENTS, PendingRequest::default()));
        assert_async_execution(&mem, &mut engine, FILE_LEN);
        let mut buf = vec![0u8; FILE_LEN as usize];
        engine.file().read_exact_at(&mut buf, offset).unwrap();
        assert_eq!(buf, data);

        // And read back from them.
        let mem = create_mem();
        assert_queued!(engine.readv(offset, &mem, &SEGMENTS, PendingRequest::default()));
        assert_async_execution(&mem, &mut engine, FILE_LEN);
        check_read_segments(&mem, &data);

        // A read that reaches the end of the file stops short, and marks dirty only what it read:
        // the first segment, in the second page.
        let mem = create_mem();
        let short_offset = offset + u64::from(FILE_LEN) - u64::from(SEGMENTS[0].1);
        assert_queued!(engine.readv(short_offset, &mem, &SEGMENTS, PendingRequest::default()));
        assert_async_execution(&mem, &mut engine, SEGMENTS[0].1);
        check_dirty_mem(&mem, SEGMENTS[0].0, SEGMENTS[0].1);
        check_clean_mem(&mem, GuestAddress(0), 4096);

        // A segment outside guest memory, whichever it is, fails the request before it is
        // submitted: a read fills no segment
        for segments in segments_with_one_outside() {
            let mem = create_mem();
            match engine.readv(offset, &mem, &segments, PendingRequest::default()) {
                Err(err) => assert!(matches!(
                    err.error,
                    BlockIoError::Async(AsyncIoError::GuestMemory(_))
                )),
                Ok(_) => panic!("read into memory outside the guest succeeded"),
            }
            assert_no_async_execution(&mem, &mut engine);
            check_untouched_mem(&mem);
        }
        // and a write leaves the file as it was.
        for segments in segments_with_one_outside() {
            let mem = create_mem();
            write_segments(&mem, &vec![0xa5; FILE_LEN as usize]);
            match engine.writev(offset, &mem, &segments, PendingRequest::default()) {
                Err(err) => assert!(matches!(
                    err.error,
                    BlockIoError::Async(AsyncIoError::GuestMemory(_))
                )),
                Ok(_) => panic!("write from memory outside the guest succeeded"),
            }
            assert_no_async_execution(&mem, &mut engine);
            let mut buf = vec![0u8; FILE_LEN as usize];
            engine.file().read_exact_at(&mut buf, offset).unwrap();
            assert_eq!(buf, data);
        }

        // Empty segments transfer nothing and do not move the file offset.
        let segments = [
            (GuestAddress(4096), 0),
            (GuestAddress(0), 512),
            (GuestAddress(2048), 0),
            (GuestAddress(4096), 512),
        ];
        let mem = create_mem();
        assert_queued!(engine.readv(offset, &mem, &segments, PendingRequest::default()));
        assert_async_execution(&mem, &mut engine, FILE_LEN);
        let mut buf = vec![0u8; FILE_LEN as usize];
        mem.read_slice(&mut buf[..512], GuestAddress(0)).unwrap();
        mem.read_slice(&mut buf[512..], GuestAddress(4096)).unwrap();
        assert_eq!(buf, data);
    }

    #[test]
    fn test_async_vectored_limits() {
        let file = TempFile::new().unwrap().into_file();
        let mut engine = FileEngine::from_file(file, FileEngineType::Async).unwrap();
        let data = vmm_sys_util::rand::rand_alphanumerics(FILE_LEN as usize)
            .as_bytes()
            .to_vec();
        let mem = create_mem();
        mem.write_slice(&data, GuestAddress(0)).unwrap();

        // The kernel takes 1024 segments in one operation, which is more than a request of
        // the device can have: here each holds one byte, and they go to the file in order.
        let one_byte_segments = |count: u64| -> Vec<(GuestAddress, u32)> {
            (0..count).map(|i| (GuestAddress(i), 1)).collect()
        };
        assert_queued!(engine.writev(0, &mem, &one_byte_segments(1024), PendingRequest::default()));
        assert_async_execution(&mem, &mut engine, 1024);
        let mut buf = vec![0u8; FILE_LEN as usize];
        engine.file().read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(buf, data);
        // One more is refused without being submitted.
        match engine.writev(0, &mem, &one_byte_segments(1025), PendingRequest::default()) {
            Err(err) => match err.error {
                BlockIoError::Async(AsyncIoError::IO(err)) => {
                    assert_eq!(err.raw_os_error(), Some(libc::EINVAL))
                }
                other => panic!("expected EINVAL, got {other:?}"),
            },
            Ok(_) => panic!("a write of 1025 segments was accepted"),
        }
        assert_no_async_execution(&mem, &mut engine);

        // A full submission queue throttles a vectored operation like any other: the request
        // comes back, and goes through once the queue has drained.
        for _ in 0..IO_URING_NUM_ENTRIES {
            assert_queued!(engine.readv(512, &mem, &SEGMENTS, PendingRequest::default()));
        }
        match engine.readv(512, &mem, &SEGMENTS, PendingRequest::default()) {
            Err(err) => assert!(err.error.is_throttling_err()),
            Ok(_) => panic!("the submission queue took more than its size"),
        }
        engine.drain(true).unwrap();
        assert_queued!(engine.readv(0, &mem, &SEGMENTS, PendingRequest::default()));
        assert_async_execution(&mem, &mut engine, FILE_LEN);
    }

    // Up to 12 segments of up to 600 bytes, some of them empty, with gaps between them, in an
    // order that is not the order of their addresses. They do not overlap, and fit the memory
    // that `create_mem` makes.
    fn arbitrary_segments() -> impl Strategy<Value = Vec<(GuestAddress, u32)>> {
        prop::collection::vec((0u64..64, 0u32..=600), 1..=12)
            .prop_map(|parts| {
                let mut addr = 0;
                parts
                    .into_iter()
                    .map(|(gap, len)| {
                        addr += gap;
                        let segment = (GuestAddress(addr), len);
                        addr += u64::from(len);
                        segment
                    })
                    .collect::<Vec<_>>()
            })
            .prop_shuffle()
    }

    // Runs a vectored read or write to its completion, on any engine, and returns its count.
    fn vectored_transfer(
        engine: &mut FileEngine,
        mem: &GuestMemoryMmap,
        read: bool,
        offset: u64,
        segments: &[(GuestAddress, u32)],
    ) -> u32 {
        let res = if read {
            engine.readv(offset, mem, segments, PendingRequest::default())
        } else {
            engine.writev(offset, mem, segments, PendingRequest::default())
        };
        match res.unwrap() {
            FileEngineOk::Executed(res) => res.count,
            FileEngineOk::Submitted => {
                let FileEngine::Async(engine) = engine else {
                    panic!("only the Async engine submits operations");
                };
                engine.drain(false).unwrap();
                engine.pop(mem).unwrap().unwrap().result().unwrap()
            }
        }
    }

    #[test]
    fn test_random_vectored_transfers() {
        const IMAGE_LEN: usize = 3 * 4096;
        let image: Vec<u8> = (0..IMAGE_LEN)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let cfg = ProptestConfig::with_cases(100);
        proptest!(cfg, |(segments in arbitrary_segments(), offset in 0u64..4096)| {
            let total: u32 = segments.iter().map(|&(_, len)| len).sum();
            let range = u64_to_usize(offset)..u64_to_usize(offset) + total as usize;
            for engine_type in [FileEngineType::Sync, FileEngineType::Async] {
                let file = TempFile::new().unwrap().into_file();
                file.write_all_at(&image, 0).unwrap();
                let mut engine = FileEngine::from_file(file, engine_type).unwrap();

                // A read fills the segments, in their order, with consecutive bytes of the
                // file. It writes nowhere else, and marks dirty the pages it wrote to and no
                // other.
                let mem = create_mem();
                prop_assert_eq!(
                    vectored_transfer(&mut engine, &mem, true, offset, &segments),
                    total
                );
                let mut expected = vec![0u8; MEM_LEN];
                let mut data = &image[range.clone()];
                for &(addr, len) in &segments {
                    let (chunk, rest) = data.split_at(len as usize);
                    expected[u64_to_usize(addr.0)..][..chunk.len()].copy_from_slice(chunk);
                    data = rest;
                }
                let mut buf = vec![0u8; MEM_LEN];
                mem.read_slice(&mut buf, GuestAddress(0)).unwrap();
                prop_assert!(buf == expected, "the read filled memory wrongly");
                let page_size = host_page_size();
                let bitmap = mem.find_region(GuestAddress(0)).unwrap().bitmap();
                for start in (0..MEM_LEN).step_by(page_size) {
                    let written = segments.iter().any(|&(addr, len)| {
                        let addr = u64_to_usize(addr.0);
                        len != 0 && addr < start + page_size && start < addr + len as usize
                    });
                    prop_assert_eq!(bitmap.dirty_at(start), written);
                }

                // A write puts the segments, in their order, at consecutive bytes of the file,
                // and leaves the rest of it alone.
                let payload: Vec<u8> = (0..total)
                    .map(|i| u8::try_from(i % 239).unwrap() ^ 0xff)
                    .collect();
                let mem = create_mem();
                let mut data = payload.as_slice();
                for &(addr, len) in &segments {
                    let (chunk, rest) = data.split_at(len as usize);
                    mem.write_slice(chunk, addr).unwrap();
                    data = rest;
                }
                prop_assert_eq!(
                    vectored_transfer(&mut engine, &mem, false, offset, &segments),
                    total
                );
                let mut expected = image.clone();
                expected[range.clone()].copy_from_slice(&payload);
                prop_assert_eq!(engine.file().metadata().unwrap().len(), IMAGE_LEN as u64);
                let mut contents = vec![0u8; IMAGE_LEN];
                engine.file().read_exact_at(&mut contents, 0).unwrap();
                prop_assert!(contents == expected, "the write changed the file wrongly");
            }
        });
    }
}
