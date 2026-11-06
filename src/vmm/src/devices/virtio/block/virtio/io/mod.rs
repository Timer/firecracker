// Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

pub mod async_io;
pub mod sync_io;

use std::fmt::Debug;
use std::fs::File;

pub use self::async_io::{AsyncFileEngine, AsyncIoError};
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

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum FileEngine {
    #[allow(unused)]
    Async(AsyncFileEngine),
    Sync(SyncFileEngine),
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
        };

        Ok(())
    }

    pub fn file(&self) -> &File {
        match self {
            FileEngine::Async(engine) => engine.file(),
            FileEngine::Sync(engine) => engine.file(),
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
        }
    }

    pub fn drain(&mut self, discard: bool) -> Result<(), BlockIoError> {
        match self {
            FileEngine::Async(engine) => engine.drain(discard).map_err(BlockIoError::Async),
            FileEngine::Sync(_engine) => Ok(()),
        }
    }

    pub fn drain_and_flush(&mut self, discard: bool) -> Result<(), BlockIoError> {
        match self {
            FileEngine::Async(engine) => {
                engine.drain_and_flush(discard).map_err(BlockIoError::Async)
            }
            FileEngine::Sync(engine) => engine.flush().map_err(BlockIoError::Sync),
        }
    }
}

#[cfg(test)]
pub mod tests {
    #![allow(clippy::undocumented_unsafe_blocks)]
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::FileExt;

    use vm_memory::{GuestMemoryBackend, GuestMemoryError, GuestMemoryRegion};
    use vmm_sys_util::tempfile::TempFile;

    use super::*;
    use crate::devices::virtio::block::virtio::device::FileEngineType;
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
    fn check_segments(mem: &GuestMemoryMmap, mut data: &[u8]) {
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
        check_segments(&mem, &data);

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
        check_segments(&mem, &data);

        // A read that reaches the end of the file stops short, and marks dirty only what it read:
        // the first segment, in the second page.
        let mem = create_mem();
        let short_offset = offset + u64::from(FILE_LEN) - u64::from(SEGMENTS[0].1);
        assert_queued!(engine.readv(short_offset, &mem, &SEGMENTS, PendingRequest::default()));
        assert_async_execution(&mem, &mut engine, SEGMENTS[0].1);
        check_dirty_mem(&mem, SEGMENTS[0].0, SEGMENTS[0].1);
        check_clean_mem(&mem, GuestAddress(0), 4096);

        // A segment outside guest memory fails the request before it is submitted.
        let segments = [SEGMENTS[0], (GuestAddress(MEM_LEN as u64), 512)];
        match engine.readv(0, &mem, &segments, PendingRequest::default()) {
            Err(err) => assert!(matches!(
                err.error,
                BlockIoError::Async(AsyncIoError::GuestMemory(_))
            )),
            Ok(_) => panic!("read into memory outside the guest succeeded"),
        }
    }
}
