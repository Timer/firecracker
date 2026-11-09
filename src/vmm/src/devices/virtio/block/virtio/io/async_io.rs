// Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;
use std::fs::File;
use std::os::fd::RawFd;
use std::os::unix::io::AsRawFd;

use libc::iovec;
use vm_memory::{GuestMemoryBackend, GuestMemoryError};
use vmm_sys_util::eventfd::EventFd;

use crate::devices::virtio::block::virtio::io::RequestError;
use crate::devices::virtio::block::virtio::{
    BLOCK_QUEUE_SIZE, IO_URING_NUM_ENTRIES, PendingRequest,
};
use crate::io_uring::operation::{Cqe, OpCode, Operation};
use crate::io_uring::restriction::Restriction;
use crate::io_uring::{IoUring, IoUringError};
use crate::logger::log_dev_preview_warning;
use crate::vstate::memory::{GuestAddress, GuestMemoryExtension, GuestMemoryMmap, mark_dirty_host};

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum AsyncIoError {
    /// Not implemented
    NotImplemented,
    /// IO: {0}
    IO(std::io::Error),
    /// IoUring: {0}
    IoUring(IoUringError),
    /// Submit: {0}
    Submit(std::io::Error),
    /// SyncAll: {0}
    SyncAll(std::io::Error),
    /// EventFd: {0}
    EventFd(std::io::Error),
    /// GuestMemory: {0}
    GuestMemory(GuestMemoryError),
}

#[derive(Debug)]
pub struct AsyncFileEngine {
    file: File,
    ring: IoUring<WrappedRequest>,
    completion_evt: EventFd,
}

#[derive(Debug)]
pub struct WrappedRequest {
    addr: Option<GuestAddress>,
    iovecs: IoVecs,
    req: PendingRequest,
}

impl WrappedRequest {
    fn new(req: PendingRequest) -> Self {
        WrappedRequest {
            addr: None,
            iovecs: IoVecs::default(),
            req,
        }
    }

    fn new_with_dirty_tracking(addr: GuestAddress, req: PendingRequest) -> Self {
        WrappedRequest {
            addr: Some(addr),
            iovecs: IoVecs::default(),
            req,
        }
    }

    fn new_vectored(iovecs: IoVecs, req: PendingRequest) -> Self {
        WrappedRequest {
            addr: None,
            iovecs,
            req,
        }
    }

    fn mark_dirty_mem_and_unwrap(self, mem: &GuestMemoryMmap, count: u32) -> PendingRequest {
        if let Some(addr) = self.addr {
            mem.mark_dirty(addr, count as usize)
        }
        self.iovecs.mark_dirty(mem, count as usize);

        self.req
    }
}

/// The most `iovec`s the kernel takes in one vectored operation (`UIO_MAXIOV`).
const MAX_SEGMENTS: usize = 1024;
// Every data descriptor of a request becomes one `iovec`, and a descriptor chain is no longer
// than its queue: a request can never have more segments than the kernel takes.
const _: () = assert!(BLOCK_QUEUE_SIZE as usize <= MAX_SEGMENTS);

/// The guest memory buffers of a vectored operation, as the `iovec` array the kernel reads.
///
/// The array is kept with the request until the operation completes, so that it outlives the
/// kernel's use of it.
#[derive(Debug, Default)]
struct IoVecs {
    vecs: Vec<iovec>,
    /// Whether the operation writes to the buffers, which then need to be marked dirty.
    dirty_tracking: bool,
}

// JUSTIFICATION: `iovec` holds a raw pointer, so it is not `Send`, but the engine and its pending
// requests move to the block worker thread.
// SAFETY: The `iovec`s only hold the addresses of guest memory buffers, which stay mapped while
// the device exists and may be accessed from any thread. `IoVecs` does not dereference them and,
// not being `Clone`, cannot be shared.
unsafe impl Send for IoVecs {}

impl IoVecs {
    fn new(
        mem: &GuestMemoryMmap,
        segments: &[(GuestAddress, u32)],
        dirty_tracking: bool,
    ) -> Result<Self, GuestMemoryError> {
        let vecs = segments
            .iter()
            .map(|&(addr, len)| {
                let slice = mem.get_slice(addr, len as usize)?;
                Ok(iovec {
                    iov_base: slice.ptr_guard_mut().as_ptr().cast(),
                    iov_len: slice.len(),
                })
            })
            .collect::<Result<_, GuestMemoryError>>()?;

        Ok(IoVecs {
            vecs,
            dirty_tracking,
        })
    }

    /// Marks the first `count` bytes of the buffers dirty, if the operation writes to them.
    fn mark_dirty(&self, mem: &GuestMemoryMmap, count: usize) {
        if !self.dirty_tracking {
            return;
        }
        let mut remaining = count;
        for iov in &self.vecs {
            let len = remaining.min(iov.iov_len);
            mark_dirty_host(mem, iov.iov_base.cast(), len);
            remaining -= len;
        }
    }
}

impl AsyncFileEngine {
    fn new_ring(
        file: &File,
        completion_fd: RawFd,
    ) -> Result<IoUring<WrappedRequest>, IoUringError> {
        IoUring::new(
            u32::from(IO_URING_NUM_ENTRIES),
            vec![file],
            vec![
                // Make sure we only allow operations on pre-registered fds.
                Restriction::RequireFixedFds,
                // Allowlist of opcodes.
                Restriction::AllowOpCode(OpCode::Read),
                Restriction::AllowOpCode(OpCode::Write),
                Restriction::AllowOpCode(OpCode::Readv),
                Restriction::AllowOpCode(OpCode::Writev),
                Restriction::AllowOpCode(OpCode::Fsync),
            ],
            Some(completion_fd),
        )
    }

    pub fn from_file(file: File) -> Result<AsyncFileEngine, AsyncIoError> {
        log_dev_preview_warning("Async file IO", Option::None);

        let completion_evt = EventFd::new(libc::EFD_NONBLOCK).map_err(AsyncIoError::EventFd)?;
        let ring =
            Self::new_ring(&file, completion_evt.as_raw_fd()).map_err(AsyncIoError::IoUring)?;

        Ok(AsyncFileEngine {
            file,
            ring,
            completion_evt,
        })
    }

    pub fn update_file(&mut self, file: File) -> Result<(), AsyncIoError> {
        let ring = Self::new_ring(&file, self.completion_evt.as_raw_fd())
            .map_err(AsyncIoError::IoUring)?;

        self.file = file;
        self.ring = ring;
        Ok(())
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn completion_evt(&self) -> &EventFd {
        &self.completion_evt
    }

    pub fn push_read(
        &mut self,
        offset: u64,
        mem: &GuestMemoryMmap,
        addr: GuestAddress,
        count: u32,
        req: PendingRequest,
    ) -> Result<(), RequestError<AsyncIoError>> {
        let buf = match mem.get_slice(addr, count as usize) {
            Ok(slice) => slice.ptr_guard_mut().as_ptr(),
            Err(err) => {
                return Err(RequestError {
                    req,
                    error: AsyncIoError::GuestMemory(err),
                });
            }
        };

        let wrapped_user_data = WrappedRequest::new_with_dirty_tracking(addr, req);

        self.ring
            .push(Operation::read(
                0,
                buf as usize,
                count,
                offset,
                wrapped_user_data,
            ))
            .map_err(|(io_uring_error, data)| RequestError {
                req: data.req,
                error: AsyncIoError::IoUring(io_uring_error),
            })
    }

    pub fn push_write(
        &mut self,
        offset: u64,
        mem: &GuestMemoryMmap,
        addr: GuestAddress,
        count: u32,
        req: PendingRequest,
    ) -> Result<(), RequestError<AsyncIoError>> {
        let buf = match mem.get_slice(addr, count as usize) {
            Ok(slice) => slice.ptr_guard_mut().as_ptr(),
            Err(err) => {
                return Err(RequestError {
                    req,
                    error: AsyncIoError::GuestMemory(err),
                });
            }
        };

        let wrapped_user_data = WrappedRequest::new(req);

        self.ring
            .push(Operation::write(
                0,
                buf as usize,
                count,
                offset,
                wrapped_user_data,
            ))
            .map_err(|(io_uring_error, data)| RequestError {
                req: data.req,
                error: AsyncIoError::IoUring(io_uring_error),
            })
    }

    /// Pushes a single read that fills the guest memory `segments` in order, from the file
    /// starting at `offset`.
    pub fn push_readv(
        &mut self,
        offset: u64,
        mem: &GuestMemoryMmap,
        segments: &[(GuestAddress, u32)],
        req: PendingRequest,
    ) -> Result<(), RequestError<AsyncIoError>> {
        self.push_vectored(true, offset, mem, segments, req)
    }

    /// Pushes a single write of the guest memory `segments` in order, to the file starting at
    /// `offset`.
    pub fn push_writev(
        &mut self,
        offset: u64,
        mem: &GuestMemoryMmap,
        segments: &[(GuestAddress, u32)],
        req: PendingRequest,
    ) -> Result<(), RequestError<AsyncIoError>> {
        self.push_vectored(false, offset, mem, segments, req)
    }

    fn push_vectored(
        &mut self,
        read: bool,
        offset: u64,
        mem: &GuestMemoryMmap,
        segments: &[(GuestAddress, u32)],
        req: PendingRequest,
    ) -> Result<(), RequestError<AsyncIoError>> {
        let iovecs = match IoVecs::new(mem, segments, read) {
            Ok(iovecs) => iovecs,
            Err(err) => {
                return Err(RequestError {
                    req,
                    error: AsyncIoError::GuestMemory(err),
                });
            }
        };
        // The `iovec` array is on the heap, so it stays put when `iovecs` moves into the request.
        let addr = iovecs.vecs.as_ptr() as usize;
        // The segments come from one descriptor chain, which is no longer than its queue, so
        // there are at most `MAX_SEGMENTS` of them. Should a caller ever pass more, fail the
        // request the way the kernel would instead of submitting it.
        let len = match u32::try_from(iovecs.vecs.len()) {
            Ok(len) if iovecs.vecs.len() <= MAX_SEGMENTS => len,
            _ => {
                return Err(RequestError {
                    req,
                    error: AsyncIoError::IO(std::io::Error::from_raw_os_error(libc::EINVAL)),
                });
            }
        };
        let wrapped_user_data = WrappedRequest::new_vectored(iovecs, req);
        let operation = if read {
            Operation::readv(0, addr, len, offset, wrapped_user_data)
        } else {
            Operation::writev(0, addr, len, offset, wrapped_user_data)
        };

        self.ring
            .push(operation)
            .map_err(|(io_uring_error, data)| RequestError {
                req: data.req,
                error: AsyncIoError::IoUring(io_uring_error),
            })
    }

    pub fn push_flush(&mut self, req: PendingRequest) -> Result<(), RequestError<AsyncIoError>> {
        let wrapped_user_data = WrappedRequest::new(req);

        self.ring
            .push(Operation::fsync(0, wrapped_user_data))
            .map_err(|(io_uring_error, data)| RequestError {
                req: data.req,
                error: AsyncIoError::IoUring(io_uring_error),
            })
    }

    pub fn discard(&mut self, _range: (u64, u32)) -> Result<u32, AsyncIoError> {
        Err(AsyncIoError::NotImplemented)
    }

    pub fn kick_submission_queue(&mut self) -> Result<(), AsyncIoError> {
        self.ring
            .submit()
            .map(|_| ())
            .map_err(AsyncIoError::IoUring)
    }

    pub fn drain(&mut self, discard_cqes: bool) -> Result<(), AsyncIoError> {
        self.ring
            .submit_and_wait_all()
            .map_err(AsyncIoError::IoUring)?;

        if discard_cqes {
            // Drain the completion queue so that we may deallocate the user_data fields.
            while self.do_pop()?.is_some() {}
        }

        Ok(())
    }

    pub fn drain_and_flush(&mut self, discard_cqes: bool) -> Result<(), AsyncIoError> {
        self.drain(discard_cqes)?;

        // Sync data out to physical media on host.
        // We don't need to call flush first since all the ops are performed through io_uring
        // and Rust shouldn't manage any data in its internal buffers.
        self.file.sync_all().map_err(AsyncIoError::SyncAll)?;

        Ok(())
    }

    fn do_pop(&mut self) -> Result<Option<Cqe<WrappedRequest>>, AsyncIoError> {
        self.ring.pop().map_err(AsyncIoError::IoUring)
    }

    pub fn pop(
        &mut self,
        mem: &GuestMemoryMmap,
    ) -> Result<Option<Cqe<PendingRequest>>, AsyncIoError> {
        let cqe = self.do_pop()?.map(|cqe| {
            let count = cqe.count();
            cqe.map_user_data(|wrapped_user_data| {
                wrapped_user_data.mark_dirty_mem_and_unwrap(mem, count)
            })
        });

        Ok(cqe)
    }
}
