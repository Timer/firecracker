// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Portions Copyright 2019 Intel Corporation. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::ops::Deref;
use std::sync::Arc;

use utils::time::{ClockType, get_time_us};
use vhost::vhost_user::Frontend;
use vhost::vhost_user::message::*;
use vmm_sys_util::eventfd::EventFd;

use super::{NUM_QUEUES, QUEUE_SIZE, VhostUserBlockError};
use crate::devices::virtio::ActivateError;
use crate::devices::virtio::block::CacheType;
use crate::devices::virtio::device::{ActiveState, DeviceState, VirtioDevice, VirtioDeviceType};
use crate::devices::virtio::generated::virtio_blk::{VIRTIO_BLK_F_FLUSH, VIRTIO_BLK_F_RO};
use crate::devices::virtio::generated::virtio_config::VIRTIO_F_VERSION_1;
use crate::devices::virtio::generated::virtio_ring::VIRTIO_RING_F_EVENT_IDX;
use crate::devices::virtio::queue::Queue;
use crate::devices::virtio::transport::{VirtioInterrupt, VirtioInterruptType};
use crate::devices::virtio::vhost_user::{
    VhostUserConfigReply, VhostUserHandleBackend, VhostUserHandleImpl,
};
use crate::devices::virtio::vhost_user_metrics::{
    VhostUserDeviceMetrics, VhostUserMetricsPerDevice,
};
use crate::logger::{IncMetric, StoreMetric, error, log_dev_preview_warning};
use crate::utils::u64_to_usize;
use crate::vmm_config::drive::BlockDeviceConfig;
use crate::vstate::memory::GuestMemoryMmap;
use crate::{MutEventSubscriber, impl_device_type};

/// Block device config space size in bytes.
const BLOCK_CONFIG_SPACE_SIZE: u32 = 60;

const AVAILABLE_FEATURES: u64 = (1 << VIRTIO_F_VERSION_1)
    | (1 << VIRTIO_RING_F_EVENT_IDX)
    // vhost-user specific bit. Not defined in standard virtio spec.
    // Specifies ability of frontend to negotiate protocol features.
    | VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits()
    // We always try to negotiate readonly with the backend.
    // If the backend is configured as readonly, we will accept it.
    | (1 << VIRTIO_BLK_F_RO);

/// Use this structure to set up the Block Device before booting the kernel.
#[derive(Debug, PartialEq, Eq)]
pub struct VhostUserBlockConfig {
    /// Unique identifier of the drive.
    pub drive_id: String,
    /// Part-UUID. Represents the unique id of the boot partition of this device. It is
    /// optional and it will be used only if the `is_root_device` field is true.
    pub partuuid: Option<String>,
    /// If set to true, it makes the current device the root block device.
    /// Setting this flag to true will mount the block device in the
    /// guest under /dev/vda unless the partuuid is present.
    pub is_root_device: bool,
    /// If set to true, the drive will ignore flush requests coming from
    /// the guest driver.
    pub cache_type: CacheType,

    /// Socket path of the vhost-user process
    pub socket: String,
}

impl TryFrom<&BlockDeviceConfig> for VhostUserBlockConfig {
    type Error = VhostUserBlockError;

    fn try_from(value: &BlockDeviceConfig) -> Result<Self, Self::Error> {
        if let (Some(socket), None, None, None, None, None, None, None) = (
            &value.socket,
            &value.is_read_only,
            &value.discard,
            &value.path_on_host,
            &value.rate_limiter,
            &value.file_engine_type,
            &value.blk_size,
            &value.topology,
        ) {
            Ok(Self {
                drive_id: value.drive_id.clone(),
                partuuid: value.partuuid.clone(),
                is_root_device: value.is_root_device,
                cache_type: value.cache_type,

                socket: socket.clone(),
            })
        } else {
            Err(VhostUserBlockError::Config)
        }
    }
}

impl From<VhostUserBlockConfig> for BlockDeviceConfig {
    fn from(value: VhostUserBlockConfig) -> Self {
        Self {
            drive_id: value.drive_id,
            partuuid: value.partuuid,
            is_root_device: value.is_root_device,
            cache_type: value.cache_type,

            is_read_only: None,
            discard: None,
            path_on_host: None,
            rate_limiter: None,
            file_engine_type: None,
            blk_size: None,
            topology: None,

            socket: Some(value.socket),
        }
    }
}

pub type VhostUserBlock = VhostUserBlockImpl<Frontend>;

/// State of the config update that a `PATCH /drives/{id}` request starts.
///
/// The device sends `VHOST_USER_GET_CONFIG` and returns. The event loop reads the reply when
/// the socket is readable, so a slow backend does not stop the VMM thread.
#[derive(Debug)]
pub enum ConfigUpdate {
    /// No request waits for a reply.
    Idle,
    /// A `VHOST_USER_GET_CONFIG` request waits for its reply.
    InProgress {
        /// The part of the reply that the device received.
        reply: VhostUserConfigReply,
        /// A config update was requested after the device sent this request. The device
        /// sends one more request after the reply.
        queued: bool,
        /// Time when the device sent the request, in microseconds.
        start_us: u64,
    },
    /// The socket is closed or failed, or the start of the next reply is not known. Config
    /// updates are not possible.
    Failed,
}

/// vhost-user block device.
pub struct VhostUserBlockImpl<T: VhostUserHandleBackend> {
    // Virtio fields.
    pub avail_features: u64,
    pub acked_features: u64,
    pub config_space: Vec<u8>,
    pub activate_evt: EventFd,

    // Transport related fields.
    pub queues: Vec<Queue>,
    pub queue_evts: [EventFd; u64_to_usize(NUM_QUEUES)],
    pub device_state: DeviceState,
    /// The config space changed while the device was not active. The guest
    /// driver gets a config change notification when it activates the device.
    pub config_change_pending: bool,
    /// Tells the event loop to watch the backend socket for a config reply.
    pub config_update_evt: EventFd,
    /// State of the config update that a `PATCH /drives/{id}` request starts.
    pub config_update: ConfigUpdate,
    /// The event loop watches the backend socket for a config reply.
    pub config_reply_watched: bool,

    // Implementation specific fields.
    pub id: String,
    pub partuuid: Option<String>,
    pub cache_type: CacheType,
    pub root_device: bool,
    pub read_only: bool,

    // Vhost user protocol handle
    pub vu_handle: VhostUserHandleImpl<T>,
    pub vu_acked_protocol_features: u64,
    pub metrics: Arc<VhostUserDeviceMetrics>,
}

// Need custom implementation because otherwise `Debug` is required for `vhost::Master`
impl<T: VhostUserHandleBackend> std::fmt::Debug for VhostUserBlockImpl<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VhostUserBlockImpl")
            .field("avail_features", &self.avail_features)
            .field("acked_features", &self.acked_features)
            .field("config_space", &self.config_space)
            .field("activate_evt", &self.activate_evt)
            .field("queues", &self.queues)
            .field("queue_evts", &self.queue_evts)
            .field("device_state", &self.device_state)
            .field("config_change_pending", &self.config_change_pending)
            .field("config_update_evt", &self.config_update_evt)
            .field("config_update", &self.config_update)
            .field("config_reply_watched", &self.config_reply_watched)
            .field("id", &self.id)
            .field("partuuid", &self.partuuid)
            .field("cache_type", &self.cache_type)
            .field("root_device", &self.root_device)
            .field("read_only", &self.read_only)
            .field("vu_handle", &self.vu_handle)
            .field(
                "vu_acked_protocol_features",
                &self.vu_acked_protocol_features,
            )
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl<T: VhostUserHandleBackend> VhostUserBlockImpl<T> {
    pub fn new(config: VhostUserBlockConfig) -> Result<Self, VhostUserBlockError> {
        log_dev_preview_warning("vhost-user-blk device", Option::None);
        let start_time = get_time_us(ClockType::Monotonic);
        let mut requested_features = AVAILABLE_FEATURES;

        if config.cache_type == CacheType::Writeback {
            requested_features |= 1 << VIRTIO_BLK_F_FLUSH;
        }

        let requested_protocol_features = VhostUserProtocolFeatures::CONFIG;

        let mut vu_handle = VhostUserHandleImpl::<T>::new(&config.socket, NUM_QUEUES)
            .map_err(VhostUserBlockError::VhostUser)?;
        let (backend_acked_features, acked_protocol_features) = vu_handle
            .negotiate_features(requested_features, requested_protocol_features)
            .map_err(VhostUserBlockError::VhostUser)?;

        // Get config from backend if CONFIG is acked or use empty buffer.
        let config_space =
            if acked_protocol_features & VhostUserProtocolFeatures::CONFIG.bits() != 0 {
                // This buffer is used for config size check in vhost crate.
                let buffer = [0u8; BLOCK_CONFIG_SPACE_SIZE as usize];
                let (_, new_config_space) = vu_handle
                    .vu
                    .get_config(
                        0,
                        BLOCK_CONFIG_SPACE_SIZE,
                        VhostUserConfigFlags::WRITABLE,
                        &buffer,
                    )
                    .map_err(VhostUserBlockError::Vhost)?;
                new_config_space
            } else {
                vec![]
            };

        let activate_evt =
            EventFd::new(libc::EFD_NONBLOCK).map_err(VhostUserBlockError::EventFd)?;
        let config_update_evt =
            EventFd::new(libc::EFD_NONBLOCK).map_err(VhostUserBlockError::EventFd)?;

        let queues = vec![Queue::new(QUEUE_SIZE)];
        let queue_evts = [EventFd::new(libc::EFD_NONBLOCK).map_err(VhostUserBlockError::EventFd)?;
            u64_to_usize(NUM_QUEUES)];
        let device_state = DeviceState::Inactive;

        let read_only = backend_acked_features & (1 << VIRTIO_BLK_F_RO) != 0;

        // Whatever the backend acked is what the guest driver is offered.
        let avail_features = backend_acked_features;
        // The guest has acked nothing yet. The vhost-user protocol bit is
        // pre-seeded because the guest never negotiates it but the backend
        // expects it kept.
        let acked_features =
            backend_acked_features & VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits();
        let vhost_user_block_metrics_name = format!("block_{}", config.drive_id);

        let metrics = VhostUserMetricsPerDevice::alloc(vhost_user_block_metrics_name);
        let delta_us = get_time_us(ClockType::Monotonic) - start_time;
        metrics.init_time_us.store(delta_us);

        Ok(Self {
            avail_features,
            acked_features,
            config_space,
            activate_evt,

            queues,
            queue_evts,
            device_state,
            config_change_pending: false,
            config_update_evt,
            config_update: ConfigUpdate::Idle,
            config_reply_watched: false,

            id: config.drive_id,
            partuuid: config.partuuid,
            cache_type: config.cache_type,
            read_only,
            root_device: config.is_root_device,

            vu_handle,
            vu_acked_protocol_features: acked_protocol_features,
            metrics,
        })
    }

    /// Prepare device for being snapshotted.
    pub fn prepare_save(&mut self) {
        unimplemented!("VhostUserBlock does not support snapshotting yet");
    }

    pub fn config(&self) -> VhostUserBlockConfig {
        VhostUserBlockConfig {
            drive_id: self.id.clone(),
            partuuid: self.partuuid.clone(),
            is_root_device: self.root_device,
            cache_type: self.cache_type,
            socket: self.vu_handle.socket_path.clone(),
        }
    }

    /// Start a config update: send `VHOST_USER_GET_CONFIG` to the backend, and do not wait
    /// for the reply. The event loop reads the reply, stores the new config, and notifies
    /// the guest driver (see [`Self::process_config_reply`]).
    ///
    /// If a request waits for its reply, the device sends one more request after the reply.
    pub fn config_update(&mut self) -> Result<(), VhostUserBlockError> {
        if self.vu_acked_protocol_features & VhostUserProtocolFeatures::CONFIG.bits() == 0 {
            return Err(VhostUserBlockError::ConfigNotSupported);
        }
        match &mut self.config_update {
            ConfigUpdate::Idle => {}
            ConfigUpdate::InProgress { queued, .. } => {
                *queued = true;
                return Ok(());
            }
            ConfigUpdate::Failed => return Err(VhostUserBlockError::ConfigSocketFailed),
        }
        // Tell the event loop to watch the socket. The VMM thread runs this function, so the
        // event loop reads the event after the request is sent.
        self.config_update_evt
            .write(1)
            .map_err(VhostUserBlockError::ConfigRequest)?;
        self.send_config_request()
    }

    fn send_config_request(&mut self) -> Result<(), VhostUserBlockError> {
        let start_us = get_time_us(ClockType::Monotonic);
        match self.vu_handle.send_get_config(
            0,
            BLOCK_CONFIG_SPACE_SIZE,
            VhostUserConfigFlags::WRITABLE,
        ) {
            Ok(()) => {
                self.config_update = ConfigUpdate::InProgress {
                    reply: VhostUserConfigReply::new(0, BLOCK_CONFIG_SPACE_SIZE),
                    queued: false,
                    start_us,
                };
                Ok(())
            }
            Err(err) => {
                self.metrics.cfg_fails.inc();
                // A send that fails with `WouldBlock` sends nothing, so the socket is
                // still usable.
                self.config_update = if err.kind() == io::ErrorKind::WouldBlock {
                    ConfigUpdate::Idle
                } else {
                    ConfigUpdate::Failed
                };
                Err(VhostUserBlockError::ConfigRequest(err))
            }
        }
    }

    /// Returns true if a config request waits for its reply.
    pub fn config_reply_expected(&self) -> bool {
        matches!(self.config_update, ConfigUpdate::InProgress { .. })
    }

    /// Read the reply to the config request without blocking. The event loop calls this
    /// function when the backend socket is readable.
    ///
    /// When the reply is complete, store the new config and notify the guest driver. If
    /// the device is not active, notify the driver when it activates the device. An error
    /// is logged and counted in the `cfg_fails` metric. The microVM continues to run.
    pub fn process_config_reply(&mut self) {
        let fd = self.vu_handle.vu.socket_fd();
        let ConfigUpdate::InProgress {
            reply,
            queued,
            start_us,
        } = &mut self.config_update
        else {
            return;
        };
        let (queued, start_us) = (*queued, *start_us);
        match reply.read(fd) {
            Ok(None) => return,
            Ok(Some(config_space)) => {
                self.config_update = ConfigUpdate::Idle;
                self.config_space = config_space;
                self.notify_config_change();
                let delta_us = get_time_us(ClockType::Monotonic) - start_us;
                self.metrics.config_change_time_us.store(delta_us);
            }
            Err(err) => {
                error!(
                    "vhost-user-block {}: config update failed: {}",
                    self.id, err
                );
                self.metrics.cfg_fails.inc();
                self.config_update = if err.is_fatal() {
                    ConfigUpdate::Failed
                } else {
                    ConfigUpdate::Idle
                };
            }
        }
        if queued
            && matches!(self.config_update, ConfigUpdate::Idle)
            && let Err(err) = self.send_config_request()
        {
            error!(
                "vhost-user-block {}: config update failed: {}",
                self.id, err
            );
        }
    }

    fn notify_config_change(&mut self) {
        match self.device_state.active_state() {
            Some(state) => {
                if let Err(err) = state.interrupt.trigger(VirtioInterruptType::Config) {
                    error!(
                        "vhost-user-block {}: cannot send the config change interrupt: {}",
                        self.id, err
                    );
                    self.metrics.cfg_fails.inc();
                }
            }
            // The guest driver can read the config space before it activates the
            // device, so it can have the old config. Notify it at activation.
            None => self.config_change_pending = true,
        }
    }
}

impl<T: VhostUserHandleBackend + Send + 'static> VirtioDevice for VhostUserBlockImpl<T>
where
    VhostUserBlockImpl<T>: MutEventSubscriber,
{
    impl_device_type!(VirtioDeviceType::Block);

    fn id(&self) -> &str {
        &self.id
    }

    fn avail_features(&self) -> u64 {
        self.avail_features
    }

    fn acked_features(&self) -> u64 {
        self.acked_features
    }

    fn set_acked_features(&mut self, acked_features: u64) {
        self.acked_features = acked_features;
    }

    fn queues(&self) -> &[Queue] {
        &self.queues
    }

    fn queues_mut(&mut self) -> &mut [Queue] {
        &mut self.queues
    }

    fn queue_events(&self) -> &[EventFd] {
        &self.queue_evts
    }

    fn interrupt_trigger(&self) -> &dyn VirtioInterrupt {
        self.device_state
            .active_state()
            .expect("Device is not initialized")
            .interrupt
            .deref()
    }

    fn config_as_bytes(&self) -> &[u8] {
        self.config_space.as_slice()
    }

    fn write_config(&mut self, _offset: u64, _data: &[u8]) {
        // We do not advertise VIRTIO_BLK_F_CONFIG_WCE
        // that would allow configuring the "writeback" field.
        // Other block config fields are immutable.
    }

    fn activate(
        &mut self,
        mem: GuestMemoryMmap,
        interrupt: Arc<dyn VirtioInterrupt>,
    ) -> Result<(), ActivateError> {
        assert!(!self.is_activated());

        for q in self.queues.iter_mut() {
            q.initialize(&mem)
                .map_err(ActivateError::QueueMemoryError)?;
        }

        let start_time = get_time_us(ClockType::Monotonic);
        // Setting features again, because now we negotiated them
        // with guest driver as well.
        self.vu_handle
            .set_features(self.acked_features)
            .and_then(|()| {
                self.vu_handle.setup_backend(
                    &mem,
                    &[(0, &self.queues[0], &self.queue_evts[0])],
                    interrupt.clone(),
                )
            })
            .map_err(|err| {
                self.metrics.activate_fails.inc();
                ActivateError::VhostUser(err)
            })?;
        if std::mem::take(&mut self.config_change_pending)
            && let Err(err) = interrupt.trigger(VirtioInterruptType::Config)
        {
            error!("Failed to send the pending config change interrupt: {err}");
        }
        self.device_state = DeviceState::Activated(ActiveState { mem, interrupt });
        let delta_us = get_time_us(ClockType::Monotonic) - start_time;
        self.metrics.activate_time_us.store(delta_us);
        Ok(())
    }

    fn is_activated(&self) -> bool {
        self.device_state.is_activated()
    }

    fn deactivate(&mut self) {
        self.device_state = DeviceState::Inactive;
    }

    fn _reset(&mut self) -> bool {
        // TODO: stop the backend before reporting success. The backend keeps the guest
        // memory it received at activation and keeps serving the device, so it may write
        // into pages that the guest has already freed and reused.
        // https://github.com/firecracker-microvm/firecracker/issues/6277
        false
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::undocumented_unsafe_blocks)]

    use std::io::{Read, Write};
    use std::os::fd::{AsRawFd, RawFd};
    use std::os::unix::net::UnixStream;
    use std::sync::Mutex;
    use std::sync::atomic::Ordering;

    use event_manager::{EventManager, EventOps, Events, MutEventSubscriber, SubscriberOps};
    use vhost::{VhostUserMemoryRegionInfo, VringConfigData};
    use vmm_sys_util::tempfile::TempFile;

    use super::*;
    use crate::devices::virtio::block::virtio::device::FileEngineType;
    use crate::devices::virtio::test_utils::{VirtQueue, default_interrupt, default_mem};
    use crate::devices::virtio::transport::mmio::{IrqTrigger, VIRTIO_MMIO_INT_CONFIG};
    use crate::devices::virtio::vhost_user::tests::{
        create_mem, get_config_reply, get_config_request_len, header,
    };
    use crate::test_utils::create_tmp_socket;
    use crate::vstate::memory::GuestAddress;

    #[test]
    fn test_from_config() {
        let block_config = BlockDeviceConfig {
            drive_id: "".to_string(),
            partuuid: None,
            is_root_device: false,
            cache_type: CacheType::Unsafe,

            is_read_only: None,
            discard: None,
            path_on_host: None,
            rate_limiter: None,
            file_engine_type: None,
            blk_size: None,
            topology: None,

            socket: Some("sock".to_string()),
        };
        VhostUserBlockConfig::try_from(&block_config).unwrap();

        let block_config = BlockDeviceConfig {
            drive_id: "".to_string(),
            partuuid: None,
            is_root_device: false,
            cache_type: CacheType::Unsafe,

            is_read_only: Some(true),
            discard: None,
            path_on_host: Some("path".to_string()),
            rate_limiter: None,
            file_engine_type: Some(FileEngineType::Sync),
            blk_size: None,
            topology: None,

            socket: None,
        };
        VhostUserBlockConfig::try_from(&block_config).unwrap_err();

        let block_config = BlockDeviceConfig {
            drive_id: "".to_string(),
            partuuid: None,
            is_root_device: false,
            cache_type: CacheType::Unsafe,

            is_read_only: Some(true),
            discard: None,
            path_on_host: Some("path".to_string()),
            rate_limiter: None,
            file_engine_type: Some(FileEngineType::Sync),
            blk_size: None,
            topology: None,

            socket: Some("sock".to_string()),
        };
        VhostUserBlockConfig::try_from(&block_config).unwrap_err();
    }

    #[test]
    fn test_new_no_features() {
        struct MockMaster {
            sock: UnixStream,
            max_queue_num: u64,
            is_owner: std::cell::UnsafeCell<bool>,
            features: u64,
            protocol_features: VhostUserProtocolFeatures,
            hdr_flags: std::cell::UnsafeCell<VhostUserHeaderFlag>,
        }

        impl VhostUserHandleBackend for MockMaster {
            fn from_stream(sock: UnixStream, max_queue_num: u64) -> Self {
                Self {
                    sock,
                    max_queue_num,
                    is_owner: std::cell::UnsafeCell::new(false),
                    features: 0,
                    protocol_features: VhostUserProtocolFeatures::empty(),
                    hdr_flags: std::cell::UnsafeCell::new(VhostUserHeaderFlag::empty()),
                }
            }

            fn set_owner(&self) -> Result<(), vhost::Error> {
                unsafe { *self.is_owner.get() = true };
                Ok(())
            }

            fn set_hdr_flags(&self, flags: VhostUserHeaderFlag) {
                unsafe { *self.hdr_flags.get() = flags };
            }

            fn get_features(&self) -> Result<u64, vhost::Error> {
                Ok(self.features)
            }

            fn get_protocol_features(&mut self) -> Result<VhostUserProtocolFeatures, vhost::Error> {
                Ok(self.protocol_features)
            }

            fn set_protocol_features(
                &mut self,
                features: VhostUserProtocolFeatures,
            ) -> Result<(), vhost::Error> {
                self.protocol_features = features;
                Ok(())
            }
        }

        impl MutEventSubscriber for VhostUserBlockImpl<MockMaster> {
            fn process(&mut self, _: Events, _: &mut EventOps) {}
            fn init(&mut self, _: &mut EventOps) {}
        }

        let (_tmp_dir, tmp_socket_path) = create_tmp_socket();

        let vhost_block_config = VhostUserBlockConfig {
            drive_id: "test_drive".to_string(),
            partuuid: None,
            is_root_device: false,
            cache_type: CacheType::Unsafe,
            socket: tmp_socket_path.clone(),
        };
        let vhost_block = VhostUserBlockImpl::<MockMaster>::new(vhost_block_config).unwrap();

        // If backend has no features, nothing should be negotiated and
        // no flags should be set.
        assert_eq!(
            vhost_block
                .vu_handle
                .vu
                .sock
                .peer_addr()
                .unwrap()
                .as_pathname()
                .unwrap()
                .to_str()
                .unwrap(),
            &tmp_socket_path,
        );
        assert_eq!(vhost_block.vu_handle.vu.max_queue_num, NUM_QUEUES);
        assert!(unsafe { *vhost_block.vu_handle.vu.is_owner.get() });
        assert_eq!(vhost_block.avail_features, 0);
        assert_eq!(vhost_block.acked_features, 0);
        assert_eq!(vhost_block.vu_acked_protocol_features, 0);
        assert_eq!(
            unsafe { &*vhost_block.vu_handle.vu.hdr_flags.get() }.bits(),
            VhostUserHeaderFlag::empty().bits()
        );
        assert!(!vhost_block.root_device);
        assert!(!vhost_block.read_only);
        assert_eq!(vhost_block.config_space, Vec::<u8>::new());
    }

    #[test]
    fn test_new_all_features() {
        struct MockMaster {
            sock: UnixStream,
            max_queue_num: u64,
            is_owner: std::cell::UnsafeCell<bool>,
            features: u64,
            protocol_features: VhostUserProtocolFeatures,
            hdr_flags: std::cell::UnsafeCell<VhostUserHeaderFlag>,
        }

        impl VhostUserHandleBackend for MockMaster {
            fn from_stream(sock: UnixStream, max_queue_num: u64) -> Self {
                Self {
                    sock,
                    max_queue_num,
                    is_owner: std::cell::UnsafeCell::new(false),
                    features: AVAILABLE_FEATURES | (1 << VIRTIO_BLK_F_FLUSH),

                    protocol_features: VhostUserProtocolFeatures::all(),
                    hdr_flags: std::cell::UnsafeCell::new(VhostUserHeaderFlag::empty()),
                }
            }

            fn set_owner(&self) -> Result<(), vhost::Error> {
                unsafe { *self.is_owner.get() = true };
                Ok(())
            }

            fn set_hdr_flags(&self, flags: VhostUserHeaderFlag) {
                unsafe { *self.hdr_flags.get() = flags };
            }

            fn get_features(&self) -> Result<u64, vhost::Error> {
                Ok(self.features)
            }

            fn get_protocol_features(&mut self) -> Result<VhostUserProtocolFeatures, vhost::Error> {
                Ok(self.protocol_features)
            }

            fn set_protocol_features(
                &mut self,
                features: VhostUserProtocolFeatures,
            ) -> Result<(), vhost::Error> {
                self.protocol_features = features;
                Ok(())
            }

            fn get_config(
                &mut self,
                _offset: u32,
                _size: u32,
                _flags: VhostUserConfigFlags,
                _buf: &[u8],
            ) -> Result<(VhostUserConfig, VhostUserConfigPayload), vhost::Error> {
                Ok((VhostUserConfig::default(), vec![0x69, 0x69, 0x69]))
            }
        }

        impl MutEventSubscriber for VhostUserBlockImpl<MockMaster> {
            fn process(&mut self, _: Events, _: &mut EventOps) {}
            fn init(&mut self, _: &mut EventOps) {}
        }

        let (_tmp_dir, tmp_socket_path) = create_tmp_socket();

        let vhost_block_config = VhostUserBlockConfig {
            drive_id: "test_drive".to_string(),
            partuuid: None,
            is_root_device: false,
            cache_type: CacheType::Writeback,
            socket: tmp_socket_path.clone(),
        };
        let mut vhost_block = VhostUserBlockImpl::<MockMaster>::new(vhost_block_config).unwrap();

        // If backend has all features, features offered by block device
        // should be negotiated and header flags should be set.
        assert_eq!(
            vhost_block
                .vu_handle
                .vu
                .sock
                .peer_addr()
                .unwrap()
                .as_pathname()
                .unwrap()
                .to_str()
                .unwrap(),
            &tmp_socket_path,
        );
        assert_eq!(vhost_block.vu_handle.vu.max_queue_num, NUM_QUEUES);
        assert!(unsafe { *vhost_block.vu_handle.vu.is_owner.get() });

        assert_eq!(
            vhost_block.avail_features,
            AVAILABLE_FEATURES | (1 << VIRTIO_BLK_F_FLUSH)
        );
        assert_eq!(
            vhost_block.acked_features,
            VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits()
        );
        assert_eq!(
            vhost_block.vu_acked_protocol_features,
            VhostUserProtocolFeatures::CONFIG.bits()
        );
        assert_eq!(
            unsafe { &*vhost_block.vu_handle.vu.hdr_flags.get() }.bits(),
            VhostUserHeaderFlag::empty().bits()
        );
        assert!(!vhost_block.root_device);
        assert!(vhost_block.read_only);
        assert_eq!(vhost_block.config_space, vec![0x69, 0x69, 0x69]);

        // Test some `VirtioDevice` methods
        assert_eq!(
            vhost_block.avail_features(),
            AVAILABLE_FEATURES | (1 << VIRTIO_BLK_F_FLUSH)
        );
        assert_eq!(
            vhost_block.acked_features(),
            VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits()
        );

        // Valid read
        let mut read_config = vec![0, 0, 0];
        vhost_block.read_config(0, &mut read_config);
        assert_eq!(read_config, vec![0x69, 0x69, 0x69]);

        // Invalid offset
        let mut read_config = vec![0, 0, 0];
        vhost_block.read_config(0x69, &mut read_config);
        assert_eq!(read_config, vec![0, 0, 0]);

        // Writing to the config does nothing
        vhost_block.write_config(0x69, &[0]);
        assert_eq!(vhost_block.config_space, vec![0x69, 0x69, 0x69]);
    }

    #[test]
    fn test_activate() {
        struct MockMaster {
            features_are_set: std::cell::UnsafeCell<bool>,
            memory_is_set: std::cell::UnsafeCell<bool>,
            vring_enabled: std::cell::UnsafeCell<bool>,
        }

        impl VhostUserHandleBackend for MockMaster {
            fn from_stream(_sock: UnixStream, _max_queue_num: u64) -> Self {
                Self {
                    features_are_set: std::cell::UnsafeCell::new(false),
                    memory_is_set: std::cell::UnsafeCell::new(false),
                    vring_enabled: std::cell::UnsafeCell::new(false),
                }
            }

            fn set_owner(&self) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_hdr_flags(&self, _flags: VhostUserHeaderFlag) {}

            fn get_features(&self) -> Result<u64, vhost::Error> {
                Ok(0)
            }

            fn get_protocol_features(&mut self) -> Result<VhostUserProtocolFeatures, vhost::Error> {
                Ok(VhostUserProtocolFeatures::empty())
            }

            fn set_protocol_features(
                &mut self,
                _features: VhostUserProtocolFeatures,
            ) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn get_config(
                &mut self,
                _offset: u32,
                _size: u32,
                _flags: VhostUserConfigFlags,
                _buf: &[u8],
            ) -> Result<(VhostUserConfig, VhostUserConfigPayload), vhost::Error> {
                Ok((VhostUserConfig::default(), vec![]))
            }

            fn set_features(&self, _features: u64) -> Result<(), vhost::Error> {
                unsafe { (*self.features_are_set.get()) = true };
                Ok(())
            }

            fn set_mem_table(
                &self,
                _regions: &[VhostUserMemoryRegionInfo],
            ) -> Result<(), vhost::Error> {
                unsafe { (*self.memory_is_set.get()) = true };
                Ok(())
            }

            fn set_vring_num(&self, _queue_index: usize, _num: u16) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_vring_addr(
                &self,
                _queue_index: usize,
                _config_data: &VringConfigData,
            ) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_vring_base(&self, _queue_index: usize, _base: u16) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_vring_call(
                &self,
                _queue_index: usize,
                _fd: &EventFd,
            ) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_vring_kick(
                &self,
                _queue_index: usize,
                _fd: &EventFd,
            ) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_vring_enable(
                &mut self,
                _queue_index: usize,
                _enable: bool,
            ) -> Result<(), vhost::Error> {
                unsafe { (*self.vring_enabled.get()) = true };
                Ok(())
            }
        }

        impl MutEventSubscriber for VhostUserBlockImpl<MockMaster> {
            fn process(&mut self, _: Events, _: &mut EventOps) {}
            fn init(&mut self, _: &mut EventOps) {}
        }

        // Block creation
        let (_tmp_dir, tmp_socket_path) = create_tmp_socket();
        let vhost_block_config = VhostUserBlockConfig {
            drive_id: "test_drive".to_string(),
            partuuid: None,
            is_root_device: false,
            cache_type: CacheType::Writeback,
            socket: tmp_socket_path,
        };
        let mut vhost_block = VhostUserBlockImpl::<MockMaster>::new(vhost_block_config).unwrap();

        // Memory creation
        let region_size = 0x10000;
        let file = TempFile::new().unwrap().into_file();
        file.set_len(region_size as u64).unwrap();
        let regions = vec![(GuestAddress(0x0), region_size)];
        let guest_memory = create_mem(file, &regions, libc::MAP_SHARED);
        let q = VirtQueue::new(GuestAddress(0), &guest_memory, 16);
        vhost_block.queues[0] = q.create_queue();
        let interrupt = default_interrupt();

        // During actiavion of the device features, memory and queues should be set and activated.
        vhost_block
            .activate(guest_memory, interrupt.clone())
            .unwrap();
        assert!(unsafe { *vhost_block.vu_handle.vu.features_are_set.get() });
        assert!(unsafe { *vhost_block.vu_handle.vu.memory_is_set.get() });
        assert!(unsafe { *vhost_block.vu_handle.vu.vring_enabled.get() });
        assert!(vhost_block.is_activated());
        // No config change is pending, so activation sends no config interrupt.
        assert!(!interrupt.has_pending_interrupt(VirtioInterruptType::Config));
    }

    /// A frontend for the config update tests. It has a socket to the backend and accepts
    /// the messages of the device activation.
    struct ConfigFrontend {
        sock: UnixStream,
    }

    impl VhostUserHandleBackend for ConfigFrontend {
        fn socket_fd(&self) -> RawFd {
            self.sock.as_raw_fd()
        }

        fn set_features(&self, _features: u64) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_mem_table(
            &self,
            _regions: &[VhostUserMemoryRegionInfo],
        ) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_vring_num(&self, _queue_index: usize, _num: u16) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_vring_addr(
            &self,
            _queue_index: usize,
            _config_data: &VringConfigData,
        ) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_vring_base(&self, _queue_index: usize, _base: u16) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_vring_call(&self, _queue_index: usize, _fd: &EventFd) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_vring_kick(&self, _queue_index: usize, _fd: &EventFd) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_vring_enable(
            &mut self,
            _queue_index: usize,
            _enable: bool,
        ) -> Result<(), vhost::Error> {
            Ok(())
        }
    }

    impl MutEventSubscriber for VhostUserBlockImpl<ConfigFrontend> {
        fn process(&mut self, _: Events, _: &mut EventOps) {}
        fn init(&mut self, _: &mut EventOps) {}
    }

    const CONFIG_SIZE: usize = BLOCK_CONFIG_SPACE_SIZE as usize;

    /// Make a device that is not active, has the CONFIG protocol feature, and uses `vu` to
    /// talk to the backend.
    fn config_test_device<T: VhostUserHandleBackend>(id: &str, vu: T) -> VhostUserBlockImpl<T> {
        VhostUserBlockImpl {
            avail_features: 0,
            acked_features: 0,
            config_space: vec![0; CONFIG_SIZE],
            activate_evt: EventFd::new(libc::EFD_NONBLOCK).unwrap(),
            queues: vec![Queue::new(QUEUE_SIZE)],
            queue_evts: std::array::from_fn(|_| EventFd::new(libc::EFD_NONBLOCK).unwrap()),
            device_state: DeviceState::Inactive,
            config_change_pending: false,
            config_update_evt: EventFd::new(libc::EFD_NONBLOCK).unwrap(),
            config_update: ConfigUpdate::Idle,
            config_reply_watched: false,
            id: id.to_string(),
            partuuid: None,
            cache_type: CacheType::Unsafe,
            root_device: false,
            read_only: false,
            vu_handle: VhostUserHandleImpl {
                vu,
                socket_path: String::new(),
            },
            vu_acked_protocol_features: VhostUserProtocolFeatures::CONFIG.bits(),
            metrics: VhostUserMetricsPerDevice::alloc(format!("block_{id}")),
        }
    }

    fn config_frontend_device(id: &str) -> (VhostUserBlockImpl<ConfigFrontend>, UnixStream) {
        let (sock, backend) = UnixStream::pair().unwrap();
        (config_test_device(id, ConfigFrontend { sock }), backend)
    }

    /// Make the device active with a new interrupt.
    fn activate_with_irq<T: VhostUserHandleBackend>(
        vhost_block: &mut VhostUserBlockImpl<T>,
    ) -> Arc<IrqTrigger> {
        let interrupt = Arc::new(IrqTrigger::new());
        vhost_block.device_state = DeviceState::Activated(ActiveState {
            mem: default_mem(),
            interrupt: interrupt.clone(),
        });
        interrupt
    }

    /// Read one `VHOST_USER_GET_CONFIG` request on the backend side of the socket.
    fn read_request(backend: &mut UnixStream) {
        let mut request = vec![0u8; get_config_request_len(CONFIG_SIZE)];
        backend.read_exact(&mut request).unwrap();
        assert_eq!(
            request[..4],
            u32::from(FrontendReq::GET_CONFIG).to_ne_bytes()
        );
    }

    /// Check that the backend has no more requests to read.
    fn assert_no_request(backend: &UnixStream) {
        backend.set_nonblocking(true).unwrap();
        let err = (&*backend).read(&mut [0u8; 1]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        backend.set_nonblocking(false).unwrap();
    }

    /// Check that the interrupt was sent exactly once, as a config interrupt, and clear it.
    fn assert_config_interrupt(interrupt: &IrqTrigger) {
        assert_eq!(interrupt.irq_evt.read().unwrap(), 1);
        assert_eq!(
            interrupt.irq_status.swap(0, Ordering::SeqCst),
            VIRTIO_MMIO_INT_CONFIG
        );
    }

    fn assert_no_interrupt(interrupt: &IrqTrigger) {
        assert_eq!(
            interrupt.irq_evt.read().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn test_config_update_does_not_wait() {
        let (mut vhost_block, mut backend) = config_frontend_device("config_update_does_not_wait");
        let interrupt = activate_with_irq(&mut vhost_block);

        // The update returns before the backend replies.
        vhost_block.config_update().unwrap();
        assert!(vhost_block.config_reply_expected());
        assert_eq!(vhost_block.config_update_evt.read().unwrap(), 1);
        read_request(&mut backend);
        vhost_block.process_config_reply();
        assert!(vhost_block.config_reply_expected());
        assert_eq!(vhost_block.config_space, vec![0; CONFIG_SIZE]);
        assert_no_interrupt(&interrupt);

        // The reply can arrive in parts.
        let reply = get_config_reply(&[0x11; CONFIG_SIZE]);
        backend.write_all(&reply[..5]).unwrap();
        vhost_block.process_config_reply();
        backend.write_all(&reply[5..30]).unwrap();
        vhost_block.process_config_reply();
        assert!(vhost_block.config_reply_expected());
        assert_eq!(vhost_block.config_space, vec![0; CONFIG_SIZE]);
        assert_no_interrupt(&interrupt);

        // The complete reply updates the config and notifies the guest driver.
        backend.write_all(&reply[30..]).unwrap();
        vhost_block.process_config_reply();
        assert!(!vhost_block.config_reply_expected());
        assert_eq!(vhost_block.config_space, vec![0x11; CONFIG_SIZE]);
        assert_config_interrupt(&interrupt);
        assert_eq!(vhost_block.metrics.cfg_fails.count(), 0);
        assert_no_request(&backend);

        // A call with no request in progress does nothing.
        vhost_block.process_config_reply();
        assert!(!vhost_block.config_reply_expected());
    }

    #[test]
    fn test_config_update_coalesces_requests() {
        let (mut vhost_block, mut backend) = config_frontend_device("config_update_coalesces");
        let interrupt = activate_with_irq(&mut vhost_block);

        // Updates that come while a request waits for its reply send no request now.
        vhost_block.config_update().unwrap();
        vhost_block.config_update().unwrap();
        vhost_block.config_update().unwrap();
        read_request(&mut backend);
        assert_no_request(&backend);

        // After the reply, the device sends one more request.
        backend
            .write_all(&get_config_reply(&[0x21; CONFIG_SIZE]))
            .unwrap();
        vhost_block.process_config_reply();
        assert_eq!(vhost_block.config_space, vec![0x21; CONFIG_SIZE]);
        assert_config_interrupt(&interrupt);
        assert!(vhost_block.config_reply_expected());
        read_request(&mut backend);
        assert_no_request(&backend);

        backend
            .write_all(&get_config_reply(&[0x22; CONFIG_SIZE]))
            .unwrap();
        vhost_block.process_config_reply();
        assert_eq!(vhost_block.config_space, vec![0x22; CONFIG_SIZE]);
        assert_config_interrupt(&interrupt);
        assert!(!vhost_block.config_reply_expected());
        assert_no_request(&backend);
    }

    #[test]
    fn test_config_update_before_activation() {
        let (mut vhost_block, mut backend) =
            config_frontend_device("config_update_before_activation");
        let region_size = 0x10000;
        let file = TempFile::new().unwrap().into_file();
        file.set_len(region_size as u64).unwrap();
        let regions = vec![(GuestAddress(0x0), region_size)];
        let guest_memory = create_mem(file, &regions, libc::MAP_SHARED);
        let q = VirtQueue::new(GuestAddress(0), &guest_memory, 16);
        vhost_block.queues[0] = q.create_queue();

        // A reply that comes before activation updates the config. The device sends the
        // config interrupt when the guest driver activates it.
        vhost_block.config_update().unwrap();
        read_request(&mut backend);
        backend
            .write_all(&get_config_reply(&[0x31; CONFIG_SIZE]))
            .unwrap();
        vhost_block.process_config_reply();
        assert_eq!(vhost_block.config_space, vec![0x31; CONFIG_SIZE]);
        assert!(vhost_block.config_change_pending);

        // A request that is in progress at activation sends its interrupt with the reply.
        vhost_block.config_update().unwrap();
        read_request(&mut backend);

        let interrupt = Arc::new(IrqTrigger::new());
        vhost_block
            .activate(guest_memory, interrupt.clone())
            .unwrap();
        assert!(!vhost_block.config_change_pending);
        assert_config_interrupt(&interrupt);

        backend
            .write_all(&get_config_reply(&[0x32; CONFIG_SIZE]))
            .unwrap();
        vhost_block.process_config_reply();
        assert_eq!(vhost_block.config_space, vec![0x32; CONFIG_SIZE]);
        assert!(!vhost_block.config_change_pending);
        assert_config_interrupt(&interrupt);
    }

    #[test]
    fn test_config_update_errors() {
        // The backend does not support config updates.
        let (mut vhost_block, _backend) = config_frontend_device("config_update_errors_0");
        vhost_block.vu_acked_protocol_features = 0;
        assert!(matches!(
            vhost_block.config_update(),
            Err(VhostUserBlockError::ConfigNotSupported)
        ));

        // A backend error or a reply for another config region fails only this update.
        let (mut vhost_block, mut backend) = config_frontend_device("config_update_errors_1");
        let interrupt = activate_with_irq(&mut vhost_block);
        vhost_block.config_update().unwrap();
        vhost_block.config_update().unwrap();
        read_request(&mut backend);
        backend
            .write_all(&header(
                u32::from(FrontendReq::GET_CONFIG),
                0x1 | VhostUserHeaderFlag::REPLY.bits(),
                0,
            ))
            .unwrap();
        vhost_block.process_config_reply();
        assert_eq!(vhost_block.metrics.cfg_fails.count(), 1);
        assert_no_interrupt(&interrupt);
        // The queued update sends its request.
        read_request(&mut backend);
        let mut wrong_size = get_config_reply(&[0x41; CONFIG_SIZE]);
        wrong_size[16..20].copy_from_slice(&1u32.to_ne_bytes());
        backend.write_all(&wrong_size).unwrap();
        vhost_block.process_config_reply();
        assert_eq!(vhost_block.metrics.cfg_fails.count(), 2);
        assert!(!vhost_block.config_reply_expected());
        assert_eq!(vhost_block.config_space, vec![0; CONFIG_SIZE]);
        assert_no_interrupt(&interrupt);
        // The socket is still usable.
        vhost_block.config_update().unwrap();
        read_request(&mut backend);
        backend
            .write_all(&get_config_reply(&[0x42; CONFIG_SIZE]))
            .unwrap();
        vhost_block.process_config_reply();
        assert_eq!(vhost_block.config_space, vec![0x42; CONFIG_SIZE]);
        assert_config_interrupt(&interrupt);

        // An invalid reply header stops config updates: the start of the next reply is not
        // known.
        vhost_block.config_update().unwrap();
        read_request(&mut backend);
        backend
            .write_all(&header(u32::from(FrontendReq::GET_FEATURES), 0x5, 8))
            .unwrap();
        vhost_block.process_config_reply();
        assert!(matches!(vhost_block.config_update, ConfigUpdate::Failed));
        assert_eq!(vhost_block.metrics.cfg_fails.count(), 3);
        assert!(matches!(
            vhost_block.config_update(),
            Err(VhostUserBlockError::ConfigSocketFailed)
        ));
        assert_eq!(vhost_block.config_space, vec![0x42; CONFIG_SIZE]);

        // The backend closes the socket while a request waits for its reply.
        let (mut vhost_block, mut backend) = config_frontend_device("config_update_errors_2");
        vhost_block.config_update().unwrap();
        vhost_block.config_update().unwrap();
        read_request(&mut backend);
        backend
            .write_all(&get_config_reply(&[0x51; CONFIG_SIZE])[..10])
            .unwrap();
        drop(backend);
        vhost_block.process_config_reply();
        assert!(matches!(vhost_block.config_update, ConfigUpdate::Failed));
        assert_eq!(vhost_block.metrics.cfg_fails.count(), 1);
        assert!(!vhost_block.config_change_pending);

        // The backend is gone before the request.
        let (mut vhost_block, backend) = config_frontend_device("config_update_errors_3");
        drop(backend);
        assert!(matches!(
            vhost_block.config_update(),
            Err(VhostUserBlockError::ConfigRequest(_))
        ));
        assert!(matches!(vhost_block.config_update, ConfigUpdate::Failed));
        assert_eq!(vhost_block.metrics.cfg_fails.count(), 1);

        // The socket buffer is full: the request is not sent, and the socket stays usable.
        let (mut vhost_block, mut backend) = config_frontend_device("config_update_errors_4");
        let chunk = [0u8; 4096];
        let mut sent = 0;
        loop {
            // SAFETY: `chunk` is a valid buffer. The return value is checked.
            let ret = unsafe {
                libc::send(
                    vhost_block.vu_handle.vu.sock.as_raw_fd(),
                    chunk.as_ptr().cast(),
                    chunk.len(),
                    libc::MSG_DONTWAIT,
                )
            };
            match usize::try_from(ret) {
                Ok(count) => sent += count,
                Err(_) => break,
            }
        }
        match vhost_block.config_update() {
            Err(VhostUserBlockError::ConfigRequest(err)) => {
                assert_eq!(err.kind(), io::ErrorKind::WouldBlock)
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert!(matches!(vhost_block.config_update, ConfigUpdate::Idle));
        assert_eq!(vhost_block.metrics.cfg_fails.count(), 1);
        let mut filler = vec![0u8; sent];
        backend.read_exact(&mut filler).unwrap();
        vhost_block.config_update().unwrap();
        read_request(&mut backend);
    }

    #[test]
    fn test_config_update_event_handler() {
        let (sock, mut backend) = UnixStream::pair().unwrap();
        let vhost_block = config_test_device(
            "config_update_event_handler",
            Frontend::from_stream(sock, NUM_QUEUES),
        );
        let vhost_block = Arc::new(Mutex::new(vhost_block));
        let mut event_manager = EventManager::new().unwrap();
        let _id = event_manager.add_subscriber(vhost_block.clone());
        let interrupt = activate_with_irq(&mut *vhost_block.lock().unwrap());

        // The device does not watch the socket while no request is in progress.
        backend.write_all(&[0]).unwrap();
        assert_eq!(event_manager.run_with_timeout(0).unwrap(), 0);
        let mut byte = [0u8; 1];
        // SAFETY: `byte` is a valid buffer. The return value is checked.
        let ret = unsafe {
            libc::recv(
                vhost_block.lock().unwrap().vu_handle.vu.socket_fd(),
                byte.as_mut_ptr().cast(),
                1,
                libc::MSG_DONTWAIT,
            )
        };
        assert_eq!(ret, 1);

        // The config update event makes the event loop watch the socket.
        vhost_block.lock().unwrap().config_update().unwrap();
        assert_eq!(event_manager.run_with_timeout(0).unwrap(), 1);
        assert!(vhost_block.lock().unwrap().config_reply_watched);
        assert_eq!(event_manager.run_with_timeout(0).unwrap(), 0);

        // The event loop reads the reply and stops to watch the socket.
        read_request(&mut backend);
        backend
            .write_all(&get_config_reply(&[0x61; CONFIG_SIZE]))
            .unwrap();
        assert_eq!(event_manager.run_with_timeout(0).unwrap(), 1);
        {
            let vhost_block = vhost_block.lock().unwrap();
            assert_eq!(vhost_block.config_space, vec![0x61; CONFIG_SIZE]);
            assert!(!vhost_block.config_reply_watched);
            assert!(!vhost_block.config_reply_expected());
        }
        assert_config_interrupt(&interrupt);
        backend.write_all(&[0]).unwrap();
        assert_eq!(event_manager.run_with_timeout(0).unwrap(), 0);

        // The reply can come before the guest driver activates the device.
        vhost_block.lock().unwrap().device_state = DeviceState::Inactive;
        // SAFETY: `byte` is a valid buffer. The return value is checked.
        let ret = unsafe {
            libc::recv(
                vhost_block.lock().unwrap().vu_handle.vu.socket_fd(),
                byte.as_mut_ptr().cast(),
                1,
                libc::MSG_DONTWAIT,
            )
        };
        assert_eq!(ret, 1);
        vhost_block.lock().unwrap().config_update().unwrap();
        assert_eq!(event_manager.run_with_timeout(0).unwrap(), 1);
        read_request(&mut backend);
        backend
            .write_all(&get_config_reply(&[0x62; CONFIG_SIZE]))
            .unwrap();
        assert_eq!(event_manager.run_with_timeout(0).unwrap(), 1);
        assert!(vhost_block.lock().unwrap().config_change_pending);
        assert_eq!(
            vhost_block.lock().unwrap().config_space,
            vec![0x62; CONFIG_SIZE]
        );

        // When the backend closes the socket, the event loop stops to watch it.
        vhost_block.lock().unwrap().config_update().unwrap();
        assert_eq!(event_manager.run_with_timeout(0).unwrap(), 1);
        drop(backend);
        assert_eq!(event_manager.run_with_timeout(0).unwrap(), 1);
        {
            let vhost_block = vhost_block.lock().unwrap();
            assert!(matches!(vhost_block.config_update, ConfigUpdate::Failed));
            assert!(!vhost_block.config_reply_watched);
            assert_eq!(vhost_block.metrics.cfg_fails.count(), 1);
        }
        assert_eq!(event_manager.run_with_timeout(0).unwrap(), 0);
    }
}
