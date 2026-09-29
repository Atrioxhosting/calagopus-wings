use super::state::ServerState;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(ToSchema, Default, Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BandwidthState {
    #[default]
    Active,
    QuotaExceeded,
    Stopping,
    StoppedQuota,
    RestorePending,
    StopFailed,
    EnforcementUnknown,
}

#[derive(ToSchema, Default, Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct BandwidthUsage {
    pub used_bytes: u64,
    pub quota_bytes: Option<u64>,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub period_start: i64,
    pub period_end: i64,
    pub bandwidth_per_gib: i64,
    pub state: BandwidthState,
    pub resume_after_quota: bool,
    pub generation: u64,
    pub last_checkpoint: i64,
}

nestify::nest! {
    #[derive(ToSchema, Default, Deserialize, Serialize, Debug, Clone, Copy, PartialEq)]
    pub struct ResourceUsage {
        pub memory_bytes: u64,
        pub memory_limit_bytes: u64,
        pub disk_bytes: u64,
        pub bandwidth: BandwidthUsage,

        pub state: ServerState,

        #[schema(inline)]
        pub network: #[derive(ToSchema, Default, Deserialize, Serialize, Debug, Clone, Copy, PartialEq)] pub struct ResourceUsageNetwork {
            pub rx_bytes: u64,
            pub rx_packets: u64,
            pub tx_bytes: u64,
            pub tx_packets: u64,
        },

        pub cpu_absolute: f64,
        pub cpu_limit_absolute: u32,

        pub uptime: u64,
    }
}

impl ResourceUsage {
    /// Resets all metrics tied to a live container, keeping only disk usage.
    pub fn wipe(&mut self, state: ServerState) {
        *self = Self {
            disk_bytes: self.disk_bytes,
            state,
            ..Default::default()
        };
    }
}

pub trait ResourceUsageWatchExt {
    fn publish_disk_usage(&self, disk_bytes: u64);
    /// Wipes all container-bound metrics, keeping only disk usage.
    fn wipe(&self, state: ServerState);
}

impl ResourceUsageWatchExt for tokio::sync::watch::Sender<ResourceUsage> {
    fn publish_disk_usage(&self, disk_bytes: u64) {
        self.send_if_modified(|usage| {
            if usage.disk_bytes == disk_bytes {
                return false;
            }

            usage.disk_bytes = disk_bytes;
            true
        });
    }

    fn wipe(&self, state: ServerState) {
        self.send_modify(|usage| usage.wipe(state));
    }
}
