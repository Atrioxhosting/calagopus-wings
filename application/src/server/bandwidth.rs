use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

const CHECKPOINT_BYTES: u64 = 1_000_000_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Correction {
    pub id: uuid::Uuid,
    pub actor: String,
    pub delta_bytes: i64,
    pub at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LedgerState {
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    pub period_id: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub used_bytes: u64,
    #[serde(default)]
    pub quota_bytes: Option<u64>,
    pub state: String,
    pub resume_after_quota: bool,
    pub generation: u64,
    #[serde(default)]
    pub fenced: bool,
    pub last_checkpoint: DateTime<Utc>,
    #[serde(default)]
    pub corrections: Vec<Correction>,
    #[serde(default)]
    pub container_rx: u64,
    #[serde(default)]
    pub container_tx: u64,
    #[serde(default)]
    pub tundra_rx: Option<u64>,
    #[serde(default)]
    pub tundra_tx: Option<u64>,
}

pub struct Ledger {
    path: PathBuf,
    state: parking_lot::Mutex<LedgerState>,
    checkpoint_usage: parking_lot::Mutex<u64>,
    checkpoint_failed: AtomicBool,
}

pub fn quota_bytes(memory_mib: i64, bandwidth_per_gib: i64) -> Option<u64> {
    if memory_mib <= 0 || bandwidth_per_gib <= 0 {
        return None;
    }
    let bytes = (memory_mib as u128)
        .saturating_mul(bandwidth_per_gib as u128)
        .saturating_mul(1_000_000_000)
        / 1024;
    Some(bytes.min(u64::MAX as u128) as u64)
}

fn boundary(year: i32, month: u32, day: u32, anchor: DateTime<Utc>) -> DateTime<Utc> {
    let next = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
    };
    let last_day = next.pred_opt().unwrap().day();
    NaiveDate::from_ymd_opt(year, month, day.min(last_day))
        .unwrap()
        .and_hms_nano_opt(
            anchor.hour(),
            anchor.minute(),
            anchor.second(),
            anchor.nanosecond(),
        )
        .unwrap()
        .and_utc()
}

pub fn created_period(
    created: DateTime<Utc>,
    now: DateTime<Utc>,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let (year, month) = if boundary(now.year(), now.month(), created.day(), created) > now {
        if now.month() == 1 {
            (now.year() - 1, 12)
        } else {
            (now.year(), now.month() - 1)
        }
    } else {
        (now.year(), now.month())
    };
    let start = boundary(year, month, created.day(), created);
    let end = if month == 12 {
        boundary(year + 1, 1, created.day(), created)
    } else {
        boundary(year, month + 1, created.day(), created)
    };
    (start, end)
}

impl Ledger {
    pub fn new(
        path: PathBuf,
        created: DateTime<Utc>,
        authoritative: Option<&super::configuration::ServerConfigurationBillingPeriod>,
    ) -> Self {
        let (period_start, period_end, period_id) = Self::period_values(created, authoritative);
        let loaded = fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<LedgerState>(&bytes).ok());
        let fresh = loaded.is_none();
        let state = loaded.unwrap_or_else(|| LedgerState {
            period_start,
            period_end,
            period_id,
            rx_bytes: 0,
            tx_bytes: 0,
            used_bytes: 0,
            quota_bytes: None,
            state: "active".into(),
            resume_after_quota: false,
            generation: 0,
            fenced: false,
            last_checkpoint: Utc::now(),
            corrections: Vec::new(),
            container_rx: 0,
            container_tx: 0,
            tundra_rx: Some(0),
            tundra_tx: Some(0),
        });
        let interrupted_stop = state.state == "stopping";
        let mut state = state;
        if interrupted_stop {
            state.state = "enforcement_unknown".into();
        }
        let checkpoint_usage = state.used_bytes;
        let ledger = Self {
            path,
            state: parking_lot::Mutex::new(state),
            checkpoint_usage: parking_lot::Mutex::new(checkpoint_usage),
            checkpoint_failed: AtomicBool::new(false),
        };
        if fresh || interrupted_stop {
            if let Err(err) = ledger.checkpoint() {
                tracing::error!(error = %err, "CRITICAL: failed to persist bandwidth ledger state on startup");
            }
        }
        ledger
    }

    pub fn snapshot(&self) -> LedgerState {
        self.state.lock().clone()
    }

    fn period_values(
        created: DateTime<Utc>,
        authoritative: Option<&super::configuration::ServerConfigurationBillingPeriod>,
    ) -> (DateTime<Utc>, DateTime<Utc>, String) {
        if let Some(period) = authoritative {
            return (period.start, period.end, period.id.to_string());
        }
        let (start, end) = created_period(created, Utc::now());
        (start, end, start.to_rfc3339())
    }

    pub fn blocked(&self) -> bool {
        let state = self.state.lock();
        state.fenced
            || matches!(
                state.state.as_str(),
                "quota_exceeded"
                    | "stopping"
                    | "stopped_quota"
                    | "stop_failed"
                    | "enforcement_unknown"
            )
    }

    fn persist(&self, state: &mut LedgerState) -> Result<(), anyhow::Error> {
        let mut durable = state.clone();
        durable.last_checkpoint = Utc::now();
        let result = (|| -> Result<(), anyhow::Error> {
            let parent = self
                .path
                .parent()
                .ok_or_else(|| anyhow::anyhow!("ledger path has no parent"))?;
            fs::create_dir_all(parent)?;
            let mut temp = tempfile::NamedTempFile::new_in(parent)?;
            serde_json::to_writer(&mut temp, &durable)?;
            temp.flush()?;
            temp.as_file().sync_all()?;
            temp.persist(&self.path)?;
            fs::File::open(parent)?.sync_all()?;
            Ok(())
        })();
        self.checkpoint_failed
            .store(result.is_err(), Ordering::Relaxed);
        if result.is_ok() {
            state.last_checkpoint = durable.last_checkpoint;
            *self.checkpoint_usage.lock() = state.used_bytes;
        }
        result
    }

    pub fn checkpoint(&self) -> Result<(), anyhow::Error> {
        self.persist(&mut self.state.lock())
    }

    pub fn retry_failed_checkpoint(&self) -> Result<(), anyhow::Error> {
        if self.checkpoint_failed.load(Ordering::Relaxed) {
            self.checkpoint()?;
        }
        Ok(())
    }

    pub fn period(
        &self,
        created: DateTime<Utc>,
        authoritative: Option<&super::configuration::ServerConfigurationBillingPeriod>,
    ) -> Result<bool, anyhow::Error> {
        let (start, end, id) = Self::period_values(created, authoritative);
        let mut state = self.state.lock();
        if state.period_id == id {
            return Ok(false);
        }
        let previous_was_derived = state.period_id == state.period_start.to_rfc3339();
        if !previous_was_derived
            && (start < state.period_start
                || (start == state.period_start && state.period_id != id))
        {
            anyhow::bail!("billing period is older than or conflicts with the current ledger");
        }
        let old = state.clone();
        let archive = self.path.with_file_name(format!(
            "{}-{}-{}.json",
            self.path.file_stem().unwrap().to_string_lossy(),
            old.period_start.format("%Y%m%d%H%M%S"),
            uuid::Uuid::new_v4(),
        ));
        fs::create_dir_all(archive.parent().unwrap())?;
        let mut archived = tempfile::NamedTempFile::new_in(archive.parent().unwrap())?;
        serde_json::to_writer(&mut archived, &old)?;
        archived.as_file().sync_all()?;
        archived.persist(archive)?;
        fs::File::open(self.path.parent().unwrap())?.sync_all()?;
        state.period_start = start;
        state.period_end = end;
        state.period_id = id;
        state.rx_bytes = 0;
        state.tx_bytes = 0;
        state.used_bytes = 0;
        // The container may survive a billing-period change. Its interface counters
        // remain cumulative, so retain the baseline instead of charging old bytes again.
        let restore_pending = state.resume_after_quota
            && (self.blocked_state(&state) || state.state == "restore_pending");
        state.state = if restore_pending {
            "restore_pending"
        } else {
            "active"
        }
        .into();
        state.resume_after_quota = restore_pending;
        state.corrections.clear();
        self.persist(&mut state)?;
        Ok(true)
    }

    pub fn observe_container(&self, rx: u64, tx: u64) {
        let mut state = self.state.lock();
        if state.fenced {
            return;
        }
        // ResourceUsage starts at zero before Docker publishes its first stats
        // sample. Treating that placeholder as a recreated container would reset
        // the persisted baseline and book the existing interface bytes twice.
        if rx == 0 && tx == 0 && (state.container_rx != 0 || state.container_tx != 0) {
            return;
        }
        // Docker starts fresh interface counters when a container is recreated.
        let rx_delta = if rx < state.container_rx {
            rx
        } else {
            rx - state.container_rx
        };
        let tx_delta = if tx < state.container_tx {
            tx
        } else {
            tx - state.container_tx
        };
        state.container_rx = rx;
        state.container_tx = tx;
        state.rx_bytes = state.rx_bytes.saturating_add(rx_delta);
        state.tx_bytes = state.tx_bytes.saturating_add(tx_delta);
        state.used_bytes = state
            .used_bytes
            .saturating_add(rx_delta)
            .saturating_add(tx_delta);
        if state
            .used_bytes
            .saturating_sub(*self.checkpoint_usage.lock())
            >= CHECKPOINT_BYTES
        {
            if let Err(err) = self.persist(&mut state) {
                tracing::error!(error = %err, "CRITICAL: bandwidth ledger checkpoint failed");
            }
        }
    }

    /// Records transferred bytes and reports whether the current quota is reached.
    pub fn record_stream(&self, rx: u64, tx: u64) -> bool {
        let mut state = self.state.lock();
        if state.fenced {
            return false;
        }
        state.rx_bytes = state.rx_bytes.saturating_add(rx);
        state.tx_bytes = state.tx_bytes.saturating_add(tx);
        state.used_bytes = state.used_bytes.saturating_add(rx).saturating_add(tx);
        if state
            .used_bytes
            .saturating_sub(*self.checkpoint_usage.lock())
            >= CHECKPOINT_BYTES
        {
            if let Err(err) = self.persist(&mut state) {
                tracing::error!(error = %err, "CRITICAL: bandwidth stream checkpoint failed");
            }
        }
        state
            .quota_bytes
            .is_some_and(|quota| state.used_bytes >= quota)
    }

    pub fn observe_tundra(&self, rx: u64, tx: u64) {
        let mut state = self.state.lock();
        if state.fenced {
            return;
        }
        let rx_delta = state
            .tundra_rx
            .map_or(0, |old| if rx < old { rx } else { rx - old });
        let tx_delta = state
            .tundra_tx
            .map_or(0, |old| if tx < old { tx } else { tx - old });
        state.tundra_rx = Some(rx);
        state.tundra_tx = Some(tx);
        state.rx_bytes = state.rx_bytes.saturating_add(rx_delta);
        state.tx_bytes = state.tx_bytes.saturating_add(tx_delta);
        state.used_bytes = state
            .used_bytes
            .saturating_add(rx_delta)
            .saturating_add(tx_delta);
        if state
            .used_bytes
            .saturating_sub(*self.checkpoint_usage.lock())
            >= CHECKPOINT_BYTES
        {
            if let Err(err) = self.persist(&mut state) {
                tracing::error!(error = %err, "CRITICAL: bandwidth Tundra checkpoint failed");
            }
        }
    }

    /// Returns true when the caller must stop the normal service.
    pub fn evaluate(&self, quota: Option<u64>, running: bool) -> bool {
        let mut state = self.state.lock();
        state.quota_bytes = quota;
        let exceeded = quota.is_some_and(|limit| state.used_bytes >= limit);
        if exceeded {
            if state.state == "active" || state.state == "restore_pending" {
                state.resume_after_quota = running;
                state.state = "quota_exceeded".into();
                if let Err(err) = self.persist(&mut state) {
                    tracing::error!(error = %err, "CRITICAL: failed to persist bandwidth block");
                }
            }
            if running && state.state == "stopped_quota" {
                state.state = "stop_failed".into();
                if let Err(err) = self.persist(&mut state) {
                    tracing::error!(error = %err, "CRITICAL: failed to persist bandwidth retry state");
                }
            }
            return running;
        }
        if self.blocked_state(&state) {
            state.state = if state.resume_after_quota {
                "restore_pending"
            } else {
                "active"
            }
            .into();
            if let Err(err) = self.persist(&mut state) {
                tracing::error!(error = %err, "CRITICAL: failed to persist bandwidth release");
            }
        }
        false
    }

    fn blocked_state(&self, state: &LedgerState) -> bool {
        matches!(
            state.state.as_str(),
            "quota_exceeded" | "stopping" | "stopped_quota" | "stop_failed" | "enforcement_unknown"
        )
    }

    pub fn stopped(&self, success: bool) {
        let mut state = self.state.lock();
        // Quota may have been released while the stop was still in progress.
        if !self.blocked_state(&state) {
            return;
        }
        state.state = if success {
            "stopped_quota"
        } else {
            "stop_failed"
        }
        .into();
        if let Err(err) = self.persist(&mut state) {
            tracing::error!(error = %err, "CRITICAL: failed to persist bandwidth stop state");
        }
    }

    pub fn enforcement_unknown(&self) {
        let mut state = self.state.lock();
        if !self.blocked_state(&state) {
            return;
        }
        state.state = "enforcement_unknown".into();
        if let Err(err) = self.persist(&mut state) {
            tracing::error!(error = %err, "CRITICAL: failed to persist unknown bandwidth enforcement state");
        }
    }

    pub fn mark_stopping(&self) -> bool {
        let mut state = self.state.lock();
        if !matches!(
            state.state.as_str(),
            "quota_exceeded" | "stop_failed" | "enforcement_unknown"
        ) {
            return false;
        }
        state.state = "stopping".into();
        if let Err(err) = self.persist(&mut state) {
            tracing::error!(error = %err, "CRITICAL: failed to persist bandwidth stopping state");
        }
        true
    }

    pub fn started(&self) {
        let mut state = self.state.lock();
        if state.state == "restore_pending" {
            state.state = "active".into();
            state.resume_after_quota = false;
            if let Err(err) = self.persist(&mut state) {
                tracing::error!(error = %err, "CRITICAL: failed to persist bandwidth restore");
            }
        }
    }

    pub fn disable_resume(&self) {
        let mut state = self.state.lock();
        if self.blocked_state(&state) && state.resume_after_quota {
            state.resume_after_quota = false;
            if let Err(err) = self.persist(&mut state) {
                tracing::error!(error = %err, "CRITICAL: failed to persist disabled bandwidth resume");
            }
        }
    }

    pub fn correct(
        &self,
        id: uuid::Uuid,
        actor: String,
        delta_bytes: i64,
        generation: u64,
    ) -> Result<LedgerState, anyhow::Error> {
        let mut state = self.state.lock();
        if state.fenced {
            anyhow::bail!("bandwidth ledger owner is fenced");
        }
        if state.generation != generation {
            anyhow::bail!("bandwidth owner generation changed");
        }
        if let Some(entry) = state.corrections.iter().find(|entry| entry.id == id) {
            if entry.delta_bytes != delta_bytes || entry.actor != actor {
                anyhow::bail!("bandwidth correction identifier conflicts with an existing entry");
            }
            return Ok(state.clone());
        }
        let corrected = i128::from(state.used_bytes) + i128::from(delta_bytes);
        if corrected < 0 {
            anyhow::bail!("The amount to subtract cannot exceed the current bandwidth usage.");
        }
        if !(0..=i128::from(u64::MAX)).contains(&corrected) {
            anyhow::bail!("correction would put usage outside the valid byte range");
        }
        state.used_bytes = corrected as u64;
        state.corrections.push(Correction {
            id,
            actor,
            delta_bytes,
            at: Utc::now(),
        });
        self.persist(&mut state)?;
        Ok(state.clone())
    }

    pub fn freeze(&self, expected_generation: u64) -> Result<LedgerState, anyhow::Error> {
        let mut state = self.state.lock();
        if state.generation != expected_generation {
            anyhow::bail!("bandwidth owner generation changed");
        }
        state.fenced = true;
        self.persist(&mut state)?;
        Ok(state.clone())
    }

    pub fn unfreeze(&self, expected_generation: u64) -> Result<(), anyhow::Error> {
        let mut state = self.state.lock();
        if state.generation != expected_generation {
            anyhow::bail!("bandwidth owner generation changed");
        }
        state.fenced = false;
        self.persist(&mut state)
    }

    pub fn accept(
        &self,
        mut incoming: LedgerState,
        expected_generation: u64,
        tundra_baseline: Option<(u64, u64)>,
    ) -> Result<(), anyhow::Error> {
        let mut state = self.state.lock();
        if incoming.generation != expected_generation || !incoming.fenced {
            anyhow::bail!("invalid bandwidth handoff generation");
        }
        if state.generation == expected_generation.saturating_add(1)
            && !state.fenced
            && state.period_id == incoming.period_id
            && state.used_bytes == incoming.used_bytes
            && state.rx_bytes == incoming.rx_bytes
            && state.tx_bytes == incoming.tx_bytes
        {
            return Ok(());
        }
        if state.generation >= expected_generation.saturating_add(1) {
            anyhow::bail!("bandwidth destination already has a different owner generation");
        }
        incoming.generation = expected_generation.saturating_add(1);
        incoming.fenced = false;
        incoming.container_rx = 0;
        incoming.container_tx = 0;
        incoming.tundra_rx = tundra_baseline.map(|(rx, _)| rx);
        incoming.tundra_tx = tundra_baseline.map(|(_, tx)| tx);
        self.persist(&mut incoming)?;
        *state = incoming;
        Ok(())
    }

    pub fn abort_import(&self, generation: u64) -> Result<(), anyhow::Error> {
        let mut state = self.state.lock();
        if state.generation != generation {
            anyhow::bail!("bandwidth owner generation changed");
        }
        state.fenced = true;
        self.persist(&mut state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_uses_decimal_gb_and_fractional_gib() {
        assert_eq!(quota_bytes(256, 200), Some(50_000_000_000));
        assert_eq!(quota_bytes(1536, 200), Some(300_000_000_000));
        assert_eq!(quota_bytes(0, 200), None);
        assert_eq!(quota_bytes(1024, 0), None);
    }

    #[test]
    fn recreated_container_and_tundra_restart_continue_accounting() {
        let directory = tempfile::tempdir().unwrap();
        let created = "2025-01-31T12:00:00Z".parse().unwrap();
        let ledger = Ledger::new(directory.path().join("service.json"), created, None);
        ledger.observe_container(100, 200);
        ledger.observe_container(5, 8);
        ledger.observe_tundra(100, 200);
        ledger.observe_tundra(5, 8);
        assert_eq!(ledger.snapshot().used_bytes, 626);
        assert_eq!(ledger.snapshot().rx_bytes, 210);
        assert_eq!(ledger.snapshot().tx_bytes, 416);
    }

    #[test]
    fn first_uninitialized_docker_sample_does_not_rebook_persisted_usage() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("service.json");
        let created = "2025-01-31T12:00:00Z".parse().unwrap();
        let ledger = Ledger::new(path.clone(), created, None);
        ledger.observe_container(100, 200);
        ledger.checkpoint().unwrap();
        drop(ledger);

        let ledger = Ledger::new(path, created, None);
        ledger.observe_container(0, 0); // No Docker sample yet.
        ledger.observe_container(100, 200); // First real sample, same container.
        assert_eq!(ledger.snapshot().used_bytes, 300);
        ledger.observe_container(5, 8); // Real counter reset after recreation.
        assert_eq!(ledger.snapshot().used_bytes, 313);
    }

    #[test]
    fn failed_checkpoint_retries_full_ram_state_after_storage_returns() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("blocked");
        std::fs::write(&parent, b"not a directory").unwrap();
        let path = parent.join("service.json");
        let created = "2025-01-31T12:00:00Z".parse().unwrap();
        let ledger = Ledger::new(path.clone(), created, None);
        ledger.record_stream(5, 7);
        assert!(!path.exists());

        std::fs::remove_file(&parent).unwrap();
        std::fs::create_dir(&parent).unwrap();
        ledger.retry_failed_checkpoint().unwrap();
        let recovered: LedgerState = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(recovered.used_bytes, 12);
        assert_eq!(recovered.rx_bytes, 5);
        assert_eq!(recovered.tx_bytes, 7);
    }

    #[test]
    fn created_anchor_clamps_without_drifting() {
        let created = "2025-01-31T12:00:00Z".parse().unwrap();
        let feb = "2025-02-28T13:00:00Z".parse().unwrap();
        let (start, end) = created_period(created, feb);
        assert_eq!(start.to_rfc3339(), "2025-02-28T12:00:00+00:00");
        assert_eq!(end.to_rfc3339(), "2025-03-31T12:00:00+00:00");
    }

    #[test]
    fn quota_block_and_correction_survive_restart_without_double_booking() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("service.json");
        let created = "2025-01-31T12:00:00Z".parse().unwrap();
        let ledger = Ledger::new(path.clone(), created, None);
        assert!(path.exists());
        ledger.record_stream(10, 20);
        assert!(ledger.evaluate(Some(30), true));
        assert!(ledger.mark_stopping());
        ledger.stopped(true);
        drop(ledger);

        let ledger = Ledger::new(path, created, None);
        assert_eq!(ledger.snapshot().used_bytes, 30);
        assert_eq!(ledger.snapshot().state, "stopped_quota");
        let id = uuid::Uuid::new_v4();
        ledger.correct(id, "admin".into(), -6, 0).unwrap();
        ledger.correct(id, "admin".into(), -6, 0).unwrap();
        assert_eq!(ledger.snapshot().used_bytes, 24);
        assert_eq!(ledger.snapshot().rx_bytes, 10);
        assert_eq!(ledger.snapshot().tx_bytes, 20);
        assert!(ledger.correct(id, "admin".into(), -5, 0).is_err());
        assert!(!ledger.evaluate(Some(30), false));
        assert_eq!(ledger.snapshot().state, "restore_pending");
    }

    #[test]
    fn subtraction_larger_than_usage_is_rejected_without_changing_ledger() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("service.json");
        let created = "2025-01-31T12:00:00Z".parse().unwrap();
        let ledger = Ledger::new(path.clone(), created, None);
        ledger.record_stream(10, 0);
        ledger.checkpoint().unwrap();

        let error = ledger
            .correct(uuid::Uuid::new_v4(), "admin".into(), -11, 0)
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "The amount to subtract cannot exceed the current bandwidth usage."
        );
        assert_eq!(ledger.snapshot().used_bytes, 10);
        assert!(ledger.snapshot().corrections.is_empty());
        let persisted: LedgerState = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(persisted.used_bytes, 10);
        assert!(persisted.corrections.is_empty());
    }

    #[test]
    fn stream_reports_quota_crossing_before_the_next_periodic_evaluation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("service.json");
        let created = "2025-01-31T12:00:00Z".parse().unwrap();
        let ledger = Ledger::new(path.clone(), created, None);
        assert!(!ledger.evaluate(Some(100), false));

        assert!(!ledger.record_stream(99, 0));
        assert!(ledger.record_stream(2, 0));
        assert!(ledger.evaluate(Some(100), true));
        assert!(ledger.blocked());
        assert_eq!(ledger.snapshot().used_bytes, 101);

        let persisted: LedgerState = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(persisted.used_bytes, 101);
        assert_eq!(persisted.state, "quota_exceeded");
    }

    #[test]
    fn rollover_preserves_resume_intent_for_quota_stopped_server() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("service.json");
        let created = "2025-01-01T00:00:00Z".parse().unwrap();
        let first = super::super::configuration::ServerConfigurationBillingPeriod {
            id: "first".into(),
            start: "2025-01-01T00:00:00Z".parse().unwrap(),
            end: "2025-02-01T00:00:00Z".parse().unwrap(),
        };
        let second = super::super::configuration::ServerConfigurationBillingPeriod {
            id: "second".into(),
            start: first.end,
            end: "2025-03-01T00:00:00Z".parse().unwrap(),
        };
        let ledger = Ledger::new(path.clone(), created, Some(&first));
        ledger.observe_container(10, 20);
        assert!(ledger.evaluate(Some(30), true));
        assert!(ledger.mark_stopping());
        ledger.stopped(true);

        assert!(ledger.period(created, Some(&second)).unwrap());
        let state = ledger.snapshot();
        assert_eq!(state.state, "restore_pending");
        assert!(state.resume_after_quota);
        assert_eq!(
            (state.rx_bytes, state.tx_bytes, state.used_bytes),
            (0, 0, 0)
        );
        assert_eq!((state.container_rx, state.container_tx), (10, 20));
        assert!(!ledger.blocked());
        assert!(!ledger.period(created, Some(&second)).unwrap());
        drop(ledger);

        let ledger = Ledger::new(path, created, Some(&second));
        assert_eq!(ledger.snapshot().state, "restore_pending");
        ledger.started();
        assert_eq!(ledger.snapshot().state, "active");
        assert!(!ledger.snapshot().resume_after_quota);
    }

    #[test]
    fn rollover_does_not_resume_when_start_intent_was_disabled() {
        let directory = tempfile::tempdir().unwrap();
        let created = "2025-01-01T00:00:00Z".parse().unwrap();
        let first = super::super::configuration::ServerConfigurationBillingPeriod {
            id: "first".into(),
            start: created,
            end: "2025-02-01T00:00:00Z".parse().unwrap(),
        };
        let second = super::super::configuration::ServerConfigurationBillingPeriod {
            id: "second".into(),
            start: first.end,
            end: "2025-03-01T00:00:00Z".parse().unwrap(),
        };
        let ledger = Ledger::new(directory.path().join("service.json"), created, Some(&first));
        ledger.record_stream(30, 0);
        assert!(ledger.evaluate(Some(30), true));
        assert!(ledger.mark_stopping());
        ledger.stopped(true);
        ledger.disable_resume();

        assert!(ledger.period(created, Some(&second)).unwrap());
        assert_eq!(ledger.snapshot().state, "active");
        assert!(!ledger.snapshot().resume_after_quota);
    }

    #[test]
    fn rollover_does_not_resume_server_that_was_offline_at_quota_block() {
        let directory = tempfile::tempdir().unwrap();
        let created = "2025-01-01T00:00:00Z".parse().unwrap();
        let first = super::super::configuration::ServerConfigurationBillingPeriod {
            id: "first".into(),
            start: created,
            end: "2025-02-01T00:00:00Z".parse().unwrap(),
        };
        let second = super::super::configuration::ServerConfigurationBillingPeriod {
            id: "second".into(),
            start: first.end,
            end: "2025-03-01T00:00:00Z".parse().unwrap(),
        };
        let ledger = Ledger::new(directory.path().join("service.json"), created, Some(&first));
        ledger.record_stream(30, 0);
        assert!(!ledger.evaluate(Some(30), false));
        assert!(!ledger.snapshot().resume_after_quota);

        assert!(ledger.period(created, Some(&second)).unwrap());
        assert_eq!(ledger.snapshot().state, "active");
        assert!(!ledger.snapshot().resume_after_quota);
    }

    #[test]
    fn rollover_during_quota_stop_keeps_restore_pending_after_stop_confirmation() {
        let directory = tempfile::tempdir().unwrap();
        let created = "2025-01-01T00:00:00Z".parse().unwrap();
        let first = super::super::configuration::ServerConfigurationBillingPeriod {
            id: "first".into(),
            start: created,
            end: "2025-02-01T00:00:00Z".parse().unwrap(),
        };
        let second = super::super::configuration::ServerConfigurationBillingPeriod {
            id: "second".into(),
            start: first.end,
            end: "2025-03-01T00:00:00Z".parse().unwrap(),
        };
        let ledger = Ledger::new(directory.path().join("service.json"), created, Some(&first));
        ledger.record_stream(30, 0);
        assert!(ledger.evaluate(Some(30), true));
        assert!(ledger.mark_stopping());

        assert!(ledger.period(created, Some(&second)).unwrap());
        ledger.stopped(true);
        assert_eq!(ledger.snapshot().state, "restore_pending");
        assert!(ledger.snapshot().resume_after_quota);
    }

    #[test]
    fn derived_rollover_keeps_running_server_active() {
        let directory = tempfile::tempdir().unwrap();
        let created = "2025-01-31T12:00:00Z".parse().unwrap();
        let ledger = Ledger::new(directory.path().join("service.json"), created, None);
        let current = ledger.snapshot().period_start;
        {
            let mut state = ledger.state.lock();
            state.period_start = current - chrono::Duration::days(1);
            state.period_id = state.period_start.to_rfc3339();
        }
        ledger.record_stream(12, 18);

        assert!(ledger.period(created, None).unwrap());
        let state = ledger.snapshot();
        assert_eq!(state.period_start, current);
        assert_eq!(
            (state.rx_bytes, state.tx_bytes, state.used_bytes),
            (0, 0, 0)
        );
        assert_eq!(state.state, "active");
        assert!(!state.resume_after_quota);
    }

    #[test]
    fn handoff_fences_old_owner_and_increments_generation() {
        let directory = tempfile::tempdir().unwrap();
        let created = "2025-01-31T12:00:00Z".parse().unwrap();
        let source = Ledger::new(directory.path().join("source.json"), created, None);
        source.record_stream(12, 18);
        let outgoing = source.freeze(0).unwrap();
        let destination = Ledger::new(directory.path().join("destination.json"), created, None);
        destination.accept(outgoing, 0, Some((0, 0))).unwrap();
        assert_eq!(destination.snapshot().used_bytes, 30);
        assert_eq!(destination.snapshot().generation, 1);
        assert!(
            source
                .correct(uuid::Uuid::new_v4(), "late".into(), 1, 0)
                .is_err()
        );
        assert!(
            destination
                .correct(uuid::Uuid::new_v4(), "admin".into(), 1, 0)
                .is_err()
        );
        destination
            .correct(uuid::Uuid::new_v4(), "admin".into(), 1, 1)
            .unwrap();
        assert_eq!(destination.snapshot().used_bytes, 31);
    }
}
