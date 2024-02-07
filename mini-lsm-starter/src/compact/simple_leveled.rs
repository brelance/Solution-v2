use std::{fmt::UpperHex, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{lsm_storage::LsmStorageState, table::SsTable};

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if _snapshot.l0_sstables.len() > self.options.level0_file_num_compaction_trigger {
            let upper_level_sst_ids = _snapshot.l0_sstables.clone();
            let lower_level_sst_ids = _snapshot.levels[0].1.clone();

            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids,
                lower_level: 1,
                lower_level_sst_ids,
                is_lower_level_bottom_level: false,
            });
        }

        for idx in 0..self.options.max_levels - 1 {
            let upper_level = &_snapshot.levels[idx];
            let lower_level = &_snapshot.levels[idx + 1];

            if upper_level.1.len() / lower_level.1.len() < self.options.size_ratio_percent {
                
                let upper_level_sst_ids = _snapshot.levels[idx].1.clone();
                let lower_level_sst_ids = _snapshot.levels[idx + 1].1.clone();
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(upper_level.0),
                    upper_level_sst_ids,
                    lower_level: lower_level.0,
                    lower_level_sst_ids,
                    is_lower_level_bottom_level: idx == self.options.max_levels - 2,
                });
            }
        }

        None
        
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut deleted_sst_id = Vec::new();
        
        if let Some(upper_level) = _task.upper_level {
            let upper_idx = _task.lower_level - 1;
            let lower_idx = upper_level - 1;
            for &id in &snapshot.levels[upper_idx].1 {
                deleted_sst_id.push(id);
            }

            for &id in &snapshot.levels[lower_idx].1 {
                deleted_sst_id.push(id);
            }

            snapshot.levels[upper_idx].1 = Vec::new();

            snapshot.levels[lower_idx].1 = _output.to_vec();
        } else {
            for &id in &snapshot.l0_sstables {
                deleted_sst_id.push(id);
            }

            for &id in &snapshot.levels[0].1 {
                deleted_sst_id.push(id);
            }

            snapshot.l0_sstables = Vec::new();

            snapshot.levels[0].1 = _output.to_vec();
        }

        (snapshot, deleted_sst_id)
    }
}
