use eyre::{bail, Result, WrapErr};
use rand::{rngs::StdRng, seq::SliceRandom, RngCore, SeedableRng};
use serde::Deserialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Mutex,
};

/// Definition of an external record pool in the workload spec.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordPoolDef {
    /// Path to a JSON or YAML array of record mappings.
    #[serde(alias = "file")]
    pub path: PathBuf,
}

/// Reference to a record pool from a sequence binding.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordRef {
    /// Record pool name.
    pub pool: String,
    /// Selection and exhaustion behavior.
    pub select: RecordSelectMode,
}

/// Record selection and exhaustion behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordSelectMode {
    /// Shuffle once and fail after every record has been selected.
    ShuffledOnce,
    /// Shuffle again and begin another pass after every record has been selected.
    ShuffledCycle,
}

/// Loaded external record pools with deterministic selection state.
#[derive(Debug, Default)]
pub struct RecordPoolManager {
    pools: HashMap<String, RecordPool>,
}

#[derive(Debug)]
struct RecordPool {
    records: Vec<serde_yaml::Value>,
    state: Mutex<RecordPoolState>,
}

#[derive(Debug)]
struct RecordPoolState {
    order: Vec<usize>,
    cursor: usize,
    selections: u64,
    mode: Option<RecordSelectMode>,
    rng: StdRng,
}

impl RecordPoolManager {
    /// Create an empty record pool manager.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Load record pools and deterministically shuffle their first passes.
    ///
    /// Pool names are processed in sorted order so a fixed workload seed always
    /// produces the same per-pool RNG streams, independent of map iteration order.
    pub fn load(
        definitions: &HashMap<String, RecordPoolDef>,
        base_path: &Path,
        rng: &mut dyn RngCore,
    ) -> Result<Self> {
        let mut names: Vec<&String> = definitions.keys().collect();
        names.sort_unstable();

        let mut pools = HashMap::with_capacity(definitions.len());
        for name in names {
            let definition = &definitions[name];
            let path = resolve_path(&definition.path, base_path);
            let records = load_records(&path)
                .wrap_err_with(|| format!("failed to load record pool '{name}'"))?;

            let mut seed = <StdRng as SeedableRng>::Seed::default();
            rng.fill_bytes(seed.as_mut());
            let mut pool_rng = StdRng::from_seed(seed);
            let mut order: Vec<usize> = (0..records.len()).collect();
            order.shuffle(&mut pool_rng);

            pools.insert(
                name.clone(),
                RecordPool {
                    records,
                    state: Mutex::new(RecordPoolState {
                        order,
                        cursor: 0,
                        selections: 0,
                        mode: None,
                        rng: pool_rng,
                    }),
                },
            );
        }

        Ok(Self { pools })
    }

    /// Select the next record according to the requested mode.
    pub fn select(&self, reference: &RecordRef) -> Result<serde_yaml::Value> {
        let pool = self
            .pools
            .get(&reference.pool)
            .ok_or_else(|| eyre::eyre!("record pool '{}' not found", reference.pool))?;
        pool.select(&reference.pool, reference.select)
    }
}

impl RecordPool {
    fn select(&self, name: &str, mode: RecordSelectMode) -> Result<serde_yaml::Value> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| eyre::eyre!("record pool '{name}' selection state lock poisoned"))?;

        if let Some(active_mode) = state.mode {
            if active_mode != mode {
                bail!(
                    "record pool '{name}' cannot mix selection modes ({active_mode:?} and {mode:?})"
                );
            }
        } else {
            state.mode = Some(mode);
        }

        if self.records.is_empty() {
            bail!("record pool '{name}' is empty");
        }

        if state.cursor == state.order.len() {
            match mode {
                RecordSelectMode::ShuffledOnce => bail!(
                    "record pool '{name}' exhausted after {} selections (`shuffled_once` does not reuse records)",
                    state.selections
                ),
                RecordSelectMode::ShuffledCycle => {
                    let RecordPoolState { order, rng, .. } = &mut *state;
                    order.shuffle(rng);
                    state.cursor = 0;
                }
            }
        }

        let index = state.order[state.cursor];
        state.cursor += 1;
        state.selections = state
            .selections
            .checked_add(1)
            .ok_or_else(|| eyre::eyre!("record pool '{name}' selection counter overflowed u64"))?;
        Ok(self.records[index].clone())
    }
}

fn resolve_path(path: &Path, base_path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_path.join(path)
    }
}

fn load_records(path: &Path) -> Result<Vec<serde_yaml::Value>> {
    let content = std::fs::read_to_string(path)
        .wrap_err_with(|| format!("failed to read record file: {}", path.display()))?;
    // Deserialize directly into the record vector. Parsing through a top-level
    // Value and cloning its sequence temporarily doubles a large fixture's
    // in-memory representation. Use the JSON parser for JSON fixtures while
    // retaining YAML support for every other extension.
    let records: Vec<serde_yaml::Value> =
        if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
            serde_json::from_str(&content)
                .wrap_err_with(|| format!("failed to parse record file: {}", path.display()))?
        } else {
            serde_yaml::from_str(&content)
                .wrap_err_with(|| format!("failed to parse record file: {}", path.display()))?
        };

    for (index, record) in records.iter().enumerate() {
        if !record.is_mapping() {
            bail!("record file {} entry {index} must be a mapping", path.display());
        }
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn fixture_dir(label: &str) -> PathBuf {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir()
            .join(format!("txgen-record-pool-{label}-{}-{nonce}", std::process::id()))
    }

    fn manager(seed: u64, label: &str) -> (PathBuf, RecordPoolManager) {
        let dir = fixture_dir(label);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("records.json"), r#"[{"id":0},{"id":1},{"id":2},{"id":3}]"#).unwrap();
        let definitions = HashMap::from([(
            "claims".to_string(),
            RecordPoolDef { path: PathBuf::from("records.json") },
        )]);
        let mut rng = StdRng::seed_from_u64(seed);
        let manager = RecordPoolManager::load(&definitions, &dir, &mut rng).unwrap();
        (dir, manager)
    }

    fn selected_id(manager: &RecordPoolManager, mode: RecordSelectMode) -> u64 {
        let record =
            manager.select(&RecordRef { pool: "claims".to_string(), select: mode }).unwrap();
        record["id"].as_u64().unwrap()
    }

    #[test]
    fn shuffled_once_is_deterministic_and_exhausts() {
        let (dir_a, manager_a) = manager(99, "once-a");
        let (dir_b, manager_b) = manager(99, "once-b");

        let selected_a: Vec<u64> =
            (0..4).map(|_| selected_id(&manager_a, RecordSelectMode::ShuffledOnce)).collect();
        let selected_b: Vec<u64> =
            (0..4).map(|_| selected_id(&manager_b, RecordSelectMode::ShuffledOnce)).collect();
        assert_eq!(selected_a, selected_b);

        let mut sorted = selected_a;
        sorted.sort_unstable();
        assert_eq!(sorted, vec![0, 1, 2, 3]);

        let err = manager_a
            .select(&RecordRef {
                pool: "claims".to_string(),
                select: RecordSelectMode::ShuffledOnce,
            })
            .expect_err("fifth selection must exhaust a four-row pool");
        assert!(err.to_string().contains("exhausted after 4 selections"));

        fs::remove_dir_all(dir_a).unwrap();
        fs::remove_dir_all(dir_b).unwrap();
    }

    #[test]
    fn shuffled_cycle_reshuffles_each_complete_pass() {
        let (dir_a, manager_a) = manager(7, "cycle-a");
        let (dir_b, manager_b) = manager(7, "cycle-b");

        let selected_a: Vec<u64> =
            (0..8).map(|_| selected_id(&manager_a, RecordSelectMode::ShuffledCycle)).collect();
        let selected_b: Vec<u64> =
            (0..8).map(|_| selected_id(&manager_b, RecordSelectMode::ShuffledCycle)).collect();
        assert_eq!(selected_a, selected_b);

        let mut first_sorted = selected_a[..4].to_vec();
        first_sorted.sort_unstable();
        let mut second_sorted = selected_a[4..].to_vec();
        second_sorted.sort_unstable();
        assert_eq!(first_sorted, vec![0, 1, 2, 3]);
        assert_eq!(second_sorted, vec![0, 1, 2, 3]);

        fs::remove_dir_all(dir_a).unwrap();
        fs::remove_dir_all(dir_b).unwrap();
    }

    #[test]
    fn selection_modes_cannot_be_mixed_within_a_pool() {
        let (dir, manager) = manager(7, "mixed");
        selected_id(&manager, RecordSelectMode::ShuffledOnce);

        let err = manager
            .select(&RecordRef {
                pool: "claims".to_string(),
                select: RecordSelectMode::ShuffledCycle,
            })
            .expect_err("mixed selection modes must fail");
        assert!(err.to_string().contains("cannot mix selection modes"));

        fs::remove_dir_all(dir).unwrap();
    }
}
