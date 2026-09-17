//! Validate explicit receipt dependencies together with implicit nonce-lane order.
use crate::{GeneratedTx, SchedulingKey, TxPhase};
use eyre::{bail, Result};
use std::collections::{HashMap, HashSet, VecDeque};

/// Validate a complete setup batch and return a submission order. Receipt edges
/// may point forward in the input, but may not contradict scheduling-key order.
/// No transaction should be submitted until this check succeeds.
pub fn setup_submission_order(transactions: &[GeneratedTx]) -> Result<Vec<usize>> {
    let mut ids = HashMap::new();
    for (index, tx) in transactions.iter().enumerate() {
        if tx.phase != TxPhase::Setup {
            bail!("setup batch contains a workload transaction");
        }
        let id = tx.id.as_deref().filter(|id| !id.is_empty()).ok_or_else(|| {
            eyre::eyre!("setup transaction at index {index} requires a nonempty id")
        })?;
        if ids.insert(id, index).is_some() {
            bail!("duplicate setup transaction id '{id}'");
        }
    }
    let mut successors = vec![Vec::new(); transactions.len()];
    let mut incoming = vec![0usize; transactions.len()];
    let mut previous = HashMap::<SchedulingKey, usize>::new();
    for (index, tx) in transactions.iter().enumerate() {
        let mut predecessors = HashSet::new();
        for dependency in &tx.depends_on {
            let Some(&prior) = ids.get(dependency.as_str()) else {
                bail!(
                    "setup transaction '{}' depends on unknown transaction '{dependency}'",
                    tx.id.as_deref().unwrap()
                );
            };
            predecessors.insert(prior);
        }
        let keys: HashSet<_> =
            tx.submission_keys.iter().chain(&tx.inclusion_keys).copied().collect();
        for key in keys {
            if let Some(prior) = previous.insert(key, index) {
                predecessors.insert(prior);
            }
        }
        // Sorting makes traversal and diagnostics deterministic despite hash iteration.
        let mut predecessors: Vec<_> = predecessors.into_iter().collect();
        predecessors.sort_unstable();
        for prior in predecessors {
            successors[prior].push(index);
            incoming[index] += 1;
        }
    }
    let mut ready: VecDeque<_> = incoming
        .iter()
        .enumerate()
        .filter_map(|(index, &count)| (count == 0).then_some(index))
        .collect();
    let mut order = Vec::with_capacity(transactions.len());
    while let Some(index) = ready.pop_front() {
        order.push(index);
        for &next in &successors[index] {
            incoming[next] -= 1;
            if incoming[next] == 0 {
                ready.push_back(next);
            }
        }
    }
    if order.len() != transactions.len() {
        let blocked: Vec<_> = incoming
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 0)
            .take(12)
            .map(|(index, _)| transactions[index].id.as_deref().unwrap())
            .collect();
        bail!("setup dependency cycle: explicit receipt dependencies conflict with each other or nonce/scheduling-key order; blocked transactions: {}", blocked.join(", "));
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn tx(id: &str, lane: u8, deps: &[&str]) -> GeneratedTx {
        GeneratedTx {
            phase: TxPhase::Setup,
            id: Some(id.into()),
            raw: Default::default(),
            late_sign: None,
            sender: None,
            submission_keys: vec![[lane; 20].into()],
            inclusion_keys: vec![],
            depends_on: deps.iter().map(|s| (*s).into()).collect(),
        }
    }
    #[test]
    fn forward_dependencies_preserve_nonce_order() {
        let transactions = [tx("a", 1, &["c"]), tx("b", 1, &[]), tx("c", 2, &[])];
        assert_eq!(setup_submission_order(&transactions).unwrap(), [2, 0, 1]);
    }
    #[test]
    fn rejects_explicit_and_implicit_cycles() {
        for transactions in [
            vec![tx("a", 1, &["b"]), tx("b", 1, &[])],
            vec![tx("a", 1, &["b"]), tx("b", 2, &["a"])],
            vec![tx("a", 1, &["c"]), tx("b", 1, &[]), tx("c", 2, &["b"])],
            vec![tx("a", 1, &["a"])],
        ] {
            assert!(setup_submission_order(&transactions)
                .unwrap_err()
                .to_string()
                .contains("cycle"));
        }
    }
    #[test]
    fn rejects_unknown_duplicate_and_missing_ids() {
        assert!(setup_submission_order(&[tx("a", 1, &["absent"])])
            .unwrap_err()
            .to_string()
            .contains("unknown"));
        assert!(setup_submission_order(&[tx("a", 1, &[]), tx("a", 2, &[])])
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
        assert!(setup_submission_order(&[tx("", 1, &[])])
            .unwrap_err()
            .to_string()
            .contains("nonempty"));
    }
}
