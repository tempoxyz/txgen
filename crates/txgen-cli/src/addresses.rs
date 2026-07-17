use alloy_primitives::Address;
use clap::Args;
use eyre::{bail, Result, WrapErr};
use std::{collections::HashSet, path::PathBuf};
use txgen_core::{AccountManager, WorkloadSpec};

#[derive(Args)]
pub struct AddressesArgs {
    /// Workload spec file (YAML)
    #[arg(short, long)]
    pub spec: PathBuf,

    /// Output format: plain (one per line), json, or shell (for xargs)
    #[arg(short, long, default_value = "plain")]
    pub format: String,
}

// ---------------------------------------------------------------------------
// Private — addresses subcommand
// ---------------------------------------------------------------------------

pub(crate) fn run_addresses(args: AddressesArgs) -> Result<()> {
    let spec = WorkloadSpec::load(&args.spec)
        .wrap_err_with(|| format!("failed to load spec: {}", args.spec.display()))?;

    let accounts = AccountManager::from_spec(&spec.accounts)?;

    let all_addresses = unique_signer_addresses(&accounts);

    match args.format.as_str() {
        "plain" => {
            for addr in &all_addresses {
                println!("{addr}");
            }
        }
        "json" => {
            let json = serde_json::to_string_pretty(&all_addresses)?;
            println!("{json}");
        }
        "shell" => {
            let line: Vec<_> = all_addresses.iter().map(|a| a.to_string()).collect();
            println!("{}", line.join(" "));
        }
        other => {
            bail!("unknown format: {}", other);
        }
    }

    Ok(())
}

fn unique_signer_addresses(accounts: &AccountManager) -> Vec<Address> {
    let mut pools: Vec<_> = accounts.all_addresses().collect();
    pools.sort_unstable_by_key(|(name, _)| *name);

    let mut seen = HashSet::new();
    let mut addresses = Vec::new();
    for (_, pool_addresses) in pools {
        for address in pool_addresses {
            if seen.insert(address) {
                addresses.push(address);
            }
        }
    }
    addresses
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use txgen_core::{derive_mnemonic_signer, AccountPoolDef};

    const MNEMONIC: &str = "test test test test test test test test test test test junk";

    #[test]
    fn signer_addresses_are_deduplicated_in_deterministic_pool_order() -> Result<()> {
        let accounts = AccountManager::from_spec(&HashMap::from([
            (
                "users".to_string(),
                AccountPoolDef { mnemonic: MNEMONIC.to_string(), index: None, range: Some([1, 3]) },
            ),
            (
                "reward_claimants".to_string(),
                AccountPoolDef { mnemonic: MNEMONIC.to_string(), index: None, range: Some([0, 2]) },
            ),
        ]))?;

        let addresses = unique_signer_addresses(&accounts);
        let expected = vec![
            derive_mnemonic_signer(MNEMONIC, 0)?.address(),
            derive_mnemonic_signer(MNEMONIC, 1)?.address(),
            derive_mnemonic_signer(MNEMONIC, 2)?.address(),
        ];
        assert_eq!(addresses, expected);
        Ok(())
    }
}
