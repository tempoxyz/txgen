use alloy_primitives::Address;
use clap::Args;
use eyre::{bail, Result, WrapErr};
use std::path::PathBuf;
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

    let all_addresses = collect_addresses(&spec)?;

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
        other => bail!("unknown format: {}", other),
    }

    Ok(())
}

fn collect_addresses(spec: &WorkloadSpec) -> Result<Vec<Address>> {
    let accounts = AccountManager::from_spec(&spec.accounts)?;
    Ok(accounts.all_addresses().flat_map(|(_, addrs)| addrs).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use txgen_core::{GenValue, ValueResolver};

    #[test]
    fn addresses_excludes_random_address_recipients() {
        let yaml = r#"
chain_id: 1
accounts:
  users:
    mnemonic: "test test test test test test test test test test test junk"
    index: 0
templates:
  erc20_transfer:
    type: eip1559
    from: { pool: users, select: random }
    to: "0x0000000000000000000000000000000000000001"
    abi: erc20
    function: transfer
    args:
      - random_address:
          prefix: "0x00000000000000000000000000000000dead"
      - 1
mix:
  - template: erc20_transfer
    weight: 100
"#;
        let spec = WorkloadSpec::parse(yaml).expect("workload spec should parse");
        let listed_addresses = collect_addresses(&spec).expect("account addresses should derive");
        let accounts = AccountManager::from_spec(&spec.accounts).expect("accounts should derive");
        let random_value: GenValue<Address> = serde_yaml::from_str(
            r#"random_address:
  prefix: "0x00000000000000000000000000000000dead"
"#,
        )
        .expect("random_address value should parse");
        let mut rng = StdRng::seed_from_u64(42);
        let mut resolver = ValueResolver { accounts: &accounts, rng: &mut rng };
        let random_address =
            resolver.resolve_gen(&random_value).expect("random_address should resolve");

        assert_eq!(listed_addresses.len(), 1);
        assert!(!listed_addresses.contains(&random_address));
    }
}
