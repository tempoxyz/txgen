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

    let accounts = AccountManager::from_spec(&spec.accounts)?;

    let all_addresses: Vec<_> = accounts.all_addresses().flat_map(|(_, addrs)| addrs).collect();

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
