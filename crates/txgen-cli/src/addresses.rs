use clap::{Args, ValueEnum};
use eyre::{Result, WrapErr};
use std::path::PathBuf;
use txgen_core::{AccountManager, WorkloadSpec};

#[derive(Args)]
pub struct AddressesArgs {
    /// Workload spec file (YAML)
    #[arg(short, long)]
    pub spec: PathBuf,

    /// Output format
    #[arg(short, long, value_enum, default_value_t = AddressesFormat::Plain)]
    pub format: AddressesFormat,
}

/// Output format of the `addresses` subcommand.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum AddressesFormat {
    /// One address per line
    Plain,
    /// A JSON array
    Json,
    /// Space-separated on one line, for xargs
    Shell,
}

// ---------------------------------------------------------------------------
// Private — addresses subcommand
// ---------------------------------------------------------------------------

pub(crate) fn run_addresses(args: AddressesArgs) -> Result<()> {
    let spec = WorkloadSpec::load(&args.spec)
        .wrap_err_with(|| format!("failed to load spec: {}", args.spec.display()))?;

    let accounts = AccountManager::from_spec(&spec.accounts)?;

    let all_addresses: Vec<_> = accounts.all_addresses().flat_map(|(_, addrs)| addrs).collect();

    match args.format {
        AddressesFormat::Plain => {
            for addr in &all_addresses {
                println!("{addr}");
            }
        }
        AddressesFormat::Json => {
            let json = serde_json::to_string_pretty(&all_addresses)?;
            println!("{json}");
        }
        AddressesFormat::Shell => {
            let line: Vec<_> = all_addresses.iter().map(|a| a.to_string()).collect();
            println!("{}", line.join(" "));
        }
    }

    Ok(())
}
