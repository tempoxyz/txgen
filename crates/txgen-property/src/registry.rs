use std::{collections::BTreeMap, fmt, future::Future, pin::Pin};

use eyre::{bail, Result};
use serde::Serialize;

use crate::{run, CampaignHarness, RunConfig, RunResult, WorkloadGenerator};

/// Stable identity of a registered model-free property campaign.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CampaignDescriptor {
    /// CLI-facing campaign name.
    pub name: &'static str,
    /// Campaign serialization/semantics version.
    pub version: &'static str,
}

trait RegisteredProperty {
    fn descriptor(&self) -> CampaignDescriptor;

    fn run<'a>(
        &'a mut self,
        config: RunConfig,
    ) -> Pin<Box<dyn Future<Output = Result<RunResult>> + 'a>>;
}

struct RegisteredCampaign<W, H> {
    workload: W,
    harness: H,
}

impl<W, H> RegisteredProperty for RegisteredCampaign<W, H>
where
    W: WorkloadGenerator + 'static,
    H: CampaignHarness<W> + 'static,
{
    fn descriptor(&self) -> CampaignDescriptor {
        CampaignDescriptor { name: W::NAME, version: W::VERSION }
    }

    fn run<'a>(
        &'a mut self,
        config: RunConfig,
    ) -> Pin<Box<dyn Future<Output = Result<RunResult>> + 'a>> {
        Box::pin(run(&self.workload, &mut self.harness, config))
    }
}

/// Executable registry of model-free Rust campaigns compiled into a txgen binary.
#[derive(Default)]
pub struct CampaignRegistry {
    campaigns: BTreeMap<&'static str, Box<dyn RegisteredProperty>>,
}

impl fmt::Debug for CampaignRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CampaignRegistry")
            .field("campaigns", &self.campaigns.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CampaignRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a workload generator together with its live verifier harness.
    pub fn register<W, H>(&mut self, workload: W, harness: H) -> Result<&mut Self>
    where
        W: WorkloadGenerator + 'static,
        H: CampaignHarness<W> + 'static,
    {
        if self.campaigns.contains_key(W::NAME) {
            bail!("property campaign '{}' is already registered", W::NAME);
        }
        self.campaigns.insert(W::NAME, Box::new(RegisteredCampaign { workload, harness }));
        Ok(self)
    }

    /// Resolve a registered campaign descriptor.
    pub fn get(&self, name: &str) -> Option<CampaignDescriptor> {
        self.campaigns.get(name).map(|campaign| campaign.descriptor())
    }

    /// Iterate registered descriptors in name order.
    pub fn iter(&self) -> impl Iterator<Item = CampaignDescriptor> + '_ {
        self.campaigns.values().map(|campaign| campaign.descriptor())
    }

    /// Execute a registered campaign by its stable CLI-facing name.
    pub async fn run(&mut self, name: &str, config: RunConfig) -> Result<RunResult> {
        let Some(campaign) = self.campaigns.get_mut(name) else {
            let available = self.campaigns.keys().copied().collect::<Vec<_>>().join(", ");
            bail!("unknown property campaign '{name}'; available campaigns: {available}");
        };
        campaign.run(config).await
    }
}
