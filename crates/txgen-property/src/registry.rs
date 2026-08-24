use std::{collections::BTreeMap, fmt, future::Future, marker::PhantomData, pin::Pin};

use eyre::{bail, Result};
use serde::Serialize;

use crate::{run, PropertyHarness, PropertyModel, RunConfig, RunResult};

/// Stable identity of a registered Rust property model.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ModelDescriptor {
    /// CLI-facing model name.
    pub name: &'static str,
    /// Model serialization/semantics version.
    pub version: &'static str,
}

trait RegisteredProperty {
    fn descriptor(&self) -> ModelDescriptor;

    fn run<'a>(
        &'a mut self,
        config: RunConfig,
    ) -> Pin<Box<dyn Future<Output = Result<RunResult>> + 'a>>;
}

struct RegisteredHarness<M, H> {
    harness: H,
    model: PhantomData<fn() -> M>,
}

impl<M, H> RegisteredProperty for RegisteredHarness<M, H>
where
    M: PropertyModel + 'static,
    H: PropertyHarness<M> + 'static,
{
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor { name: M::NAME, version: M::VERSION }
    }

    fn run<'a>(
        &'a mut self,
        config: RunConfig,
    ) -> Pin<Box<dyn Future<Output = Result<RunResult>> + 'a>> {
        Box::pin(run::<M, H>(&mut self.harness, config))
    }
}

/// Executable registry of Rust property models compiled into a txgen binary.
#[derive(Default)]
pub struct ModelRegistry {
    models: BTreeMap<&'static str, Box<dyn RegisteredProperty>>,
}

impl fmt::Debug for ModelRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelRegistry")
            .field("models", &self.models.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ModelRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a model together with its topology/execution harness.
    pub fn register<M, H>(&mut self, harness: H) -> Result<&mut Self>
    where
        M: PropertyModel + 'static,
        H: PropertyHarness<M> + 'static,
    {
        if self.models.contains_key(M::NAME) {
            bail!("property model '{}' is already registered", M::NAME);
        }
        self.models
            .insert(M::NAME, Box::new(RegisteredHarness::<M, H> { harness, model: PhantomData }));
        Ok(self)
    }

    /// Resolve a registered model descriptor.
    pub fn get(&self, name: &str) -> Option<ModelDescriptor> {
        self.models.get(name).map(|model| model.descriptor())
    }

    /// Iterate registered descriptors in name order.
    pub fn iter(&self) -> impl Iterator<Item = ModelDescriptor> + '_ {
        self.models.values().map(|model| model.descriptor())
    }

    /// Execute a registered model by its stable CLI-facing name.
    pub async fn run(&mut self, name: &str, config: RunConfig) -> Result<RunResult> {
        let Some(model) = self.models.get_mut(name) else {
            let available = self.models.keys().copied().collect::<Vec<_>>().join(", ");
            bail!("unknown property model '{name}'; available models: {available}");
        };
        model.run(config).await
    }
}
