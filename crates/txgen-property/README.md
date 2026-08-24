# txgen-property

`txgen-property` is the protocol-agnostic property runner for txgen. It deliberately uses
randomized swarm configurations and ABI-shaped generation instead of LibAFL or RPC coverage.

Each Rust model supplies:

- a serializable committed state;
- optional behaviors and generator strategies selected independently for each case;
- currently executable action kinds;
- pure `predict` logic;
- transition and full-state invariant checks.

Each harness supplies topology reset, transaction execution, and RPC observations. The runner
owns the loop:

```text
reset + observe -> generate swarm -> generate action -> predict
                -> execute -> observe -> verify -> commit
```

Failed verification stops the run and can write a concrete YAML artifact containing the seed,
swarm, generated actions, last committed state, prediction, trace, and observation. There is no
corpus, fingerprinting, coverage feedback, or shrinking.

## Registering a model

A network-specific txgen binary constructs its harness and registers the model by stable name:

```rust,ignore
let mut models = ModelRegistry::new();
models.register::<ZoneSolvencyModel, _>(ZoneRpcHarness::new(configuration).await?)?;

let result = models
    .run("zone-solvency", RunConfig::random(1_000, 100))
    .await?;
```

An explicit `RunConfig::seeded` is intended for tests and replay. Normal runs use
`RunConfig::random`, so seed choice is not embedded in model behavior.

## Live Tempo/Zone runner

`txgen-tempo-property` registers the concrete `zone-solvency` model with a live RPC harness.
The harness:

- signs and submits deposits on Tempo L1 and withdrawals on the public Zone RPC;
- creates a fresh `X-Authorization-Token` for every private Zone RPC observation;
- installs portal and outbox allowances once;
- polls receipts and waits for portal-escrow/Zone-supply convergence;
- refreshes user balances after every transaction so gas debits are not modeled as bridge value;
- verifies `portal token balance >= Zone token totalSupply`;
- runs fee-aware deposit/withdrawal loops that restore Zone supply and retain exactly the
  withdrawal fee as excess portal collateral.

With a generated Zones configuration:

```bash
export L1_RPC_URL=http://localhost:8545
export ZONE_RPC_URL=http://localhost:8546
export ZONE_PRIVATE_RPC_URL=http://localhost:8544
export ZONE_TOKEN=0x20C0000000000000000000000000000000000000
export PRIVATE_KEY=0x...

cargo run -p txgen-tempo --bin txgen-tempo-property -- \
  --zone-config /path/to/zones/generated/my-zone/zone.json \
  --cases 100 \
  --max-steps 50
```

The signing key is read from an environment variable and is never included in logs or failure
artifacts. Use `--private-key-env` to select a variable other than `PRIVATE_KEY`.
