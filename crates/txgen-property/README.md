# txgen-property

`txgen-property` is the model-free property campaign runner for txgen. It uses randomized swarm
configurations and ABI-shaped generation instead of LibAFL or RPC coverage.

A workload generator supplies only:

- optional action families selected independently for each case;
- ABI-shaped randomized values;
- concrete replayable actions.

A live harness submits those actions, correlates actual terminal lifecycle events, and runs an
independent chain-derived verifier. It never predicts the next protocol state or expected
success/revert classification.

```text
generate swarm -> generate action -> submit -> collect receipt
               -> await correlated terminal event -> verify
               -> periodic verify -> repeat -> final verify
```

Failed verification stops the run and can write a YAML artifact containing the generated seed,
swarm, actions, receipts, terminal evidence, and the verifier's complete report. There is no
corpus, fingerprinting, coverage feedback, shrinking, predicted state, or committed model state.

## Registering a campaign

```rust,ignore
let mut campaigns = CampaignRegistry::new();
campaigns.register(ZoneWorkload::default(), ZoneRpcHarness::new(configuration).await?)?;

let mut config = RunConfig::random(1_000, 100);
config.verify_every_steps = 25;
let result = campaigns.run("zone-backing", config).await?;
```

Normal runs use `RunConfig::random`, so seed choice is not embedded in workload behavior. An
explicit `RunConfig::seeded` exists only for replay and tests.

## Tempo/Zones verification

The Tempo/Zones campaign uses the reusable `zone-portal-backing` library also called by
`cargo xtask verify-portal-backing`. It reconstructs the authoritative invariant from pinned L1
and Zone snapshots plus complete event histories:

```text
required backing = Zone supply
                 + pending deposits
                 + pending withdrawals
                 + Portal refunds
                 + Inbox refunds

Portal balance >= required backing
```

The campaign uses Tempo L1 RPC and the full operator Zone RPC for global verification. The
authenticated redacted Zone RPC is used only for user-scoped operations and observations.
