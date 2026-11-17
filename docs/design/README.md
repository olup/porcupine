# Design and validation records

These records explain decisions and report measurements at the commit and configuration tested. They are not current setup instructions or hosted capacity guarantees. Start with the [product guides](../README.md) to use Porcupine.

## Architecture decisions

- [Native ownership boundaries](native-only-architecture.md)
- [Shared deployment design](shared-deployment-design.md)
- [Tenant budgets](tenant-budget-design.md)
- [Numerical usage accounting](usage-contract.md)
- [SaaS and separate-compute plans](saas-and-compute-plans.md)
- [Export design](export-design.md)
- [v0.14.0 review follow-up](review-v0.14.md)

## Experiments and measurements

- [Initial DuckLake experiment](ducklake-experiment.md)
- [Validation plan](ducklake-validation-plan.md) and [validation results](ducklake-validation.md)
- [Maintenance investigation](ducklake-maintenance.md) and [prolonged-load record](ducklake-maintenance-soak.md)
- [Large-batch behavior](ducklake-large-batches.md)
- [Load-testing tools](load-testing.md) and [SDK contract tests](sdk-compatibility-tests.md)
- [Test and CI feedback measurements](ci-feedback.md)

## Contributing

- [Publish a release](releasing.md)
- Local development: use `make test PKG=./internal/porcupine RUN=TestTenant` for a focused short test. `make test-core` runs pure-Go modules without CGO. `make check` is the short feedback loop; full race, SDK, S3, and restore checks remain release validation.

Raw benchmark files remain alongside these records so measurements can be traced to their fixtures and limits. A short passing test does not establish sustainable throughput, availability, or a deletion deadline.
