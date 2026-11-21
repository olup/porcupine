# Design and validation records

Start with the [product guides](../README.md) to use Porcupine. Measurements apply to the commit and configuration tested.

## Architecture decisions

- [Native ownership boundaries](native-only-architecture.md)
- [Hosted architecture](hosted-architecture.md)
- [Replaceable built-ins and the commercial control plane](control-plane-contracts.md)
- [Tenant budgets](tenant-budget-design.md)
- [Export design](export-design.md)

## Experiments and measurements

- [Initial DuckLake experiment](ducklake-experiment.md)
- [DuckLake Parquet encryption](ducklake-parquet-encryption.md)
- [Validation plan](ducklake-validation-plan.md) and [validation results](ducklake-validation.md)
- [Maintenance investigation](ducklake-maintenance.md) and [prolonged-load record](ducklake-maintenance-soak.md)
- [Large-batch behavior](ducklake-large-batches.md)
- [Load-testing tools](load-testing.md) and [SDK contract tests](sdk-compatibility-tests.md)
- [Test and CI feedback measurements](ci-feedback.md)

## Contributing

- [Publish a release](releasing.md)
- Use `make test PKG=./internal/porcupine RUN=TestTenant` for a focused short test. Use `make test-core` for pure-Go modules without CGO and `make check` for the short local suite. Pull request CI requires the race, SDK, S3, and restore jobs.

The [archive](archive/README.md) holds the earlier hosted designs, verification records, and fixture results.
