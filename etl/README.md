# ETL

## Description

TBD

## Environment

### Prod

To run all ETL jobs for the prod environment, you should execute the following:
```bash
make -C etl etl_complete_prod
```

### Staging

Please remember to put a user-specific value in the `etl/configs/config-stage.yaml`
file for `output_prefix`

To run all ETL jobs for the stage environment, you should execute the following:
```bash
make -C etl etl_complete_stage
```

### Dev

Please remember to put a user-specific value in the `etl/configs/config-dev.yaml`
file for `output_prefix`

To run all ETL jobs for the dev environment, you should execute the following:
```bash
make -C etl etl_complete_dev
```

## Tests

To execute ETL tests, please run the following:

```bash
make -C etl tests
```