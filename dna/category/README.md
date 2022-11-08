# Category DNA

## Description

TBD

## Environment

### Prod

To run all Category DNA jobs for the prod environment, you should execute the
following:
```bash
make -C dna/category run_all_full_prod
```

### Staging

Please remember to put a user-specific value in the `dna/category/configs/stage/reqs.yaml`
file for `output_prefix`

To run all Categpru DNA jobs for the stage environment, you should execute the
following:
```bash
make -C dna/category run_all_full_stage
```

### Dev

Please remember to put a user-specific value in the `dna/category/configs/dev/reqs.yaml`
file for `output_prefix`

To run all Category DNA jobs for the dev environment, you should execute the following:
```bash
make -C dna/category run_all_full_dev
```

## Tests

To execute Category DNA tests, please run the following:

```bash
make -C dna/category tests
```