# Member DNA

## Description

TBD

## Environment

### Prod

To run all Member DNA jobs for the prod environment, you should execute the
following:
```bash
make -C dna/member run_member_dna_prod
```

### Staging

Please remember to put a user-specific value in the `dna/member/configs/config-stage.yaml`
file for `output_prefix`

To run all Categpru DNA jobs for the stage environment, you should execute the
following:
```bash
make -C dna/member run_member_dna_stage
```

### Dev

Please remember to put a user-specific value in the `dna/member/configs/config-dev.yaml`
file for `output_prefix`

To run all Member DNA jobs for the dev environment, you should execute the following:
```bash
make -C dna/member run_member_dna_dev
```

## Tests

To execute Member DNA tests, please run the following:

```bash
make -C dna/member tests
```