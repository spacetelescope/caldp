- default base docker image set to CALDP_drizcosstis_CAL_rc2
- default crds update to hst_1063.pmap
- significant refactoring to generalize processing terms and support
  HAP processing workflows
  - enabled S3 output tests and made related adjustments
  - overhauled logging, dropping Python logging for print(),  to:
  - reduce complexity
  - obtain log output running under pytest
  - avoid writing to same log file from two processes concurrently: the
    .py’s and caldp-process
  - re-enable console output of caldp .py logs
