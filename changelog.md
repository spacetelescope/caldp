- enabled S3 output tests and made related adjustments
- overhauled logging, dropping Python logging for print(),  to:
  reduce complexity
  obtain log output running under pytest
  avoid writing to same log file from two processes
     concurrently:  the .py's and caldp-process
  re-enable console output of caldp .py logs

- overhauled scripts and github actions to shift to 100% Docker
  based development and test.   native development and test is
  still possible but no longer supported and discouraged.
- default base docker image set to CALDP_20221010_CAL_final
- default crds update to hst_1038.pmap
