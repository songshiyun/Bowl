# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj980079ks7kdbsi3nu0Z`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | macro_find_files | 0.047 | 0.056 | seconds |
| bowl | macro_git_clone | 5.668 | 5.783 | seconds |
| bowl | macro_git_diff_dirty | 0.030 | 0.037 | seconds |
| bowl | macro_git_status_dirty | 0.080 | 0.089 | seconds |
| bowl | macro_go_build | 0.429 | 1.954 | seconds |
| bowl | mkdir | 674.775 | 739.513 | ops/s |
| bowl | random_read_4k | 4.975 | 5.289 | MiB/s |
| bowl | readdir_large | 161788.841 | 176802.188 | ops/s |
| bowl | readdir_small | 28514.562 | 28708.027 | ops/s |
| bowl | rename | 2495.695 | 2626.583 | ops/s |
| bowl | sequential_read | 653.524 | 676.766 | MiB/s |
| bowl | sequential_write | 1142.867 | 1173.363 | MiB/s |
| bowl | small_write_4k_fsync | 158.431 | 160.751 | ops/s |
| bowl | stat | 54741.159 | 58167.479 | ops/s |
| bowl | unlink | 2596.604 | 2638.209 | ops/s |
| drive9_default | macro_find_files | 0.011 | 0.011 | seconds |
| drive9_default | macro_git_clone | 1.913 | 1.930 | seconds |
| drive9_default | macro_git_diff_dirty | 0.800 | 0.857 | seconds |
| drive9_default | macro_git_status_dirty | 1.660 | 1.752 | seconds |
| drive9_default | macro_go_build | 0.402 | 0.417 | seconds |
| drive9_default | mkdir | 231.626 | 234.141 | ops/s |
| drive9_default | random_read_4k | 2.172 | 2.301 | MiB/s |
| drive9_default | readdir_large | 60289.501 | 63733.775 | ops/s |
| drive9_default | readdir_small | 10846.352 | 10875.417 | ops/s |
| drive9_default | rename | 91.152 | 91.640 | ops/s |
| drive9_default | sequential_read | 575.153 | 612.156 | MiB/s |
| drive9_default | sequential_write | 650.377 | 661.983 | MiB/s |
| drive9_default | small_write_4k_fsync | 2120.291 | 2187.075 | ops/s |
| drive9_default | stat | 12190.250 | 12614.180 | ops/s |
| drive9_default | unlink | 99.283 | 99.700 | ops/s |
| drive9_sync | macro_find_files | 0.048 | 0.049 | seconds |
| drive9_sync | macro_git_clone | 6.535 | 6.558 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.131 | 0.144 | seconds |
| drive9_sync | macro_git_status_dirty | 0.761 | 0.765 | seconds |
| drive9_sync | macro_go_build | 0.381 | 0.384 | seconds |
| drive9_sync | mkdir | 223.724 | 223.872 | ops/s |
| drive9_sync | random_read_4k | 0.621 | 0.671 | MiB/s |
| drive9_sync | readdir_large | 157005.343 | 160515.716 | ops/s |
| drive9_sync | readdir_small | 24925.349 | 25370.798 | ops/s |
| drive9_sync | rename | 58.762 | 58.983 | ops/s |
| drive9_sync | sequential_read | 53.544 | 53.554 | MiB/s |
| drive9_sync | sequential_write | 345.520 | 348.135 | MiB/s |
| drive9_sync | small_write_4k_fsync | 138.808 | 140.413 | ops/s |
| drive9_sync | stat | 33755.771 | 35229.760 | ops/s |
| drive9_sync | unlink | 41.602 | 41.947 | ops/s |
