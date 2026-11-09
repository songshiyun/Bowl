# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj90x3rcwcyduv529x02Z`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | macro_find_files | 0.005 | 0.009 | seconds |
| bowl | macro_git_clone | 5.543 | 5.643 | seconds |
| bowl | macro_git_diff_dirty | 0.005 | 0.006 | seconds |
| bowl | macro_git_status_dirty | 0.030 | 0.039 | seconds |
| bowl | macro_go_build | 0.086 | 0.092 | seconds |
| bowl | mkdir | 434.445 | 674.579 | ops/s |
| bowl | random_read_4k | 394.571 | 402.856 | MiB/s |
| bowl | readdir_large | 1155025.168 | 1158352.045 | ops/s |
| bowl | readdir_small | 423307.300 | 436221.053 | ops/s |
| bowl | rename | 317.404 | 368.070 | ops/s |
| bowl | sequential_read | 6503.594 | 6505.188 | MiB/s |
| bowl | sequential_write | 189.515 | 197.834 | MiB/s |
| bowl | small_write_4k_fsync | 126.020 | 142.853 | ops/s |
| bowl | stat | 723083.575 | 734005.921 | ops/s |
| bowl | unlink | 333.772 | 423.744 | ops/s |
| drive9_default | macro_find_files | 0.005 | 0.005 | seconds |
| drive9_default | macro_git_clone | 1.985 | 2.011 | seconds |
| drive9_default | macro_git_diff_dirty | 0.016 | 0.025 | seconds |
| drive9_default | macro_git_status_dirty | 0.170 | 0.231 | seconds |
| drive9_default | macro_go_build | 0.126 | 0.127 | seconds |
| drive9_default | mkdir | 212.173 | 212.368 | ops/s |
| drive9_default | random_read_4k | 167.515 | 192.628 | MiB/s |
| drive9_default | readdir_large | 80105.098 | 82089.596 | ops/s |
| drive9_default | readdir_small | 106125.005 | 136280.110 | ops/s |
| drive9_default | rename | 84.407 | 85.215 | ops/s |
| drive9_default | sequential_read | 444.836 | 1896.141 | MiB/s |
| drive9_default | sequential_write | 666.512 | 670.716 | MiB/s |
| drive9_default | small_write_4k_fsync | 2181.606 | 2187.799 | ops/s |
| drive9_default | stat | 584488.380 | 591083.985 | ops/s |
| drive9_default | unlink | 86.378 | 86.766 | ops/s |
| drive9_sync | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_sync | macro_git_clone | 7.162 | 7.317 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.013 | 0.020 | seconds |
| drive9_sync | macro_git_status_dirty | 0.050 | 0.060 | seconds |
| drive9_sync | macro_go_build | 0.111 | 0.174 | seconds |
| drive9_sync | mkdir | 205.603 | 206.925 | ops/s |
| drive9_sync | random_read_4k | 2.525 | 2.556 | MiB/s |
| drive9_sync | readdir_large | 238320.508 | 239415.475 | ops/s |
| drive9_sync | readdir_small | 217462.216 | 218207.732 | ops/s |
| drive9_sync | rename | 49.889 | 50.450 | ops/s |
| drive9_sync | sequential_read | 606.568 | 617.908 | MiB/s |
| drive9_sync | sequential_write | 344.391 | 349.876 | MiB/s |
| drive9_sync | small_write_4k_fsync | 104.286 | 106.829 | ops/s |
| drive9_sync | stat | 471514.845 | 526424.013 | ops/s |
| drive9_sync | unlink | 35.449 | 35.478 | ops/s |
