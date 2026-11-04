# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj901hvcvkdw6txxqwdvZ`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 209.421 | 215.084 | MiB/s |
| bowl | sequential_read | 7077.749 | 7120.913 | MiB/s |
| bowl | random_read_4k | 249.376 | 258.894 | MiB/s |
| bowl | small_write_4k_fsync | 485.068 | 519.090 | ops/s |
| bowl | mkdir | 1065.052 | 1074.339 | ops/s |
| bowl | rename | 1110.364 | 1127.494 | ops/s |
| bowl | unlink | 1378.993 | 1701.264 | ops/s |
| bowl | readdir_small | 349509.813 | 354303.760 | ops/s |
| bowl | readdir_large | 224005.505 | 237249.168 | ops/s |
| bowl | stat | 117494.620 | 118694.522 | ops/s |
| bowl | macro_git_clone | 1.497 | 1.528 | seconds |
| bowl | macro_git_status_dirty | 0.029 | 0.034 | seconds |
| bowl | macro_git_diff_dirty | 0.014 | 0.023 | seconds |
| bowl | macro_find_files | 0.003 | 0.004 | seconds |
| bowl | macro_go_build | 0.079 | 0.088 | seconds |
| drive9_default | sequential_write | 272.603 | 432.058 | MiB/s |
| drive9_default | sequential_read | 2306.685 | 2307.765 | MiB/s |
| drive9_default | random_read_4k | 178.342 | 179.164 | MiB/s |
| drive9_default | small_write_4k_fsync | 1010.796 | 1012.162 | ops/s |
| drive9_default | mkdir | 244.838 | 247.286 | ops/s |
| drive9_default | rename | 91.731 | 93.749 | ops/s |
| drive9_default | unlink | 99.193 | 99.593 | ops/s |
| drive9_default | readdir_small | 130995.500 | 165645.801 | ops/s |
| drive9_default | readdir_large | 59119.104 | 60894.224 | ops/s |
| drive9_default | stat | 627054.387 | 635111.644 | ops/s |
| drive9_default | macro_git_clone | 1.879 | 1.903 | seconds |
| drive9_default | macro_git_status_dirty | 0.216 | 0.217 | seconds |
| drive9_default | macro_git_diff_dirty | 0.012 | 0.018 | seconds |
| drive9_default | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_default | macro_go_build | 0.105 | 0.112 | seconds |
| drive9_sync | sequential_write | 187.916 | 260.481 | MiB/s |
| drive9_sync | sequential_read | 648.771 | 663.165 | MiB/s |
| drive9_sync | random_read_4k | 4.368 | 4.408 | MiB/s |
| drive9_sync | small_write_4k_fsync | 142.886 | 142.968 | ops/s |
| drive9_sync | mkdir | 245.846 | 246.384 | ops/s |
| drive9_sync | rename | 58.417 | 59.787 | ops/s |
| drive9_sync | unlink | 51.820 | 52.111 | ops/s |
| drive9_sync | readdir_small | 208591.901 | 220104.273 | ops/s |
| drive9_sync | readdir_large | 267390.689 | 271002.409 | ops/s |
| drive9_sync | stat | 51605.403 | 69853.608 | ops/s |
| drive9_sync | macro_git_clone | 6.233 | 6.248 | seconds |
| drive9_sync | macro_git_status_dirty | 0.060 | 0.065 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.014 | 0.020 | seconds |
| drive9_sync | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_sync | macro_go_build | 0.190 | 0.195 | seconds |
