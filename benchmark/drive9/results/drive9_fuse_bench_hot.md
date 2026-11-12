# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj93vq1u38hlqnxjflpzZ`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | macro_find_files | 0.003 | 0.003 | seconds |
| bowl | macro_git_clone | 0.339 | 0.341 | seconds |
| bowl | macro_git_diff_dirty | 0.025 | 0.043 | seconds |
| bowl | macro_git_status_dirty | 0.030 | 0.048 | seconds |
| bowl | macro_go_build | 0.031 | 0.057 | seconds |
| bowl | mkdir | 3664.143 | 3793.362 | ops/s |
| bowl | random_read_4k | 497.093 | 510.264 | MiB/s |
| bowl | readdir_large | 1094599.683 | 1501847.015 | ops/s |
| bowl | readdir_small | 407199.284 | 534763.953 | ops/s |
| bowl | rename | 3583.439 | 3610.869 | ops/s |
| bowl | sequential_read | 8316.349 | 8499.170 | MiB/s |
| bowl | sequential_write | 1312.585 | 1403.388 | MiB/s |
| bowl | small_write_4k_fsync | 759.708 | 764.260 | ops/s |
| bowl | stat | 834705.032 | 836920.046 | ops/s |
| bowl | unlink | 3871.515 | 4115.766 | ops/s |
| drive9_default | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_default | macro_git_clone | 1.918 | 1.956 | seconds |
| drive9_default | macro_git_diff_dirty | 0.013 | 0.013 | seconds |
| drive9_default | macro_git_status_dirty | 0.219 | 0.223 | seconds |
| drive9_default | macro_go_build | 0.113 | 0.116 | seconds |
| drive9_default | mkdir | 226.062 | 226.683 | ops/s |
| drive9_default | random_read_4k | 182.191 | 184.716 | MiB/s |
| drive9_default | readdir_large | 56770.533 | 87163.836 | ops/s |
| drive9_default | readdir_small | 121972.044 | 129925.057 | ops/s |
| drive9_default | rename | 86.735 | 88.829 | ops/s |
| drive9_default | sequential_read | 1564.767 | 2115.202 | MiB/s |
| drive9_default | sequential_write | 719.289 | 722.647 | MiB/s |
| drive9_default | small_write_4k_fsync | 2483.465 | 2512.654 | ops/s |
| drive9_default | stat | 620651.560 | 710300.803 | ops/s |
| drive9_default | unlink | 93.007 | 93.143 | ops/s |
| drive9_sync | macro_find_files | 0.004 | 0.007 | seconds |
| drive9_sync | macro_git_clone | 6.563 | 6.681 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.012 | 0.013 | seconds |
| drive9_sync | macro_git_status_dirty | 0.063 | 0.072 | seconds |
| drive9_sync | macro_go_build | 0.142 | 0.157 | seconds |
| drive9_sync | mkdir | 222.466 | 224.530 | ops/s |
| drive9_sync | random_read_4k | 5.690 | 5.832 | MiB/s |
| drive9_sync | readdir_large | 248752.754 | 265120.902 | ops/s |
| drive9_sync | readdir_small | 223301.513 | 231907.853 | ops/s |
| drive9_sync | rename | 52.547 | 56.076 | ops/s |
| drive9_sync | sequential_read | 597.059 | 640.257 | MiB/s |
| drive9_sync | sequential_write | 358.639 | 361.919 | MiB/s |
| drive9_sync | small_write_4k_fsync | 132.317 | 135.162 | ops/s |
| drive9_sync | stat | 564441.113 | 578918.037 | ops/s |
| drive9_sync | unlink | 43.457 | 43.518 | ops/s |
