# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj901hvcvkdw6txxqwdvZ`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 162.993 | 234.009 | MiB/s |
| bowl | sequential_read | 489.795 | 510.050 | MiB/s |
| bowl | random_read_4k | 3.828 | 3.859 | MiB/s |
| bowl | small_write_4k_fsync | 380.216 | 393.412 | ops/s |
| bowl | mkdir | 910.175 | 912.889 | ops/s |
| bowl | rename | 856.951 | 866.105 | ops/s |
| bowl | unlink | 987.966 | 1005.022 | ops/s |
| bowl | readdir_small | 19430.813 | 25354.548 | ops/s |
| bowl | readdir_large | 92782.711 | 97165.394 | ops/s |
| bowl | stat | 3322.581 | 3808.035 | ops/s |
| bowl | macro_git_clone | 1.571 | 1.614 | seconds |
| bowl | macro_git_status_dirty | 0.138 | 0.181 | seconds |
| bowl | macro_git_diff_dirty | 0.079 | 0.103 | seconds |
| bowl | macro_find_files | 0.018 | 0.018 | seconds |
| bowl | macro_go_build | 0.316 | 1.837 | seconds |
| drive9_default | sequential_write | 270.928 | 417.472 | MiB/s |
| drive9_default | sequential_read | 183.640 | 183.704 | MiB/s |
| drive9_default | random_read_4k | 2.769 | 2.772 | MiB/s |
| drive9_default | small_write_4k_fsync | 981.923 | 989.000 | ops/s |
| drive9_default | mkdir | 251.583 | 252.034 | ops/s |
| drive9_default | rename | 97.632 | 98.074 | ops/s |
| drive9_default | unlink | 96.605 | 98.638 | ops/s |
| drive9_default | readdir_small | 7676.659 | 8145.057 | ops/s |
| drive9_default | readdir_large | 45001.467 | 52402.880 | ops/s |
| drive9_default | stat | 22138.761 | 25178.569 | ops/s |
| drive9_default | macro_git_clone | 1.871 | 1.959 | seconds |
| drive9_default | macro_git_status_dirty | 2.194 | 2.208 | seconds |
| drive9_default | macro_git_diff_dirty | 0.774 | 0.792 | seconds |
| drive9_default | macro_find_files | 0.016 | 0.018 | seconds |
| drive9_default | macro_go_build | 0.338 | 0.355 | seconds |
| drive9_sync | sequential_write | 185.943 | 265.068 | MiB/s |
| drive9_sync | sequential_read | 58.161 | 59.236 | MiB/s |
| drive9_sync | random_read_4k | 5.111 | 5.123 | MiB/s |
| drive9_sync | small_write_4k_fsync | 145.158 | 145.310 | ops/s |
| drive9_sync | mkdir | 247.359 | 249.837 | ops/s |
| drive9_sync | rename | 60.097 | 61.977 | ops/s |
| drive9_sync | unlink | 51.652 | 51.900 | ops/s |
| drive9_sync | readdir_small | 12448.425 | 14740.372 | ops/s |
| drive9_sync | readdir_large | 136448.098 | 139410.900 | ops/s |
| drive9_sync | stat | 42705.470 | 46302.294 | ops/s |
| drive9_sync | macro_git_clone | 6.087 | 6.185 | seconds |
| drive9_sync | macro_git_status_dirty | 0.227 | 0.268 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.035 | 0.039 | seconds |
| drive9_sync | macro_find_files | 0.044 | 0.051 | seconds |
| drive9_sync | macro_go_build | 0.354 | 0.408 | seconds |
