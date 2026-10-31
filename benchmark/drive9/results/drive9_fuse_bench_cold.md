# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj919o5nf211olqico6kZ`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 226.093 | 226.215 | MiB/s |
| bowl | sequential_read | 141.133 | 246.562 | MiB/s |
| bowl | random_read_4k | 5.524 | 5.647 | MiB/s |
| bowl | small_write_4k_fsync | 131.807 | 133.818 | ops/s |
| bowl | mkdir | 423.720 | 454.940 | ops/s |
| bowl | rename | 345.501 | 366.517 | ops/s |
| bowl | unlink | 373.069 | 393.129 | ops/s |
| bowl | readdir_small | 2738.559 | 4032.097 | ops/s |
| bowl | readdir_large | 8240.398 | 8255.371 | ops/s |
| bowl | stat | 2562.098 | 2610.014 | ops/s |
| bowl | macro_git_clone | 5.153 | 5.431 | seconds |
| bowl | macro_git_status_dirty | 0.361 | 0.369 | seconds |
| bowl | macro_git_diff_dirty | 0.147 | 0.149 | seconds |
| bowl | macro_find_files | 0.096 | 0.109 | seconds |
| bowl | macro_go_build | 0.584 | 2.099 | seconds |
| drive9_default | sequential_write | 325.522 | 432.022 | MiB/s |
| drive9_default | sequential_read | 184.436 | 184.700 | MiB/s |
| drive9_default | random_read_4k | 2.825 | 2.861 | MiB/s |
| drive9_default | small_write_4k_fsync | 1035.917 | 1057.474 | ops/s |
| drive9_default | mkdir | 248.082 | 252.146 | ops/s |
| drive9_default | rename | 96.248 | 96.682 | ops/s |
| drive9_default | unlink | 98.112 | 98.206 | ops/s |
| drive9_default | readdir_small | 6643.909 | 6830.576 | ops/s |
| drive9_default | readdir_large | 41769.859 | 43318.057 | ops/s |
| drive9_default | stat | 20714.043 | 20892.443 | ops/s |
| drive9_default | macro_git_clone | 1.908 | 1.934 | seconds |
| drive9_default | macro_git_status_dirty | 2.081 | 2.121 | seconds |
| drive9_default | macro_git_diff_dirty | 0.782 | 0.789 | seconds |
| drive9_default | macro_find_files | 0.016 | 0.019 | seconds |
| drive9_default | macro_go_build | 0.361 | 0.379 | seconds |
| drive9_sync | sequential_write | 191.089 | 262.485 | MiB/s |
| drive9_sync | sequential_read | 82.268 | 83.193 | MiB/s |
| drive9_sync | random_read_4k | 5.111 | 5.112 | MiB/s |
| drive9_sync | small_write_4k_fsync | 143.738 | 147.250 | ops/s |
| drive9_sync | mkdir | 248.039 | 248.319 | ops/s |
| drive9_sync | rename | 61.498 | 62.033 | ops/s |
| drive9_sync | unlink | 51.043 | 51.323 | ops/s |
| drive9_sync | readdir_small | 13949.258 | 14633.111 | ops/s |
| drive9_sync | readdir_large | 104847.322 | 118442.879 | ops/s |
| drive9_sync | stat | 35844.352 | 42499.739 | ops/s |
| drive9_sync | macro_git_clone | 6.290 | 6.344 | seconds |
| drive9_sync | macro_git_status_dirty | 0.305 | 0.308 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.074 | 0.114 | seconds |
| drive9_sync | macro_find_files | 0.043 | 0.048 | seconds |
| drive9_sync | macro_go_build | 0.342 | 0.346 | seconds |
