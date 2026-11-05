# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj9irkcbex24zbqogdapZ`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 183.192 | 281.127 | MiB/s |
| bowl | sequential_read | 592.696 | 776.471 | MiB/s |
| bowl | random_read_4k | 7.610 | 7.840 | MiB/s |
| bowl | small_write_4k_fsync | 339.498 | 341.747 | ops/s |
| bowl | mkdir | 909.840 | 943.615 | ops/s |
| bowl | rename | 860.243 | 898.202 | ops/s |
| bowl | unlink | 593.281 | 721.270 | ops/s |
| bowl | readdir_small | 21466.160 | 24657.224 | ops/s |
| bowl | readdir_large | 47619.057 | 77090.428 | ops/s |
| bowl | stat | 3275.293 | 3402.855 | ops/s |
| bowl | macro_git_clone | 1.127 | 1.209 | seconds |
| bowl | macro_git_status_dirty | 0.192 | 0.255 | seconds |
| bowl | macro_git_diff_dirty | 0.049 | 0.115 | seconds |
| bowl | macro_find_files | 0.025 | 0.029 | seconds |
| bowl | macro_go_build | 0.271 | 1.979 | seconds |
| drive9_default | sequential_write | 271.201 | 417.110 | MiB/s |
| drive9_default | sequential_read | 183.463 | 184.592 | MiB/s |
| drive9_default | random_read_4k | 2.793 | 2.802 | MiB/s |
| drive9_default | small_write_4k_fsync | 1020.822 | 1024.441 | ops/s |
| drive9_default | mkdir | 252.575 | 253.442 | ops/s |
| drive9_default | rename | 95.675 | 98.272 | ops/s |
| drive9_default | unlink | 80.972 | 85.141 | ops/s |
| drive9_default | readdir_small | 6809.002 | 6877.165 | ops/s |
| drive9_default | readdir_large | 46113.023 | 47209.162 | ops/s |
| drive9_default | stat | 21472.231 | 23066.499 | ops/s |
| drive9_default | macro_git_clone | 1.929 | 1.934 | seconds |
| drive9_default | macro_git_status_dirty | 2.024 | 2.129 | seconds |
| drive9_default | macro_git_diff_dirty | 0.790 | 0.791 | seconds |
| drive9_default | macro_find_files | 0.016 | 0.017 | seconds |
| drive9_default | macro_go_build | 0.348 | 0.358 | seconds |
| drive9_sync | sequential_write | 195.482 | 250.960 | MiB/s |
| drive9_sync | sequential_read | 58.707 | 59.141 | MiB/s |
| drive9_sync | random_read_4k | 5.086 | 5.088 | MiB/s |
| drive9_sync | small_write_4k_fsync | 145.566 | 146.425 | ops/s |
| drive9_sync | mkdir | 244.173 | 249.825 | ops/s |
| drive9_sync | rename | 60.259 | 61.725 | ops/s |
| drive9_sync | unlink | 52.273 | 52.438 | ops/s |
| drive9_sync | readdir_small | 12716.919 | 13099.573 | ops/s |
| drive9_sync | readdir_large | 98313.839 | 126522.612 | ops/s |
| drive9_sync | stat | 35988.865 | 36256.735 | ops/s |
| drive9_sync | macro_git_clone | 6.159 | 6.271 | seconds |
| drive9_sync | macro_git_status_dirty | 0.295 | 0.349 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.036 | 0.507 | seconds |
| drive9_sync | macro_find_files | 0.043 | 0.051 | seconds |
| drive9_sync | macro_go_build | 0.320 | 0.451 | seconds |
