# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj9f3lh7wxbe9ef3t03hZ`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 354.564 | 370.210 | MiB/s |
| bowl | sequential_read | 459.659 | 637.168 | MiB/s |
| bowl | random_read_4k | 6.686 | 6.792 | MiB/s |
| bowl | small_write_4k_fsync | 198.643 | 205.283 | ops/s |
| bowl | mkdir | 477.342 | 485.002 | ops/s |
| bowl | rename | 356.397 | 358.331 | ops/s |
| bowl | unlink | 429.451 | 435.070 | ops/s |
| bowl | readdir_small | 4784.302 | 4816.661 | ops/s |
| bowl | readdir_large | 3037.984 | 11558.613 | ops/s |
| bowl | stat | 2653.597 | 2770.490 | ops/s |
| bowl | macro_git_clone | 3.321 | 3.337 | seconds |
| bowl | macro_git_status_dirty | 0.288 | 0.291 | seconds |
| bowl | macro_git_diff_dirty | 0.188 | 0.216 | seconds |
| bowl | macro_find_files | 0.076 | 0.083 | seconds |
| bowl | macro_go_build | 0.561 | 2.113 | seconds |
| drive9_default | sequential_write | 288.655 | 433.195 | MiB/s |
| drive9_default | sequential_read | 183.038 | 183.695 | MiB/s |
| drive9_default | random_read_4k | 2.775 | 2.776 | MiB/s |
| drive9_default | small_write_4k_fsync | 1042.405 | 1042.542 | ops/s |
| drive9_default | mkdir | 257.438 | 257.551 | ops/s |
| drive9_default | rename | 99.147 | 100.155 | ops/s |
| drive9_default | unlink | 100.383 | 102.405 | ops/s |
| drive9_default | readdir_small | 7533.093 | 7824.775 | ops/s |
| drive9_default | readdir_large | 49945.065 | 53322.352 | ops/s |
| drive9_default | stat | 20875.073 | 21078.404 | ops/s |
| drive9_default | macro_git_clone | 1.829 | 1.866 | seconds |
| drive9_default | macro_git_status_dirty | 2.027 | 2.062 | seconds |
| drive9_default | macro_git_diff_dirty | 0.755 | 0.755 | seconds |
| drive9_default | macro_find_files | 0.014 | 0.027 | seconds |
| drive9_default | macro_go_build | 0.355 | 0.362 | seconds |
| drive9_sync | sequential_write | 191.379 | 262.669 | MiB/s |
| drive9_sync | sequential_read | 56.764 | 56.969 | MiB/s |
| drive9_sync | random_read_4k | 5.071 | 5.087 | MiB/s |
| drive9_sync | small_write_4k_fsync | 149.547 | 150.518 | ops/s |
| drive9_sync | mkdir | 253.700 | 256.397 | ops/s |
| drive9_sync | rename | 62.648 | 63.070 | ops/s |
| drive9_sync | unlink | 56.213 | 56.251 | ops/s |
| drive9_sync | readdir_small | 17121.690 | 17335.772 | ops/s |
| drive9_sync | readdir_large | 115467.174 | 122151.015 | ops/s |
| drive9_sync | stat | 39476.077 | 40946.857 | ops/s |
| drive9_sync | macro_git_clone | 5.944 | 5.972 | seconds |
| drive9_sync | macro_git_status_dirty | 0.267 | 0.303 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.029 | 0.034 | seconds |
| drive9_sync | macro_find_files | 0.039 | 0.044 | seconds |
| drive9_sync | macro_go_build | 0.315 | 0.318 | seconds |
