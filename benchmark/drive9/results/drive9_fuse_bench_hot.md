# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj912d5rbcy2stqm8dwjZ`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 381.034 | 392.967 | MiB/s |
| bowl | sequential_read | 397.110 | 649.373 | MiB/s |
| bowl | random_read_4k | 7.187 | 7.405 | MiB/s |
| bowl | small_write_4k_fsync | 196.199 | 196.628 | ops/s |
| bowl | mkdir | 551.582 | 571.168 | ops/s |
| bowl | rename | 573.888 | 615.304 | ops/s |
| bowl | unlink | 585.688 | 604.722 | ops/s |
| bowl | readdir_small | 6852.821 | 8694.993 | ops/s |
| bowl | readdir_large | 15137.140 | 15182.986 | ops/s |
| bowl | stat | 46697.244 | 50692.479 | ops/s |
| bowl | macro_git_clone | 3.159 | 3.243 | seconds |
| bowl | macro_git_status_dirty | 0.171 | 0.208 | seconds |
| bowl | macro_git_diff_dirty | 0.062 | 0.085 | seconds |
| bowl | macro_find_files | 0.071 | 0.076 | seconds |
| bowl | macro_go_build | 0.249 | 0.301 | seconds |
| drive9_default | sequential_write | 270.393 | 439.103 | MiB/s |
| drive9_default | sequential_read | 2159.658 | 2237.605 | MiB/s |
| drive9_default | random_read_4k | 166.967 | 178.390 | MiB/s |
| drive9_default | small_write_4k_fsync | 1077.219 | 1099.155 | ops/s |
| drive9_default | mkdir | 264.859 | 265.060 | ops/s |
| drive9_default | rename | 100.138 | 101.915 | ops/s |
| drive9_default | unlink | 105.224 | 105.532 | ops/s |
| drive9_default | readdir_small | 144620.480 | 158027.905 | ops/s |
| drive9_default | readdir_large | 85787.897 | 88181.258 | ops/s |
| drive9_default | stat | 626913.654 | 652416.776 | ops/s |
| drive9_default | macro_git_clone | 1.824 | 1.851 | seconds |
| drive9_default | macro_git_status_dirty | 0.217 | 0.231 | seconds |
| drive9_default | macro_git_diff_dirty | 0.013 | 0.014 | seconds |
| drive9_default | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_default | macro_go_build | 0.116 | 0.117 | seconds |
| drive9_sync | sequential_write | 186.615 | 262.615 | MiB/s |
| drive9_sync | sequential_read | 655.835 | 718.084 | MiB/s |
| drive9_sync | random_read_4k | 4.913 | 4.942 | MiB/s |
| drive9_sync | small_write_4k_fsync | 149.301 | 149.692 | ops/s |
| drive9_sync | mkdir | 257.391 | 260.756 | ops/s |
| drive9_sync | rename | 56.167 | 56.944 | ops/s |
| drive9_sync | unlink | 58.801 | 58.850 | ops/s |
| drive9_sync | readdir_small | 227614.148 | 248983.012 | ops/s |
| drive9_sync | readdir_large | 255947.058 | 256031.871 | ops/s |
| drive9_sync | stat | 434725.149 | 436059.427 | ops/s |
| drive9_sync | macro_git_clone | 5.790 | 5.794 | seconds |
| drive9_sync | macro_git_status_dirty | 0.048 | 0.068 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.011 | 0.012 | seconds |
| drive9_sync | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_sync | macro_go_build | 0.157 | 0.169 | seconds |
