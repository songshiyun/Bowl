# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj96chrxq3htln3b3rumZ`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 346.777 | 359.701 | MiB/s |
| bowl | sequential_read | 189.348 | 318.659 | MiB/s |
| bowl | random_read_4k | 6.816 | 6.943 | MiB/s |
| bowl | small_write_4k_fsync | 214.002 | 218.731 | ops/s |
| bowl | mkdir | 559.800 | 575.414 | ops/s |
| bowl | rename | 444.586 | 462.277 | ops/s |
| bowl | unlink | 555.673 | 572.936 | ops/s |
| bowl | readdir_small | 3491.680 | 4631.197 | ops/s |
| bowl | readdir_large | 12402.285 | 12499.543 | ops/s |
| bowl | stat | 2928.709 | 2972.733 | ops/s |
| bowl | macro_git_clone | 3.958 | 4.132 | seconds |
| bowl | macro_git_status_dirty | 0.321 | 0.322 | seconds |
| bowl | macro_git_diff_dirty | 0.129 | 0.145 | seconds |
| bowl | macro_find_files | 0.098 | 0.107 | seconds |
| bowl | macro_go_build | 0.515 | 1.956 | seconds |
| drive9_default | sequential_write | 260.353 | 435.011 | MiB/s |
| drive9_default | sequential_read | 183.501 | 183.766 | MiB/s |
| drive9_default | random_read_4k | 2.778 | 2.778 | MiB/s |
| drive9_default | small_write_4k_fsync | 1011.597 | 1033.554 | ops/s |
| drive9_default | mkdir | 251.269 | 253.645 | ops/s |
| drive9_default | rename | 94.401 | 95.801 | ops/s |
| drive9_default | unlink | 98.655 | 100.050 | ops/s |
| drive9_default | readdir_small | 7024.475 | 7149.621 | ops/s |
| drive9_default | readdir_large | 42420.990 | 49278.853 | ops/s |
| drive9_default | stat | 21812.659 | 22785.680 | ops/s |
| drive9_default | macro_git_clone | 1.881 | 1.932 | seconds |
| drive9_default | macro_git_status_dirty | 2.027 | 2.034 | seconds |
| drive9_default | macro_git_diff_dirty | 0.777 | 0.785 | seconds |
| drive9_default | macro_find_files | 0.014 | 0.015 | seconds |
| drive9_default | macro_go_build | 0.354 | 0.357 | seconds |
| drive9_sync | sequential_write | 189.588 | 260.486 | MiB/s |
| drive9_sync | sequential_read | 60.651 | 60.918 | MiB/s |
| drive9_sync | random_read_4k | 5.079 | 5.099 | MiB/s |
| drive9_sync | small_write_4k_fsync | 147.704 | 148.097 | ops/s |
| drive9_sync | mkdir | 243.796 | 248.821 | ops/s |
| drive9_sync | rename | 57.459 | 61.384 | ops/s |
| drive9_sync | unlink | 52.880 | 52.945 | ops/s |
| drive9_sync | readdir_small | 13529.255 | 16635.617 | ops/s |
| drive9_sync | readdir_large | 116730.382 | 125871.019 | ops/s |
| drive9_sync | stat | 43792.010 | 49563.912 | ops/s |
| drive9_sync | macro_git_clone | 6.158 | 6.204 | seconds |
| drive9_sync | macro_git_status_dirty | 0.239 | 0.296 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.066 | 0.502 | seconds |
| drive9_sync | macro_find_files | 0.050 | 0.054 | seconds |
| drive9_sync | macro_go_build | 0.323 | 0.356 | seconds |
