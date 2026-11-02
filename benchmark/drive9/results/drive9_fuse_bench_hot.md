# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj96chrxq3htln3b3rumZ`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 300.699 | 300.904 | MiB/s |
| bowl | sequential_read | 7225.674 | 7718.019 | MiB/s |
| bowl | random_read_4k | 321.861 | 379.945 | MiB/s |
| bowl | small_write_4k_fsync | 176.251 | 184.125 | ops/s |
| bowl | mkdir | 496.436 | 514.790 | ops/s |
| bowl | rename | 400.878 | 414.980 | ops/s |
| bowl | unlink | 540.055 | 576.890 | ops/s |
| bowl | readdir_small | 6981.675 | 8044.234 | ops/s |
| bowl | readdir_large | 13832.564 | 14196.392 | ops/s |
| bowl | stat | 51136.185 | 62392.709 | ops/s |
| bowl | macro_git_clone | 3.738 | 3.779 | seconds |
| bowl | macro_git_status_dirty | 0.154 | 0.157 | seconds |
| bowl | macro_git_diff_dirty | 0.061 | 0.092 | seconds |
| bowl | macro_find_files | 0.078 | 0.081 | seconds |
| bowl | macro_go_build | 0.224 | 0.251 | seconds |
| drive9_default | sequential_write | 273.715 | 437.252 | MiB/s |
| drive9_default | sequential_read | 2205.552 | 2235.350 | MiB/s |
| drive9_default | random_read_4k | 185.797 | 190.123 | MiB/s |
| drive9_default | small_write_4k_fsync | 1056.957 | 1116.141 | ops/s |
| drive9_default | mkdir | 249.239 | 251.646 | ops/s |
| drive9_default | rename | 93.687 | 93.989 | ops/s |
| drive9_default | unlink | 100.971 | 101.398 | ops/s |
| drive9_default | readdir_small | 127292.052 | 149657.151 | ops/s |
| drive9_default | readdir_large | 90058.042 | 97328.018 | ops/s |
| drive9_default | stat | 673677.403 | 691973.891 | ops/s |
| drive9_default | macro_git_clone | 2.027 | 2.061 | seconds |
| drive9_default | macro_git_status_dirty | 0.237 | 0.240 | seconds |
| drive9_default | macro_git_diff_dirty | 0.015 | 0.015 | seconds |
| drive9_default | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_default | macro_go_build | 0.117 | 0.129 | seconds |
| drive9_sync | sequential_write | 188.926 | 260.748 | MiB/s |
| drive9_sync | sequential_read | 708.392 | 716.667 | MiB/s |
| drive9_sync | random_read_4k | 4.524 | 4.547 | MiB/s |
| drive9_sync | small_write_4k_fsync | 145.443 | 147.412 | ops/s |
| drive9_sync | mkdir | 248.201 | 248.508 | ops/s |
| drive9_sync | rename | 54.994 | 57.940 | ops/s |
| drive9_sync | unlink | 52.975 | 53.077 | ops/s |
| drive9_sync | readdir_small | 211153.107 | 232211.958 | ops/s |
| drive9_sync | readdir_large | 274306.279 | 293574.470 | ops/s |
| drive9_sync | stat | 13189.612 | 31655.762 | ops/s |
| drive9_sync | macro_git_clone | 6.293 | 6.346 | seconds |
| drive9_sync | macro_git_status_dirty | 0.038 | 0.066 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.012 | 0.024 | seconds |
| drive9_sync | macro_find_files | 0.006 | 0.015 | seconds |
| drive9_sync | macro_go_build | 0.157 | 0.175 | seconds |
