# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj9f3lh7wxbe35hkyhyuZ`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 182.660 | 184.924 | MiB/s |
| bowl | sequential_read | 789.646 | 801.202 | MiB/s |
| bowl | random_read_4k | 7.106 | 7.218 | MiB/s |
| bowl | small_write_4k_fsync | 153.966 | 162.732 | ops/s |
| bowl | mkdir | 482.644 | 484.823 | ops/s |
| bowl | rename | 359.973 | 370.745 | ops/s |
| bowl | unlink | 482.089 | 523.618 | ops/s |
| bowl | readdir_small | 6711.572 | 6853.892 | ops/s |
| bowl | readdir_large | 14142.830 | 15415.636 | ops/s |
| bowl | stat | 2863.277 | 2949.597 | ops/s |
| bowl | macro_git_clone | 3.266 | 3.416 | seconds |
| bowl | macro_git_status_dirty | 0.302 | 0.322 | seconds |
| bowl | macro_git_diff_dirty | 0.203 | 0.204 | seconds |
| bowl | macro_find_files | 0.091 | 0.099 | seconds |
| bowl | macro_go_build | 0.587 | 1.993 | seconds |
| drive9_default | sequential_write | 257.485 | 439.542 | MiB/s |
| drive9_default | sequential_read | 183.871 | 183.960 | MiB/s |
| drive9_default | random_read_4k | 2.774 | 2.779 | MiB/s |
| drive9_default | small_write_4k_fsync | 981.531 | 1012.226 | ops/s |
| drive9_default | mkdir | 257.178 | 257.417 | ops/s |
| drive9_default | rename | 98.649 | 99.846 | ops/s |
| drive9_default | unlink | 100.263 | 100.900 | ops/s |
| drive9_default | readdir_small | 7230.789 | 8108.334 | ops/s |
| drive9_default | readdir_large | 45668.636 | 45789.022 | ops/s |
| drive9_default | stat | 20718.553 | 21592.828 | ops/s |
| drive9_default | macro_git_clone | 1.870 | 1.931 | seconds |
| drive9_default | macro_git_status_dirty | 2.033 | 2.040 | seconds |
| drive9_default | macro_git_diff_dirty | 0.765 | 0.790 | seconds |
| drive9_default | macro_find_files | 0.016 | 0.028 | seconds |
| drive9_default | macro_go_build | 0.360 | 0.375 | seconds |
| drive9_sync | sequential_write | 187.589 | 260.337 | MiB/s |
| drive9_sync | sequential_read | 61.068 | 61.195 | MiB/s |
| drive9_sync | random_read_4k | 5.094 | 5.095 | MiB/s |
| drive9_sync | small_write_4k_fsync | 145.755 | 149.254 | ops/s |
| drive9_sync | mkdir | 251.823 | 255.636 | ops/s |
| drive9_sync | rename | 62.559 | 62.640 | ops/s |
| drive9_sync | unlink | 56.160 | 56.260 | ops/s |
| drive9_sync | readdir_small | 14761.270 | 17538.778 | ops/s |
| drive9_sync | readdir_large | 71025.825 | 134946.978 | ops/s |
| drive9_sync | stat | 39597.145 | 39620.289 | ops/s |
| drive9_sync | macro_git_clone | 5.915 | 5.943 | seconds |
| drive9_sync | macro_git_status_dirty | 0.257 | 0.290 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.030 | 0.062 | seconds |
| drive9_sync | macro_find_files | 0.040 | 0.047 | seconds |
| drive9_sync | macro_go_build | 0.326 | 0.335 | seconds |
