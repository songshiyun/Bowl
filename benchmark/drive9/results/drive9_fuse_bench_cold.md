# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36`
- Host: `iZrj94b8ccceaoxjysslfyZ`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 40.853 | 40.933 | MiB/s |
| bowl | sequential_read | 16.944 | 17.039 | MiB/s |
| bowl | random_read_4k | 0.494 | 0.504 | MiB/s |
| bowl | small_write_4k_fsync | 38.360 | 38.694 | ops/s |
| bowl | mkdir | 78.955 | 79.315 | ops/s |
| bowl | rename | 61.988 | 62.213 | ops/s |
| bowl | unlink | 78.454 | 79.145 | ops/s |
| bowl | readdir_small | 2901.055 | 3056.436 | ops/s |
| bowl | readdir_large | 34627.897 | 35295.107 | ops/s |
| bowl | stat | 295.289 | 295.754 | ops/s |
| bowl | macro_git_clone | 26.255 | 26.301 | seconds |
| bowl | macro_git_status_dirty | 0.973 | 0.996 | seconds |
| bowl | macro_git_diff_dirty | 0.089 | 0.092 | seconds |
| bowl | macro_find_files | 0.070 | 0.071 | seconds |
| bowl | macro_go_build | 1.366 | 2.837 | seconds |
| drive9_default | sequential_write | 258.041 | 434.689 | MiB/s |
| drive9_default | sequential_read | 184.151 | 184.554 | MiB/s |
| drive9_default | random_read_4k | 2.768 | 2.768 | MiB/s |
| drive9_default | small_write_4k_fsync | 950.078 | 1000.405 | ops/s |
| drive9_default | mkdir | 257.627 | 258.287 | ops/s |
| drive9_default | rename | 99.491 | 100.962 | ops/s |
| drive9_default | unlink | 100.536 | 101.019 | ops/s |
| drive9_default | readdir_small | 7093.965 | 7501.039 | ops/s |
| drive9_default | readdir_large | 42409.015 | 49342.353 | ops/s |
| drive9_default | stat | 22554.740 | 24154.711 | ops/s |
| drive9_default | macro_git_clone | 1.913 | 1.924 | seconds |
| drive9_default | macro_git_status_dirty | 2.005 | 2.041 | seconds |
| drive9_default | macro_git_diff_dirty | 0.773 | 0.774 | seconds |
| drive9_default | macro_find_files | 0.028 | 0.030 | seconds |
| drive9_default | macro_go_build | 0.341 | 0.364 | seconds |
| drive9_sync | sequential_write | 189.392 | 258.433 | MiB/s |
| drive9_sync | sequential_read | 58.079 | 58.213 | MiB/s |
| drive9_sync | random_read_4k | 5.089 | 5.103 | MiB/s |
| drive9_sync | small_write_4k_fsync | 149.055 | 150.469 | ops/s |
| drive9_sync | mkdir | 255.861 | 256.518 | ops/s |
| drive9_sync | rename | 63.173 | 63.197 | ops/s |
| drive9_sync | unlink | 56.155 | 56.887 | ops/s |
| drive9_sync | readdir_small | 18718.775 | 18792.478 | ops/s |
| drive9_sync | readdir_large | 120109.963 | 137359.570 | ops/s |
| drive9_sync | stat | 35286.565 | 37483.082 | ops/s |
| drive9_sync | macro_git_clone | 5.960 | 5.973 | seconds |
| drive9_sync | macro_git_status_dirty | 0.232 | 0.282 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.034 | 0.036 | seconds |
| drive9_sync | macro_find_files | 0.039 | 0.040 | seconds |
| drive9_sync | macro_go_build | 0.315 | 0.389 | seconds |
