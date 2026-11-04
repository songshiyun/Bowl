# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36`
- Host: `iZrj94b8ccceaoxjysslfyZ`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 39.944 | 40.319 | MiB/s |
| bowl | sequential_read | 17.520 | 17.600 | MiB/s |
| bowl | random_read_4k | 0.511 | 0.522 | MiB/s |
| bowl | small_write_4k_fsync | 35.430 | 35.769 | ops/s |
| bowl | mkdir | 72.724 | 73.956 | ops/s |
| bowl | rename | 57.863 | 58.552 | ops/s |
| bowl | unlink | 73.687 | 74.147 | ops/s |
| bowl | readdir_small | 285384.055 | 309533.753 | ops/s |
| bowl | readdir_large | 81171.058 | 108095.844 | ops/s |
| bowl | stat | 35819.416 | 46645.428 | ops/s |
| bowl | macro_git_clone | 27.689 | 27.804 | seconds |
| bowl | macro_git_status_dirty | 0.144 | 0.166 | seconds |
| bowl | macro_git_diff_dirty | 0.029 | 0.029 | seconds |
| bowl | macro_find_files | 0.008 | 0.008 | seconds |
| bowl | macro_go_build | 0.381 | 0.405 | seconds |
| drive9_default | sequential_write | 258.340 | 431.571 | MiB/s |
| drive9_default | sequential_read | 2042.988 | 2095.974 | MiB/s |
| drive9_default | random_read_4k | 183.421 | 185.981 | MiB/s |
| drive9_default | small_write_4k_fsync | 1012.449 | 1019.807 | ops/s |
| drive9_default | mkdir | 255.374 | 256.683 | ops/s |
| drive9_default | rename | 96.195 | 97.448 | ops/s |
| drive9_default | unlink | 100.999 | 102.004 | ops/s |
| drive9_default | readdir_small | 100404.631 | 116036.294 | ops/s |
| drive9_default | readdir_large | 85457.815 | 93109.040 | ops/s |
| drive9_default | stat | 628696.737 | 630229.823 | ops/s |
| drive9_default | macro_git_clone | 1.827 | 1.845 | seconds |
| drive9_default | macro_git_status_dirty | 0.141 | 0.147 | seconds |
| drive9_default | macro_git_diff_dirty | 0.012 | 0.016 | seconds |
| drive9_default | macro_find_files | 0.004 | 0.007 | seconds |
| drive9_default | macro_go_build | 0.106 | 0.113 | seconds |
| drive9_sync | sequential_write | 190.131 | 260.778 | MiB/s |
| drive9_sync | sequential_read | 714.555 | 720.119 | MiB/s |
| drive9_sync | random_read_4k | 4.524 | 4.574 | MiB/s |
| drive9_sync | small_write_4k_fsync | 145.320 | 146.875 | ops/s |
| drive9_sync | mkdir | 248.575 | 249.978 | ops/s |
| drive9_sync | rename | 55.139 | 55.711 | ops/s |
| drive9_sync | unlink | 56.212 | 56.583 | ops/s |
| drive9_sync | readdir_small | 218686.786 | 219875.411 | ops/s |
| drive9_sync | readdir_large | 222124.290 | 241252.459 | ops/s |
| drive9_sync | stat | 411207.206 | 444750.519 | ops/s |
| drive9_sync | macro_git_clone | 6.033 | 6.057 | seconds |
| drive9_sync | macro_git_status_dirty | 0.041 | 0.056 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.018 | 0.019 | seconds |
| drive9_sync | macro_find_files | 0.003 | 0.008 | seconds |
| drive9_sync | macro_go_build | 0.171 | 0.179 | seconds |
