# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj93vq1u38hlqnxjflpzZ`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | macro_find_files | 0.008 | 0.043 | seconds |
| bowl | macro_git_clone | 0.399 | 1.067 | seconds |
| bowl | macro_git_diff_dirty | 0.163 | 0.286 | seconds |
| bowl | macro_git_status_dirty | 0.199 | 0.401 | seconds |
| bowl | macro_go_build | 0.590 | 2.100 | seconds |
| bowl | mkdir | 3490.502 | 3604.378 | ops/s |
| bowl | random_read_4k | 7.262 | 7.431 | MiB/s |
| bowl | readdir_large | 234137.314 | 241518.882 | ops/s |
| bowl | readdir_small | 65134.911 | 71961.373 | ops/s |
| bowl | rename | 3062.261 | 3474.527 | ops/s |
| bowl | sequential_read | 1181.470 | 1325.613 | MiB/s |
| bowl | sequential_write | 1363.603 | 1365.223 | MiB/s |
| bowl | small_write_4k_fsync | 761.721 | 789.749 | ops/s |
| bowl | stat | 56560.369 | 60615.909 | ops/s |
| bowl | unlink | 3312.720 | 3447.960 | ops/s |
| drive9_default | macro_find_files | 0.010 | 0.010 | seconds |
| drive9_default | macro_git_clone | 1.912 | 1.981 | seconds |
| drive9_default | macro_git_diff_dirty | 0.801 | 0.807 | seconds |
| drive9_default | macro_git_status_dirty | 1.607 | 1.705 | seconds |
| drive9_default | macro_go_build | 0.397 | 0.400 | seconds |
| drive9_default | mkdir | 229.961 | 230.378 | ops/s |
| drive9_default | random_read_4k | 5.017 | 5.071 | MiB/s |
| drive9_default | readdir_large | 57487.962 | 61697.658 | ops/s |
| drive9_default | readdir_small | 10537.885 | 10754.981 | ops/s |
| drive9_default | rename | 88.527 | 91.680 | ops/s |
| drive9_default | sequential_read | 644.742 | 673.104 | MiB/s |
| drive9_default | sequential_write | 674.543 | 679.511 | MiB/s |
| drive9_default | small_write_4k_fsync | 2289.594 | 2290.614 | ops/s |
| drive9_default | stat | 13529.835 | 15088.746 | ops/s |
| drive9_default | unlink | 95.395 | 102.332 | ops/s |
| drive9_sync | macro_find_files | 0.049 | 0.050 | seconds |
| drive9_sync | macro_git_clone | 6.508 | 6.567 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.132 | 0.139 | seconds |
| drive9_sync | macro_git_status_dirty | 0.785 | 0.856 | seconds |
| drive9_sync | macro_go_build | 0.388 | 0.412 | seconds |
| drive9_sync | mkdir | 219.551 | 221.215 | ops/s |
| drive9_sync | random_read_4k | 5.577 | 5.662 | MiB/s |
| drive9_sync | readdir_large | 23026.218 | 155449.014 | ops/s |
| drive9_sync | readdir_small | 28970.099 | 29036.928 | ops/s |
| drive9_sync | rename | 54.357 | 54.753 | ops/s |
| drive9_sync | sequential_read | 57.910 | 57.918 | MiB/s |
| drive9_sync | sequential_write | 365.282 | 368.919 | MiB/s |
| drive9_sync | small_write_4k_fsync | 132.455 | 135.304 | ops/s |
| drive9_sync | stat | 87.188 | 87.233 | ops/s |
| drive9_sync | unlink | 38.723 | 38.869 | ops/s |
