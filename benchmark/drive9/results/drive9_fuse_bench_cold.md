# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj9ip6zekn1tbk2yerd8Z`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 359.913 | 360.377 | MiB/s |
| bowl | sequential_read | 262.008 | 618.407 | MiB/s |
| bowl | random_read_4k | 6.900 | 7.048 | MiB/s |
| bowl | small_write_4k_fsync | 245.699 | 249.039 | ops/s |
| bowl | mkdir | 521.312 | 527.712 | ops/s |
| bowl | rename | 441.114 | 441.275 | ops/s |
| bowl | unlink | 543.022 | 548.039 | ops/s |
| bowl | readdir_small | 6056.110 | 6200.382 | ops/s |
| bowl | readdir_large | 4675.370 | 4764.088 | ops/s |
| bowl | stat | 2939.482 | 3020.182 | ops/s |
| bowl | macro_git_clone | 3.787 | 3.842 | seconds |
| bowl | macro_git_status_dirty | 0.256 | 0.298 | seconds |
| bowl | macro_git_diff_dirty | 0.134 | 0.165 | seconds |
| bowl | macro_find_files | 0.082 | 0.091 | seconds |
| bowl | macro_go_build | 0.572 | 2.052 | seconds |
| drive9_default | sequential_write | 307.239 | 434.673 | MiB/s |
| drive9_default | sequential_read | 184.123 | 184.985 | MiB/s |
| drive9_default | random_read_4k | 2.773 | 2.777 | MiB/s |
| drive9_default | small_write_4k_fsync | 922.342 | 951.102 | ops/s |
| drive9_default | mkdir | 242.786 | 248.640 | ops/s |
| drive9_default | rename | 93.628 | 94.943 | ops/s |
| drive9_default | unlink | 98.475 | 99.655 | ops/s |
| drive9_default | readdir_small | 7085.369 | 7520.877 | ops/s |
| drive9_default | readdir_large | 45417.959 | 49459.803 | ops/s |
| drive9_default | stat | 20549.608 | 21037.180 | ops/s |
| drive9_default | macro_git_clone | 1.899 | 1.912 | seconds |
| drive9_default | macro_git_status_dirty | 1.813 | 1.886 | seconds |
| drive9_default | macro_git_diff_dirty | 0.783 | 0.796 | seconds |
| drive9_default | macro_find_files | 0.017 | 0.028 | seconds |
| drive9_default | macro_go_build | 0.374 | 0.385 | seconds |
| drive9_sync | sequential_write | 188.208 | 258.619 | MiB/s |
| drive9_sync | sequential_read | 57.382 | 57.864 | MiB/s |
| drive9_sync | random_read_4k | 5.054 | 5.070 | MiB/s |
| drive9_sync | small_write_4k_fsync | 145.044 | 145.621 | ops/s |
| drive9_sync | mkdir | 243.529 | 249.358 | ops/s |
| drive9_sync | rename | 60.500 | 60.946 | ops/s |
| drive9_sync | unlink | 52.674 | 52.857 | ops/s |
| drive9_sync | readdir_small | 12542.952 | 13874.423 | ops/s |
| drive9_sync | readdir_large | 120155.712 | 121173.298 | ops/s |
| drive9_sync | stat | 36908.020 | 42493.161 | ops/s |
| drive9_sync | macro_git_clone | 6.172 | 6.225 | seconds |
| drive9_sync | macro_git_status_dirty | 0.250 | 0.286 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.100 | 0.513 | seconds |
| drive9_sync | macro_find_files | 0.046 | 0.073 | seconds |
| drive9_sync | macro_go_build | 0.344 | 0.440 | seconds |
