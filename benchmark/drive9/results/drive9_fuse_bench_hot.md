# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj93j7f2ntjd6mgs48erZ`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 182.227 | 273.889 | MiB/s |
| bowl | sequential_read | 8162.547 | 8269.280 | MiB/s |
| bowl | random_read_4k | 467.739 | 476.687 | MiB/s |
| bowl | small_write_4k_fsync | 605.630 | 625.723 | ops/s |
| bowl | mkdir | 3291.523 | 3642.323 | ops/s |
| bowl | rename | 3048.520 | 3127.578 | ops/s |
| bowl | unlink | 3070.023 | 3087.570 | ops/s |
| bowl | readdir_small | 481602.774 | 487742.186 | ops/s |
| bowl | readdir_large | 1323171.377 | 1368655.598 | ops/s |
| bowl | stat | 749038.983 | 804654.534 | ops/s |
| bowl | macro_git_clone | 0.739 | 0.741 | seconds |
| bowl | macro_git_status_dirty | 0.029 | 0.051 | seconds |
| bowl | macro_git_diff_dirty | 0.024 | 0.046 | seconds |
| bowl | macro_find_files | 0.003 | 0.003 | seconds |
| bowl | macro_go_build | 0.037 | 0.056 | seconds |
| drive9_default | sequential_write | 269.822 | 440.899 | MiB/s |
| drive9_default | sequential_read | 2283.917 | 2295.786 | MiB/s |
| drive9_default | random_read_4k | 178.642 | 181.516 | MiB/s |
| drive9_default | small_write_4k_fsync | 928.156 | 975.076 | ops/s |
| drive9_default | mkdir | 250.779 | 251.069 | ops/s |
| drive9_default | rename | 92.705 | 93.703 | ops/s |
| drive9_default | unlink | 99.158 | 99.444 | ops/s |
| drive9_default | readdir_small | 139697.973 | 155382.510 | ops/s |
| drive9_default | readdir_large | 58925.845 | 59906.049 | ops/s |
| drive9_default | stat | 620852.704 | 642356.063 | ops/s |
| drive9_default | macro_git_clone | 1.868 | 1.918 | seconds |
| drive9_default | macro_git_status_dirty | 0.219 | 0.219 | seconds |
| drive9_default | macro_git_diff_dirty | 0.012 | 0.012 | seconds |
| drive9_default | macro_find_files | 0.004 | 0.005 | seconds |
| drive9_default | macro_go_build | 0.113 | 0.116 | seconds |
| drive9_sync | sequential_write | 192.743 | 261.689 | MiB/s |
| drive9_sync | sequential_read | 840.691 | 860.944 | MiB/s |
| drive9_sync | random_read_4k | 4.578 | 4.592 | MiB/s |
| drive9_sync | small_write_4k_fsync | 143.346 | 146.457 | ops/s |
| drive9_sync | mkdir | 238.653 | 239.222 | ops/s |
| drive9_sync | rename | 58.036 | 59.254 | ops/s |
| drive9_sync | unlink | 48.495 | 51.663 | ops/s |
| drive9_sync | readdir_small | 183837.047 | 218757.736 | ops/s |
| drive9_sync | readdir_large | 213627.011 | 260188.569 | ops/s |
| drive9_sync | stat | 127569.375 | 500493.612 | ops/s |
| drive9_sync | macro_git_clone | 6.278 | 6.423 | seconds |
| drive9_sync | macro_git_status_dirty | 0.052 | 0.056 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.013 | 0.019 | seconds |
| drive9_sync | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_sync | macro_go_build | 0.148 | 0.157 | seconds |
