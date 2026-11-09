# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj90x3rcwcyduv529x02Z`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | macro_find_files | 0.058 | 0.062 | seconds |
| bowl | macro_git_clone | 5.587 | 5.630 | seconds |
| bowl | macro_git_diff_dirty | 0.066 | 0.076 | seconds |
| bowl | macro_git_status_dirty | 0.136 | 0.140 | seconds |
| bowl | macro_go_build | 0.476 | 1.927 | seconds |
| bowl | mkdir | 607.889 | 635.919 | ops/s |
| bowl | random_read_4k | 1.304 | 1.324 | MiB/s |
| bowl | readdir_large | 169272.046 | 170669.707 | ops/s |
| bowl | readdir_small | 19435.666 | 23881.757 | ops/s |
| bowl | rename | 390.350 | 391.698 | ops/s |
| bowl | sequential_read | 487.531 | 492.728 | MiB/s |
| bowl | sequential_write | 186.860 | 202.937 | MiB/s |
| bowl | small_write_4k_fsync | 136.666 | 141.112 | ops/s |
| bowl | stat | 55818.404 | 59882.147 | ops/s |
| bowl | unlink | 349.167 | 410.934 | ops/s |
| drive9_default | macro_find_files | 0.011 | 0.013 | seconds |
| drive9_default | macro_git_clone | 1.994 | 1.997 | seconds |
| drive9_default | macro_git_diff_dirty | 0.860 | 0.882 | seconds |
| drive9_default | macro_git_status_dirty | 1.722 | 1.781 | seconds |
| drive9_default | macro_go_build | 0.414 | 0.419 | seconds |
| drive9_default | mkdir | 215.217 | 215.740 | ops/s |
| drive9_default | random_read_4k | 2.149 | 2.268 | MiB/s |
| drive9_default | readdir_large | 44764.156 | 45089.060 | ops/s |
| drive9_default | readdir_small | 10022.792 | 10713.233 | ops/s |
| drive9_default | rename | 85.236 | 86.337 | ops/s |
| drive9_default | sequential_read | 61.752 | 539.199 | MiB/s |
| drive9_default | sequential_write | 655.681 | 665.725 | MiB/s |
| drive9_default | small_write_4k_fsync | 2149.998 | 2186.635 | ops/s |
| drive9_default | stat | 12214.026 | 12825.012 | ops/s |
| drive9_default | unlink | 89.750 | 91.565 | ops/s |
| drive9_sync | macro_find_files | 0.067 | 0.089 | seconds |
| drive9_sync | macro_git_clone | 7.152 | 7.153 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.715 | 0.722 | seconds |
| drive9_sync | macro_git_status_dirty | 0.266 | 0.359 | seconds |
| drive9_sync | macro_go_build | 0.343 | 0.448 | seconds |
| drive9_sync | mkdir | 208.108 | 209.165 | ops/s |
| drive9_sync | random_read_4k | 0.612 | 0.617 | MiB/s |
| drive9_sync | readdir_large | 21683.793 | 21727.863 | ops/s |
| drive9_sync | readdir_small | 23651.901 | 23763.778 | ops/s |
| drive9_sync | rename | 51.099 | 51.300 | ops/s |
| drive9_sync | sequential_read | 53.281 | 53.434 | MiB/s |
| drive9_sync | sequential_write | 346.444 | 348.052 | MiB/s |
| drive9_sync | small_write_4k_fsync | 112.294 | 114.830 | ops/s |
| drive9_sync | stat | 72.680 | 72.903 | ops/s |
| drive9_sync | unlink | 33.433 | 33.666 | ops/s |
