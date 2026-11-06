# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj93j7f2ntjd6mgs48erZ`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 184.909 | 282.707 | MiB/s |
| bowl | sequential_read | 972.396 | 1015.342 | MiB/s |
| bowl | random_read_4k | 8.387 | 8.622 | MiB/s |
| bowl | small_write_4k_fsync | 571.112 | 607.190 | ops/s |
| bowl | mkdir | 3772.700 | 3953.633 | ops/s |
| bowl | rename | 3080.292 | 3272.275 | ops/s |
| bowl | unlink | 2920.271 | 2941.493 | ops/s |
| bowl | readdir_small | 11319.599 | 37984.809 | ops/s |
| bowl | readdir_large | 176172.960 | 184947.022 | ops/s |
| bowl | stat | 66981.865 | 67988.186 | ops/s |
| bowl | macro_git_clone | 0.774 | 0.794 | seconds |
| bowl | macro_git_status_dirty | 0.174 | 0.178 | seconds |
| bowl | macro_git_diff_dirty | 0.055 | 0.151 | seconds |
| bowl | macro_find_files | 0.017 | 0.028 | seconds |
| bowl | macro_go_build | 0.380 | 1.690 | seconds |
| drive9_default | sequential_write | 258.233 | 424.431 | MiB/s |
| drive9_default | sequential_read | 178.275 | 184.179 | MiB/s |
| drive9_default | random_read_4k | 2.885 | 6.020 | MiB/s |
| drive9_default | small_write_4k_fsync | 981.817 | 1030.000 | ops/s |
| drive9_default | mkdir | 246.575 | 251.744 | ops/s |
| drive9_default | rename | 95.711 | 98.824 | ops/s |
| drive9_default | unlink | 98.080 | 98.301 | ops/s |
| drive9_default | readdir_small | 6124.901 | 6587.687 | ops/s |
| drive9_default | readdir_large | 42323.428 | 45747.648 | ops/s |
| drive9_default | stat | 19756.707 | 21124.250 | ops/s |
| drive9_default | macro_git_clone | 1.910 | 1.935 | seconds |
| drive9_default | macro_git_status_dirty | 1.959 | 1.985 | seconds |
| drive9_default | macro_git_diff_dirty | 0.780 | 0.798 | seconds |
| drive9_default | macro_find_files | 0.030 | 0.032 | seconds |
| drive9_default | macro_go_build | 0.371 | 0.377 | seconds |
| drive9_sync | sequential_write | 199.761 | 252.042 | MiB/s |
| drive9_sync | sequential_read | 81.289 | 81.404 | MiB/s |
| drive9_sync | random_read_4k | 5.056 | 5.075 | MiB/s |
| drive9_sync | small_write_4k_fsync | 144.858 | 147.159 | ops/s |
| drive9_sync | mkdir | 246.999 | 247.487 | ops/s |
| drive9_sync | rename | 60.838 | 62.268 | ops/s |
| drive9_sync | unlink | 51.592 | 51.799 | ops/s |
| drive9_sync | readdir_small | 13650.293 | 14159.625 | ops/s |
| drive9_sync | readdir_large | 73663.588 | 143800.072 | ops/s |
| drive9_sync | stat | 38945.793 | 40243.611 | ops/s |
| drive9_sync | macro_git_clone | 6.130 | 6.145 | seconds |
| drive9_sync | macro_git_status_dirty | 0.302 | 0.310 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.556 | 0.564 | seconds |
| drive9_sync | macro_find_files | 0.052 | 0.054 | seconds |
| drive9_sync | macro_go_build | 0.376 | 0.448 | seconds |
