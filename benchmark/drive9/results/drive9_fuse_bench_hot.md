# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj9ip6zekn1tbk2yerd8Z`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 314.297 | 357.397 | MiB/s |
| bowl | sequential_read | 7177.728 | 7283.367 | MiB/s |
| bowl | random_read_4k | 349.892 | 363.975 | MiB/s |
| bowl | small_write_4k_fsync | 218.186 | 222.554 | ops/s |
| bowl | mkdir | 506.135 | 507.156 | ops/s |
| bowl | rename | 370.648 | 392.184 | ops/s |
| bowl | unlink | 523.409 | 573.720 | ops/s |
| bowl | readdir_small | 9230.972 | 9965.279 | ops/s |
| bowl | readdir_large | 4751.909 | 4778.843 | ops/s |
| bowl | stat | 37522.531 | 43277.598 | ops/s |
| bowl | macro_git_clone | 3.783 | 3.800 | seconds |
| bowl | macro_git_status_dirty | 0.105 | 0.111 | seconds |
| bowl | macro_git_diff_dirty | 0.033 | 0.055 | seconds |
| bowl | macro_find_files | 0.066 | 0.078 | seconds |
| bowl | macro_go_build | 0.243 | 0.250 | seconds |
| drive9_default | sequential_write | 258.779 | 431.225 | MiB/s |
| drive9_default | sequential_read | 1996.332 | 2147.769 | MiB/s |
| drive9_default | random_read_4k | 180.672 | 182.348 | MiB/s |
| drive9_default | small_write_4k_fsync | 907.606 | 921.997 | ops/s |
| drive9_default | mkdir | 248.166 | 248.769 | ops/s |
| drive9_default | rename | 92.729 | 93.254 | ops/s |
| drive9_default | unlink | 98.423 | 100.662 | ops/s |
| drive9_default | readdir_small | 136854.134 | 145231.667 | ops/s |
| drive9_default | readdir_large | 82191.015 | 90247.604 | ops/s |
| drive9_default | stat | 621981.060 | 637607.414 | ops/s |
| drive9_default | macro_git_clone | 1.879 | 1.926 | seconds |
| drive9_default | macro_git_status_dirty | 0.157 | 0.217 | seconds |
| drive9_default | macro_git_diff_dirty | 0.013 | 0.014 | seconds |
| drive9_default | macro_find_files | 0.004 | 0.005 | seconds |
| drive9_default | macro_go_build | 0.113 | 0.128 | seconds |
| drive9_sync | sequential_write | 187.643 | 263.148 | MiB/s |
| drive9_sync | sequential_read | 600.258 | 616.715 | MiB/s |
| drive9_sync | random_read_4k | 4.442 | 4.453 | MiB/s |
| drive9_sync | small_write_4k_fsync | 144.167 | 145.190 | ops/s |
| drive9_sync | mkdir | 240.960 | 241.086 | ops/s |
| drive9_sync | rename | 54.548 | 54.803 | ops/s |
| drive9_sync | unlink | 52.716 | 52.819 | ops/s |
| drive9_sync | readdir_small | 225227.762 | 240082.242 | ops/s |
| drive9_sync | readdir_large | 255121.045 | 272719.078 | ops/s |
| drive9_sync | stat | 23603.527 | 23701.765 | ops/s |
| drive9_sync | macro_git_clone | 6.312 | 6.347 | seconds |
| drive9_sync | macro_git_status_dirty | 0.063 | 0.070 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.013 | 0.018 | seconds |
| drive9_sync | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_sync | macro_go_build | 0.174 | 0.177 | seconds |
