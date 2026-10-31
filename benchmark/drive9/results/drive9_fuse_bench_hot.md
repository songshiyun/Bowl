# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj919o5nf211olqico6kZ`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 222.600 | 228.230 | MiB/s |
| bowl | sequential_read | 185.536 | 215.786 | MiB/s |
| bowl | random_read_4k | 5.192 | 5.531 | MiB/s |
| bowl | small_write_4k_fsync | 132.416 | 133.357 | ops/s |
| bowl | mkdir | 492.506 | 512.775 | ops/s |
| bowl | rename | 509.907 | 513.081 | ops/s |
| bowl | unlink | 474.142 | 484.783 | ops/s |
| bowl | readdir_small | 5283.015 | 6869.721 | ops/s |
| bowl | readdir_large | 7373.058 | 8333.691 | ops/s |
| bowl | stat | 81730.142 | 93123.682 | ops/s |
| bowl | macro_git_clone | 5.177 | 5.384 | seconds |
| bowl | macro_git_status_dirty | 0.145 | 0.215 | seconds |
| bowl | macro_git_diff_dirty | 0.041 | 0.043 | seconds |
| bowl | macro_find_files | 0.090 | 0.093 | seconds |
| bowl | macro_go_build | 0.395 | 0.400 | seconds |
| drive9_default | sequential_write | 247.540 | 427.789 | MiB/s |
| drive9_default | sequential_read | 2339.869 | 2378.701 | MiB/s |
| drive9_default | random_read_4k | 172.642 | 180.343 | MiB/s |
| drive9_default | small_write_4k_fsync | 996.018 | 1034.344 | ops/s |
| drive9_default | mkdir | 244.329 | 248.441 | ops/s |
| drive9_default | rename | 92.914 | 95.972 | ops/s |
| drive9_default | unlink | 99.276 | 99.936 | ops/s |
| drive9_default | readdir_small | 125187.782 | 156840.905 | ops/s |
| drive9_default | readdir_large | 77357.672 | 83386.337 | ops/s |
| drive9_default | stat | 676560.690 | 679421.543 | ops/s |
| drive9_default | macro_git_clone | 1.857 | 1.861 | seconds |
| drive9_default | macro_git_status_dirty | 0.217 | 0.219 | seconds |
| drive9_default | macro_git_diff_dirty | 0.013 | 0.013 | seconds |
| drive9_default | macro_find_files | 0.005 | 0.005 | seconds |
| drive9_default | macro_go_build | 0.113 | 0.117 | seconds |
| drive9_sync | sequential_write | 186.472 | 273.755 | MiB/s |
| drive9_sync | sequential_read | 893.071 | 906.870 | MiB/s |
| drive9_sync | random_read_4k | 4.839 | 4.866 | MiB/s |
| drive9_sync | small_write_4k_fsync | 143.000 | 143.550 | ops/s |
| drive9_sync | mkdir | 239.328 | 245.987 | ops/s |
| drive9_sync | rename | 58.865 | 59.087 | ops/s |
| drive9_sync | unlink | 51.708 | 51.857 | ops/s |
| drive9_sync | readdir_small | 233642.130 | 236695.018 | ops/s |
| drive9_sync | readdir_large | 225341.550 | 247411.147 | ops/s |
| drive9_sync | stat | 228492.044 | 369587.705 | ops/s |
| drive9_sync | macro_git_clone | 6.270 | 6.306 | seconds |
| drive9_sync | macro_git_status_dirty | 0.048 | 0.057 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.012 | 0.012 | seconds |
| drive9_sync | macro_find_files | 0.006 | 0.007 | seconds |
| drive9_sync | macro_go_build | 0.155 | 0.195 | seconds |
