# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj9f3lh7wxbe35hkyhyuZ`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 181.835 | 185.846 | MiB/s |
| bowl | sequential_read | 792.033 | 802.599 | MiB/s |
| bowl | random_read_4k | 7.339 | 7.439 | MiB/s |
| bowl | small_write_4k_fsync | 155.619 | 155.656 | ops/s |
| bowl | mkdir | 510.191 | 519.426 | ops/s |
| bowl | rename | 367.379 | 371.582 | ops/s |
| bowl | unlink | 589.464 | 597.305 | ops/s |
| bowl | readdir_small | 8137.229 | 9094.016 | ops/s |
| bowl | readdir_large | 13960.267 | 14393.278 | ops/s |
| bowl | stat | 39946.248 | 42479.442 | ops/s |
| bowl | macro_git_clone | 3.304 | 3.311 | seconds |
| bowl | macro_git_status_dirty | 0.104 | 0.228 | seconds |
| bowl | macro_git_diff_dirty | 0.038 | 0.109 | seconds |
| bowl | macro_find_files | 0.080 | 0.109 | seconds |
| bowl | macro_go_build | 0.231 | 0.278 | seconds |
| drive9_default | sequential_write | 288.148 | 430.470 | MiB/s |
| drive9_default | sequential_read | 2156.327 | 2193.810 | MiB/s |
| drive9_default | random_read_4k | 179.626 | 188.405 | MiB/s |
| drive9_default | small_write_4k_fsync | 1012.134 | 1028.402 | ops/s |
| drive9_default | mkdir | 254.827 | 255.170 | ops/s |
| drive9_default | rename | 97.542 | 98.376 | ops/s |
| drive9_default | unlink | 101.315 | 101.467 | ops/s |
| drive9_default | readdir_small | 141149.237 | 149705.080 | ops/s |
| drive9_default | readdir_large | 56662.598 | 83597.811 | ops/s |
| drive9_default | stat | 618468.968 | 679529.927 | ops/s |
| drive9_default | macro_git_clone | 1.850 | 1.869 | seconds |
| drive9_default | macro_git_status_dirty | 0.212 | 0.213 | seconds |
| drive9_default | macro_git_diff_dirty | 0.012 | 0.013 | seconds |
| drive9_default | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_default | macro_go_build | 0.113 | 0.119 | seconds |
| drive9_sync | sequential_write | 192.105 | 263.693 | MiB/s |
| drive9_sync | sequential_read | 646.735 | 675.788 | MiB/s |
| drive9_sync | random_read_4k | 4.858 | 4.879 | MiB/s |
| drive9_sync | small_write_4k_fsync | 143.657 | 143.918 | ops/s |
| drive9_sync | mkdir | 251.117 | 252.039 | ops/s |
| drive9_sync | rename | 55.466 | 55.841 | ops/s |
| drive9_sync | unlink | 56.129 | 56.145 | ops/s |
| drive9_sync | readdir_small | 198092.370 | 205190.865 | ops/s |
| drive9_sync | readdir_large | 102581.862 | 223029.274 | ops/s |
| drive9_sync | stat | 483721.326 | 497375.794 | ops/s |
| drive9_sync | macro_git_clone | 6.050 | 6.087 | seconds |
| drive9_sync | macro_git_status_dirty | 0.039 | 0.051 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.012 | 0.017 | seconds |
| drive9_sync | macro_find_files | 0.003 | 0.003 | seconds |
| drive9_sync | macro_go_build | 0.172 | 0.172 | seconds |
