# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj95goqmagl76j2rt5r5Z`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | macro_find_files | 0.014 | 0.026 | seconds |
| bowl | macro_git_clone | 0.760 | 0.784 | seconds |
| bowl | macro_git_diff_dirty | 0.100 | 0.106 | seconds |
| bowl | macro_git_status_dirty | 0.112 | 0.166 | seconds |
| bowl | macro_go_build | 0.332 | 1.781 | seconds |
| bowl | mkdir | 3146.572 | 3804.523 | ops/s |
| bowl | random_read_4k | 6.744 | 6.854 | MiB/s |
| bowl | readdir_large | 139715.695 | 148477.073 | ops/s |
| bowl | readdir_small | 18188.002 | 18941.436 | ops/s |
| bowl | rename | 3000.246 | 3194.014 | ops/s |
| bowl | sequential_read | 1073.559 | 1113.036 | MiB/s |
| bowl | sequential_write | 1207.106 | 1226.849 | MiB/s |
| bowl | small_write_4k_fsync | 754.897 | 770.711 | ops/s |
| bowl | stat | 51930.625 | 52140.929 | ops/s |
| bowl | unlink | 3084.787 | 3197.459 | ops/s |
| drive9_default | macro_find_files | 0.011 | 0.024 | seconds |
| drive9_default | macro_git_clone | 1.976 | 1.993 | seconds |
| drive9_default | macro_git_diff_dirty | 0.828 | 0.848 | seconds |
| drive9_default | macro_git_status_dirty | 1.751 | 1.817 | seconds |
| drive9_default | macro_go_build | 0.418 | 0.429 | seconds |
| drive9_default | mkdir | 213.537 | 215.771 | ops/s |
| drive9_default | random_read_4k | 4.686 | 4.837 | MiB/s |
| drive9_default | readdir_large | 60239.970 | 61292.151 | ops/s |
| drive9_default | readdir_small | 10587.164 | 11001.836 | ops/s |
| drive9_default | rename | 84.206 | 85.934 | ops/s |
| drive9_default | sequential_read | 535.321 | 562.329 | MiB/s |
| drive9_default | sequential_write | 656.757 | 664.936 | MiB/s |
| drive9_default | small_write_4k_fsync | 2042.146 | 2115.399 | ops/s |
| drive9_default | stat | 12938.992 | 14234.225 | ops/s |
| drive9_default | unlink | 92.172 | 93.329 | ops/s |
| drive9_sync | macro_find_files | 0.070 | 0.071 | seconds |
| drive9_sync | macro_git_clone | 6.967 | 6.997 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.717 | 0.729 | seconds |
| drive9_sync | macro_git_status_dirty | 0.298 | 0.303 | seconds |
| drive9_sync | macro_go_build | 0.308 | 0.313 | seconds |
| drive9_sync | mkdir | 207.160 | 208.504 | ops/s |
| drive9_sync | random_read_4k | 5.185 | 5.217 | MiB/s |
| drive9_sync | readdir_large | 21651.271 | 21808.993 | ops/s |
| drive9_sync | readdir_small | 27488.462 | 28691.913 | ops/s |
| drive9_sync | rename | 51.104 | 51.611 | ops/s |
| drive9_sync | sequential_read | 51.427 | 51.598 | MiB/s |
| drive9_sync | sequential_write | 334.312 | 335.939 | MiB/s |
| drive9_sync | small_write_4k_fsync | 124.219 | 127.145 | ops/s |
| drive9_sync | stat | 73.255 | 73.624 | ops/s |
| drive9_sync | unlink | 34.993 | 35.022 | ops/s |
