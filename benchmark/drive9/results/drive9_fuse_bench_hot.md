# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj9f3lh7wxbe9ef3t03hZ`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 352.120 | 363.789 | MiB/s |
| bowl | sequential_read | 567.305 | 706.033 | MiB/s |
| bowl | random_read_4k | 6.830 | 6.980 | MiB/s |
| bowl | small_write_4k_fsync | 195.952 | 206.070 | ops/s |
| bowl | mkdir | 525.373 | 528.070 | ops/s |
| bowl | rename | 393.680 | 402.968 | ops/s |
| bowl | unlink | 539.034 | 553.148 | ops/s |
| bowl | readdir_small | 7118.403 | 7727.686 | ops/s |
| bowl | readdir_large | 13269.621 | 14096.328 | ops/s |
| bowl | stat | 39664.722 | 42354.424 | ops/s |
| bowl | macro_git_clone | 3.470 | 3.661 | seconds |
| bowl | macro_git_status_dirty | 0.134 | 0.209 | seconds |
| bowl | macro_git_diff_dirty | 0.065 | 0.117 | seconds |
| bowl | macro_find_files | 0.083 | 0.086 | seconds |
| bowl | macro_go_build | 0.285 | 0.320 | seconds |
| drive9_default | sequential_write | 288.784 | 415.085 | MiB/s |
| drive9_default | sequential_read | 2050.404 | 2096.586 | MiB/s |
| drive9_default | random_read_4k | 167.988 | 168.226 | MiB/s |
| drive9_default | small_write_4k_fsync | 1046.534 | 1055.035 | ops/s |
| drive9_default | mkdir | 254.688 | 254.749 | ops/s |
| drive9_default | rename | 96.039 | 97.427 | ops/s |
| drive9_default | unlink | 106.635 | 106.731 | ops/s |
| drive9_default | readdir_small | 147733.400 | 159168.405 | ops/s |
| drive9_default | readdir_large | 87414.803 | 89747.773 | ops/s |
| drive9_default | stat | 649911.092 | 685993.407 | ops/s |
| drive9_default | macro_git_clone | 1.821 | 1.852 | seconds |
| drive9_default | macro_git_status_dirty | 0.210 | 0.216 | seconds |
| drive9_default | macro_git_diff_dirty | 0.012 | 0.015 | seconds |
| drive9_default | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_default | macro_go_build | 0.107 | 0.179 | seconds |
| drive9_sync | sequential_write | 190.266 | 261.344 | MiB/s |
| drive9_sync | sequential_read | 650.170 | 705.248 | MiB/s |
| drive9_sync | random_read_4k | 4.542 | 4.614 | MiB/s |
| drive9_sync | small_write_4k_fsync | 147.448 | 149.143 | ops/s |
| drive9_sync | mkdir | 247.883 | 251.041 | ops/s |
| drive9_sync | rename | 55.772 | 56.824 | ops/s |
| drive9_sync | unlink | 56.460 | 56.466 | ops/s |
| drive9_sync | readdir_small | 222375.414 | 226134.234 | ops/s |
| drive9_sync | readdir_large | 289600.392 | 291427.996 | ops/s |
| drive9_sync | stat | 475846.507 | 483327.156 | ops/s |
| drive9_sync | macro_git_clone | 6.072 | 6.087 | seconds |
| drive9_sync | macro_git_status_dirty | 0.051 | 0.054 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.012 | 0.018 | seconds |
| drive9_sync | macro_find_files | 0.003 | 0.004 | seconds |
| drive9_sync | macro_go_build | 0.172 | 0.187 | seconds |
