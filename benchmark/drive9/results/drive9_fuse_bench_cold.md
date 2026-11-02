# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj912d5rbcy2stqm8dwjZ`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 374.274 | 381.962 | MiB/s |
| bowl | sequential_read | 531.053 | 533.126 | MiB/s |
| bowl | random_read_4k | 6.916 | 7.404 | MiB/s |
| bowl | small_write_4k_fsync | 211.652 | 224.109 | ops/s |
| bowl | mkdir | 585.137 | 590.826 | ops/s |
| bowl | rename | 466.637 | 467.924 | ops/s |
| bowl | unlink | 559.877 | 584.791 | ops/s |
| bowl | readdir_small | 6701.544 | 7210.700 | ops/s |
| bowl | readdir_large | 13694.964 | 14858.161 | ops/s |
| bowl | stat | 3140.304 | 3144.086 | ops/s |
| bowl | macro_git_clone | 3.234 | 3.238 | seconds |
| bowl | macro_git_status_dirty | 0.305 | 0.313 | seconds |
| bowl | macro_git_diff_dirty | 0.122 | 0.207 | seconds |
| bowl | macro_find_files | 0.081 | 0.091 | seconds |
| bowl | macro_go_build | 0.629 | 2.021 | seconds |
| drive9_default | sequential_write | 324.496 | 443.481 | MiB/s |
| drive9_default | sequential_read | 184.118 | 184.912 | MiB/s |
| drive9_default | random_read_4k | 2.779 | 2.787 | MiB/s |
| drive9_default | small_write_4k_fsync | 1010.420 | 1025.963 | ops/s |
| drive9_default | mkdir | 260.193 | 261.130 | ops/s |
| drive9_default | rename | 99.933 | 100.522 | ops/s |
| drive9_default | unlink | 101.080 | 101.111 | ops/s |
| drive9_default | readdir_small | 6552.925 | 6971.796 | ops/s |
| drive9_default | readdir_large | 42682.951 | 43429.061 | ops/s |
| drive9_default | stat | 20482.837 | 21826.497 | ops/s |
| drive9_default | macro_git_clone | 1.864 | 1.864 | seconds |
| drive9_default | macro_git_status_dirty | 2.016 | 2.121 | seconds |
| drive9_default | macro_git_diff_dirty | 0.760 | 0.764 | seconds |
| drive9_default | macro_find_files | 0.016 | 0.027 | seconds |
| drive9_default | macro_go_build | 0.375 | 0.377 | seconds |
| drive9_sync | sequential_write | 189.903 | 256.660 | MiB/s |
| drive9_sync | sequential_read | 56.209 | 56.597 | MiB/s |
| drive9_sync | random_read_4k | 5.123 | 5.162 | MiB/s |
| drive9_sync | small_write_4k_fsync | 149.413 | 150.294 | ops/s |
| drive9_sync | mkdir | 256.567 | 257.997 | ops/s |
| drive9_sync | rename | 63.169 | 64.058 | ops/s |
| drive9_sync | unlink | 57.212 | 57.582 | ops/s |
| drive9_sync | readdir_small | 15299.187 | 16922.665 | ops/s |
| drive9_sync | readdir_large | 106676.270 | 127407.775 | ops/s |
| drive9_sync | stat | 39206.707 | 44409.200 | ops/s |
| drive9_sync | macro_git_clone | 5.871 | 5.945 | seconds |
| drive9_sync | macro_git_status_dirty | 0.267 | 0.307 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.034 | 0.037 | seconds |
| drive9_sync | macro_find_files | 0.041 | 0.042 | seconds |
| drive9_sync | macro_go_build | 0.332 | 0.336 | seconds |
