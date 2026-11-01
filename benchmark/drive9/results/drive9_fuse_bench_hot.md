# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj9dcseepzra184oqk7zZ`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 249.829 | 256.300 | MiB/s |
| bowl | sequential_read | 371.625 | 427.742 | MiB/s |
| bowl | random_read_4k | 6.731 | 6.742 | MiB/s |
| bowl | small_write_4k_fsync | 158.915 | 163.382 | ops/s |
| bowl | mkdir | 534.088 | 546.928 | ops/s |
| bowl | rename | 399.836 | 414.672 | ops/s |
| bowl | unlink | 482.269 | 506.018 | ops/s |
| bowl | readdir_small | 5922.152 | 6879.182 | ops/s |
| bowl | readdir_large | 12923.384 | 13111.495 | ops/s |
| bowl | stat | 72263.943 | 73518.965 | ops/s |
| bowl | macro_git_clone | 4.630 | 4.728 | seconds |
| bowl | macro_git_status_dirty | 0.168 | 0.171 | seconds |
| bowl | macro_git_diff_dirty | 0.053 | 0.093 | seconds |
| bowl | macro_find_files | 0.071 | 0.103 | seconds |
| bowl | macro_go_build | 0.291 | 0.304 | seconds |
| drive9_default | sequential_write | 246.312 | 435.881 | MiB/s |
| drive9_default | sequential_read | 2360.315 | 2372.759 | MiB/s |
| drive9_default | random_read_4k | 182.770 | 189.639 | MiB/s |
| drive9_default | small_write_4k_fsync | 1021.348 | 1052.046 | ops/s |
| drive9_default | mkdir | 249.725 | 250.577 | ops/s |
| drive9_default | rename | 93.272 | 94.927 | ops/s |
| drive9_default | unlink | 101.263 | 101.967 | ops/s |
| drive9_default | readdir_small | 121028.744 | 152705.905 | ops/s |
| drive9_default | readdir_large | 83904.922 | 87302.945 | ops/s |
| drive9_default | stat | 611469.704 | 612057.196 | ops/s |
| drive9_default | macro_git_clone | 1.891 | 1.906 | seconds |
| drive9_default | macro_git_status_dirty | 0.214 | 0.218 | seconds |
| drive9_default | macro_git_diff_dirty | 0.012 | 0.014 | seconds |
| drive9_default | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_default | macro_go_build | 0.114 | 0.123 | seconds |
| drive9_sync | sequential_write | 186.273 | 276.947 | MiB/s |
| drive9_sync | sequential_read | 836.052 | 871.261 | MiB/s |
| drive9_sync | random_read_4k | 4.596 | 4.660 | MiB/s |
| drive9_sync | small_write_4k_fsync | 146.115 | 146.857 | ops/s |
| drive9_sync | mkdir | 239.968 | 249.483 | ops/s |
| drive9_sync | rename | 55.227 | 55.958 | ops/s |
| drive9_sync | unlink | 56.100 | 56.210 | ops/s |
| drive9_sync | readdir_small | 247255.464 | 257828.798 | ops/s |
| drive9_sync | readdir_large | 256214.351 | 263180.845 | ops/s |
| drive9_sync | stat | 447660.838 | 462234.444 | ops/s |
| drive9_sync | macro_git_clone | 6.175 | 6.226 | seconds |
| drive9_sync | macro_git_status_dirty | 0.050 | 0.061 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.018 | 0.018 | seconds |
| drive9_sync | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_sync | macro_go_build | 0.168 | 0.168 | seconds |
