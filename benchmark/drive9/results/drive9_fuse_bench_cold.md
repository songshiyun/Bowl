# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj9dcseepzra184oqk7zZ`
- Environment: `lan`
- Cache state: `cold`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 242.690 | 249.028 | MiB/s |
| bowl | sequential_read | 861.358 | 911.436 | MiB/s |
| bowl | random_read_4k | 6.612 | 6.813 | MiB/s |
| bowl | small_write_4k_fsync | 154.091 | 157.311 | ops/s |
| bowl | mkdir | 451.878 | 452.041 | ops/s |
| bowl | rename | 410.610 | 417.699 | ops/s |
| bowl | unlink | 422.657 | 462.636 | ops/s |
| bowl | readdir_small | 4889.850 | 5486.866 | ops/s |
| bowl | readdir_large | 12771.123 | 13487.042 | ops/s |
| bowl | stat | 2848.195 | 2919.878 | ops/s |
| bowl | macro_git_clone | 4.660 | 4.665 | seconds |
| bowl | macro_git_status_dirty | 0.245 | 0.251 | seconds |
| bowl | macro_git_diff_dirty | 0.177 | 0.181 | seconds |
| bowl | macro_find_files | 0.099 | 0.100 | seconds |
| bowl | macro_go_build | 0.620 | 2.165 | seconds |
| drive9_default | sequential_write | 259.221 | 430.806 | MiB/s |
| drive9_default | sequential_read | 177.675 | 180.639 | MiB/s |
| drive9_default | random_read_4k | 2.829 | 3.033 | MiB/s |
| drive9_default | small_write_4k_fsync | 1023.804 | 1050.866 | ops/s |
| drive9_default | mkdir | 250.218 | 251.324 | ops/s |
| drive9_default | rename | 95.390 | 98.291 | ops/s |
| drive9_default | unlink | 99.543 | 100.005 | ops/s |
| drive9_default | readdir_small | 6745.222 | 7056.200 | ops/s |
| drive9_default | readdir_large | 42657.301 | 46231.355 | ops/s |
| drive9_default | stat | 21478.900 | 24317.359 | ops/s |
| drive9_default | macro_git_clone | 1.925 | 1.937 | seconds |
| drive9_default | macro_git_status_dirty | 1.794 | 1.802 | seconds |
| drive9_default | macro_git_diff_dirty | 0.774 | 0.775 | seconds |
| drive9_default | macro_find_files | 0.015 | 0.021 | seconds |
| drive9_default | macro_go_build | 0.365 | 0.385 | seconds |
| drive9_sync | sequential_write | 189.015 | 260.052 | MiB/s |
| drive9_sync | sequential_read | 83.412 | 83.695 | MiB/s |
| drive9_sync | random_read_4k | 5.086 | 5.091 | MiB/s |
| drive9_sync | small_write_4k_fsync | 146.120 | 150.042 | ops/s |
| drive9_sync | mkdir | 249.116 | 250.729 | ops/s |
| drive9_sync | rename | 58.027 | 61.387 | ops/s |
| drive9_sync | unlink | 56.147 | 56.409 | ops/s |
| drive9_sync | readdir_small | 14092.774 | 14812.097 | ops/s |
| drive9_sync | readdir_large | 127356.250 | 156989.928 | ops/s |
| drive9_sync | stat | 37380.855 | 44876.249 | ops/s |
| drive9_sync | macro_git_clone | 6.069 | 6.136 | seconds |
| drive9_sync | macro_git_status_dirty | 0.335 | 0.352 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.132 | 0.520 | seconds |
| drive9_sync | macro_find_files | 0.041 | 0.057 | seconds |
| drive9_sync | macro_go_build | 0.350 | 0.384 | seconds |
