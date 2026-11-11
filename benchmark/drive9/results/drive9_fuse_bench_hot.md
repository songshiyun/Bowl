# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj980079ks7kdbsi3nu0Z`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | macro_find_files | 0.009 | 0.010 | seconds |
| bowl | macro_git_clone | 5.724 | 5.985 | seconds |
| bowl | macro_git_diff_dirty | 0.008 | 0.010 | seconds |
| bowl | macro_git_status_dirty | 0.029 | 0.031 | seconds |
| bowl | macro_go_build | 0.073 | 0.080 | seconds |
| bowl | mkdir | 484.465 | 698.534 | ops/s |
| bowl | random_read_4k | 540.248 | 541.381 | MiB/s |
| bowl | readdir_large | 1063198.654 | 1077065.895 | ops/s |
| bowl | readdir_small | 376541.467 | 421375.821 | ops/s |
| bowl | rename | 2526.404 | 2810.546 | ops/s |
| bowl | sequential_read | 7827.433 | 7929.854 | MiB/s |
| bowl | sequential_write | 1120.720 | 1152.235 | MiB/s |
| bowl | small_write_4k_fsync | 151.858 | 153.006 | ops/s |
| bowl | stat | 740831.836 | 748437.489 | ops/s |
| bowl | unlink | 2726.978 | 2857.848 | ops/s |
| drive9_default | macro_find_files | 0.004 | 0.005 | seconds |
| drive9_default | macro_git_clone | 1.966 | 1.972 | seconds |
| drive9_default | macro_git_diff_dirty | 0.013 | 0.023 | seconds |
| drive9_default | macro_git_status_dirty | 0.219 | 0.220 | seconds |
| drive9_default | macro_go_build | 0.118 | 0.119 | seconds |
| drive9_default | mkdir | 229.881 | 230.670 | ops/s |
| drive9_default | random_read_4k | 153.251 | 161.323 | MiB/s |
| drive9_default | readdir_large | 80094.332 | 85418.161 | ops/s |
| drive9_default | readdir_small | 110909.010 | 133049.557 | ops/s |
| drive9_default | rename | 89.254 | 89.550 | ops/s |
| drive9_default | sequential_read | 256.265 | 1750.758 | MiB/s |
| drive9_default | sequential_write | 627.214 | 644.848 | MiB/s |
| drive9_default | small_write_4k_fsync | 2170.703 | 2239.720 | ops/s |
| drive9_default | stat | 587325.286 | 665263.656 | ops/s |
| drive9_default | unlink | 96.045 | 96.393 | ops/s |
| drive9_sync | macro_find_files | 0.004 | 0.005 | seconds |
| drive9_sync | macro_git_clone | 6.794 | 6.848 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.013 | 0.013 | seconds |
| drive9_sync | macro_git_status_dirty | 0.044 | 0.065 | seconds |
| drive9_sync | macro_go_build | 0.119 | 0.159 | seconds |
| drive9_sync | mkdir | 227.570 | 230.036 | ops/s |
| drive9_sync | random_read_4k | 2.721 | 2.795 | MiB/s |
| drive9_sync | readdir_large | 215641.886 | 222904.489 | ops/s |
| drive9_sync | readdir_small | 198495.405 | 212583.472 | ops/s |
| drive9_sync | rename | 56.391 | 57.195 | ops/s |
| drive9_sync | sequential_read | 606.010 | 626.421 | MiB/s |
| drive9_sync | sequential_write | 346.908 | 349.659 | MiB/s |
| drive9_sync | small_write_4k_fsync | 137.977 | 138.129 | ops/s |
| drive9_sync | stat | 504475.708 | 548826.731 | ops/s |
| drive9_sync | unlink | 44.797 | 46.584 | ops/s |
