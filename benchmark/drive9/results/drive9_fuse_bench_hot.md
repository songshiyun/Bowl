# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj95goqmagl76j2rt5r5Z`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | macro_find_files | 0.003 | 0.003 | seconds |
| bowl | macro_git_clone | 0.764 | 0.828 | seconds |
| bowl | macro_git_diff_dirty | 0.021 | 0.037 | seconds |
| bowl | macro_git_status_dirty | 0.021 | 0.042 | seconds |
| bowl | macro_go_build | 0.060 | 0.076 | seconds |
| bowl | mkdir | 3612.056 | 3686.848 | ops/s |
| bowl | random_read_4k | 443.136 | 463.913 | MiB/s |
| bowl | readdir_large | 1063485.851 | 1082362.501 | ops/s |
| bowl | readdir_small | 344109.702 | 462421.908 | ops/s |
| bowl | rename | 3554.261 | 3743.389 | ops/s |
| bowl | sequential_read | 7001.874 | 7313.837 | MiB/s |
| bowl | sequential_write | 1089.676 | 1112.057 | MiB/s |
| bowl | small_write_4k_fsync | 756.193 | 813.579 | ops/s |
| bowl | stat | 807608.316 | 811770.421 | ops/s |
| bowl | unlink | 3961.394 | 3999.214 | ops/s |
| drive9_default | macro_find_files | 0.004 | 0.005 | seconds |
| drive9_default | macro_git_clone | 1.980 | 1.991 | seconds |
| drive9_default | macro_git_diff_dirty | 0.013 | 0.014 | seconds |
| drive9_default | macro_git_status_dirty | 0.224 | 0.242 | seconds |
| drive9_default | macro_go_build | 0.117 | 0.132 | seconds |
| drive9_default | mkdir | 205.725 | 206.437 | ops/s |
| drive9_default | random_read_4k | 157.591 | 168.062 | MiB/s |
| drive9_default | readdir_large | 74870.104 | 83363.510 | ops/s |
| drive9_default | readdir_small | 112799.977 | 139537.196 | ops/s |
| drive9_default | rename | 81.922 | 82.680 | ops/s |
| drive9_default | sequential_read | 982.255 | 1823.573 | MiB/s |
| drive9_default | sequential_write | 691.491 | 696.816 | MiB/s |
| drive9_default | small_write_4k_fsync | 2030.987 | 2313.256 | ops/s |
| drive9_default | stat | 632160.002 | 670278.976 | ops/s |
| drive9_default | unlink | 87.119 | 87.249 | ops/s |
| drive9_sync | macro_find_files | 0.004 | 0.017 | seconds |
| drive9_sync | macro_git_clone | 7.018 | 7.053 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.013 | 0.013 | seconds |
| drive9_sync | macro_git_status_dirty | 0.041 | 0.064 | seconds |
| drive9_sync | macro_go_build | 0.119 | 0.123 | seconds |
| drive9_sync | mkdir | 206.917 | 209.608 | ops/s |
| drive9_sync | random_read_4k | 5.302 | 5.373 | MiB/s |
| drive9_sync | readdir_large | 240325.689 | 258298.611 | ops/s |
| drive9_sync | readdir_small | 212305.210 | 222437.764 | ops/s |
| drive9_sync | rename | 49.729 | 49.838 | ops/s |
| drive9_sync | sequential_read | 597.582 | 604.101 | MiB/s |
| drive9_sync | sequential_write | 336.878 | 344.180 | MiB/s |
| drive9_sync | small_write_4k_fsync | 126.759 | 127.719 | ops/s |
| drive9_sync | stat | 525362.947 | 562545.509 | ops/s |
| drive9_sync | unlink | 35.559 | 35.639 | ops/s |
