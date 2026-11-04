# Drive9 benchmark summary

- Schema: `drive9-bench/v1`
- Version: `b529a36f`
- Host: `iZrj9irkcbex24zbqogdapZ`
- Environment: `lan`
- Cache state: `hot`

| Target | Workload | Median | P95 | Unit |
|---|---|---:|---:|---|
| bowl | sequential_write | 177.281 | 279.787 | MiB/s |
| bowl | sequential_read | 7437.165 | 7468.483 | MiB/s |
| bowl | random_read_4k | 445.786 | 457.231 | MiB/s |
| bowl | small_write_4k_fsync | 334.347 | 339.784 | ops/s |
| bowl | mkdir | 925.569 | 946.998 | ops/s |
| bowl | rename | 1166.907 | 1186.598 | ops/s |
| bowl | unlink | 309.622 | 343.237 | ops/s |
| bowl | readdir_small | 346692.553 | 417509.505 | ops/s |
| bowl | readdir_large | 224756.982 | 230169.124 | ops/s |
| bowl | stat | 787057.005 | 801271.097 | ops/s |
| bowl | macro_git_clone | 1.098 | 1.154 | seconds |
| bowl | macro_git_status_dirty | 0.021 | 0.028 | seconds |
| bowl | macro_git_diff_dirty | 0.008 | 0.009 | seconds |
| bowl | macro_find_files | 0.003 | 0.003 | seconds |
| bowl | macro_go_build | 0.067 | 0.068 | seconds |
| drive9_default | sequential_write | 259.578 | 430.533 | MiB/s |
| drive9_default | sequential_read | 1897.127 | 1902.050 | MiB/s |
| drive9_default | random_read_4k | 179.251 | 179.348 | MiB/s |
| drive9_default | small_write_4k_fsync | 1025.135 | 1105.774 | ops/s |
| drive9_default | mkdir | 241.774 | 242.566 | ops/s |
| drive9_default | rename | 93.736 | 94.934 | ops/s |
| drive9_default | unlink | 99.467 | 99.909 | ops/s |
| drive9_default | readdir_small | 107198.371 | 146984.372 | ops/s |
| drive9_default | readdir_large | 71473.160 | 80982.311 | ops/s |
| drive9_default | stat | 619615.541 | 625200.806 | ops/s |
| drive9_default | macro_git_clone | 1.860 | 1.938 | seconds |
| drive9_default | macro_git_status_dirty | 0.146 | 0.214 | seconds |
| drive9_default | macro_git_diff_dirty | 0.014 | 0.016 | seconds |
| drive9_default | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_default | macro_go_build | 0.071 | 0.122 | seconds |
| drive9_sync | sequential_write | 198.267 | 257.969 | MiB/s |
| drive9_sync | sequential_read | 611.930 | 635.183 | MiB/s |
| drive9_sync | random_read_4k | 4.459 | 4.471 | MiB/s |
| drive9_sync | small_write_4k_fsync | 142.037 | 144.228 | ops/s |
| drive9_sync | mkdir | 241.280 | 241.891 | ops/s |
| drive9_sync | rename | 58.947 | 59.536 | ops/s |
| drive9_sync | unlink | 52.580 | 52.783 | ops/s |
| drive9_sync | readdir_small | 238882.983 | 248697.211 | ops/s |
| drive9_sync | readdir_large | 272152.388 | 281062.434 | ops/s |
| drive9_sync | stat | 18381.274 | 31389.164 | ops/s |
| drive9_sync | macro_git_clone | 6.172 | 6.178 | seconds |
| drive9_sync | macro_git_status_dirty | 0.061 | 0.064 | seconds |
| drive9_sync | macro_git_diff_dirty | 0.012 | 0.014 | seconds |
| drive9_sync | macro_find_files | 0.004 | 0.004 | seconds |
| drive9_sync | macro_go_build | 0.174 | 0.176 | seconds |
