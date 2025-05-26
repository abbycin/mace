## [0.0.7] 2025-05-26
### Changes
- `consolidation` will collect remote frames
- Simplify wal file cleaning
- Optimize branch misses by rearranging code layout

### Bug fixes
- Fix typo in `need_split`
- Fix `last_ckpt` init in `log.rs`
- Fix `use-after-free` issue in consolidate

## [0.0.6] 2025-05-25
### New Features
- Support key-value size up to half `buffer_size` (it may cause up to 7% degradation in read/write performance, we will try to optimize it in subsequent versions)

### Bug fixes
- Fix a `use-after-free` issue that may occur when manually drop db instance

## [0.0.5] 2025-05-16
### New Features
- Support `scalable-logging`