use crate::CachedRecord;
use foyer::{DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder};
use std::path::Path;

pub async fn firehose_cache(
    cache_dir: impl AsRef<Path>,
) -> Result<HybridCache<String, CachedRecord>, String> {
    let cache = HybridCacheBuilder::new()
        .with_name("firehose")
        .memory(64 * 2_usize.pow(20))
        .with_weighter(|k: &String, v| k.len() + std::mem::size_of_val(v))
        .storage(Engine::large())
        .with_device_options(DirectFsDeviceOptions::new(cache_dir))
        .build()
        .await
        .map_err(|e| format!("foyer setup error: {e:?}"))?;
    Ok(cache)
}
