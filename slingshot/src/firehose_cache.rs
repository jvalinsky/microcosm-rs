use std::path::Path;
use crate::CachedRecord;
use foyer::{HybridCache, DirectFsDeviceOptions, Engine, HybridCacheBuilder};


pub async fn firehose_cache(dir: impl AsRef<Path>) -> Result<HybridCache<String, CachedRecord>, String> {
    let cache = HybridCacheBuilder::new()
        .with_name("firehose")
        .memory(64 * 2_usize.pow(20))
        .with_weighter(|k: &String, v| k.len() + std::mem::size_of_val(v))
        .storage(Engine::large())
        .with_device_options(DirectFsDeviceOptions::new(dir))
        .build()
        .await
        .map_err(|e| format!("foyer setup error: {e:?}"))?;
    Ok(cache)
}
