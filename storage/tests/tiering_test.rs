use storage::hyper_index::HyperIndex;
use storage::repo::Repository;
use storage::tiering::{GpuRuntime, StorageProfile, StorageTier, ZeroCopyStrategy};
use tempfile::tempdir;

#[test]
fn default_profile_stays_cpu_first() {
    let index = HyperIndex::new();
    let capabilities = index.storage_capabilities();

    assert_eq!(capabilities.hot_tier, StorageTier::CpuMemory);
    assert_eq!(capabilities.zero_copy_strategy, ZeroCopyStrategy::Disabled);
    assert!(!capabilities.gpu_resident);
    assert!(capabilities.fallback_reason.is_none());
}

#[test]
fn gpu_first_profile_falls_back_without_gpu_runtime() {
    let profile = StorageProfile::gpu_first(8 * 1024 * 1024 * 1024);
    let index = HyperIndex::with_storage_profile(profile);
    let capabilities = index.storage_capabilities();

    assert_eq!(capabilities.hot_tier, StorageTier::CpuMemory);
    assert_eq!(capabilities.zero_copy_strategy, ZeroCopyStrategy::Disabled);
    assert!(!capabilities.gpu_resident);
    assert!(capabilities
        .fallback_reason
        .as_deref()
        .unwrap_or_default()
        .contains("GPU runtime is disabled"));
}

#[test]
fn gpu_first_profile_is_retained_when_mock_gpu_runtime_is_enabled() {
    let profile =
        StorageProfile::gpu_first(16 * 1024 * 1024 * 1024).with_gpu_runtime(GpuRuntime::Mock);
    let index = HyperIndex::with_storage_profile(profile);
    let capabilities = index.storage_capabilities();

    assert_eq!(capabilities.hot_tier, StorageTier::GpuVram);
    assert_eq!(capabilities.zero_copy_strategy, ZeroCopyStrategy::GpuDirect);
    assert!(capabilities.gpu_resident);
    assert_eq!(
        capabilities.vram_budget_bytes,
        Some(16 * 1024 * 1024 * 1024)
    );
}

#[tokio::test]
async fn repository_open_with_profile_preserves_effective_capabilities() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("tiering_repo.wal");
    let profile = StorageProfile::gpu_first(4 * 1024 * 1024 * 1024);

    let repo = Repository::open_with_profile(&wal_path, profile)
        .await
        .unwrap();
    let capabilities = repo.storage_capabilities();

    assert_eq!(capabilities.hot_tier, StorageTier::CpuMemory);
    assert_eq!(capabilities.zero_copy_strategy, ZeroCopyStrategy::Disabled);
    assert!(!capabilities.gpu_resident);
}
