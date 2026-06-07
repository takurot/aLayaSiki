use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTier {
    CpuMemory,
    GpuVram,
    Nvme,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ZeroCopyStrategy {
    Disabled,
    MemoryMapped,
    GpuDirect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GpuRuntime {
    Disabled,
    Mock,
    Cuda,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageProfile {
    pub hot_tier: StorageTier,
    pub cold_tier: StorageTier,
    pub zero_copy_strategy: ZeroCopyStrategy,
    pub gpu_runtime: GpuRuntime,
    pub vram_budget_bytes: Option<u64>,
    pub spillback_to_cpu: bool,
}

impl StorageProfile {
    pub fn cpu_default() -> Self {
        Self {
            hot_tier: StorageTier::CpuMemory,
            cold_tier: StorageTier::Nvme,
            zero_copy_strategy: ZeroCopyStrategy::Disabled,
            gpu_runtime: GpuRuntime::Disabled,
            vram_budget_bytes: None,
            spillback_to_cpu: true,
        }
    }

    pub fn gpu_first(vram_budget_bytes: u64) -> Self {
        Self {
            hot_tier: StorageTier::GpuVram,
            cold_tier: StorageTier::CpuMemory,
            zero_copy_strategy: ZeroCopyStrategy::GpuDirect,
            gpu_runtime: GpuRuntime::Disabled,
            vram_budget_bytes: Some(vram_budget_bytes),
            spillback_to_cpu: true,
        }
    }

    pub fn with_gpu_runtime(mut self, gpu_runtime: GpuRuntime) -> Self {
        self.gpu_runtime = gpu_runtime;
        self
    }

    pub fn with_zero_copy_strategy(mut self, zero_copy_strategy: ZeroCopyStrategy) -> Self {
        self.zero_copy_strategy = zero_copy_strategy;
        self
    }

    pub fn resolve_capabilities(&self) -> StorageCapabilities {
        let gpu_resident =
            self.hot_tier == StorageTier::GpuVram && self.gpu_runtime != GpuRuntime::Disabled;

        if self.hot_tier == StorageTier::GpuVram && !gpu_resident {
            return StorageCapabilities {
                hot_tier: StorageTier::CpuMemory,
                zero_copy_strategy: ZeroCopyStrategy::Disabled,
                gpu_resident: false,
                vram_budget_bytes: self.vram_budget_bytes,
                fallback_reason: Some("GPU runtime is disabled; falling back to CPU memory".into()),
            };
        }

        let zero_copy_strategy = match (self.zero_copy_strategy, self.hot_tier) {
            (ZeroCopyStrategy::GpuDirect, StorageTier::GpuVram) => ZeroCopyStrategy::GpuDirect,
            (ZeroCopyStrategy::GpuDirect, _) => ZeroCopyStrategy::Disabled,
            (other, _) => other,
        };

        StorageCapabilities {
            hot_tier: self.hot_tier,
            zero_copy_strategy,
            gpu_resident,
            vram_budget_bytes: self.vram_budget_bytes,
            fallback_reason: None,
        }
    }
}

impl Default for StorageProfile {
    fn default() -> Self {
        Self::cpu_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageCapabilities {
    pub hot_tier: StorageTier,
    pub zero_copy_strategy: ZeroCopyStrategy,
    pub gpu_resident: bool,
    pub vram_budget_bytes: Option<u64>,
    pub fallback_reason: Option<String>,
}
