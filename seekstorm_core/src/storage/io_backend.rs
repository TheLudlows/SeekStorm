//! 跨平台异步文件 I/O 抽象。
//!
//! 设计取舍（Phase 1）：
//! - 使用 `std::fs::File` + `tokio::sync::Mutex` 缓存文件句柄，inline 执行阻塞 I/O。
//!   短临界区（单块 4 KiB 读写）下阻塞 executor 的代价可接受。
//! - Linux 上的 `IoUringBackend` 留作占位，后续阶段接入 `tokio-epoll-uring`。

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;

/// I/O 后端抽象。所有上层存储模块（WAL、SSTable、Manifest）均通过此 trait 访问磁盘，
/// 便于在不同平台与不同 I/O 后端（阻塞 fs / io_uring）间切换。
#[async_trait]
pub trait IoBackend: Send + Sync {
    /// 从 `path` 的 `offset` 处读取字节到 `buf`，返回实际读取字节数。
    async fn read_at(&self, path: &Path, offset: u64, buf: &mut [u8]) -> Result<usize>;

    /// 将 `buf` 写入 `path` 的 `offset` 处，返回实际写入字节数。
    /// 若文件不存在则创建。
    async fn write_at(&self, path: &Path, offset: u64, buf: &[u8]) -> Result<usize>;

    /// 将 `path` 的内核页缓存刷盘。
    async fn fsync(&self, path: &Path) -> Result<()>;

    /// 创建空文件。若已存在则截断为 0。
    async fn create_file(&self, path: &Path) -> Result<()>;

    /// 删除文件。若不存在则返回 `Ok(())`。
    async fn delete_file(&self, path: &Path) -> Result<()>;

    /// 返回文件当前大小（字节）。
    async fn file_size(&self, path: &Path) -> Result<u64>;

    /// 将目录的目录项变更刷盘（用于保证 create/rename 持久化）。
    async fn sync_dir(&self, path: &Path) -> Result<()>;
}

/// I/O 后端类型。
#[derive(Debug, Clone, Copy)]
pub enum IoBackendKind {
    /// 跨平台阻塞 fs（tokio::fs + spawn_blocking 等价物）。
    AsyncFs,
    /// Linux io_uring。
    #[cfg(target_os = "linux")]
    IoUring,
}

/// 选择 I/O 后端。
pub fn select_backend(kind: IoBackendKind) -> Arc<dyn IoBackend> {
    match kind {
        IoBackendKind::AsyncFs => Arc::new(AsyncFsBackend::new()),
        #[cfg(target_os = "linux")]
        IoBackendKind::IoUring => Arc::new(IoUringBackend::new()),
    }
}

/// 跨平台阻塞 fs 后端实现。
///
/// 通过 `tokio::sync::Mutex<HashMap<PathBuf, Arc<std::sync::Mutex<File>>>>` 缓存文件句柄，
/// 避免每次 I/O 都重新 open。句柄以 `read+write+create` 模式打开，可同时支持读写。
pub struct AsyncFsBackend {
    files: Mutex<HashMap<PathBuf, Arc<std::sync::Mutex<std::fs::File>>>>,
}

impl AsyncFsBackend {
    pub fn new() -> Self {
        Self {
            files: Mutex::new(HashMap::new()),
        }
    }

    async fn get_or_open(&self, path: &Path) -> Result<Arc<std::sync::Mutex<std::fs::File>>> {
        let mut cache = self.files.lock().await;
        if let Some(f) = cache.get(path) {
            return Ok(f.clone());
        }
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map_err(|e| anyhow::anyhow!("open {:?} failed: {}", path, e))?;
        let arc = Arc::new(std::sync::Mutex::new(f));
        cache.insert(path.to_path_buf(), arc.clone());
        Ok(arc)
    }

    async fn invalidate(&self, path: &Path) {
        let mut cache = self.files.lock().await;
        cache.remove(path);
    }
}

impl Default for AsyncFsBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IoBackend for AsyncFsBackend {
    async fn read_at(&self, path: &Path, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let arc = self.get_or_open(path).await?;
        let mut f = arc.lock().map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        f.seek(SeekFrom::Start(offset))?;
        let mut total = 0;
        while total < buf.len() {
            let n = f.read(&mut buf[total..])?;
            if n == 0 {
                break;
            }
            total += n;
        }
        Ok(total)
    }

    async fn write_at(&self, path: &Path, offset: u64, buf: &[u8]) -> Result<usize> {
        let arc = self.get_or_open(path).await?;
        let mut f = arc.lock().map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(buf)?;
        Ok(buf.len())
    }

    async fn fsync(&self, path: &Path) -> Result<()> {
        let arc = self.get_or_open(path).await?;
        let f = arc.lock().map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        f.sync_all()?;
        Ok(())
    }

    async fn create_file(&self, path: &Path) -> Result<()> {
        self.invalidate(path).await;
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| anyhow::anyhow!("create {:?} failed: {}", path, e))?;
        Ok(())
    }

    async fn delete_file(&self, path: &Path) -> Result<()> {
        self.invalidate(path).await;
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(anyhow::anyhow!("delete {:?} failed: {}", path, e)),
        }
    }

    async fn file_size(&self, path: &Path) -> Result<u64> {
        let meta = std::fs::metadata(path)
            .map_err(|e| anyhow::anyhow!("stat {:?} failed: {}", path, e))?;
        Ok(meta.len())
    }

    async fn sync_dir(&self, path: &Path) -> Result<()> {
        // Windows 上目录 fsync 是 no-op；Unix 上需要刷目录元数据。
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::File::open(path)
                .map_err(|e| anyhow::anyhow!("open dir {:?} failed: {}", path, e))?;
            f.sync_all()?;
        }
        let _ = path;
        Ok(())
    }
}

/// Linux io_uring 后端占位。Phase 1 不实现具体逻辑，留给后续阶段接入 `tokio-epoll-uring`。
#[cfg(target_os = "linux")]
pub struct IoUringBackend;

#[cfg(target_os = "linux")]
impl IoUringBackend {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(target_os = "linux")]
#[async_trait]
impl IoBackend for IoUringBackend {
    async fn read_at(&self, path: &Path, offset: u64, buf: &mut [u8]) -> Result<usize> {
        // Phase 1 占位：直接委托 AsyncFsBackend。
        let backend = AsyncFsBackend::new();
        backend.read_at(path, offset, buf).await
    }

    async fn write_at(&self, path: &Path, offset: u64, buf: &[u8]) -> Result<usize> {
        let backend = AsyncFsBackend::new();
        backend.write_at(path, offset, buf).await
    }

    async fn fsync(&self, path: &Path) -> Result<()> {
        let backend = AsyncFsBackend::new();
        backend.fsync(path).await
    }

    async fn create_file(&self, path: &Path) -> Result<()> {
        let backend = AsyncFsBackend::new();
        backend.create_file(path).await
    }

    async fn delete_file(&self, path: &Path) -> Result<()> {
        let backend = AsyncFsBackend::new();
        backend.delete_file(path).await
    }

    async fn file_size(&self, path: &Path) -> Result<u64> {
        let backend = AsyncFsBackend::new();
        backend.file_size(path).await
    }

    async fn sync_dir(&self, path: &Path) -> Result<()> {
        let backend = AsyncFsBackend::new();
        backend.sync_dir(path).await
    }
}
