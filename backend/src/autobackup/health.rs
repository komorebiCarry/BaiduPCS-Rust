//! 自动备份模块的健康检查
//!
//! 从 `manager.rs` 抽出，包含：
//! - `HealthCheckResult` 数据结构
//! - `check_health()` 异步函数（实际 TCP 探测 + 磁盘空间检查）
//!
//! 设计原则：所有检查在 3 秒内返回，失败不阻塞主流程。

use std::path::Path;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

/// 健康检查结果
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    /// 数据库连接状态
    pub database_ok: bool,
    /// 加密密钥状态
    pub encryption_key_ok: bool,
    /// 文件监听状态
    pub file_watcher_ok: bool,
    /// 网络连接状态（TCP 可达百度服务器）
    pub network_ok: bool,
    /// 磁盘空间状态
    pub disk_space_ok: bool,
}

impl HealthCheckResult {
    /// 整体是否健康（任何关键项失败则 false）
    pub fn is_healthy(&self) -> bool {
        // network_ok 与 disk_space_ok 是软失败，警告即可；其余是硬失败
        self.database_ok && self.encryption_key_ok && self.file_watcher_ok
    }
}

/// 探测百度网盘主机的 TCP 可达性（443 端口，3 秒超时）。
///
/// 用 TCP 而非 HTTPS 的好处：不需要 TLS 握手与证书校验，对外网变更零依赖；
/// 在受限网络下 3 秒内能给出"网络不通"的明确信号。
pub async fn check_network_reachable() -> bool {
    // 备选：pan.baidu.com / yun.baidu.com / pcs.baidu.com
    let targets: &[&str] = &["pan.baidu.com:443", "pcs.baidu.com:443"];
    for target in targets {
        match timeout(Duration::from_secs(3), TcpStream::connect(target)).await {
            Ok(Ok(_)) => return true,
            _ => continue, // 下一个 host 试
        }
    }
    false
}

/// 检查给定临时目录所在文件系统是否可写（最简形式：目录存在）。
///
/// 更完整的实现可调用 `fs2::available_space` 查剩余字节数并与阈值比较，
/// 但 `fs2` 不在依赖中。后续若引入再增强。
pub fn check_disk_space(temp_dir: &Path) -> bool {
    temp_dir.exists() && temp_dir.is_dir()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[tokio::test]
    async fn test_check_network_reachable_returns_bool() {
        // 网络可达性取决于环境，仅断言返回 bool 不 panic
        let r = check_network_reachable().await;
        let _ = r; // 静默 ignore
    }

    #[test]
    fn test_check_disk_space_on_existing_dir() {
        let tmp = env::temp_dir();
        assert!(check_disk_space(&tmp));
    }

    #[test]
    fn test_check_disk_space_on_missing_dir() {
        let missing = env::temp_dir().join("definitely-not-exists-12345");
        assert!(!check_disk_space(&missing));
    }

    #[test]
    fn test_health_is_healthy() {
        let h = HealthCheckResult {
            database_ok: true,
            encryption_key_ok: true,
            file_watcher_ok: true,
            network_ok: false,    // 软失败
            disk_space_ok: false, // 软失败
        };
        assert!(h.is_healthy(), "network/disk 是软失败，不应影响整体健康");

        let h = HealthCheckResult {
            database_ok: false,
            encryption_key_ok: true,
            file_watcher_ok: true,
            network_ok: true,
            disk_space_ok: true,
        };
        assert!(!h.is_healthy(), "database 失败 → 不健康");
    }
}
