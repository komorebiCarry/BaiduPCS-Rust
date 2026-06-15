//! 通用阶梯式重试工具
//!
//! 针对百度网盘 `pcs/file`（locatedownload / locateupload）等接口偶发的
//! **临时风控** 错误做指数退避 + 随机抖动重试：
//! - `errno=8002`「参数异常,请更新最新版本客户端」
//! - `errno=9019`「need verify」
//!
//! 这类错误是临时性的（风控/需验证），过几秒到几十秒通常自动恢复；其余
//! 确定性错误（参数错误、文件不存在、鉴权失败、传输层失败等）不重试，立即
//! 向上返回，避免无谓等待。
//!
//! 退避序列：3s → 8s → 20s（每次叠加 ±20% 随机抖动，避免多任务同步重试
//! 形成"重试风暴"再次触发风控），最多重试 3 次（共 4 次尝试，最坏总等待
//! 约 31s）。

use std::future::Future;
use std::time::Duration;

use rand::Rng;
use tracing::warn;

/// 最大重试次数（不含首次尝试）。共 `1 + PCS_RETRY_MAX_RETRIES` 次尝试。
pub const PCS_RETRY_MAX_RETRIES: u32 = 3;

/// 退避基数序列（秒）：第 1/2/3 次重试前分别等待 3s / 8s / 20s。
const PCS_RETRY_BACKOFF_SECS: [u64; 3] = [3, 8, 20];

/// 抖动比例：在基数上叠加 ±20% 随机抖动。
const PCS_RETRY_JITTER_RATIO: f64 = 0.2;

/// 命中百度 **临时风控**（可重试）的 errno 列表。
const RETRYABLE_ERRNOS: [&str; 2] = ["8002", "9019"];

/// 判断错误是否为「百度临时风控」类（可重试）。
///
/// 现有 client 把百度错误码格式化进错误链（如 `百度 API 错误 8002: ...`、
/// `errno=9019, errmsg=...`），故按错误链文本（`{:#}`）匹配 errno 数字判定。
/// 为避免 `18002` / `90190` 等误伤，要求匹配到的 errno 前后均非数字。
pub fn is_baidu_riskcontrol_error(err: &anyhow::Error) -> bool {
    let msg = format!("{err:#}");
    RETRYABLE_ERRNOS
        .iter()
        .any(|code| message_has_errno(&msg, code))
}

/// 在 `msg` 中查找作为独立数字 token 出现的 `code`（前后字符均非 ASCII 数字）。
fn message_has_errno(msg: &str, code: &str) -> bool {
    let bytes = msg.as_bytes();
    let mut start = 0;
    while let Some(pos) = msg[start..].find(code) {
        let abs = start + pos;
        let before_ok = abs == 0 || !bytes[abs - 1].is_ascii_digit();
        let after_idx = abs + code.len();
        let after_ok = after_idx >= bytes.len() || !bytes[after_idx].is_ascii_digit();
        if before_ok && after_ok {
            return true;
        }
        start = abs + code.len();
    }
    false
}

/// 计算第 `attempt`（从 0 开始）次重试前的退避时长（含 ±20% 抖动）。
fn backoff_delay(attempt: u32) -> Duration {
    let base_secs = PCS_RETRY_BACKOFF_SECS
        .get(attempt as usize)
        .copied()
        .unwrap_or_else(|| *PCS_RETRY_BACKOFF_SECS.last().unwrap());
    let base_ms = (base_secs * 1000) as i64;
    let jitter_span = (base_ms as f64 * PCS_RETRY_JITTER_RATIO) as i64;
    let jitter = if jitter_span > 0 {
        rand::thread_rng().gen_range(-jitter_span..=jitter_span)
    } else {
        0
    };
    Duration::from_millis((base_ms + jitter).max(0) as u64)
}

/// 对返回 `anyhow::Result<T>` 的异步操作做阶梯式重试。
///
/// 仅当错误被 [`is_baidu_riskcontrol_error`] 判定为临时风控、且仍有剩余重试
/// 次数时，按 [`backoff_delay`] 等待后重试；其余情况立即返回。
///
/// `op_name` 仅用于日志标识。
pub async fn retry_pcs_riskcontrol<T, F, Fut>(op_name: &str, op: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    retry_inner(op_name, op, PCS_RETRY_MAX_RETRIES, backoff_delay).await
}

/// 重试核心逻辑。延迟时长通过 `delay_for(attempt)` 注入，便于测试以零延迟运行。
async fn retry_inner<T, F, Fut, D>(
    op_name: &str,
    mut op: F,
    max_retries: u32,
    delay_for: D,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
    D: Fn(u32) -> Duration,
{
    let mut attempt: u32 = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt < max_retries && is_baidu_riskcontrol_error(&e) {
                    let delay = delay_for(attempt);
                    warn!(
                        "{} 命中百度临时风控，{}ms 后重试（第 {}/{} 次）: {:#}",
                        op_name,
                        delay.as_millis(),
                        attempt + 1,
                        max_retries,
                        e
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                    continue;
                }
                return Err(e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn test_is_baidu_riskcontrol_error_matches_8002_9019() {
        let e1 = anyhow::anyhow!("百度 API 错误 8002: 参数异常,请更新最新版本客户端");
        let e2 = anyhow::anyhow!("filemetas 取 dlink 失败: errno=9019, errmsg=need verify");
        assert!(is_baidu_riskcontrol_error(&e1));
        assert!(is_baidu_riskcontrol_error(&e2));
    }

    #[test]
    fn test_is_baidu_riskcontrol_error_ignores_other_errnos() {
        let e = anyhow::anyhow!("百度 API 错误 -6: 鉴权失败");
        assert!(!is_baidu_riskcontrol_error(&e));
    }

    #[test]
    fn test_is_baidu_riskcontrol_error_no_substring_false_positive() {
        // 18002 / 90190 不应被误判为 8002 / 9019
        let e = anyhow::anyhow!("request_id=18002, errno=90190");
        assert!(!is_baidu_riskcontrol_error(&e));
    }

    /// 测试用零延迟，避免真实退避拖慢测试。
    fn no_delay(_attempt: u32) -> Duration {
        Duration::ZERO
    }

    #[tokio::test]
    async fn test_retry_stops_after_success() {
        let calls = AtomicU32::new(0);
        let r: anyhow::Result<u32> = retry_inner(
            "test",
            || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n == 0 {
                        Err(anyhow::anyhow!("百度 API 错误 8002: x"))
                    } else {
                        Ok(42)
                    }
                }
            },
            PCS_RETRY_MAX_RETRIES,
            no_delay,
        )
            .await;
        assert_eq!(r.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_retry_does_not_retry_non_riskcontrol() {
        let calls = AtomicU32::new(0);
        let r: anyhow::Result<u32> = retry_inner(
            "test",
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Err(anyhow::anyhow!("百度 API 错误 -6: 鉴权失败")) }
            },
            PCS_RETRY_MAX_RETRIES,
            no_delay,
        )
            .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_exhausts_max_retries() {
        let calls = AtomicU32::new(0);
        let r: anyhow::Result<u32> = retry_inner(
            "test",
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Err(anyhow::anyhow!("errno=9019 need verify")) }
            },
            PCS_RETRY_MAX_RETRIES,
            no_delay,
        )
            .await;
        assert!(r.is_err());
        // 1 次首发 + 3 次重试 = 4 次
        assert_eq!(calls.load(Ordering::SeqCst), 1 + PCS_RETRY_MAX_RETRIES);
    }
}
