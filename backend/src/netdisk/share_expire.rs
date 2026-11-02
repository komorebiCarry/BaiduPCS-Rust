//! 修改已有分享的有效期（网页端「我的分享」→ 有效期下拉）
//!
//! 接口：`POST https://pan.baidu.com/share/mexpire?clienttype=0&app_id=250528&web=1`，
//! `application/x-www-form-urlencoded` 表单：`share_id`、`expire_time`、`op_expire_days`。
//!
//! 参数编码以百度网盘网页端脚本为准（`nd-static.bdstatic.com/m-static/v20-main/home/js/home.*.js`
//! 中我的分享有效期组件 `selectOption`）：
//! - 选项只有 1 / 3 / 7 / 30 天和永久有效（值 `"-1"`）；
//! - `expire_time = 永久 ? -1 : 当前 Unix 秒 + 86400 × 天数`；
//! - `op_expire_days = 天数`（永久为 `-1`）。
//!
//! 3 天有真实抓包佐证（1/7/30 天编码与之相同，只是天数不同）；永久有效的 `-1` 编码来自
//! 上述网页端脚本，**尚未实际请求验证**，失败时错误会原样带回 errno 供排查。
//!
//! 鉴权与同类网页分享接口（`/share/cancel`、`/share/record`）一致：只带 BDUSS/STOKEN Cookie，
//! 不带 bdstoken（抓包的请求 URL 里也没有 bdstoken）。

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// 网页端提供的有效期选项（天）；`PERMANENT_DAYS` 表示永久有效
pub const SHARE_EXPIRE_DAY_OPTIONS: [i32; 4] = [1, 3, 7, 30];

/// 永久有效在网页端的编码（`expire_time` 与 `op_expire_days` 都传 -1）
pub const PERMANENT_DAYS: i32 = -1;

/// 一次批量修改最多处理的分享数（逐条调用百度接口，避免请求过多被限流）
pub const MAX_BATCH_SHARE_EXPIRE: usize = 100;

/// 校验有效期选项：只接受网页端提供的 1/3/7/30 天和永久（-1）
pub fn validate_expire_days(days: i32) -> Result<()> {
    if days == PERMANENT_DAYS || SHARE_EXPIRE_DAY_OPTIONS.contains(&days) {
        Ok(())
    } else {
        bail!(
            "不支持的有效期：{}（仅支持 1 / 3 / 7 / 30 天或永久有效）",
            days
        )
    }
}

/// 构造 `share/mexpire` 的表单参数（与网页端一致）
///
/// `now` 为当前 Unix 秒，便于测试。
pub fn build_mexpire_form(
    share_id: u64,
    days: i32,
    now: i64,
) -> Result<Vec<(&'static str, String)>> {
    validate_expire_days(days)?;
    let expire_time = if days == PERMANENT_DAYS {
        -1
    } else {
        now + 86_400 * i64::from(days)
    };
    Ok(vec![
        ("share_id", share_id.to_string()),
        ("expire_time", expire_time.to_string()),
        ("op_expire_days", days.to_string()),
    ])
}

/// `share/mexpire` 固定查询参数（与网页端请求一致）
pub fn mexpire_query() -> [(&'static str, String); 3] {
    [
        ("clienttype", "0".to_string()),
        ("app_id", crate::auth::constants::BAIDU_APP_ID.to_string()),
        ("web", "1".to_string()),
    ]
}

/// `share/mexpire` 响应
///
/// 示例：`{"ExpireDate":"2026-11-05 15:29:11","ExpireTime":1793863751,"ExpireType":0,
/// "errno":0,"newno":"","request_id":2622596276642371600,"show_msg":""}`
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ShareMexpireResponse {
    pub errno: i32,
    /// 新的过期时间（北京时间字符串，如 `2026-11-05 15:29:11`）
    #[serde(rename = "ExpireDate", default)]
    pub expire_date: String,
    /// 新的过期时间点（Unix 秒，由服务端按自己的时间计算）
    #[serde(rename = "ExpireTime", default)]
    pub expire_time: i64,
    /// 过期类型（与分享列表 `expiredType` 同义：0=有期限，1=永久）
    #[serde(rename = "ExpireType", default)]
    pub expire_type: i32,
    #[serde(default)]
    pub show_msg: String,
    #[serde(default)]
    pub errmsg: String,
}

impl ShareMexpireResponse {
    pub fn is_success(&self) -> bool {
        self.errno == 0
    }

    /// 失败原因（不含前缀），末尾附 errno
    ///
    /// 优先 `show_msg`，其次网页端对该接口的错误提示表，再次 `errmsg`
    pub fn error_reason(&self) -> String {
        let show_msg = self.show_msg.trim();
        let errmsg = self.errmsg.trim();
        let reason = if !show_msg.is_empty() {
            show_msg.to_string()
        } else if let Some(known) = mexpire_errno_reason(self.errno) {
            known.to_string()
        } else if !errmsg.is_empty() {
            errmsg.to_string()
        } else {
            "设置失败".to_string()
        };
        format!("{}（errno={}）", reason, self.errno)
    }

    /// 修改成功后的有效期：`(expired_type, expired_time)`，与分享列表接口返回的语义一致
    ///
    /// - 永久：`(1, 0)`；
    /// - 有期限：`(0, 过期时间点)`，优先用服务端返回的 `ExpireTime`，缺失时按 `now + 天数` 推算。
    pub fn applied_expiry(&self, days: i32, now: i64) -> (i32, i64) {
        if days == PERMANENT_DAYS || self.expire_type == 1 {
            (1, 0)
        } else if self.expire_time > 0 {
            (0, self.expire_time)
        } else {
            (0, now + 86_400 * i64::from(days.max(0)))
        }
    }
}

/// 「修改有效期」的错误提示：-6 / 14 / 24 / 60024 取自网页端脚本 `showExpiryError`，
/// 2 为百度接口通用的参数错误
fn mexpire_errno_reason(errno: i32) -> Option<&'static str> {
    Some(match errno {
        -6 => "登录已过期，请重新登录",
        2 => "参数错误",
        14 => "设置失败，百度提示：文件保存失败",
        24 => "分享链接不存在（可能已被取消）",
        60024 => "该分享已开启自动转存，无法修改或恢复有效期",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn form_matches_web_capture_for_days() {
        let now = 1_793_604_566; // 抓包时刻附近
        let form = build_mexpire_form(41_865_092_248, 3, now).unwrap();
        assert_eq!(
            form,
            vec![
                ("share_id", "41865092248".to_string()),
                ("expire_time", (now + 3 * 86_400).to_string()),
                ("op_expire_days", "3".to_string()),
            ]
        );
        for d in [1, 7, 30] {
            let f = build_mexpire_form(1, d, 100).unwrap();
            assert_eq!(f[1].1, (100 + 86_400 * i64::from(d)).to_string());
            assert_eq!(f[2].1, d.to_string());
        }
    }

    #[test]
    fn form_permanent_uses_minus_one() {
        let form = build_mexpire_form(9, PERMANENT_DAYS, 123).unwrap();
        assert_eq!(form[1], ("expire_time", "-1".to_string()));
        assert_eq!(form[2], ("op_expire_days", "-1".to_string()));
    }

    #[test]
    fn rejects_options_web_does_not_offer() {
        for d in [0, 2, 14, 365, -2] {
            let err = build_mexpire_form(1, d, 0).unwrap_err().to_string();
            assert!(err.contains("仅支持"), "{d}: {err}");
        }
    }

    #[test]
    fn query_matches_capture() {
        let q = mexpire_query();
        assert_eq!(q[0], ("clienttype", "0".to_string()));
        assert_eq!(q[1], ("app_id", "250528".to_string()));
        assert_eq!(q[2], ("web", "1".to_string()));
    }

    #[test]
    fn parses_captured_success_response() {
        let body = r#"{"ExpireDate":"2026-11-05 15:29:11","ExpireTime":1793863751,"ExpireType":0,"errno":0,"newno":"","request_id":2622596276642371600,"show_msg":""}"#;
        let r: ShareMexpireResponse = serde_json::from_str(body).unwrap();
        assert!(r.is_success());
        assert_eq!(r.expire_date, "2026-11-05 15:29:11");
        assert_eq!(r.expire_time, 1_793_863_751);
        assert_eq!(r.expire_type, 0);
        // 以服务端返回的时间为准，而不是请求里发送的时间
        assert_eq!(r.applied_expiry(3, 1_793_604_566), (0, 1_793_863_751));
    }

    #[test]
    fn applied_expiry_permanent_and_fallback() {
        let ok = ShareMexpireResponse::default();
        assert_eq!(ok.applied_expiry(PERMANENT_DAYS, 100), (1, 0));
        let typed = ShareMexpireResponse {
            expire_type: 1,
            ..Default::default()
        };
        assert_eq!(typed.applied_expiry(7, 100), (1, 0));
        // 服务端未返回 ExpireTime 时按天数推算
        assert_eq!(ok.applied_expiry(7, 100), (0, 100 + 7 * 86_400));
    }

    #[test]
    fn error_reasons() {
        let r: ShareMexpireResponse =
            serde_json::from_str(r#"{"errno":60024,"show_msg":""}"#).unwrap();
        assert!(!r.is_success());
        assert_eq!(
            r.error_reason(),
            "该分享已开启自动转存，无法修改或恢复有效期（errno=60024）"
        );
        let r: ShareMexpireResponse = serde_json::from_str(r#"{"errno":-6}"#).unwrap();
        assert!(r.error_reason().starts_with("登录已过期"));
        let r: ShareMexpireResponse =
            serde_json::from_str(r#"{"errno":31,"show_msg":" 操作太频繁 "}"#).unwrap();
        assert_eq!(r.error_reason(), "操作太频繁（errno=31）");
        let r: ShareMexpireResponse =
            serde_json::from_str(r#"{"errno":12345,"errmsg":"boom"}"#).unwrap();
        assert_eq!(r.error_reason(), "boom（errno=12345）");
        let r: ShareMexpireResponse = serde_json::from_str(r#"{"errno":777}"#).unwrap();
        assert_eq!(r.error_reason(), "设置失败（errno=777）");
    }

    #[test]
    fn tolerates_missing_or_extra_fields() {
        let r: ShareMexpireResponse =
            serde_json::from_str(r#"{"errno":0,"foo":[1],"request_id":"x"}"#).unwrap();
        assert!(r.is_success());
        assert_eq!(r.expire_time, 0);
        assert!(r.expire_date.is_empty());
    }
}
