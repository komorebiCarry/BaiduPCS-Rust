// 分享API处理器

use crate::server::handlers::ApiResponse;
use crate::server::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

// =====================================================
// 请求/响应数据结构
// =====================================================

/// 创建分享请求
#[derive(Debug, Deserialize)]
pub struct CreateShareRequest {
    /// 文件路径列表
    pub paths: Vec<String>,
    /// 有效期（0=永久, 1=1天, 7=7天, 30=30天）
    pub period: i32,
    /// 提取码（4位字符，可选，不提供则自动生成）
    #[serde(default)]
    pub pwd: Option<String>,
}

/// 取消分享请求
#[derive(Debug, Deserialize)]
pub struct CancelShareRequest {
    /// 分享ID列表
    pub share_ids: Vec<u64>,
}

/// 修改分享有效期请求
#[derive(Debug, Deserialize)]
pub struct ModifyShareExpireRequest {
    /// 分享ID列表（单条修改时只传一个）
    pub share_ids: Vec<u64>,
    /// 有效期：1 / 3 / 7 / 30 天，-1 = 永久有效（与网页端选项一致）
    pub days: i32,
}

/// 分享列表查询参数
#[derive(Debug, Deserialize)]
pub struct ShareListQuery {
    /// 页码（从1开始，默认1）
    #[serde(default = "default_page")]
    pub page: u32,
}

fn default_page() -> u32 {
    1
}

/// 分享创建结果
#[derive(Debug, Serialize)]
pub struct ShareResult {
    /// 分享链接
    pub link: String,
    /// 提取码
    pub pwd: String,
    /// 分享ID
    pub shareid: u64,
}

/// 取消分享结果
#[derive(Debug, Serialize)]
pub struct CancelShareResult {
    /// 是否成功
    pub success: bool,
}

/// 单条分享修改有效期的结果
#[derive(Debug, Serialize, PartialEq)]
pub struct ShareExpireItemResult {
    #[serde(rename = "shareId")]
    pub share_id: u64,
    pub success: bool,
    /// 新的过期状态（与分享列表 `expiredType` 同义；失败时省略）
    #[serde(rename = "expiredType", skip_serializing_if = "Option::is_none")]
    pub expired_type: Option<i32>,
    /// 新的过期时间点（Unix 秒，永久为 0；失败时省略）
    #[serde(rename = "expiredTime", skip_serializing_if = "Option::is_none")]
    pub expired_time: Option<i64>,
    /// 百度返回的过期时间文本（如 `2026-11-05 15:29:11`，北京时间；可能为空）
    #[serde(rename = "expireDate", skip_serializing_if = "Option::is_none")]
    pub expire_date: Option<String>,
    /// 失败原因
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// 修改分享有效期结果
#[derive(Debug, Serialize)]
pub struct ModifyShareExpireResult {
    pub results: Vec<ShareExpireItemResult>,
    #[serde(rename = "successCount")]
    pub success_count: usize,
    #[serde(rename = "failedCount")]
    pub failed_count: usize,
}

/// 分享列表数据
#[derive(Debug, Serialize)]
pub struct ShareListData {
    /// 分享记录列表
    pub list: Vec<ShareRecordData>,
    /// 总数（百度 `/share/record` 实际不返回，通常为 0；前端不要依赖）
    pub total: u32,
    /// 本页条数（百度 `count`，缺失时为 list 长度）
    pub count: usize,
    /// 是否还有下一页（百度 `nextpage` > 0）
    pub has_more: bool,
    /// 百度返回的下一页页码（0 = 没有下一页）
    pub nextpage: i64,
    /// 当前页码
    pub page: u32,
}

/// 百度分享列表响应 → API 响应（`expiredTime` 统一转成过期时间点）
fn share_list_data(
    response: crate::netdisk::ShareListResponse,
    page: u32,
    now: i64,
) -> ShareListData {
    let count = response.page_count();
    let has_more = response.has_more();
    let nextpage = response.nextpage.unwrap_or(0);
    let list: Vec<ShareRecordData> = response
        .list
        .into_iter()
        .map(|record| ShareRecordData {
            expired_time: record.expire_at(now),
            share_id: record.share_id,
            fs_ids: record.fs_ids,
            shortlink: record.shortlink,
            status: record.status,
            public: record.public,
            typical_category: record.typical_category,
            typical_path: record.typical_path,
            expired_type: record.expired_type,
            view_count: record.view_count,
            download_count: record.download_count,
            transfer_count: record.transfer_count,
        })
        .collect();
    ShareListData {
        list,
        total: response.total,
        count,
        has_more,
        nextpage,
        page,
    }
}

/// 分享记录数据（用于API响应）
#[derive(Debug, Serialize)]
pub struct ShareRecordData {
    /// 分享ID
    #[serde(rename = "shareId")]
    pub share_id: u64,
    /// 文件ID列表
    #[serde(rename = "fsIds")]
    pub fs_ids: Vec<i64>,
    /// 短链接
    pub shortlink: String,
    /// 状态（0=正常, 其他=异常）
    pub status: i32,
    /// 是否公开（0=私密, 1=公开）
    pub public: i32,
    /// 文件类型
    #[serde(rename = "typicalCategory")]
    pub typical_category: i32,
    /// 文件路径
    #[serde(rename = "typicalPath")]
    pub typical_path: String,
    /// 过期状态（-1=已过期, 0=有期限且未过期, 1=永久有效）
    #[serde(rename = "expiredType")]
    pub expired_type: i32,
    /// 过期时间点（Unix 秒级时间戳，0=永久或已过期）
    #[serde(rename = "expiredTime")]
    pub expired_time: i64,
    /// 浏览次数
    #[serde(rename = "viewCount")]
    pub view_count: i32,
    /// 下载次数（百度未返回时省略该字段）
    #[serde(rename = "downloadCount", skip_serializing_if = "Option::is_none")]
    pub download_count: Option<i64>,
    /// 保存（转存）次数（百度未返回时省略该字段）
    #[serde(rename = "transferCount", skip_serializing_if = "Option::is_none")]
    pub transfer_count: Option<i64>,
}

/// 分享详情数据
#[derive(Debug, Serialize)]
pub struct ShareDetailData {
    /// 提取码
    pub pwd: String,
    /// 短链接
    pub shorturl: String,
}

// =====================================================
// 验证函数
// =====================================================

/// 有效的有效期参数值
pub const VALID_PERIODS: [i32; 4] = [0, 1, 7, 30];

/// 验证有效期参数
///
/// # 参数
/// * `period` - 有效期参数（0=永久, 1=1天, 7=7天, 30=30天）
///
/// # 返回
/// * `Ok(())` - 参数有效
/// * `Err(String)` - 参数无效，包含错误信息
pub fn validate_period(period: i32) -> Result<(), String> {
    if VALID_PERIODS.contains(&period) {
        Ok(())
    } else {
        Err(format!(
            "无效的有效期参数: {}，有效值为 0(永久), 1(1天), 7(7天), 30(30天)",
            period
        ))
    }
}

/// 验证提取码格式
///
/// # 参数
/// * `pwd` - 提取码字符串
///
/// # 返回
/// * `Ok(())` - 提取码格式正确（4位字母数字）
/// * `Err(String)` - 提取码格式错误，包含错误信息
pub fn validate_pwd(pwd: &str) -> Result<(), String> {
    if pwd.len() != 4 {
        return Err(format!("提取码必须为4位字符，当前长度: {}", pwd.len()));
    }
    // 检查是否为字母数字
    if !pwd.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err("提取码只能包含字母和数字".to_string());
    }
    Ok(())
}

/// 生成随机提取码（4位字母数字）
///
/// # 返回
/// 4位随机字母数字组成的提取码
pub fn generate_random_pwd() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..4)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

// =====================================================
// API 处理函数
// =====================================================

/// 创建分享
///
/// POST /api/v1/shares
pub async fn create_share(
    State(state): State<AppState>,
    Json(request): Json<CreateShareRequest>,
) -> Result<Json<ApiResponse<ShareResult>>, StatusCode> {
    info!(
        "API: 创建分享 paths={:?}, period={}, pwd={:?}",
        request.paths, request.period, request.pwd
    );

    // 验证 paths 非空
    if request.paths.is_empty() {
        return Ok(Json(ApiResponse::error(
            400,
            "文件路径列表不能为空".to_string(),
        )));
    }

    // 验证有效期
    if let Err(e) = validate_period(request.period) {
        return Ok(Json(ApiResponse::error(400, e)));
    }

    // 处理提取码
    let pwd = match &request.pwd {
        Some(p) if !p.is_empty() => {
            // 验证提取码格式
            if let Err(e) = validate_pwd(p) {
                return Ok(Json(ApiResponse::error(400, e)));
            }
            p.clone()
        }
        _ => {
            // 自动生成提取码
            let generated = generate_random_pwd();
            info!("自动生成提取码: {}", generated);
            generated
        }
    };

    // 多账号路由：按 active_uid 取客户端
    let client = match state.active_client().await {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    // 调用分享API
    match client.share_set(&request.paths, request.period, &pwd).await {
        Ok(response) => {
            if response.is_success() {
                // 使用我们传入的 pwd，而不是 response.pwd（百度API可能不返回）
                let result = ShareResult {
                    link: response.link,
                    pwd: pwd.clone(), // 使用我们生成/传入的提取码
                    shareid: response.shareid,
                };
                info!(
                    "分享创建成功: shareid={}, pwd={}",
                    result.shareid, result.pwd
                );
                Ok(Json(ApiResponse::success(result)))
            } else {
                // 检查是否需要预热重试 (errno=-6)
                if response.errno == -6 {
                    warn!("分享创建遇到 errno=-6，触发预热重试...");

                    // 触发预热
                    match state.trigger_warmup().await {
                        Ok(true) => {
                            info!("预热成功，重试创建分享...");
                            // 重新获取活跃账号客户端（cookies 已刷新）
                            if let Some(c) = state.active_client().await {
                                match c.share_set(&request.paths, request.period, &pwd).await {
                                    Ok(retry_response) => {
                                        if retry_response.is_success() {
                                            let result = ShareResult {
                                                link: retry_response.link,
                                                pwd: pwd.clone(), // 使用我们生成/传入的提取码
                                                shareid: retry_response.shareid,
                                            };
                                            info!(
                                                "预热重试成功，分享创建: shareid={}, pwd={}",
                                                result.shareid, result.pwd
                                            );
                                            return Ok(Json(ApiResponse::success(result)));
                                        } else {
                                            error!(
                                                "预热重试后仍失败: errno={}, errmsg={}, show_msg={}",
                                                retry_response.errno,
                                                retry_response.errmsg,
                                                retry_response.show_msg
                                            );
                                            return Ok(Json(ApiResponse::error(
                                                retry_response.errno,
                                                format!(
                                                    "创建分享失败（已重试）: {}",
                                                    retry_response.error_reason()
                                                ),
                                            )));
                                        }
                                    }
                                    Err(retry_err) => {
                                        error!("预热重试后请求失败: {}", retry_err);
                                        return Ok(Json(ApiResponse::error(
                                            500,
                                            format!("创建分享失败（已重试）: {}", retry_err),
                                        )));
                                    }
                                }
                            }
                        }
                        Ok(false) => {
                            warn!("预热跳过（用户未登录）");
                        }
                        Err(warmup_err) => {
                            error!("预热失败: {}", warmup_err);
                        }
                    }
                }

                error!(
                    "创建分享失败: errno={}, errmsg={}, show_msg={}",
                    response.errno, response.errmsg, response.show_msg
                );
                Ok(Json(ApiResponse::error(
                    response.errno,
                    format!("创建分享失败: {}", response.error_reason()),
                )))
            }
        }
        Err(e) => {
            error!("创建分享请求失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("创建分享失败: {}", e),
            )))
        }
    }
}

/// 取消分享
///
/// DELETE /api/v1/shares
pub async fn cancel_share(
    State(state): State<AppState>,
    Json(request): Json<CancelShareRequest>,
) -> Result<Json<ApiResponse<CancelShareResult>>, StatusCode> {
    info!("API: 取消分享 share_ids={:?}", request.share_ids);

    // 验证 share_ids 非空
    if request.share_ids.is_empty() {
        return Ok(Json(ApiResponse::error(
            400,
            "分享ID列表不能为空".to_string(),
        )));
    }

    // 多账号路由：按 active_uid 取客户端
    let client = match state.active_client().await {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    // 调用取消分享API
    match client.share_cancel(&request.share_ids).await {
        Ok(response) => {
            if response.is_success() {
                info!("取消分享成功: {:?}", request.share_ids);
                Ok(Json(ApiResponse::success(CancelShareResult {
                    success: true,
                })))
            } else {
                error!(
                    "取消分享失败: errno={}, errmsg={}",
                    response.errno, response.errmsg
                );
                Ok(Json(ApiResponse::error(
                    response.errno,
                    format!("取消分享失败: {}", response.errmsg),
                )))
            }
        }
        Err(e) => {
            error!("取消分享请求失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("取消分享失败: {}", e),
            )))
        }
    }
}

/// 校验修改有效期请求，返回去重后的分享ID（保持顺序）
fn validate_modify_expire_request(
    request: &ModifyShareExpireRequest,
) -> std::result::Result<Vec<u64>, String> {
    use crate::netdisk::share_expire::{validate_expire_days, MAX_BATCH_SHARE_EXPIRE};

    validate_expire_days(request.days).map_err(|e| e.to_string())?;
    let mut seen = std::collections::HashSet::new();
    let ids: Vec<u64> = request
        .share_ids
        .iter()
        .copied()
        .filter(|id| seen.insert(*id))
        .collect();
    if ids.is_empty() {
        return Err("分享ID列表不能为空".to_string());
    }
    if ids.len() > MAX_BATCH_SHARE_EXPIRE {
        return Err(format!(
            "一次最多修改 {} 个分享的有效期",
            MAX_BATCH_SHARE_EXPIRE
        ));
    }
    Ok(ids)
}

/// 把单条 `share/mexpire` 调用结果转换成接口结果
fn share_expire_item_result(
    share_id: u64,
    days: i32,
    now: i64,
    outcome: anyhow::Result<crate::netdisk::ShareMexpireResponse>,
) -> ShareExpireItemResult {
    match outcome {
        Ok(resp) if resp.is_success() => {
            let (expired_type, expired_time) = resp.applied_expiry(days, now);
            ShareExpireItemResult {
                share_id,
                success: true,
                expired_type: Some(expired_type),
                expired_time: Some(expired_time),
                expire_date: Some(resp.expire_date).filter(|d| !d.is_empty()),
                error: None,
            }
        }
        Ok(resp) => ShareExpireItemResult {
            share_id,
            success: false,
            expired_type: None,
            expired_time: None,
            expire_date: None,
            error: Some(resp.error_reason()),
        },
        Err(e) => ShareExpireItemResult {
            share_id,
            success: false,
            expired_type: None,
            expired_time: None,
            expire_date: None,
            error: Some(format!("请求失败：{:#}", e)),
        },
    }
}

/// 修改分享有效期（单条或批量）
///
/// POST /api/v1/shares/expire  `{ "share_ids": [..], "days": 1|3|7|30|-1 }`
///
/// 逐条调用百度 `share/mexpire`；返回每条的结果（部分失败不影响其它条）。
pub async fn modify_share_expire(
    State(state): State<AppState>,
    Json(request): Json<ModifyShareExpireRequest>,
) -> Result<Json<ApiResponse<ModifyShareExpireResult>>, StatusCode> {
    info!(
        "API: 修改分享有效期 share_ids={:?}, days={}",
        request.share_ids, request.days
    );

    let ids = match validate_modify_expire_request(&request) {
        Ok(ids) => ids,
        Err(msg) => return Ok(Json(ApiResponse::error(400, msg))),
    };

    // 多账号路由：按 active_uid 取客户端
    let client = match state.active_client().await {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    let mut results = Vec::with_capacity(ids.len());
    for share_id in ids {
        let outcome = client.share_mexpire(share_id, request.days).await;
        let item = share_expire_item_result(
            share_id,
            request.days,
            chrono::Utc::now().timestamp(),
            outcome,
        );
        if let Some(err) = &item.error {
            warn!("修改分享有效期失败: share_id={}, {}", share_id, err);
        }
        results.push(item);
    }

    let success_count = results.iter().filter(|r| r.success).count();
    let failed_count = results.len() - success_count;
    info!(
        "修改分享有效期完成: 成功 {}, 失败 {}",
        success_count, failed_count
    );
    Ok(Json(ApiResponse::success(ModifyShareExpireResult {
        results,
        success_count,
        failed_count,
    })))
}

/// 获取分享列表
///
/// GET /api/v1/shares?page=1
pub async fn get_share_list(
    State(state): State<AppState>,
    Query(params): Query<ShareListQuery>,
) -> Result<Json<ApiResponse<ShareListData>>, StatusCode> {
    info!("API: 获取分享列表 page={}", params.page);

    // 多账号路由：按 active_uid 取客户端
    let client = match state.active_client().await {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    // 调用分享列表API
    match client.share_list(params.page).await {
        Ok(response) => {
            if response.is_success() {
                // 百度返回的 expiredTime 是剩余秒数，这里统一转成过期时间点
                let data = share_list_data(response, params.page, chrono::Utc::now().timestamp());
                info!(
                    "获取分享列表成功: page={}, count={}, has_more={}",
                    data.page, data.count, data.has_more
                );
                Ok(Json(ApiResponse::success(data)))
            } else {
                error!(
                    "获取分享列表失败: errno={}, errmsg={}",
                    response.errno, response.errmsg
                );
                Ok(Json(ApiResponse::error(
                    response.errno,
                    format!("获取分享列表失败: {}", response.errmsg),
                )))
            }
        }
        Err(e) => {
            error!("获取分享列表请求失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("获取分享列表失败: {}", e),
            )))
        }
    }
}

/// 获取分享详情
///
/// GET /api/v1/shares/:id
pub async fn get_share_detail(
    State(state): State<AppState>,
    Path(share_id): Path<u64>,
) -> Result<Json<ApiResponse<ShareDetailData>>, StatusCode> {
    info!("API: 获取分享详情 share_id={}", share_id);

    // 多账号路由：按 active_uid 取客户端
    let client = match state.active_client().await {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    // 调用分享详情API
    match client.share_surl_info(share_id).await {
        Ok(response) => {
            if response.is_success() {
                let data = ShareDetailData {
                    // 使用 actual_pwd() 处理 "0" 转空字符串
                    pwd: response.actual_pwd().to_string(),
                    shorturl: response.shorturl,
                };

                info!(
                    "获取分享详情成功: share_id={}, has_pwd={}",
                    share_id,
                    !data.pwd.is_empty()
                );
                Ok(Json(ApiResponse::success(data)))
            } else {
                error!(
                    "获取分享详情失败: errno={}, errmsg={}",
                    response.errno, response.errmsg
                );
                Ok(Json(ApiResponse::error(
                    response.errno,
                    format!("获取分享详情失败: {}", response.errmsg),
                )))
            }
        }
        Err(e) => {
            error!("获取分享详情请求失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("获取分享详情失败: {}", e),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::netdisk::ShareMexpireResponse;

    fn req(ids: Vec<u64>, days: i32) -> ModifyShareExpireRequest {
        ModifyShareExpireRequest {
            share_ids: ids,
            days,
        }
    }

    #[test]
    fn share_list_data_uses_count_and_nextpage() {
        let resp: crate::netdisk::ShareListResponse = serde_json::from_str(include_str!(
            "../../netdisk/testdata/share_record_page1.json"
        ))
        .unwrap();
        let data = share_list_data(resp, 1, 1_761_000_000);
        assert_eq!(
            (data.total, data.count, data.has_more, data.nextpage),
            (0, 4, false, 0)
        );
        assert_eq!(data.list[0].expired_time, 1_764_592_000);
        let v = serde_json::to_value(&data).unwrap();
        assert_eq!(v["count"], 4);
        assert_eq!(v["has_more"], false);
        assert_eq!(v["list"][3]["typicalPath"], "分享的文件已被删除");
        assert_eq!(v["list"][1]["downloadCount"], 0);

        let more: crate::netdisk::ShareListResponse =
            serde_json::from_str(r#"{"errno":0,"list":[],"count":50,"nextpage":2}"#).unwrap();
        let data = share_list_data(more, 1, 0);
        assert!(data.has_more);
        assert_eq!(data.nextpage, 2);
    }

    #[test]
    fn validate_expire_request_days_and_ids() {
        for d in [1, 3, 7, 30, -1] {
            assert_eq!(
                validate_modify_expire_request(&req(vec![5], d)),
                Ok(vec![5])
            );
        }
        for d in [0, 2, 14, 365, -2] {
            assert!(validate_modify_expire_request(&req(vec![5], d))
                .unwrap_err()
                .contains("仅支持"));
        }
        assert_eq!(
            validate_modify_expire_request(&req(vec![], 7)).unwrap_err(),
            "分享ID列表不能为空"
        );
        // 去重且保持顺序
        assert_eq!(
            validate_modify_expire_request(&req(vec![3, 1, 3, 2, 1], 7)),
            Ok(vec![3, 1, 2])
        );
        let too_many: Vec<u64> =
            (0..=crate::netdisk::share_expire::MAX_BATCH_SHARE_EXPIRE as u64).collect();
        assert!(validate_modify_expire_request(&req(too_many, 7))
            .unwrap_err()
            .contains("最多"));
    }

    #[test]
    fn deserialize_request_body() {
        let r: ModifyShareExpireRequest =
            serde_json::from_str(r#"{"share_ids":[41865092248],"days":-1}"#).unwrap();
        assert_eq!(r.share_ids, vec![41865092248]);
        assert_eq!(r.days, -1);
    }

    #[test]
    fn item_result_success_uses_server_time() {
        let resp: ShareMexpireResponse = serde_json::from_str(
            r#"{"ExpireDate":"2026-11-05 15:29:11","ExpireTime":1793863751,"ExpireType":0,"errno":0,"show_msg":""}"#,
        )
        .unwrap();
        let item = share_expire_item_result(1, 3, 1_793_604_566, Ok(resp));
        assert_eq!(
            item,
            ShareExpireItemResult {
                share_id: 1,
                success: true,
                expired_type: Some(0),
                expired_time: Some(1_793_863_751),
                expire_date: Some("2026-11-05 15:29:11".to_string()),
                error: None,
            }
        );
        let json = serde_json::to_value(&item).unwrap();
        assert_eq!(json["shareId"], 1);
        assert_eq!(json["expiredTime"], 1_793_863_751i64);
        assert!(json.get("error").is_none());
    }

    #[test]
    fn item_result_permanent() {
        let item = share_expire_item_result(2, -1, 100, Ok(ShareMexpireResponse::default()));
        assert!(item.success);
        assert_eq!(item.expired_type, Some(1));
        assert_eq!(item.expired_time, Some(0));
        assert_eq!(item.expire_date, None);
    }

    #[test]
    fn item_result_errors() {
        let resp: ShareMexpireResponse = serde_json::from_str(r#"{"errno":24}"#).unwrap();
        let item = share_expire_item_result(3, 7, 0, Ok(resp));
        assert!(!item.success);
        assert_eq!(item.expired_type, None);
        assert_eq!(
            item.error.as_deref(),
            Some("分享链接不存在（可能已被取消）（errno=24）")
        );

        let item = share_expire_item_result(4, 7, 0, Err(anyhow::anyhow!("timeout")));
        assert_eq!(item.error.as_deref(), Some("请求失败：timeout"));
        let json = serde_json::to_value(&item).unwrap();
        assert!(json.get("expiredType").is_none());
    }
}
