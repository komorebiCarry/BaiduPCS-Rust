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

/// 分享列表数据
#[derive(Debug, Serialize)]
pub struct ShareListData {
    /// 分享记录列表
    pub list: Vec<ShareRecordData>,
    /// 总数
    pub total: u32,
    /// 当前页码
    pub page: u32,
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
    /// 过期类型
    #[serde(rename = "expiredType")]
    pub expired_type: i32,
    /// 过期时间戳
    #[serde(rename = "expiredTime")]
    pub expired_time: i64,
    /// 浏览次数
    #[serde(rename = "viewCount")]
    pub view_count: i32,
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
                                                "预热重试后仍失败: errno={}, errmsg={}",
                                                retry_response.errno, retry_response.errmsg
                                            );
                                            return Ok(Json(ApiResponse::error(
                                                retry_response.errno,
                                                format!(
                                                    "创建分享失败（已重试）: {}",
                                                    retry_response.errmsg
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
                    "创建分享失败: errno={}, errmsg={}",
                    response.errno, response.errmsg
                );
                Ok(Json(ApiResponse::error(
                    response.errno,
                    format!("创建分享失败: {}", response.errmsg),
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
                // 转换为API响应格式
                let list: Vec<ShareRecordData> = response
                    .list
                    .into_iter()
                    .map(|record| ShareRecordData {
                        share_id: record.share_id,
                        fs_ids: record.fs_ids,
                        shortlink: record.shortlink,
                        status: record.status,
                        public: record.public,
                        typical_category: record.typical_category,
                        typical_path: record.typical_path,
                        expired_type: record.expired_type,
                        expired_time: record.expired_time,
                        view_count: record.view_count,
                    })
                    .collect();

                let data = ShareListData {
                    list,
                    total: response.total,
                    page: params.page,
                };

                info!("获取分享列表成功: total={}", data.total);
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
