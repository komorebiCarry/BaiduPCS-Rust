use axum::{
    middleware,
    routing::{delete, get, post, put},
    Json, Router,
};
use baidu_netdisk_rust::{
    config::LogConfig, logging, server::handlers, server::websocket,
    web_auth::{self, WebAuthState},
    common::proxy_fallback::ProxyHotUpdater,
    AppState,
};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing::info;

/// 智能检测前端资源目录
/// 按优先级尝试以下路径：
/// 1. ./frontend/dist - 开发环境标准路径
/// 2. ./frontend - GitHub Actions 打包路径（dist 内容直接在 frontend 下）
/// 3. ../frontend/dist - 开发环境，源码目录结构
/// 4. ../frontend - GitHub Actions 打包路径（上级目录）
/// 5. /app/frontend/dist - Docker 容器标准路径
/// 6. /app/frontend - Docker 容器 GitHub 打包路径
/// 7. ./dist - 备选路径（手动部署）
/// 8. {exe_dir}/frontend/dist - 相对于可执行文件的路径
/// 9. {exe_dir}/frontend - 相对于可执行文件的 GitHub 打包路径
fn detect_frontend_dir() -> PathBuf {
    let mut candidates = vec![
        // 1. 开发环境标准路径
        PathBuf::from("./frontend/dist"),
        // 2. GitHub Actions 打包路径（dist 内容直接在 frontend 下）
        PathBuf::from("./frontend"),
        // 3. 开发环境，源码目录结构
        PathBuf::from("../frontend/dist"),
        // 4. GitHub Actions 打包路径（上级目录）
        PathBuf::from("../frontend"),
        // 5. Docker 容器标准路径
        PathBuf::from("/app/frontend/dist"),
        // 6. Docker 容器 GitHub 打包路径
        PathBuf::from("/app/frontend"),
        // 7. 备选路径（手动部署时可能使用）
        PathBuf::from("./dist"),
    ];

    // 8-9. 可执行文件所在目录的 frontend/dist 和 frontend
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            candidates.push(exe_dir.join("frontend/dist"));
            candidates.push(exe_dir.join("frontend"));
            candidates.push(exe_dir.join("dist"));
        }
    }

    // 按顺序尝试每个候选路径
    for path in &candidates {
        if path.exists() && path.is_dir() {
            // 验证是否包含 index.html（确保是有效的前端构建）
            if path.join("index.html").exists() {
                info!(
                    "✓ 找到前端资源目录: {:?}",
                    path.canonicalize().unwrap_or(path.clone())
                );
                return path.clone();
            }
        }
    }

    // 如果都找不到，返回默认路径并警告
    let default = PathBuf::from("./frontend/dist");
    tracing::warn!(
        "⚠️  未找到前端资源目录，使用默认路径: {:?}\n\
         尝试过的路径: {:?}\n\
         请确保前端已构建，或将 frontend/dist 目录放在可执行文件同级目录",
        default,
        candidates
    );
    default
}

/// 加载日志配置
///
/// 尝试从配置文件加载，失败时返回默认配置
async fn load_log_config() -> LogConfig {
    // 尝试读取配置文件中的日志配置
    let config_path = "config/app.toml";
    if let Ok(content) = tokio::fs::read_to_string(config_path).await {
        if let Ok(config) = toml::from_str::<toml::Value>(&content) {
            if let Some(log_table) = config.get("log") {
                if let Ok(log_config) = log_table.clone().try_into::<LogConfig>() {
                    return log_config;
                }
            }
        }
    }

    // 返回默认配置
    LogConfig::default()
}

/// 检查下载目录是否可访问和可读写
///
/// 若目录不存在则尝试创建；若无权限则打印说明日志，延迟 30 秒后退出服务。
async fn ensure_download_dir_accessible(download_dir: &std::path::Path) {
    // 目录不存在时先尝试创建
    if !download_dir.exists() {
        match std::fs::create_dir_all(download_dir) {
            Ok(_) => {
                info!("✓ 下载目录已创建: {:?}", download_dir);
                return;
            }
            Err(e) => {
                info!(
                    "\n========================================\n\
                     [启动失败] 无法创建下载目录\n\
                     目录路径: {:?}\n\
                     错误原因: {}\n\
                     \n\
                     请前往配置文件 config/app.toml\n\
                     将 [download] 下的 download_dir 修改为\n\
                     一个存在且具有读写权限的目录路径。\n\
                     \n\
                     Windows 示例: download_dir = \"D:\\\\Downloads\"\n\
                     Linux/macOS 示例: download_dir = \"/home/user/downloads\"\n\
                     \n\
                     服务将在 30 秒后自动退出...\n\
                     ========================================",
                    download_dir, e
                );
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                std::process::exit(1);
            }
        }
    }

    // 目录存在，验证读写权限（写入测试文件）
    let test_file = download_dir.join(".baidu_write_test");
    match std::fs::write(&test_file, b"permission_test") {
        Ok(_) => {
            let _ = std::fs::remove_file(&test_file);
            info!("✓ 下载目录权限验证通过: {:?}", download_dir);
        }
        Err(e) => {
            info!(
                "\n========================================\n\
                 [启动失败] 下载目录没有访问或读写权限\n\
                 目录路径: {:?}\n\
                 错误原因: {}\n\
                 \n\
                 请前往配置文件 config/app.toml\n\
                 将 [download] 下的 download_dir 修改为\n\
                 一个存在且具有读写权限的目录路径。\n\
                 \n\
                 Windows 示例: download_dir = \"D:\\\\Downloads\"\n\
                 Linux/macOS 示例: download_dir = \"/home/user/downloads\"\n\
                 \n\
                 服务将在 30 秒后自动退出...\n\
                 ========================================",
                download_dir, e
            );
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            std::process::exit(1);
        }
    }
}

/// 初始化 Web 认证状态
///
/// 从配置和凭证文件加载认证状态，并根据认证模式启动清理任务。
async fn init_web_auth_state(config: &baidu_netdisk_rust::config::WebAuthConfig) -> Arc<WebAuthState> {
    use baidu_netdisk_rust::web_auth::create_auth_store;

    // 创建凭证存储并加载
    let auth_store = Arc::new(create_auth_store());
    if let Err(e) = auth_store.load().await {
        tracing::warn!("加载认证凭证失败，使用空凭证: {}", e);
    }

    // 获取凭证
    let credentials = auth_store.get_credentials().await;

    // 创建认证状态
    let state = WebAuthState::new(
        config.clone(),
        credentials,
        None, // JWT 密钥自动生成
        auth_store,
    );

    let state = Arc::new(state);

    // 根据认证模式启动清理任务
    state.start_cleanup_tasks().await;

    state
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 🔥 先尝试加载日志配置，失败时使用默认配置
    let log_config = load_log_config().await;

    // 🔥 初始化日志系统（必须保持 _log_guard 存活）
    let _log_guard = logging::init_logging(&log_config);

    info!("Baidu Netdisk Rust v1.3.0 启动中...");

    // 创建应用状态
    let app_state = AppState::new().await?;

    // 检查下载目录访问权限
    {
        let config = app_state.config.read().await;
        let download_dir = config.download.download_dir.clone();
        drop(config);
        ensure_download_dir_accessible(&download_dir).await;
    }
    // 注入热更新执行器到代理回退管理器（使回退触发时能自动执行热更新和启动探测任务）
    app_state.fallback_mgr.set_updater(
        Arc::new(app_state.clone()) as Arc<dyn ProxyHotUpdater>
    ).await;

    // 启动时探测代理连通性，失败则立即回退直连
    // 必须在 load_initial_session 之前执行，否则预热和 BDUSS 验证会走死代理
    {
        let cfg = app_state.config.read().await;
        let proxy = &cfg.network.proxy;
        if proxy.proxy_type != baidu_netdisk_rust::common::ProxyType::None {
            let proxy_clone = proxy.clone();
            let allow_fallback = proxy.allow_fallback;
            drop(cfg);

            // 🔥 先保存用户代理配置（供探测任务恢复时使用）
            app_state.fallback_mgr
                .set_user_proxy_config(Some(proxy_clone.clone()))
                .await;

            match baidu_netdisk_rust::common::probe_proxy(&proxy_clone).await {
                Ok(()) => {
                    info!("✓ 启动代理探测成功");
                    app_state.fallback_mgr
                        .set_runtime_status(baidu_netdisk_rust::common::ProxyRuntimeStatus::Normal)
                        .await;
                }
                Err(e) => {
                    tracing::warn!("✗ 启动代理探测失败: {}，触发回退", e);
                    if allow_fallback {
                        app_state.fallback_mgr.execute_fallback().await;
                    } else {
                        app_state.fallback_mgr
                            .set_runtime_status(baidu_netdisk_rust::common::ProxyRuntimeStatus::FallenBackToDirect)
                            .await;
                    }
                }
            }
        }
    }

    // 获取配置
    let config = app_state.config.read().await.clone();
    let addr = format!("{}:{}", config.server.host, config.server.port);

    // 🔥 初始化 Web 认证状态
    let web_auth_state = init_web_auth_state(&config.web_auth).await;
    info!("Web 认证状态初始化完成 (模式: {:?})", config.web_auth.mode);

    // 配置中间件层
    let middleware = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http()) // HTTP 请求日志
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );

    // API 路由
    let api_routes = Router::new()
        // 认证API
        .route("/auth/qrcode/generate", post(handlers::generate_qrcode))
        .route("/auth/qrcode/status", get(handlers::qrcode_status))
        .route("/auth/cookie/login", post(handlers::cookie_login))
        .route("/auth/user", get(handlers::get_current_user))
        .route("/auth/logout", post(handlers::logout))
        // 文件API
        .route("/files", get(handlers::get_file_list))
        .route("/files/download", get(handlers::get_download_url))
        .route("/files/folder", post(handlers::create_folder))
        .route("/files/delete", post(handlers::delete_files))
        // 下载API
        .route("/downloads", post(handlers::create_download))
        .route("/downloads", get(handlers::get_all_downloads))
        .route("/downloads/all", get(handlers::get_all_downloads_mixed)) // 新增：统一接口
        .route("/downloads/active", get(handlers::get_active_downloads)) // 🔥 活跃任务（降级轮询）
        .route("/downloads/batch", post(handlers::create_batch_download)) // 批量下载
        .route("/downloads/:id", get(handlers::get_download))
        .route("/downloads/:id/pause", post(handlers::pause_download))
        .route("/downloads/:id/resume", post(handlers::resume_download))
        .route("/downloads/:id", delete(handlers::delete_download))
        .route(
            "/downloads/clear/completed",
            delete(handlers::clear_completed),
        )
        .route("/downloads/clear/failed", delete(handlers::clear_failed))
        // 下载批量操作
        .route("/downloads/batch/pause", post(handlers::batch_pause_downloads))
        .route("/downloads/batch/resume", post(handlers::batch_resume_downloads))
        .route("/downloads/batch/delete", post(handlers::batch_delete_downloads))
        // 文件夹下载API
        .route("/downloads/folder", post(handlers::create_folder_download))
        .route(
            "/downloads/folders",
            get(handlers::get_all_folder_downloads),
        )
        .route("/downloads/folder/:id", get(handlers::get_folder_download))
        .route(
            "/downloads/folder/:id/pause",
            post(handlers::pause_folder_download),
        )
        .route(
            "/downloads/folder/:id/resume",
            post(handlers::resume_folder_download),
        )
        .route(
            "/downloads/folder/:id",
            delete(handlers::cancel_folder_download),
        )
        // 上传API
        .route("/uploads", post(handlers::create_upload))
        .route("/uploads", get(handlers::get_all_uploads))
        .route("/uploads/:id", get(handlers::get_upload))
        .route("/uploads/:id/pause", post(handlers::pause_upload))
        .route("/uploads/:id/resume", post(handlers::resume_upload))
        .route("/uploads/:id", delete(handlers::delete_upload))
        .route("/uploads/folder", post(handlers::create_folder_upload))
        .route("/uploads/batch", post(handlers::create_batch_upload))
        .route("/uploads/scan/:id", get(handlers::get_scan_status))
        .route("/uploads/scan/:id/cancel", post(handlers::cancel_scan))
        .route(
            "/uploads/clear/completed",
            post(handlers::clear_completed_uploads),
        )
        .route(
            "/uploads/clear/failed",
            post(handlers::clear_failed_uploads),
        )
        // 上传批量操作
        .route("/uploads/batch/pause", post(handlers::batch_pause_uploads))
        .route("/uploads/batch/resume", post(handlers::batch_resume_uploads))
        .route("/uploads/batch/delete", post(handlers::batch_delete_uploads))
        // 转存API
        .route("/transfers", post(handlers::create_transfer))
        .route("/transfers", get(handlers::get_all_transfers))
        .route("/transfers/preview", post(handlers::preview_share_files))
        .route("/transfers/preview/dir", post(handlers::preview_share_dir))
        .route("/transfers/cleanup", post(handlers::cleanup_orphaned_temp_dirs))
        .route("/transfers/:id", get(handlers::get_transfer))
        .route("/transfers/:id", delete(handlers::delete_transfer))
        .route("/transfers/:id/cancel", post(handlers::cancel_transfer))
        // 本地文件系统API
        .route("/fs/list", get(handlers::list_directory))
        .route("/fs/goto", get(handlers::goto_path))
        .route("/fs/validate", get(handlers::validate_path))
        .route("/fs/roots", get(handlers::get_roots))
        // 配置API
        .route("/config", get(handlers::get_config))
        .route("/config", put(handlers::update_config))
        .route("/config/recommended", get(handlers::get_recommended_config))
        .route("/config/reset", post(handlers::reset_to_recommended))
        .route("/config/recent-dir", post(handlers::update_recent_dir))
        .route(
            "/config/default-download-dir",
            post(handlers::set_default_download_dir),
        )
        // 转存配置API
        .route("/config/transfer", get(handlers::get_transfer_config))
        .route("/config/transfer", put(handlers::update_transfer_config))
        // 🔥 代理运行状态API
        .route("/proxy/status", get(handlers::get_proxy_status))
        .route("/proxy/test", post(handlers::test_proxy_connection))
        // 🔥 自动备份API
        .route("/autobackup/configs", get(handlers::autobackup::list_backup_configs))
        .route("/autobackup/configs", post(handlers::autobackup::create_backup_config))
        .route("/autobackup/configs/:id", get(handlers::autobackup::get_backup_config))
        .route("/autobackup/configs/:id", put(handlers::autobackup::update_backup_config))
        .route("/autobackup/configs/:id", delete(handlers::autobackup::delete_backup_config))
        .route("/autobackup/configs/:id/enable", post(handlers::autobackup::enable_backup_config))
        .route("/autobackup/configs/:id/disable", post(handlers::autobackup::disable_backup_config))
        .route("/autobackup/configs/:id/trigger", post(handlers::autobackup::trigger_backup))
        .route("/autobackup/configs/:id/tasks", get(handlers::autobackup::list_backup_tasks))
        .route("/autobackup/tasks/:id", get(handlers::autobackup::get_backup_task))
        .route("/autobackup/tasks/:id/cancel", post(handlers::autobackup::cancel_backup_task))
        .route("/autobackup/tasks/:id/pause", post(handlers::autobackup::pause_backup_task))
        .route("/autobackup/tasks/:id/resume", post(handlers::autobackup::resume_backup_task))
        .route("/autobackup/tasks/:id", delete(handlers::autobackup::delete_backup_task))
        .route("/autobackup/tasks/:id/files", get(handlers::autobackup::list_file_tasks))
        .route("/autobackup/tasks/:task_id/files/:file_task_id/retry", post(handlers::autobackup::retry_file_task))
        .route("/autobackup/status", get(handlers::autobackup::get_manager_status))
        .route("/autobackup/stats", get(handlers::autobackup::get_record_stats))
        .route("/autobackup/cleanup", post(handlers::autobackup::cleanup_records))
        // 🔥 加密API
        .route("/encryption/status", get(handlers::autobackup::get_encryption_status))
        .route("/encryption/key/generate", post(handlers::autobackup::generate_encryption_key))
        .route("/encryption/key/import", post(handlers::autobackup::import_encryption_key))
        .route("/encryption/key/export", get(handlers::autobackup::export_encryption_key))
        .route("/encryption/key", delete(handlers::autobackup::delete_encryption_key))
        .route("/encryption/key/force", delete(handlers::autobackup::force_delete_encryption_key))
        // 🔥 加密数据导出 API
        .route("/encryption/export-bundle", post(handlers::export_bundle))
        .route("/encryption/export-mapping", get(handlers::export_mapping))
        .route("/encryption/export-keys", get(handlers::export_keys))
        // 🔥 离线下载 API
        .route("/cloud-dl/tasks", post(handlers::cloud_dl::add_task))
        .route("/cloud-dl/tasks", get(handlers::cloud_dl::list_tasks))
        .route("/cloud-dl/tasks/clear", delete(handlers::cloud_dl::clear_tasks))
        .route("/cloud-dl/tasks/refresh", post(handlers::cloud_dl::refresh_tasks))
        .route("/cloud-dl/tasks/:task_id", get(handlers::cloud_dl::query_task))
        .route("/cloud-dl/tasks/:task_id", delete(handlers::cloud_dl::delete_task))
        .route("/cloud-dl/tasks/:task_id/cancel", post(handlers::cloud_dl::cancel_task))
        // 🔥 分享 API
        .route("/shares", post(handlers::create_share))
        .route("/shares", get(handlers::get_share_list))
        .route("/shares/cancel", post(handlers::cancel_share))
        .route("/shares/:id", get(handlers::get_share_detail))
        // 🔥 系统能力检测 API
        .route("/system/watch-capability", get(handlers::autobackup::get_watch_capability))
        // 🔥 自动备份全局触发配置 API
        .route("/config/autobackup/trigger", get(handlers::autobackup::get_trigger_config))
        .route("/config/autobackup/trigger", put(handlers::autobackup::update_trigger_config))
        // 🔥 WebSocket 路由
        .route("/ws", get(websocket::handle_websocket))
        .with_state(app_state.clone())
        // 🔥 应用 Web 认证中间件到所有 API 路由
        .layer(middleware::from_fn_with_state(
            web_auth_state.clone(),
            web_auth::web_auth_middleware,
        ));

    // 🔥 Web 访问认证 API 路由（使用独立的 WebAuthState）
    let web_auth_routes = Router::new()
        .route("/login", post(web_auth::login))
        .route("/refresh", post(web_auth::refresh))
        .route("/logout", post(web_auth::logout))
        .route("/status", get(web_auth::status))
        .route("/config", get(web_auth::get_config))
        .route("/config", put(web_auth::update_config))
        .route("/password/set", post(web_auth::set_password))
        .route("/totp/setup", post(web_auth::totp_setup))
        .route("/totp/verify", post(web_auth::totp_verify))
        .route("/totp/disable", post(web_auth::totp_disable))
        .route("/recovery-codes/regenerate", post(web_auth::regenerate_recovery_codes))
        .with_state(web_auth_state.clone());

    // 自动检测前端资源目录
    let frontend_dir = detect_frontend_dir();
    let index_html_path = frontend_dir.join("index.html");

    // 静态文件服务（前端资源）
    let static_service =
        ServeDir::new(&frontend_dir).not_found_service(ServeFile::new(&index_html_path));

    // 健康检查响应结构
    #[derive(Serialize)]
    struct HealthResponse {
        status: String,
        service: String,
    }

    // 健康检查处理器
    async fn health_check() -> Json<HealthResponse> {
        Json(HealthResponse {
            status: "ok".to_string(),
            service: "baidu-netdisk-rust".to_string(),
        })
    }

    // 🔥 先加载会话和初始化所有管理器，确保前端访问时一切就绪
    app_state.load_initial_session().await?;
    info!("应用状态初始化完成");

    // 构建完整应用
    let app = Router::new()
        .nest("/api/v1", api_routes)
        .nest("/api/v1/web-auth", web_auth_routes)
        .route("/health", get(health_check))
        .fallback_service(static_service)
        .layer(middleware);

    // 🔥 绑定端口并启动服务器
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    // 🔥 所有管理器初始化完成后再打印日志，前端访问时一切就绪
    info!("服务器启动在: http://{}", addr);
    info!("API 基础路径: http://{}/api/v1", addr);
    info!("WebSocket: ws://{}/api/v1/ws", addr);
    info!("健康检查: http://{}/health", addr);
    info!("前端页面: http://{}/", addr);

    // 🔥 使用 select! 监听关闭信号，支持优雅关闭
    let server = axum::serve(listener, app);

    tokio::select! {
        result = server => {
            if let Err(e) = result {
                tracing::error!("服务器错误: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("收到 Ctrl+C，开始优雅关闭...");
        }
    }

    // 🔥 优雅关闭
    info!("正在关闭 Web 认证清理任务...");
    web_auth_state.stop_cleanup_tasks().await;
    info!("正在关闭持久化管理器...");
    app_state.shutdown().await;
    info!("应用已安全退出");

    Ok(())
}
