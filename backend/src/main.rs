use axum::{
    routing::{delete, get, post, put},
    Router,
};
use baidu_netdisk_rust::{server::handlers, AppState};
use std::path::PathBuf;
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
                info!("✓ 找到前端资源目录: {:?}", path.canonicalize().unwrap_or(path.clone()));
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
        default, candidates
    );
    default
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化日志系统
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    info!("Baidu Netdisk Rust v1.3.0 启动中...");

    // 创建应用状态
    let app_state = AppState::new().await?;
    app_state.load_initial_session().await?;
    info!("应用状态初始化完成");

    // 获取配置
    let config = app_state.config.read().await.clone();
    let addr = format!("{}:{}", config.server.host, config.server.port);

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
        .route("/auth/user", get(handlers::get_current_user))
        .route("/auth/logout", post(handlers::logout))
        // 文件API
        .route("/files", get(handlers::get_file_list))
        .route("/files/download", get(handlers::get_download_url))
        .route("/files/folder", post(handlers::create_folder))
        // 下载API
        .route("/downloads", post(handlers::create_download))
        .route("/downloads", get(handlers::get_all_downloads))
        .route("/downloads/all", get(handlers::get_all_downloads_mixed))  // 新增：统一接口
        .route("/downloads/:id", get(handlers::get_download))
        .route("/downloads/:id/pause", post(handlers::pause_download))
        .route("/downloads/:id/resume", post(handlers::resume_download))
        .route("/downloads/:id", delete(handlers::delete_download))
        .route(
            "/downloads/clear/completed",
            delete(handlers::clear_completed),
        )
        .route("/downloads/clear/failed", delete(handlers::clear_failed))
        // 文件夹下载API
        .route("/downloads/folder", post(handlers::create_folder_download))
        .route("/downloads/folders", get(handlers::get_all_folder_downloads))
        .route("/downloads/folder/:id", get(handlers::get_folder_download))
        .route("/downloads/folder/:id/pause", post(handlers::pause_folder_download))
        .route("/downloads/folder/:id/resume", post(handlers::resume_folder_download))
        .route("/downloads/folder/:id", delete(handlers::cancel_folder_download))
        // 上传API
        .route("/uploads", post(handlers::create_upload))
        .route("/uploads", get(handlers::get_all_uploads))
        .route("/uploads/:id", get(handlers::get_upload))
        .route("/uploads/:id/pause", post(handlers::pause_upload))
        .route("/uploads/:id/resume", post(handlers::resume_upload))
        .route("/uploads/:id", delete(handlers::delete_upload))
        .route("/uploads/folder", post(handlers::create_folder_upload))
        .route("/uploads/batch", post(handlers::create_batch_upload))
        .route("/uploads/clear/completed", post(handlers::clear_completed_uploads))
        .route("/uploads/clear/failed", post(handlers::clear_failed_uploads))
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
        .with_state(app_state.clone());

    // 自动检测前端资源目录
    let frontend_dir = detect_frontend_dir();
    let index_html_path = frontend_dir.join("index.html");

    // 静态文件服务（前端资源）
    let static_service = ServeDir::new(&frontend_dir)
        .not_found_service(ServeFile::new(&index_html_path));

    // 构建完整应用
    let app = Router::new()
        .nest("/api/v1", api_routes)
        .route("/health", get(|| async { "OK" }))
        .fallback_service(static_service)
        .layer(middleware);

    // 启动服务器
    info!("服务器启动在: http://{}", addr);
    info!("API 基础路径: http://{}/api/v1", addr);
    info!("健康检查: http://{}/health", addr);
    info!("前端页面: http://{}/", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
