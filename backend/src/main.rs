use axum::{
    routing::{delete, get, post, put},
    Router,
};
use baidu_netdisk_rust::{server::handlers, AppState};
use tower::ServiceBuilder;
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化日志系统
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    info!("Baidu Netdisk Rust v1.0.0 启动中...");

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
        // 下载API
        .route("/downloads", post(handlers::create_download))
        .route("/downloads", get(handlers::get_all_downloads))
        .route("/downloads/:id", get(handlers::get_download))
        .route("/downloads/:id/pause", post(handlers::pause_download))
        .route("/downloads/:id/resume", post(handlers::resume_download))
        .route("/downloads/:id", delete(handlers::delete_download))
        .route(
            "/downloads/clear/completed",
            delete(handlers::clear_completed),
        )
        .route("/downloads/clear/failed", delete(handlers::clear_failed))
        // 配置API
        .route("/config", get(handlers::get_config))
        .route("/config", put(handlers::update_config))
        .route("/config/recommended", get(handlers::get_recommended_config))
        .route("/config/reset", post(handlers::reset_to_recommended))
        .with_state(app_state.clone());

    // 静态文件服务（前端资源）
    let static_service = ServeDir::new("../frontend/dist")
        .not_found_service(ServeFile::new("../frontend/dist/index.html"));

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
