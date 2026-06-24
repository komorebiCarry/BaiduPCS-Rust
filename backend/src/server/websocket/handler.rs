//! WebSocket 路由处理器

use crate::server::websocket::message::{WsClientMessage, WsServerMessage};
use crate::AppState;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// WebSocket 路由处理器
///
/// 升级 HTTP 连接为 WebSocket，处理消息收发
pub async fn handle_websocket(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// 处理 WebSocket 连接
async fn handle_socket(socket: WebSocket, state: AppState) {
    let connection_id = Uuid::new_v4().to_string();
    info!("新的 WebSocket 连接: {}", connection_id);

    // 注册连接
    let mut message_receiver = state.ws_manager.register(connection_id.clone());

    // 发送连接成功消息
    let (mut sender, mut receiver) = socket.split();

    let connected_msg = WsServerMessage::connected(connection_id.clone());
    if let Ok(json) = serde_json::to_string(&connected_msg) {
        if sender.send(Message::Text(json)).await.is_err() {
            error!("发送连接成功消息失败");
            state.ws_manager.unregister(&connection_id);
            return;
        }
    }

    let ws_manager = Arc::clone(&state.ws_manager);
    let _conn_id = connection_id.clone();

    // 启动发送任务
    let send_task = tokio::spawn(async move {
        while let Some(message) = message_receiver.recv().await {
            match serde_json::to_string(&message) {
                Ok(json) => {
                    if sender.send(Message::Text(json)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    error!("序列化消息失败: {}", e);
                }
            }
        }
    });

    let state_recv = state.clone();
    let conn_id_recv = connection_id.clone();

    // 启动接收任务
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(message)) = receiver.next().await {
            match message {
                Message::Text(text) => {
                    handle_client_message(&state_recv, &conn_id_recv, &text).await;
                }
                Message::Binary(data) => {
                    if let Ok(text) = String::from_utf8(data) {
                        handle_client_message(&state_recv, &conn_id_recv, &text).await;
                    }
                }
                Message::Ping(_data) => {
                    state_recv.ws_manager.touch(&conn_id_recv);
                    debug!("收到 Ping: {}", conn_id_recv);
                }
                Message::Pong(_) => {
                    state_recv.ws_manager.touch(&conn_id_recv);
                    debug!("收到 Pong: {}", conn_id_recv);
                }
                Message::Close(_) => {
                    info!("收到关闭消息: {}", conn_id_recv);
                    break;
                }
            }
        }
    });

    // 等待任一任务结束
    tokio::select! {
        _ = send_task => {
            debug!("发送任务结束: {}", connection_id);
        }
        _ = recv_task => {
            debug!("接收任务结束: {}", connection_id);
        }
    }

    // 检查连接是否订阅了 cloud_dl，如果是则减少订阅者计数
    let subscriptions = ws_manager.get_subscriptions(&connection_id);
    let was_subscribed_cloud_dl = subscriptions
        .iter()
        .any(|s| s == "cloud_dl" || s.starts_with("cloud_dl:"));

    if was_subscribed_cloud_dl {
        // 🔥 按订阅时记录的 uid 减计数
        // （而不是 active_uid——切换账号后两者不一致会导致计数泄漏）
        if let Some((_, recorded_uid)) = state.cloud_dl_ws_subscribers.remove(&connection_id) {
            if let Some(monitor) = state.cloud_dl_monitor_for(recorded_uid) {
                monitor.remove_subscriber();
                debug!(
                    "连接关闭，cloud_dl 订阅者减少: connection={}, uid={}",
                    connection_id,
                    recorded_uid.raw()
                );
            }
        }
    }

    // 清理连接
    ws_manager.unregister(&connection_id);
    info!("WebSocket 连接已关闭: {}", connection_id);
}

/// 处理客户端消息
async fn handle_client_message(state: &AppState, connection_id: &str, text: &str) {
    state.ws_manager.touch(connection_id);

    match serde_json::from_str::<WsClientMessage>(text) {
        Ok(message) => match message {
            WsClientMessage::Ping { timestamp } => {
                let pong = WsServerMessage::pong(Some(timestamp));
                state.ws_manager.send_to(connection_id, pong);
            }
            WsClientMessage::RequestSnapshot => {
                debug!("收到状态快照请求: {}", connection_id);
                let snapshot = get_snapshot(state).await;
                state.ws_manager.send_to(connection_id, snapshot);
            }
            WsClientMessage::Subscribe { subscriptions } => {
                debug!("收到订阅请求: {} - {:?}", connection_id, subscriptions);

                // 检查是否订阅了 cloud_dl
                let subscribing_cloud_dl = subscriptions
                    .iter()
                    .any(|s| s == "cloud_dl" || s.starts_with("cloud_dl:"));

                // 检查之前是否已经订阅过 cloud_dl（防止重复计数）
                let was_subscribed_cloud_dl = state
                    .ws_manager
                    .get_subscriptions(connection_id)
                    .iter()
                    .any(|s| s == "cloud_dl" || s.starts_with("cloud_dl:"));

                // 添加订阅
                state.ws_manager.subscribe(connection_id, subscriptions);

                // 只有之前没订阅过 cloud_dl，现在新订阅了，才增加订阅者计数
                if subscribing_cloud_dl && !was_subscribed_cloud_dl {
                    // 🔥 记录订阅时的 uid，
                    // 让后续切账号 / 取消订阅都能找到正确的 monitor，而不是
                    // 依赖订阅时刻 vs 取消订阅时刻 active_uid 一致这种脆弱假设
                    if let Some(active_uid) = *state.active_uid.read().await {
                        // 🔥 lazy init monitor。
                        // 切到「启动时未初始化 monitor 的账号」后首次订阅时，
                        // 必须按需创建 monitor，否则 add_subscriber 静默 skip 实时事件断流。
                        // lazy init 前先 ensure_client_for_uid，
                        // 避免 monitor 用 legacy client（已移除 fallback）造成的「构造失败」。
                        if !state.cloud_dl_monitors.contains_key(&active_uid) {
                            if let Err(e) = state.ensure_client_for_uid(active_uid).await {
                                warn!(
                                    "cloud_dl 订阅前 ensure_client_for_uid 失败: uid={}, err={:?}",
                                    active_uid.raw(),
                                    e
                                );
                            } else {
                                state.init_cloud_dl_monitor_for(active_uid).await;
                            }
                        }
                        if let Some(monitor) = state.cloud_dl_monitor_for(active_uid) {
                            monitor.add_subscriber();
                            state
                                .cloud_dl_ws_subscribers
                                .insert(connection_id.to_string(), active_uid);
                            debug!(
                                "cloud_dl 订阅者增加: connection={}, uid={}",
                                connection_id,
                                active_uid.raw()
                            );
                        }
                    }
                }

                // 返回订阅成功消息
                let current_subs = state.ws_manager.get_subscriptions(connection_id);
                state.ws_manager.send_to(
                    connection_id,
                    WsServerMessage::subscribe_success(current_subs),
                );
            }
            WsClientMessage::Unsubscribe { subscriptions } => {
                debug!("收到取消订阅请求: {} - {:?}", connection_id, subscriptions);

                // 检查是否取消订阅了 cloud_dl
                let unsubscribing_cloud_dl = subscriptions
                    .iter()
                    .any(|s| s == "cloud_dl" || s.starts_with("cloud_dl:"));

                // 移除订阅
                state.ws_manager.unsubscribe(connection_id, subscriptions);

                // 如果取消订阅了 cloud_dl，**仅当退订后该连接已不再订阅任何 cloud_dl
                // 派生项时**才减 monitor 计数 + 移除映射。
                //
                // 原版本一旦本次 unsubscribe 涉及任意 cloud_dl topic 就立即减计数，
                // 但同一连接可能还订阅着其他 cloud_dl 子 topic（如 `cloud_dl:status_changed`
                // 仍在），结果连接仍命中 cloud_dl 通配路径但 monitor 认为没有订阅者
                // 不再轮询、实时事件断流。本版退订后用 `get_subscriptions` 反查派生
                // 视图，与 handler subscribe 路径用的「`s == "cloud_dl"
                // \|\| s.starts_with("cloud_dl:")`」同款判定，确保只有"完全没有
                // cloud_dl 订阅"时才释放计数。
                if unsubscribing_cloud_dl {
                    let remaining = state.ws_manager.get_subscriptions(connection_id);
                    let still_has_cloud_dl = remaining
                        .iter()
                        .any(|s| s == "cloud_dl" || s.starts_with("cloud_dl:"));
                    if !still_has_cloud_dl {
                        // 🔥 按订阅时记录的 uid 减计数
                        if let Some((_, recorded_uid)) =
                            state.cloud_dl_ws_subscribers.remove(connection_id)
                        {
                            if let Some(monitor) = state.cloud_dl_monitor_for(recorded_uid) {
                                monitor.remove_subscriber();
                                debug!(
                                    "cloud_dl 订阅者减少（最后一条 cloud_dl 派生项已退订）: connection={}, uid={}",
                                    connection_id,
                                    recorded_uid.raw()
                                );
                            }
                        }
                    } else {
                        debug!(
                            "cloud_dl 订阅未完全退订，保留 monitor 计数: connection={}, 剩余 cloud_dl 派生项={}",
                            connection_id,
                            remaining
                                .iter()
                                .filter(|s| *s == "cloud_dl" || s.starts_with("cloud_dl:"))
                                .count()
                        );
                    }
                }

                // 返回取消订阅成功消息
                let current_subs = state.ws_manager.get_subscriptions(connection_id);
                state.ws_manager.send_to(
                    connection_id,
                    WsServerMessage::unsubscribe_success(current_subs),
                );
            }
        },
        Err(e) => {
            warn!("解析客户端消息失败: {} - {}", connection_id, e);
            let error = WsServerMessage::error("PARSE_ERROR", format!("消息解析失败: {}", e));
            state.ws_manager.send_to(connection_id, error);
        }
    }
}

/// 获取当前任务状态快照
async fn get_snapshot(state: &AppState) -> WsServerMessage {
    // 获取下载任务
    let downloads: Vec<serde_json::Value> = match state.download_manager_for_active().await {
        Some(dm) => dm
            .get_all_tasks()
            .await
            .into_iter()
            .filter_map(|t| serde_json::to_value(&t).ok())
            .collect(),
        None => vec![],
    };

    // 获取文件夹下载任务
    let folders: Vec<serde_json::Value> = {
        state
            .folder_download_manager
            .get_all_folders()
            .await
            .into_iter()
            .filter_map(|f| serde_json::to_value(&f).ok())
            .collect()
    };

    // 获取上传任务
    let uploads: Vec<serde_json::Value> = match state.upload_manager_for_active().await {
        Some(um) => um
            .get_all_tasks()
            .await
            .into_iter()
            .filter_map(|t| serde_json::to_value(&t).ok())
            .collect(),
        None => vec![],
    };

    // 获取转存任务
    let transfers: Vec<serde_json::Value> = match state.transfer_manager_for_active().await {
        Some(tm) => tm
            .get_all_tasks()
            .await
            .into_iter()
            .filter_map(|t| serde_json::to_value(&t).ok())
            .collect(),
        None => vec![],
    };

    // 多账号上下文
    let (active_uid, accounts) = {
        let mgr = state.account_manager.lock().await;
        (mgr.active_uid().map(|u| u.raw()), mgr.list_accounts())
    };
    let readonly_mode = state
        .readonly_mode
        .load(std::sync::atomic::Ordering::Relaxed);

    WsServerMessage::Snapshot {
        downloads,
        uploads,
        transfers,
        folders,
        active_uid,
        accounts,
        readonly_mode,
    }
}
