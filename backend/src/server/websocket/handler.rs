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
    let was_subscribed_cloud_dl = subscriptions.iter().any(|s| {
        s == "cloud_dl" || s.starts_with("cloud_dl:")
    });

    if was_subscribed_cloud_dl {
        if let Some(ref monitor) = *state.cloud_dl_monitor.read().await {
            monitor.remove_subscriber();
            debug!("连接关闭，cloud_dl 订阅者减少: {}", connection_id);
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
                let subscribing_cloud_dl = subscriptions.iter().any(|s| {
                    s == "cloud_dl" || s.starts_with("cloud_dl:")
                });

                // 检查之前是否已经订阅过 cloud_dl（防止重复计数）
                let was_subscribed_cloud_dl = state.ws_manager.get_subscriptions(connection_id)
                    .iter()
                    .any(|s| s == "cloud_dl" || s.starts_with("cloud_dl:"));

                // 添加订阅
                state.ws_manager.subscribe(connection_id, subscriptions);

                // 只有之前没订阅过 cloud_dl，现在新订阅了，才增加订阅者计数
                if subscribing_cloud_dl && !was_subscribed_cloud_dl {
                    if let Some(ref monitor) = *state.cloud_dl_monitor.read().await {
                        monitor.add_subscriber();
                        debug!("cloud_dl 订阅者增加: {}", connection_id);
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
                let unsubscribing_cloud_dl = subscriptions.iter().any(|s| {
                    s == "cloud_dl" || s.starts_with("cloud_dl:")
                });

                // 移除订阅
                state.ws_manager.unsubscribe(connection_id, subscriptions);

                // 如果取消订阅了 cloud_dl，通知监听服务减少订阅者
                if unsubscribing_cloud_dl {
                    if let Some(ref monitor) = *state.cloud_dl_monitor.read().await {
                        monitor.remove_subscriber();
                        debug!("cloud_dl 订阅者减少: {}", connection_id);
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
    let downloads: Vec<serde_json::Value> = {
        let dm_lock = state.download_manager.read().await;
        if let Some(ref dm) = *dm_lock {
            dm.get_all_tasks()
                .await
                .into_iter()
                .filter_map(|t| serde_json::to_value(&t).ok())
                .collect()
        } else {
            vec![]
        }
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
    let uploads: Vec<serde_json::Value> = {
        let um_lock = state.upload_manager.read().await;
        if let Some(ref um) = *um_lock {
            um.get_all_tasks()
                .await
                .into_iter()
                .filter_map(|t| serde_json::to_value(&t).ok())
                .collect()
        } else {
            vec![]
        }
    };

    // 获取转存任务
    let transfers: Vec<serde_json::Value> = {
        let tm_lock = state.transfer_manager.read().await;
        if let Some(ref tm) = *tm_lock {
            tm.get_all_tasks()
                .await
                .into_iter()
                .filter_map(|t| serde_json::to_value(&t).ok())
                .collect()
        } else {
            vec![]
        }
    };

    WsServerMessage::Snapshot {
        downloads,
        uploads,
        transfers,
        folders,
    }
}
