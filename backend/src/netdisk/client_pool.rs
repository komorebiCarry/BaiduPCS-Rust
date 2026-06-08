// 多账号 NetdiskClient 池
//
// 按 Uid 隔离客户端实例，**禁止**暴露 iter_all_clients / get_any 类 API。

use crate::auth::Uid;
use crate::netdisk::NetdiskClient;
use std::collections::HashMap;
use std::sync::Arc;

/// Per-uid 网盘客户端池。
#[derive(Debug)]
pub struct ClientPool {
    clients: HashMap<Uid, Arc<NetdiskClient>>,
}

impl ClientPool {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }

    /// 注册一个账号的客户端。
    pub fn add_client(&mut self, uid: Uid, client: Arc<NetdiskClient>) {
        self.clients.insert(uid, client);
    }

    /// 按 Uid 获取客户端。
    pub fn get_client(&self, uid: Uid) -> Option<Arc<NetdiskClient>> {
        self.clients.get(&uid).cloned()
    }

    /// 移除一个账号的客户端（账号删除时调用）。
    pub fn remove_client(&mut self, uid: Uid) -> Option<Arc<NetdiskClient>> {
        self.clients.remove(&uid)
    }

    /// 当前已注册的账号数。
    pub fn len(&self) -> usize {
        self.clients.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    /// 返回所有已注册的 Uid（仅用于启动预热等场景）。
    pub fn uids(&self) -> Vec<Uid> {
        self.clients.keys().copied().collect()
    }
}

impl Default for ClientPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::UserAuth;

    fn make_client(uid: u64) -> Arc<NetdiskClient> {
        let user = UserAuth::new(uid, format!("user_{uid}"), format!("bduss_{uid}"));
        Arc::new(NetdiskClient::new(user).expect("create test client"))
    }

    #[test]
    fn add_get_remove() {
        let mut pool = ClientPool::new();
        let uid = Uid::new(42);
        let client = make_client(42);

        pool.add_client(uid, client);
        assert_eq!(pool.len(), 1);
        assert!(pool.get_client(uid).is_some());
        assert!(pool.get_client(Uid::new(99)).is_none());

        pool.remove_client(uid);
        assert!(pool.get_client(uid).is_none());
        assert!(pool.is_empty());
    }

    #[test]
    fn uids_listing() {
        let mut pool = ClientPool::new();
        pool.add_client(Uid::new(1), make_client(1));
        pool.add_client(Uid::new(2), make_client(2));

        let mut uids = pool.uids();
        uids.sort();
        assert_eq!(uids, vec![Uid::new(1), Uid::new(2)]);
    }
}
