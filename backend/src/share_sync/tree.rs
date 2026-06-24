//! share-sync 分享树构建与子树切分(纯函数,无 I/O)
//!
//! ## 角色
//!
//! 阶段 4 "Quota 二分递归" 的基石。share-sync 抓取得到一组扁平 `ShareSnapshotItem`
//! (含 path / fs_id / is_dir),这里把它们重建成一棵带父子关系的树,并提供:
//! 1. 整棵子树拍扁后提交给 transfer 的入口集合(给 transfer 一个目录 fs_id 就行)
//! 2. 当 quota / 目录场景失败时,把一个父节点对半切成两组子节点 idx,递归地再各自试一次
//! 3. 失败兜底时把子树所有叶子(文件级)展开出来,转成单文件粒度
//!
//! ## 设计要点
//!
//! - **虚拟根**: 真实分享顶层可能有多个文件/目录,统一挂在 `nodes[0]` 这个
//!   path="/" 的虚拟根下面,简化 split_two 调用方
//! - **缺失中间目录的兼容**: snapshot BFS 抓取理论上目录都有 item,但万一 path
//!   含跳层(只 list 了文件没 list 父目录),build 会自动 placeholder 一个
//!   `fs_id=0` 的占位目录节点;调用方应通过 `is_placeholder()` 判断,placeholder
//!   节点不能直接喂给 transfer(没有真 fs_id),要 fallback 到展开子树
//! - **split_two 的对半策略**: LPT(Longest Processing Time first)启发式 —
//!   按子节点 subtree_size 降序排,每次塞进"当前总 size 较小"的那组。
//!   既不至于把大目录全塞一边,也保持 O(N log N) 不动用花式 NP

use crate::share_sync::snapshot::ShareSnapshotItem;

/// 单个树节点
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeNode {
    /// path,与原始 ShareSnapshotItem.path 一致;虚拟根的 path 为 "/"
    pub path: String,
    /// 百度 fs_id;占位节点为 0(见 is_placeholder)
    pub fs_id: u64,
    pub is_dir: bool,
    /// 节点自身的 size;子树总 size 不在这里,要用 subtree_size()
    pub size: u64,
    /// 节点名 basename;虚拟根为空字符串
    pub name: String,
    /// 子节点在 Tree::nodes 里的下标
    pub children: Vec<usize>,
    /// 父节点下标;虚拟根为 None
    pub parent: Option<usize>,
}

impl TreeNode {
    /// 占位节点:build 时为缺失的中间目录补的伪节点,不能直接走 transfer
    #[inline]
    pub fn is_placeholder(&self) -> bool {
        self.is_dir && self.fs_id == 0 && self.parent.is_some()
    }
}

/// 一棵分享树。`nodes[0]` 永远是虚拟根。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tree {
    pub nodes: Vec<TreeNode>,
    /// 虚拟根的 idx,固定 0
    pub root: usize,
}

impl Tree {
    /// 节点数(含虚拟根)
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// 只含虚拟根? 即 items 为空。
    pub fn is_empty(&self) -> bool {
        self.nodes.len() <= 1
    }

    /// 取节点(panics 越界,内部使用为主)
    pub fn get(&self, idx: usize) -> &TreeNode {
        &self.nodes[idx]
    }

    /// 子树(以 root_idx 为根)的累计 size,目录的 size 折叠为子节点 size 之和。
    /// 用 DFS,O(N),非递归(分享深度极端时避免栈爆)。
    pub fn subtree_size(&self, root_idx: usize) -> u64 {
        let mut total: u64 = 0;
        let mut stack = vec![root_idx];
        while let Some(idx) = stack.pop() {
            let n = &self.nodes[idx];
            // 目录自己的 size 在 snapshot 里固定 0;文件 size 累加。
            // placeholder 也 0 不影响。
            total = total.saturating_add(n.size);
            for &c in &n.children {
                stack.push(c);
            }
        }
        total
    }

    /// 子树叶子(文件,不含目录)的 idx 列表 — 当一个目录最终无法整体转存,
    /// 兜底要逐文件提交。深度优先,稳定按 children 顺序。
    pub fn descendants_leaves(&self, root_idx: usize) -> Vec<usize> {
        let mut out = Vec::new();
        let mut stack = vec![root_idx];
        while let Some(idx) = stack.pop() {
            let n = &self.nodes[idx];
            if !n.is_dir {
                out.push(idx);
                continue;
            }
            // 反向 push 保证最终顺序与 children 一致
            for &c in n.children.iter().rev() {
                stack.push(c);
            }
        }
        out
    }
}

/// 由扁平 items 列表重建一棵树
///
/// items 不要求按任何顺序;path 必须以 "/" 开头(share-sync 的 normalize_share_path
/// 已保证)。重复 path 后写覆盖先写(理论不应发生,持久化 snapshot 有 UNIQUE 约束)。
pub fn build(items: &[ShareSnapshotItem]) -> Tree {
    // 1) 准备虚拟根
    let mut nodes: Vec<TreeNode> = vec![TreeNode {
        path: "/".to_string(),
        fs_id: 0,
        is_dir: true,
        size: 0,
        name: String::new(),
        children: Vec::new(),
        parent: None,
    }];
    // path → idx 索引,O(log N) 查找祖先链
    use std::collections::BTreeMap;
    let mut path_to_idx: BTreeMap<String, usize> = BTreeMap::new();
    path_to_idx.insert("/".to_string(), 0);

    // 2) 按 path 段数升序处理,父节点先建好。同一段数下按字典序稳定
    let mut sorted: Vec<&ShareSnapshotItem> = items.iter().collect();
    sorted.sort_by(|a, b| {
        let da = a.path.matches('/').count();
        let db = b.path.matches('/').count();
        da.cmp(&db).then_with(|| a.path.cmp(&b.path))
    });

    // 3) 逐 item 挂树
    for item in sorted {
        // 找/造父链。parent_path 是 item.path 去掉最后一段。
        let parent_path = parent_of(&item.path);
        let parent_idx = ensure_path(&mut nodes, &mut path_to_idx, &parent_path);
        // item 自己可能已经被前面 placeholder 过(罕见:子文件先抵达?),
        // 我们已按 depth 升序,所以正常不会;但保险起见若已存在就 overwrite 关键字段。
        if let Some(&existing) = path_to_idx.get(&item.path) {
            let n = &mut nodes[existing];
            n.fs_id = item.fs_id;
            n.is_dir = item.is_dir;
            n.size = item.size;
            n.name = item.name.clone();
            // parent / children 不动
            continue;
        }
        let idx = nodes.len();
        nodes.push(TreeNode {
            path: item.path.clone(),
            fs_id: item.fs_id,
            is_dir: item.is_dir,
            size: item.size,
            name: item.name.clone(),
            children: Vec::new(),
            parent: Some(parent_idx),
        });
        path_to_idx.insert(item.path.clone(), idx);
        nodes[parent_idx].children.push(idx);
    }

    Tree { nodes, root: 0 }
}

/// 把 `node_idx` 节点的子节点对半切成两组(LPT 启发式装箱)
///
/// 返回值:
/// - 空向量 `[]`: node 是叶子(无 children),调用方应当走"单文件提交"路径
/// - `[[c1, c2, ...]]`: node 只有 1 个子节点,无法再切两半(返回单组)
/// - `[[group_a], [group_b]]`: 正常两组,每组按 LPT 平衡 subtree_size
///
/// 复杂度: O(K log K),K = node.children.len()
pub fn split_two(tree: &Tree, node_idx: usize) -> Vec<Vec<usize>> {
    let node = &tree.nodes[node_idx];
    split_indices_two(tree, &node.children)
}

/// 把任意一组节点 idx 按 LPT 启发式对半切成两组。
///
/// 与 `split_two` 的区别:`split_two` 切的是某个节点的 children;这里切的是
/// 任意一组兄弟节点 — 适用于阶段 4 二分递归"上一轮已经从 split_two 出来的
/// 一组 idx 又失败了,再对这组继续对半切"的场景。
///
/// - `[]` 入参 → `[]`
/// - 1 个入参 → `[[那一个]]`
/// - ≥2 个 → `[[group_a], [group_b]]`
pub fn split_indices_two(tree: &Tree, indices: &[usize]) -> Vec<Vec<usize>> {
    if indices.is_empty() {
        return Vec::new();
    }
    if indices.len() == 1 {
        return vec![vec![indices[0]]];
    }
    let mut weighted: Vec<(usize, u64)> =
        indices.iter().map(|&c| (c, tree.subtree_size(c))).collect();
    weighted.sort_by(|a, b| b.1.cmp(&a.1));

    let mut group_a: Vec<usize> = Vec::new();
    let mut group_b: Vec<usize> = Vec::new();
    let mut size_a: u64 = 0;
    let mut size_b: u64 = 0;
    for (idx, sz) in weighted {
        if size_a <= size_b {
            group_a.push(idx);
            size_a = size_a.saturating_add(sz);
        } else {
            group_b.push(idx);
            size_b = size_b.saturating_add(sz);
        }
    }
    if group_b.is_empty() {
        return vec![group_a];
    }
    vec![group_a, group_b]
}

/// 把一组节点 idx 转换为可提交给 transfer 的 ShareSnapshotItem 列表
///
/// 不展开子树:目录就是目录 fs_id,百度 transfer API 服务端会递归整目录搬走。
/// 占位节点(`is_placeholder()`)会被丢弃,因为没有真 fs_id。
pub fn nodes_to_items(tree: &Tree, indices: &[usize]) -> Vec<ShareSnapshotItem> {
    indices
        .iter()
        .filter_map(|&idx| {
            let n = &tree.nodes[idx];
            if n.is_placeholder() {
                return None;
            }
            Some(ShareSnapshotItem {
                path: n.path.clone(),
                raw_path: n.path.clone(),
                fs_id: n.fs_id,
                size: n.size,
                name: n.name.clone(),
                is_dir: n.is_dir,
            })
        })
        .collect()
}

// ============ 内部工具 ============

/// 从 path 取父 path。"/foo/bar/baz.csv" → "/foo/bar"; "/foo" → "/"
fn parent_of(path: &str) -> String {
    match path.rsplit_once('/') {
        // "/foo" -> ("", "foo") -> 父是 "/"
        Some(("", _)) => "/".to_string(),
        Some((parent, _)) => parent.to_string(),
        None => "/".to_string(),
    }
}

/// 从 nodes 里找一个 path 对应的 idx,不存在则创建占位 + 递归补祖先链。
fn ensure_path(
    nodes: &mut Vec<TreeNode>,
    path_to_idx: &mut std::collections::BTreeMap<String, usize>,
    path: &str,
) -> usize {
    if let Some(&idx) = path_to_idx.get(path) {
        return idx;
    }
    // 递归补父
    let parent_path = parent_of(path);
    let parent_idx = ensure_path(nodes, path_to_idx, &parent_path);
    let name = path.rsplit('/').next().unwrap_or("").to_string();
    let idx = nodes.len();
    nodes.push(TreeNode {
        path: path.to_string(),
        fs_id: 0, // placeholder
        is_dir: true,
        size: 0,
        name,
        children: Vec::new(),
        parent: Some(parent_idx),
    });
    path_to_idx.insert(path.to_string(), idx);
    nodes[parent_idx].children.push(idx);
    idx
}

// =====================================================
// 单元测试
// =====================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::share_sync::snapshot::ShareSnapshotItem;

    fn file(path: &str, fs_id: u64, size: u64) -> ShareSnapshotItem {
        let name = path.rsplit('/').next().unwrap_or("").to_string();
        ShareSnapshotItem::new(path, name, fs_id, size, false)
    }

    fn dir(path: &str, fs_id: u64) -> ShareSnapshotItem {
        let name = path.rsplit('/').next().unwrap_or("").to_string();
        ShareSnapshotItem::new(path, name, fs_id, 0, true)
    }

    #[test]
    fn test_build_empty_items_only_root() {
        let t = build(&[]);
        assert!(t.is_empty());
        assert_eq!(t.len(), 1);
        assert_eq!(t.get(0).path, "/");
        assert!(t.get(0).is_dir);
    }

    #[test]
    fn test_build_single_file_root() {
        let t = build(&[file("/a.txt", 11, 100)]);
        assert_eq!(t.len(), 2);
        assert_eq!(t.get(0).children, vec![1]);
        assert_eq!(t.get(1).path, "/a.txt");
        assert_eq!(t.get(1).fs_id, 11);
        assert!(!t.get(1).is_dir);
    }

    #[test]
    fn test_build_nested_with_dirs() {
        let items = vec![
            dir("/curated", 100),
            dir("/curated/fina", 101),
            file("/curated/fina/600.csv", 1, 1000),
            file("/curated/fina/601.csv", 2, 2000),
            file("/curated/fina/602.csv", 3, 3000),
        ];
        let t = build(&items);
        assert_eq!(t.len(), 6);
        // 根 → curated
        assert_eq!(t.get(0).children.len(), 1);
        let curated = t.get(0).children[0];
        assert_eq!(t.get(curated).path, "/curated");
        assert_eq!(t.get(curated).fs_id, 100);
        // curated → fina
        let fina = t.get(curated).children[0];
        assert_eq!(t.get(fina).path, "/curated/fina");
        // fina → 3 files
        assert_eq!(t.get(fina).children.len(), 3);
        // subtree_size 累加
        assert_eq!(t.subtree_size(fina), 6000);
        assert_eq!(t.subtree_size(curated), 6000);
        assert_eq!(t.subtree_size(0), 6000);
    }

    #[test]
    fn test_build_missing_intermediate_dir_creates_placeholder() {
        // 只给文件,跳过中间 dir
        let items = vec![
            file("/curated/fina/a.csv", 1, 100),
            file("/curated/fina/b.csv", 2, 200),
        ];
        let t = build(&items);
        // 应当 placeholder 两个 dir: /curated, /curated/fina
        let curated_idx = t
            .nodes
            .iter()
            .position(|n| n.path == "/curated")
            .expect("curated 应被 placeholder");
        let fina_idx = t
            .nodes
            .iter()
            .position(|n| n.path == "/curated/fina")
            .expect("fina 应被 placeholder");
        assert!(t.get(curated_idx).is_placeholder());
        assert!(t.get(fina_idx).is_placeholder());
        assert_eq!(t.subtree_size(fina_idx), 300);
    }

    #[test]
    fn test_descendants_leaves_skips_dirs() {
        let items = vec![
            dir("/curated", 100),
            dir("/curated/fina", 101),
            file("/curated/fina/a.csv", 1, 100),
            file("/curated/b.csv", 2, 200),
        ];
        let t = build(&items);
        let leaves = t.descendants_leaves(0);
        let paths: Vec<&str> = leaves.iter().map(|&i| t.get(i).path.as_str()).collect();
        // 顺序:dir 优先深度 DFS,因 sort 后 /curated/b.csv 与 fina 同级
        // 实际 DFS 顺序按 children 推入栈反向出栈,具体顺序见测试断言两条都在
        assert_eq!(paths.len(), 2);
        assert!(paths.contains(&"/curated/fina/a.csv"));
        assert!(paths.contains(&"/curated/b.csv"));
    }

    #[test]
    fn test_split_two_leaf_returns_empty() {
        let items = vec![file("/a.txt", 1, 100)];
        let t = build(&items);
        // 文件节点(idx=1)是叶子
        let groups = split_two(&t, 1);
        assert!(groups.is_empty(), "叶子无 children,应返回空");
    }

    #[test]
    fn test_split_two_single_child_returns_one_group() {
        let items = vec![dir("/c", 10), file("/c/a.txt", 1, 100)];
        let t = build(&items);
        let c_idx = t.nodes.iter().position(|n| n.path == "/c").unwrap();
        let groups = split_two(&t, c_idx);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 1);
    }

    #[test]
    fn test_split_two_lpt_balances_by_size() {
        // 4 个子文件: size 100, 200, 300, 400
        // LPT 期望: 降序 [400,300,200,100],400→A(A=400), 300→B(B=300),
        //          200→B(B=500),100→A(A=500) → 两组 size 都 500
        let items = vec![
            dir("/c", 10),
            file("/c/a.txt", 1, 100),
            file("/c/b.txt", 2, 200),
            file("/c/c.txt", 3, 300),
            file("/c/d.txt", 4, 400),
        ];
        let t = build(&items);
        let c_idx = t.nodes.iter().position(|n| n.path == "/c").unwrap();
        let groups = split_two(&t, c_idx);
        assert_eq!(groups.len(), 2);
        let size_a: u64 = groups[0].iter().map(|&i| t.subtree_size(i)).sum();
        let size_b: u64 = groups[1].iter().map(|&i| t.subtree_size(i)).sum();
        assert_eq!(size_a, 500);
        assert_eq!(size_b, 500);
    }

    #[test]
    fn test_split_two_three_children_uneven() {
        // 3 个 size 100, 100, 500 → LPT: 500→A(A=500), 100→B, 100→B
        // 结果: A=[500], B=[100,100],size 500 vs 200 — 仍是最佳两分
        let items = vec![
            dir("/c", 10),
            file("/c/big.csv", 1, 500),
            file("/c/sm1.csv", 2, 100),
            file("/c/sm2.csv", 3, 100),
        ];
        let t = build(&items);
        let c_idx = t.nodes.iter().position(|n| n.path == "/c").unwrap();
        let groups = split_two(&t, c_idx);
        assert_eq!(groups.len(), 2);
        let sizes: Vec<u64> = groups
            .iter()
            .map(|g| g.iter().map(|&i| t.subtree_size(i)).sum())
            .collect();
        assert!(sizes.contains(&500));
        assert!(sizes.contains(&200));
    }

    #[test]
    fn test_nodes_to_items_skips_placeholder() {
        let items = vec![
            // 跳过 /curated 不给 item,/curated 会变 placeholder
            dir("/curated/fina", 101),
            file("/curated/fina/a.csv", 1, 100),
        ];
        let t = build(&items);
        let curated_idx = t.nodes.iter().position(|n| n.path == "/curated").unwrap();
        let fina_idx = t
            .nodes
            .iter()
            .position(|n| n.path == "/curated/fina")
            .unwrap();
        // 喂 placeholder + 真实 → placeholder 被丢
        let out = nodes_to_items(&t, &[curated_idx, fina_idx]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].path, "/curated/fina");
        assert_eq!(out[0].fs_id, 101);
    }

    #[test]
    fn test_nodes_to_items_preserves_is_dir() {
        let items = vec![dir("/c", 10), file("/c/a.txt", 1, 100)];
        let t = build(&items);
        let c_idx = t.nodes.iter().position(|n| n.path == "/c").unwrap();
        let out = nodes_to_items(&t, &[c_idx]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fs_id, 10);
        assert!(out[0].is_dir, "目录节点的 is_dir 应保留");
    }

    #[test]
    fn test_parent_of() {
        assert_eq!(parent_of("/foo"), "/");
        assert_eq!(parent_of("/foo/bar"), "/foo");
        assert_eq!(parent_of("/foo/bar/baz.csv"), "/foo/bar");
        assert_eq!(parent_of("/"), "/");
    }

    /// 真实场景模拟: 量化资料同步那种 200+ 文件的同分享,
    /// build 不应 panic,split_two 应当能正常对半切
    #[test]
    fn test_build_large_fan_out_smoke() {
        let mut items = vec![dir("/curated", 100), dir("/curated/fina", 101)];
        for i in 0..200_u64 {
            items.push(file(
                &format!("/curated/fina/600{:03}.SH.csv", i),
                1000 + i,
                1024 * i,
            ));
        }
        let t = build(&items);
        assert_eq!(t.len(), 1 + 2 + 200);
        let fina_idx = t
            .nodes
            .iter()
            .position(|n| n.path == "/curated/fina")
            .unwrap();
        let leaves = t.descendants_leaves(fina_idx);
        assert_eq!(leaves.len(), 200);
        let groups = split_two(&t, fina_idx);
        assert_eq!(groups.len(), 2);
        // LPT 平衡:两组 size 差 ≤ 最大单文件 size(假设无并列时的 LPT bound)
        let sa: u64 = groups[0].iter().map(|&i| t.subtree_size(i)).sum();
        let sb: u64 = groups[1].iter().map(|&i| t.subtree_size(i)).sum();
        let diff = sa.abs_diff(sb);
        let max_single = (199u64) * 1024;
        assert!(
            diff <= max_single,
            "LPT 两组差 {} 应当 ≤ 最大单文件 {}",
            diff,
            max_single
        );
    }
}
