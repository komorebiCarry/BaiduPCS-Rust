//! 跳过秒传（对应 BaiduPCS-Go v4.0.0 `upload --norapid`）
//!
//! 正常上传流程：先整文件读一遍计算每个分片的 MD5（block_list），用真实 block_list 调用
//! precreate —— 若网盘已有相同内容则直接秒传成功；否则拿 uploadid 分片上传，最后用同一份
//! block_list 调用 create 合并。
//!
//! 跳过秒传流程（与 BaiduPCS-Go `FakeRapidUpload` 思路一致）：
//! 1. 不读文件、不算 MD5，precreate 时传**占位** block_list（BaiduPCS-Go 使用的两个固定 MD5），
//!    网盘不可能据此秒传，只会返回 uploadid；
//! 2. 正常分片上传，每个分片的上传响应里服务端会返回该分片的 MD5；
//! 3. 合并（create）时用服务端返回的各分片 MD5 组成真正的 block_list。
//!
//! 与 BaiduPCS-Go 的差异：BaiduPCS-Go 的 precreate 不带 `size`，占位条数固定为 1/2 条；
//! 本项目 precreate 会带 `size`，为避免条数与大小不一致，占位条数取实际分片数（循环使用两个占位值）。

use anyhow::{bail, Result};

/// BaiduPCS-Go v4.0.0 `baidupcs/upload.go` 中的 `fakeBlockListMD5`
pub const NORAPID_PLACEHOLDER_MD5: [&str; 2] = [
    "5910a591dd8fc18c32a8f3df4fdc1761",
    "a5fc157d78e6ad1c7e114b056c92821e",
];

/// 生成 precreate 使用的占位 block_list（JSON 数组字符串）
///
/// 条数 = `chunk_count`（至少 1 条），内容循环使用 [`NORAPID_PLACEHOLDER_MD5`]。
pub fn placeholder_block_list(chunk_count: usize) -> String {
    let list: Vec<&str> = (0..chunk_count.max(1))
        .map(|i| NORAPID_PLACEHOLDER_MD5[i % NORAPID_PLACEHOLDER_MD5.len()])
        .collect();
    serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_string())
}

/// 用各分片上传时服务端返回的 MD5 组成合并用的 block_list（JSON 数组字符串）
///
/// 任何一个分片缺少 MD5（或为空字符串）都返回错误——此时无法正确合并，
/// 宁可让任务失败也不能用不完整的列表去 create。
pub fn block_list_from_chunk_md5s<'a, I>(chunk_md5s: I) -> Result<String>
where
    I: IntoIterator<Item = Option<&'a str>>,
{
    let mut list = Vec::new();
    let mut missing = Vec::new();
    for (idx, md5) in chunk_md5s.into_iter().enumerate() {
        match md5.map(str::trim).filter(|m| !m.is_empty()) {
            Some(m) => list.push(m.to_string()),
            None => missing.push(idx),
        }
    }
    if list.is_empty() && missing.is_empty() {
        bail!("跳过秒传合并失败：没有任何分片");
    }
    if !missing.is_empty() {
        bail!(
            "跳过秒传合并失败：{} 个分片缺少服务端返回的 MD5（如分片 {:?}），请关闭“跳过秒传”后重新上传",
            missing.len(),
            &missing[..missing.len().min(5)]
        );
    }
    Ok(serde_json::to_string(&list)?)
}

/// 恢复的已完成分片是否都带有服务端 MD5
///
/// 跳过秒传模式下合并依赖这些 MD5；老版本持久化数据可能缺失，
/// 这种情况下应回退为正常计算 block_list（仍可续传，只是需要读一遍文件）。
pub fn restored_chunks_have_md5(completed_chunks: &[usize], chunk_md5s: &[Option<String>]) -> bool {
    completed_chunks.iter().all(|&idx| {
        chunk_md5s
            .get(idx)
            .and_then(|m| m.as_deref())
            .is_some_and(|m| !m.trim().is_empty())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_matches_chunk_count_and_cycles() {
        let one: Vec<String> = serde_json::from_str(&placeholder_block_list(1)).unwrap();
        assert_eq!(one, vec![NORAPID_PLACEHOLDER_MD5[0].to_string()]);

        let zero: Vec<String> = serde_json::from_str(&placeholder_block_list(0)).unwrap();
        assert_eq!(zero.len(), 1, "至少 1 条");

        let five: Vec<String> = serde_json::from_str(&placeholder_block_list(5)).unwrap();
        assert_eq!(five.len(), 5);
        assert_eq!(five[0], NORAPID_PLACEHOLDER_MD5[0]);
        assert_eq!(five[1], NORAPID_PLACEHOLDER_MD5[1]);
        assert_eq!(five[4], NORAPID_PLACEHOLDER_MD5[0]);
    }

    #[test]
    fn merge_block_list_requires_every_chunk_md5() {
        let ok = block_list_from_chunk_md5s(vec![Some("aa"), Some(" bb ")]).unwrap();
        assert_eq!(ok, r#"["aa","bb"]"#);

        let err = block_list_from_chunk_md5s(vec![Some("aa"), None, Some("")]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("2 个分片"), "{msg}");
        assert!(msg.contains("[1, 2]"), "{msg}");

        assert!(block_list_from_chunk_md5s(Vec::<Option<&str>>::new()).is_err());
    }

    #[test]
    fn restored_md5_check() {
        let md5s = vec![Some("a".to_string()), None, Some(" ".to_string())];
        assert!(restored_chunks_have_md5(&[0], &md5s));
        assert!(!restored_chunks_have_md5(&[0, 1], &md5s));
        assert!(!restored_chunks_have_md5(&[2], &md5s));
        assert!(!restored_chunks_have_md5(&[7], &md5s));
        assert!(restored_chunks_have_md5(&[], &md5s));
    }
}
