//! 排除规则（glob 与显式正则）的编译、校验与路径匹配。
//!
//! 两类规则都是**整条路径**匹配（`^(?:pattern)$`）、默认区分大小写，且目录命中时
//! 连同整棵子树一起排除。glob 沿用历史语义（仅 `*` / `?`，`*` 可跨越 `/`，其余字符
//! 字面匹配）；正则使用 Rust 语法，不支持 lookaround / 反向引用。
//!
//! 校验（`validate_*`）与编译（`CompiledRules::compile`）共用同一套边界与限制，
//! 因此订阅保存、全局预设保存和实际扫描看到的判定完全一致。

use regex::{RegexBuilder, RegexSet, RegexSetBuilder};

/// 正则规则（含预设）条数上限。
pub const MAX_REGEX_RULES: usize = 64;
/// 单条正则的 UTF-8 字节上限。
pub const MAX_REGEX_RULE_BYTES: usize = 1024;
/// 全部正则规则的 UTF-8 字节上限。
pub const MAX_REGEX_TOTAL_BYTES: usize = 32768;
/// 试匹配路径的 UTF-8 字节上限。
pub const MAX_TEST_PATH_BYTES: usize = 4096;

const REGEX_NEST_LIMIT: u32 = 64;
const REGEX_SIZE_LIMIT: usize = 4 * 1024 * 1024;
const REGEX_DFA_SIZE_LIMIT: usize = 2 * 1024 * 1024;
/// 历史 glob 沿用 regex crate 默认的 10MiB 编译预算。
const GLOB_SIZE_LIMIT: usize = 10 * 1024 * 1024;

/// 规则错误所属的规则类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleKind {
    Glob,
    Regex,
    /// 试匹配路径本身不合法（例如超过长度上限）。
    Path,
    /// 规则列表整体不合法（条数 / 总字节），没有可指向的单行。
    Rules,
}

impl RuleKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RuleKind::Glob => "glob",
            RuleKind::Regex => "regex",
            RuleKind::Path => "path",
            RuleKind::Rules => "rules",
        }
    }
}

/// 一条可读的规则错误：类型 + 0 起的列表下标（如有）+ 简短原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleError {
    pub kind: RuleKind,
    pub index: Option<usize>,
    pub message: String,
}

impl RuleError {
    fn new(kind: RuleKind, index: Option<usize>, message: impl Into<String>) -> Self {
        Self {
            kind,
            index,
            message: message.into(),
        }
    }

    fn glob(index: Option<usize>, message: impl Into<String>) -> Self {
        Self::new(RuleKind::Glob, index, message)
    }

    fn regex(index: Option<usize>, message: impl Into<String>) -> Self {
        Self::new(RuleKind::Regex, index, message)
    }

    fn rules(message: impl Into<String>) -> Self {
        Self::new(RuleKind::Rules, None, message)
    }

    fn path(message: impl Into<String>) -> Self {
        Self::new(RuleKind::Path, None, message)
    }
}

impl std::fmt::Display for RuleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.index {
            Some(i) => write!(
                f,
                "排除{}规则第 {} 条：{}",
                match self.kind {
                    RuleKind::Glob => "通配符",
                    _ => "正则",
                },
                i + 1,
                self.message
            ),
            None => write!(
                f,
                "排除{}规则：{}",
                match self.kind {
                    RuleKind::Glob => "通配符",
                    RuleKind::Regex => "正则",
                    RuleKind::Path => "试匹配路径",
                    RuleKind::Rules => "",
                },
                self.message
            ),
        }
    }
}

impl std::error::Error for RuleError {}

/// 简单 glob → regex 转换（仅 `*` 和 `?`，其余元字符转义）。
pub fn glob_to_regex(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() * 2);
    for ch in pattern.chars() {
        match ch {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            '.' | '+' | '(' | ')' | '|' | '^' | '$' | '{' | '}' | '[' | ']' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn anchor(pattern: &str) -> String {
    format!("^(?:{})$", pattern)
}

fn glob_regexes(patterns: &[String]) -> Vec<String> {
    patterns
        .iter()
        .map(|p| format!("^{}$", glob_to_regex(p)))
        .collect()
}

fn compile_glob_set(patterns: &[String]) -> Result<Option<RegexSet>, RuleError> {
    if patterns.is_empty() {
        return Ok(None);
    }
    RegexSetBuilder::new(glob_regexes(patterns))
        .size_limit(GLOB_SIZE_LIMIT)
        .build()
        .map(Some)
        .map_err(|e| RuleError::glob(None, format!("无法编译通配符规则：{e}")))
}

fn validate_one_regex(index: usize, pattern: &str) -> Result<(), RuleError> {
    if pattern.trim().is_empty() {
        return Err(RuleError::regex(Some(index), "正则规则不能为空"));
    }
    if pattern.len() > MAX_REGEX_RULE_BYTES {
        return Err(RuleError::regex(
            Some(index),
            format!(
                "单条正则最多 {MAX_REGEX_RULE_BYTES} 字节（当前 {} 字节）",
                pattern.len()
            ),
        ));
    }
    RegexBuilder::new(&anchor(pattern))
        .nest_limit(REGEX_NEST_LIMIT)
        .size_limit(REGEX_SIZE_LIMIT)
        .dfa_size_limit(REGEX_DFA_SIZE_LIMIT)
        .build()
        .map(|_| ())
        .map_err(|e| RuleError::regex(Some(index), format!("无法编译正则：{e}")))
}

fn compile_regex_set(patterns: &[String]) -> Result<Option<RegexSet>, RuleError> {
    if patterns.is_empty() {
        return Ok(None);
    }
    if patterns.len() > MAX_REGEX_RULES {
        return Err(RuleError::rules(format!(
            "正则规则最多 {MAX_REGEX_RULES} 条（当前 {} 条）",
            patterns.len()
        )));
    }
    let mut total = 0usize;
    for (i, p) in patterns.iter().enumerate() {
        validate_one_regex(i, p)?;
        total += p.len();
    }
    if total > MAX_REGEX_TOTAL_BYTES {
        return Err(RuleError::rules(format!(
            "正则规则合计最多 {MAX_REGEX_TOTAL_BYTES} 字节（当前 {total} 字节）"
        )));
    }
    let anchored: Vec<String> = patterns.iter().map(|p| anchor(p)).collect();
    RegexSetBuilder::new(anchored)
        .nest_limit(REGEX_NEST_LIMIT)
        .size_limit(REGEX_SIZE_LIMIT)
        .dfa_size_limit(REGEX_DFA_SIZE_LIMIT)
        .build()
        .map(Some)
        .map_err(|e| RuleError::regex(None, format!("无法编译正则规则集：{e}")))
}

/// 校验通配符规则（历史规则不施加条数 / 文本长度限制，但编译失败必须显式报错）。
pub fn validate_globs(patterns: &[String]) -> Result<(), RuleError> {
    compile_glob_set(patterns).map(|_| ())
}

/// 校验正则规则：条数、单条字节、总字节、嵌套与编译预算。
pub fn validate_regexes(patterns: &[String]) -> Result<(), RuleError> {
    compile_regex_set(patterns).map(|_| ())
}

/// 校验一份订阅的完整排除配置（glob + 正则）。
pub fn validate_rules(globs: &[String], regexes: &[String]) -> Result<(), RuleError> {
    validate_globs(globs)?;
    validate_regexes(regexes)
}

/// 按「相对分享根」语义归一化试匹配路径（与快照条目 path 一致）。
///
/// 统一分隔符、补前导 `/`、折叠重复 `/`、去掉尾部 `/`；空路径（含仅斜杠）返回 `/`。
pub fn normalize_test_path(raw: &str) -> Result<String, RuleError> {
    if raw.len() > MAX_TEST_PATH_BYTES {
        return Err(RuleError::path(format!(
            "试匹配路径最多 {MAX_TEST_PATH_BYTES} 字节"
        )));
    }
    let trimmed = raw.trim().replace('\\', "/");
    if trimmed.is_empty() {
        return Ok("/".to_string());
    }
    let mut collapsed = String::with_capacity(trimmed.len() + 1);
    if !trimmed.starts_with('/') {
        collapsed.push('/');
    }
    let mut prev_slash = false;
    for ch in trimmed.chars() {
        if ch == '/' {
            if !prev_slash {
                collapsed.push(ch);
            }
            prev_slash = true;
        } else {
            collapsed.push(ch);
            prev_slash = false;
        }
    }
    if collapsed == "/" {
        return Ok(collapsed);
    }
    Ok(collapsed.trim_end_matches('/').to_string())
}

/// 编译好的 glob + 正则规则集；一次编译，逐条目复用。
#[derive(Debug, Default, Clone)]
pub struct CompiledRules {
    glob: Option<RegexSet>,
    regex: Option<RegexSet>,
}

impl CompiledRules {
    /// 编译并顺带校验（与 `validate_rules` 使用同一套限制）。
    pub fn compile(globs: &[String], regexes: &[String]) -> Result<Self, RuleError> {
        Ok(Self {
            glob: compile_glob_set(globs)?,
            regex: compile_regex_set(regexes)?,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.glob.is_none() && self.regex.is_none()
    }

    /// 仅 `path` 自身是否命中。
    pub fn matches_self(&self, path: &str) -> bool {
        self.glob.as_ref().is_some_and(|s| s.is_match(path))
            || self.regex.as_ref().is_some_and(|s| s.is_match(path))
    }

    /// `path` 自身或任一祖先目录是否命中。
    ///
    /// 目录规则命中的是整棵子树，因此测试后代路径时也按祖先目录判定，
    /// 与扫描阶段的「目录命中即剪枝」保持一致。
    pub fn matches_path(&self, path: &str) -> bool {
        if self.is_empty() {
            return false;
        }
        let mut cur = path.trim_end_matches('/');
        if cur.is_empty() {
            cur = "/";
        }
        loop {
            if self.matches_self(cur) {
                return true;
            }
            if cur == "/" {
                return false;
            }
            match cur.rfind('/') {
                Some(0) => cur = "/",
                Some(i) => cur = &cur[..i],
                None => return false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn patterns(items: &[&str]) -> Vec<String> {
        items.iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn glob_semantics_unchanged() {
        let set = CompiledRules::compile(&patterns(&["*.txt", "/a/b?c"]), &[]).unwrap();
        assert!(set.matches_self("a.txt"));
        assert!(set.matches_self(".hidden.txt"));
        assert!(!set.matches_self("a.png"));
        assert!(set.matches_self("/a/b/c"), "`*` 跨目录");
        assert!(set.matches_self("/a/bxc"));
        assert!(!set.matches_self("/a/bx/c"));
    }

    #[test]
    fn glob_literal_metachars_are_escaped() {
        let set =
            CompiledRules::compile(&patterns(&["/a(1)", "/b[2]", "/c+3", "re:x"]), &[]).unwrap();
        assert!(set.matches_self("/a(1)"));
        assert!(!set.matches_self("/a1"));
        assert!(set.matches_self("/b[2]"));
        assert!(set.matches_self("/c+3"));
        assert!(set.matches_self("re:x"));
        assert!(!set.matches_self("/re:x"));
    }

    #[test]
    fn regex_full_path_and_ancestor_matching() {
        let set = CompiledRules::compile(&[], &patterns(&["^/股票/按月归档/2025.*$"])).unwrap();
        assert!(set.matches_self("/股票/按月归档/2025-01"));
        assert!(set.matches_self("/股票/按月归档/2025-12/x.mp4"));
        assert!(!set.matches_self("/股票/按月归档/2026-01"));

        // 匹配父目录的规则会连带排除后代
        let parent =
            CompiledRules::compile(&[], &patterns(&["^/股票/按月归档/临时文件$"])).unwrap();
        assert!(parent.matches_path("/股票/按月归档/临时文件"));
        assert!(parent.matches_path("/股票/按月归档/临时文件/深层/x.bin"));
        assert!(!parent.matches_path("/股票/按月归档/2026-01"));
    }

    #[test]
    fn regex_is_case_sensitive_by_default() {
        let set = CompiledRules::compile(&[], &patterns(&["^/ABC$"])).unwrap();
        assert!(set.matches_self("/ABC"));
        assert!(!set.matches_self("/abc"));
    }

    #[test]
    fn match_all_regex_matches_root_and_everything() {
        let set = CompiledRules::compile(&[], &patterns(&[".*"])).unwrap();
        assert!(set.matches_path("/"));
        assert!(set.matches_path("/a/b"));
    }

    #[test]
    fn empty_regex_rejected_with_index() {
        let err = validate_regexes(&patterns(&["^/a$", "   "])).unwrap_err();
        assert_eq!(err.kind, RuleKind::Regex);
        assert_eq!(err.index, Some(1));
    }

    #[test]
    fn unsupported_syntax_reports_index() {
        let err = validate_regexes(&patterns(&["^/ok$", "(?=lookahead)"])).unwrap_err();
        assert_eq!(err.kind, RuleKind::Regex);
        assert_eq!(err.index, Some(1));
        assert!(err.message.contains("正则"));
    }

    #[test]
    fn too_many_regex_rules_rejected() {
        let list: Vec<String> = (0..=MAX_REGEX_RULES).map(|i| format!("^/{i}$")).collect();
        let err = validate_regexes(&list).unwrap_err();
        assert_eq!(err.kind, RuleKind::Rules);
        assert_eq!(err.index, None);
    }

    #[test]
    fn oversized_regex_rejected() {
        let big = "a".repeat(MAX_REGEX_RULE_BYTES + 1);
        let err = validate_regexes(&[big]).unwrap_err();
        assert_eq!(err.kind, RuleKind::Regex);
        assert_eq!(err.index, Some(0));
    }

    #[test]
    fn total_bytes_limit_rejected() {
        // 每条 1000 字节、40 条 → 合计超过 32768
        let list: Vec<String> = (0..40).map(|_| "a".repeat(1000)).collect();
        let err = validate_regexes(&list).unwrap_err();
        assert_eq!(err.kind, RuleKind::Rules);
    }

    #[test]
    fn deep_nesting_rejected() {
        let deep = format!("{}a{}", "^(".repeat(80), ")$".repeat(80));
        assert!(validate_regexes(&[deep]).is_err());
    }

    #[test]
    fn glob_rules_have_no_count_limit() {
        let list: Vec<String> = (0..200).map(|i| format!("*.{i}")).collect();
        assert!(validate_globs(&list).is_ok());
    }

    #[test]
    fn normalize_test_path_matches_snapshot_shape() {
        assert_eq!(normalize_test_path("/a//b/").unwrap(), "/a/b");
        assert_eq!(normalize_test_path("a/b").unwrap(), "/a/b");
        assert_eq!(normalize_test_path("  ").unwrap(), "/");
        assert_eq!(normalize_test_path("/").unwrap(), "/");
        assert_eq!(normalize_test_path("a\\b").unwrap(), "/a/b");
    }

    #[test]
    fn normalize_test_path_rejects_oversized() {
        let err = normalize_test_path(&"a".repeat(MAX_TEST_PATH_BYTES + 1)).unwrap_err();
        assert_eq!(err.kind, RuleKind::Path);
    }

    #[test]
    fn error_display_uses_one_based_line() {
        let err = validate_regexes(&patterns(&["^/a$", "("])).unwrap_err();
        assert!(err.to_string().contains("第 2 条"), "{err}");
    }
}
