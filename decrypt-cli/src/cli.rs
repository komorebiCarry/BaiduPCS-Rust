//! CLI 参数解析模块
//!
//! 使用 clap 定义命令行参数结构，支持批量解密和单文件解密两种模式。

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

use crate::types::DecryptError;

/// decrypt-cli - 独立的命令行解密工具
///
/// 用于解密 BaiduPCS-Rust 加密的文件，支持批量解密和单文件解密两种模式。
#[derive(Parser, Debug)]
#[command(name = "decrypt-cli")]
#[command(author = "BaiduPCS-Rust Team")]
#[command(version)]
#[command(about = "独立的命令行解密工具，用于解密 BaiduPCS-Rust 加密的文件")]
#[command(long_about = r#"
decrypt-cli 是一个独立的命令行工具，用于解密通过 BaiduPCS-Rust 客户端加密并上传到网盘的文件。

支持两种解密模式：
  1. 批量解密模式：使用 --in-dir 和 --out-dir 参数，配合 --map 映射文件
  2. 单文件解密模式：使用 --in 和 --out 参数，无需映射文件

示例：
  # 批量解密（保留目录结构）
  decrypt-cli decrypt --key-file encryption.json --map mapping.json --in-dir ./encrypted --out-dir ./decrypted

  # 批量解密（镜像输入目录结构）
  decrypt-cli decrypt --key-file encryption.json --map mapping.json --in-dir ./encrypted --out-dir ./decrypted --mirror

  # 单文件解密
  decrypt-cli decrypt --key-file encryption.json --in file.age --out file.txt
"#)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

/// 可用的子命令
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// 解密加密文件
    Decrypt(DecryptArgs),
}

/// 解密命令参数
#[derive(Args, Debug, Clone)]
pub struct DecryptArgs {
    /// 密钥配置文件路径（encryption.json）
    #[arg(long, value_name = "FILE")]
    pub key_file: PathBuf,

    /// 映射文件路径（mapping.json，批量模式必需）
    #[arg(long, value_name = "FILE")]
    pub map: Option<PathBuf>,

    /// 输入目录（批量模式）
    #[arg(long, value_name = "DIR")]
    pub in_dir: Option<PathBuf>,

    /// 输出目录（批量模式）
    #[arg(long, value_name = "DIR")]
    pub out_dir: Option<PathBuf>,

    /// 单个输入文件（单文件模式）
    #[arg(long = "in", value_name = "FILE")]
    pub input: Option<PathBuf>,

    /// 单个输出文件（单文件模式）
    #[arg(long, value_name = "FILE")]
    pub out: Option<PathBuf>,

    /// 指定密钥版本（单文件模式可选，不指定则遍历所有密钥）
    #[arg(long, value_name = "VERSION")]
    pub key_version: Option<u32>,

    /// 镜像输入目录结构：按 --in-dir 的目录结构输出，而不是恢复原始上传路径
    /// 同名文件会自动添加 (1)、(2) 等后缀
    #[arg(long, default_value = "false")]
    pub mirror: bool,
}

/// 解密模式
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecryptMode {
    /// 批量解密模式
    Batch {
        in_dir: PathBuf,
        out_dir: PathBuf,
        map_file: PathBuf, /// 镜像输入目录结构
        mirror: bool,
    },
    /// 单文件解密模式
    SingleFile {
        input: PathBuf,
        output: PathBuf,
        key_version: Option<u32>,
    },
}

impl DecryptArgs {
    /// 验证参数并确定解密模式
    ///
    /// 批量模式和单文件模式参数互斥：
    /// - 批量模式：需要 --in-dir, --out-dir, --map
    /// - 单文件模式：需要 --in, --out
    ///
    /// 返回解密模式或参数错误
    pub fn validate(&self) -> Result<DecryptMode, DecryptError> {
        let has_batch_params = self.in_dir.is_some() || self.out_dir.is_some();
        let has_single_params = self.input.is_some() || self.out.is_some();

        // 检查互斥性
        if has_batch_params && has_single_params {
            return Err(DecryptError::ArgumentError(
                "批量模式参数（--in-dir, --out-dir）和单文件模式参数（--in, --out）不能同时使用".to_string(),
            ));
        }

        // 批量模式验证
        if has_batch_params {
            let in_dir = self.in_dir.clone().ok_or_else(|| {
                DecryptError::ArgumentError("批量模式需要指定 --in-dir 参数".to_string())
            })?;

            let out_dir = self.out_dir.clone().ok_or_else(|| {
                DecryptError::ArgumentError("批量模式需要指定 --out-dir 参数".to_string())
            })?;

            let map_file = self.map.clone().ok_or_else(|| {
                DecryptError::ArgumentError("批量模式需要指定 --map 参数".to_string())
            })?;

            // --key-version 在批量模式下不应使用
            if self.key_version.is_some() {
                return Err(DecryptError::ArgumentError(
                    "--key-version 参数仅在单文件模式下可用".to_string(),
                ));
            }

            return Ok(DecryptMode::Batch {
                in_dir,
                out_dir,
                map_file,
                mirror: self.mirror,
            });
        }

        // 单文件模式验证
        if has_single_params {
            let input = self.input.clone().ok_or_else(|| {
                DecryptError::ArgumentError("单文件模式需要指定 --in 参数".to_string())
            })?;

            let output = self.out.clone().ok_or_else(|| {
                DecryptError::ArgumentError("单文件模式需要指定 --out 参数".to_string())
            })?;

            if self.key_version.is_some() {
                return Err(DecryptError::ArgumentError(
                    "当前 age 模式只使用用户提供的单一口令，不支持 --key-version".to_string(),
                ));
            }

            return Ok(DecryptMode::SingleFile {
                input,
                output,
                key_version: self.key_version,
            });
        }

        // 没有指定任何模式参数
        Err(DecryptError::ArgumentError(
            "请指定解密模式：批量模式（--in-dir, --out-dir, --map）或单文件模式（--in, --out）".to_string(),
        ))
    }

    /// 获取密钥文件路径
    pub fn key_file(&self) -> &PathBuf {
        &self.key_file
    }

    /// 获取映射文件路径（如果有）
    #[allow(dead_code)]
    pub fn map_file(&self) -> Option<&PathBuf> {
        self.map.as_ref()
    }
}

/// 解析命令行参数
pub fn parse_args() -> Cli {
    Cli::parse()
}

/// 尝试解析命令行参数，返回 Result
#[allow(dead_code)]
pub fn try_parse_args() -> Result<Cli, clap::Error> {
    Cli::try_parse()
}

/// 从字符串数组解析参数（用于测试）
#[allow(dead_code)]
pub fn parse_from<I, T>(iter: I) -> Cli
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    Cli::parse_from(iter)
}

/// 尝试从字符串数组解析参数（用于测试）
#[allow(dead_code)]
pub fn try_parse_from<I, T>(iter: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    Cli::try_parse_from(iter)
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_batch_mode() {
        let cli = parse_from([
            "decrypt-cli",
            "decrypt",
            "--key-file",
            "encryption.json",
            "--map",
            "mapping.json",
            "--in-dir",
            "./encrypted",
            "--out-dir",
            "./decrypted",
        ]);

        match cli.command {
            Commands::Decrypt(args) => {
                assert_eq!(args.key_file, PathBuf::from("encryption.json"));
                assert_eq!(args.map, Some(PathBuf::from("mapping.json")));
                assert_eq!(args.in_dir, Some(PathBuf::from("./encrypted")));
                assert_eq!(args.out_dir, Some(PathBuf::from("./decrypted")));
                assert!(args.input.is_none());
                assert!(args.out.is_none());
                assert!(args.key_version.is_none());

                let mode = args.validate().unwrap();
                assert!(matches!(mode, DecryptMode::Batch { .. }));
            }
        }
    }

    #[test]
    fn test_parse_single_file_mode() {
        let cli = parse_from([
            "decrypt-cli",
            "decrypt",
            "--key-file",
            "encryption.json",
            "--in",
            "file.dat",
            "--out",
            "file.txt",
        ]);

        match cli.command {
            Commands::Decrypt(args) => {
                assert_eq!(args.key_file, PathBuf::from("encryption.json"));
                assert!(args.map.is_none());
                assert!(args.in_dir.is_none());
                assert!(args.out_dir.is_none());
                assert_eq!(args.input, Some(PathBuf::from("file.dat")));
                assert_eq!(args.out, Some(PathBuf::from("file.txt")));
                assert!(args.key_version.is_none());

                let mode = args.validate().unwrap();
                assert!(matches!(mode, DecryptMode::SingleFile { .. }));
            }
        }
    }

    #[test]
    fn test_parse_single_file_mode_with_key_version() {
        let cli = parse_from([
            "decrypt-cli",
            "decrypt",
            "--key-file",
            "encryption.json",
            "--in",
            "file.dat",
            "--out",
            "file.txt",
            "--key-version",
            "2",
        ]);

        match cli.command {
            Commands::Decrypt(args) => {
                assert_eq!(args.key_version, Some(2));

                assert!(args.validate().is_err());
            }
        }
    }

    #[test]
    fn test_validate_batch_mode_missing_in_dir() {
        let args = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: Some(PathBuf::from("mapping.json")),
            in_dir: None,
            out_dir: Some(PathBuf::from("./decrypted")),
            input: None,
            out: None,
            key_version: None,
            mirror: false,
        };

        let result = args.validate();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DecryptError::ArgumentError(_)));
    }

    #[test]
    fn test_validate_batch_mode_missing_out_dir() {
        let args = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: Some(PathBuf::from("mapping.json")),
            in_dir: Some(PathBuf::from("./encrypted")),
            out_dir: None,
            input: None,
            out: None,
            key_version: None,
            mirror: false,
        };

        let result = args.validate();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DecryptError::ArgumentError(_)));
    }

    #[test]
    fn test_validate_batch_mode_missing_map() {
        let args = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: None,
            in_dir: Some(PathBuf::from("./encrypted")),
            out_dir: Some(PathBuf::from("./decrypted")),
            input: None,
            out: None,
            key_version: None,
            mirror: false,
        };

        let result = args.validate();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DecryptError::ArgumentError(_)));
    }

    #[test]
    fn test_validate_single_file_mode_missing_in() {
        let args = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: None,
            in_dir: None,
            out_dir: None,
            input: None,
            out: Some(PathBuf::from("file.txt")),
            key_version: None,
            mirror: false,
        };

        let result = args.validate();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DecryptError::ArgumentError(_)));
    }

    #[test]
    fn test_validate_single_file_mode_missing_out() {
        let args = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: None,
            in_dir: None,
            out_dir: None,
            input: Some(PathBuf::from("file.dat")),
            out: None,
            key_version: None,
            mirror: false,
        };

        let result = args.validate();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DecryptError::ArgumentError(_)));
    }

    #[test]
    fn test_validate_mode_conflict() {
        let args = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: Some(PathBuf::from("mapping.json")),
            in_dir: Some(PathBuf::from("./encrypted")),
            out_dir: Some(PathBuf::from("./decrypted")),
            input: Some(PathBuf::from("file.dat")),
            out: Some(PathBuf::from("file.txt")),
            key_version: None,
            mirror: false,
        };

        let result = args.validate();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, DecryptError::ArgumentError(_)));
        if let DecryptError::ArgumentError(msg) = err {
            assert!(msg.contains("不能同时使用"));
        }
    }

    #[test]
    fn test_validate_key_version_in_batch_mode() {
        let args = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: Some(PathBuf::from("mapping.json")),
            in_dir: Some(PathBuf::from("./encrypted")),
            out_dir: Some(PathBuf::from("./decrypted")),
            input: None,
            out: None,
            key_version: Some(2),
            mirror: false,
        };

        let result = args.validate();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, DecryptError::ArgumentError(_)));
        if let DecryptError::ArgumentError(msg) = err {
            assert!(msg.contains("--key-version"));
        }
    }

    #[test]
    fn test_validate_no_mode_specified() {
        let args = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: None,
            in_dir: None,
            out_dir: None,
            input: None,
            out: None,
            key_version: None,
            mirror: false,
        };

        let result = args.validate();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DecryptError::ArgumentError(_)));
    }

    #[test]
    fn test_decrypt_mode_batch_fields() {
        let args = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: Some(PathBuf::from("mapping.json")),
            in_dir: Some(PathBuf::from("./encrypted")),
            out_dir: Some(PathBuf::from("./decrypted")),
            input: None,
            out: None,
            key_version: None,
            mirror: false,
        };

        let mode = args.validate().unwrap();
        match mode {
            DecryptMode::Batch {
                in_dir,
                out_dir,
                map_file,
                ..
            } => {
                assert_eq!(in_dir, PathBuf::from("./encrypted"));
                assert_eq!(out_dir, PathBuf::from("./decrypted"));
                assert_eq!(map_file, PathBuf::from("mapping.json"));
            }
            _ => panic!("Expected Batch mode"),
        }
    }

    #[test]
    fn test_decrypt_mode_single_file_fields() {
        let args = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: None,
            in_dir: None,
            out_dir: None,
            input: Some(PathBuf::from("file.dat")),
            out: Some(PathBuf::from("file.txt")),
            key_version: None,
            mirror: false,
        };

        let mode = args.validate().unwrap();
        match mode {
            DecryptMode::SingleFile {
                input,
                output,
                key_version,
            } => {
                assert_eq!(input, PathBuf::from("file.dat"));
                assert_eq!(output, PathBuf::from("file.txt"));
                assert!(key_version.is_none());
            }
            _ => panic!("Expected SingleFile mode"),
        }
    }

    #[test]
    fn test_key_file_accessor() {
        let args = DecryptArgs {
            key_file: PathBuf::from("my_keys.json"),
            map: None,
            in_dir: None,
            out_dir: None,
            input: Some(PathBuf::from("file.dat")),
            out: Some(PathBuf::from("file.txt")),
            key_version: None,
            mirror: false,
        };

        assert_eq!(args.key_file(), &PathBuf::from("my_keys.json"));
    }

    #[test]
    fn test_map_file_accessor() {
        let args_with_map = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: Some(PathBuf::from("mapping.json")),
            in_dir: Some(PathBuf::from("./encrypted")),
            out_dir: Some(PathBuf::from("./decrypted")),
            input: None,
            out: None,
            key_version: None,
            mirror: false,
        };
        assert_eq!(
            args_with_map.map_file(),
            Some(&PathBuf::from("mapping.json"))
        );

        let args_without_map = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: None,
            in_dir: None,
            out_dir: None,
            input: Some(PathBuf::from("file.dat")),
            out: Some(PathBuf::from("file.txt")),
            key_version: None,
            mirror: false,
        };
        assert!(args_without_map.map_file().is_none());
    }

    #[test]
    fn test_help_output() {
        // 测试 --help 不会导致 panic
        let result = try_parse_from(["decrypt-cli", "--help"]);
        // --help 会返回错误（因为它会退出程序）
        assert!(result.is_err());
    }

    #[test]
    fn test_version_output() {
        // 测试 --version 不会导致 panic
        let result = try_parse_from(["decrypt-cli", "--version"]);
        // --version 会返回错误（因为它会退出程序）
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_key_file() {
        // 测试缺少必需的 --key-file 参数
        let result = try_parse_from([
            "decrypt-cli",
            "decrypt",
            "--in",
            "file.dat",
            "--out",
            "file.txt",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_partial_batch_with_single_in() {
        // 测试部分批量参数与单文件 --in 冲突
        let args = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: None,
            in_dir: Some(PathBuf::from("./encrypted")),
            out_dir: None,
            input: Some(PathBuf::from("file.dat")),
            out: None,
            key_version: None,
            mirror: false,
        };

        let result = args.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_partial_batch_with_single_out() {
        // 测试部分批量参数与单文件 --out 冲突
        let args = DecryptArgs {
            key_file: PathBuf::from("encryption.json"),
            map: None,
            in_dir: None,
            out_dir: Some(PathBuf::from("./decrypted")),
            input: None,
            out: Some(PathBuf::from("file.txt")),
            key_version: None,
            mirror: false,
        };

        let result = args.validate();
        assert!(result.is_err());
    }
}


// ============================================================================
// 属性测试 (Property-Based Tests)
// ============================================================================

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// 生成随机路径字符串
    fn arb_path() -> impl Strategy<Value = PathBuf> {
        "[a-zA-Z0-9_./]{1,50}".prop_map(PathBuf::from)
    }

    /// 生成可选的随机路径
    fn arb_optional_path() -> impl Strategy<Value = Option<PathBuf>> {
        prop_oneof![Just(None), arb_path().prop_map(Some)]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// **Property 10: 参数模式互斥性**
        ///
        /// 批量模式参数（--in-dir, --out-dir）和单文件模式参数（--in, --out）
        /// 必须互斥，同时提供两种模式的参数应返回参数错误。
        ///
        /// **Validates: Requirements 7.4, 7.5, 7.6, 7.7, 6.3**
        #[test]
        fn prop_mode_exclusivity(
            key_file in arb_path(),
            map in arb_optional_path(),
            in_dir in arb_optional_path(),
            out_dir in arb_optional_path(),
            input in arb_optional_path(),
            out in arb_optional_path(),
        ) {
            let args = DecryptArgs {
                key_file,
                map,
                in_dir: in_dir.clone(),
                out_dir: out_dir.clone(),
                input: input.clone(),
                out: out.clone(),
                key_version: None,
                mirror: false,
            };

            let has_batch_params = in_dir.is_some() || out_dir.is_some();
            let has_single_params = input.is_some() || out.is_some();

            let result = args.validate();

            // 如果同时有批量模式和单文件模式参数，应该返回错误
            if has_batch_params && has_single_params {
                prop_assert!(
                    result.is_err(),
                    "同时提供批量模式和单文件模式参数时应返回错误"
                );
                if let Err(DecryptError::ArgumentError(msg)) = result {
                    prop_assert!(
                        msg.contains("不能同时使用"),
                        "错误信息应包含'不能同时使用'"
                    );
                }
            }
        }

        /// **Property 10 补充: 批量模式参数完整性**
        ///
        /// 批量模式需要同时提供 --in-dir, --out-dir, --map 三个参数。
        /// 缺少任何一个都应返回参数错误。
        ///
        /// **Validates: Requirements 7.4, 7.5, 6.3**
        #[test]
        fn prop_batch_mode_requires_all_params(
            key_file in arb_path(),
            has_map in any::<bool>(),
            has_in_dir in any::<bool>(),
            has_out_dir in any::<bool>(),
        ) {
            // 至少有一个批量参数，但不是全部
            prop_assume!(has_in_dir || has_out_dir);

            let args = DecryptArgs {
                key_file,
                map: if has_map { Some(PathBuf::from("mapping.json")) } else { None },
                in_dir: if has_in_dir { Some(PathBuf::from("./encrypted")) } else { None },
                out_dir: if has_out_dir { Some(PathBuf::from("./decrypted")) } else { None },
                input: None,
                out: None,
                key_version: None,
                mirror: false,
            };

            let result = args.validate();

            // 如果三个参数都有，应该成功
            if has_map && has_in_dir && has_out_dir {
                prop_assert!(
                    result.is_ok(),
                    "批量模式参数完整时应成功: {:?}",
                    result
                );
                if let Ok(DecryptMode::Batch { .. }) = result {
                    // 正确
                } else {
                    prop_assert!(false, "应返回 Batch 模式");
                }
            } else {
                // 缺少参数时应返回错误
                prop_assert!(
                    result.is_err(),
                    "批量模式缺少参数时应返回错误"
                );
            }
        }

        /// **Property 10 补充: 单文件模式参数完整性**
        ///
        /// 单文件模式需要同时提供 --in 和 --out 两个参数。
        /// 缺少任何一个都应返回参数错误。
        ///
        /// **Validates: Requirements 7.6, 7.7, 6.3**
        #[test]
        fn prop_single_file_mode_requires_both_params(
            key_file in arb_path(),
            has_input in any::<bool>(),
            has_out in any::<bool>(),
        ) {
            // 至少有一个单文件参数
            prop_assume!(has_input || has_out);

            let args = DecryptArgs {
                key_file,
                map: None,
                in_dir: None,
                out_dir: None,
                input: if has_input { Some(PathBuf::from("file.dat")) } else { None },
                out: if has_out { Some(PathBuf::from("file.txt")) } else { None },
                key_version: None,
                mirror: false,
            };

            let result = args.validate();

            // 如果两个参数都有，应该成功
            if has_input && has_out {
                prop_assert!(
                    result.is_ok(),
                    "单文件模式参数完整时应成功: {:?}",
                    result
                );
                if let Ok(DecryptMode::SingleFile { .. }) = result {
                    // 正确
                } else {
                    prop_assert!(false, "应返回 SingleFile 模式");
                }
            } else {
                // 缺少参数时应返回错误
                prop_assert!(
                    result.is_err(),
                    "单文件模式缺少参数时应返回错误"
                );
            }
        }

        /// **Property 10 补充: --key-version 已被 age 单口令模式移除**
        ///
        /// 新实现不支持按版本选择密钥，任何模式下都应返回参数错误。
        ///
        /// **Validates: Requirements 5.6, 6.3**
        #[test]
        fn prop_key_version_only_in_single_file_mode(
            key_file in arb_path(),
            key_version in 1u32..100,
        ) {
            // 批量模式 + key_version 应该失败
            let batch_args = DecryptArgs {
                key_file: key_file.clone(),
                map: Some(PathBuf::from("mapping.json")),
                in_dir: Some(PathBuf::from("./encrypted")),
                out_dir: Some(PathBuf::from("./decrypted")),
                input: None,
                out: None,
                key_version: Some(key_version),
                mirror: false,
            };

            let batch_result = batch_args.validate();
            prop_assert!(
                batch_result.is_err(),
                "批量模式下使用 --key-version 应返回错误"
            );
            if let Err(DecryptError::ArgumentError(msg)) = batch_result {
                prop_assert!(
                    msg.contains("--key-version"),
                    "错误信息应提及 --key-version"
                );
            }

            // 单文件模式 + key_version 也应该失败
            let single_args = DecryptArgs {
                key_file,
                map: None,
                in_dir: None,
                out_dir: None,
                input: Some(PathBuf::from("file.dat")),
                out: Some(PathBuf::from("file.txt")),
                key_version: Some(key_version),
                mirror: false,
            };

            let single_result = single_args.validate();
            prop_assert!(
                single_result.is_err(),
                "单文件模式下使用 --key-version 应返回错误: {:?}",
                single_result
            );
        }

        /// **Property 10 补充: 无模式参数时返回错误**
        ///
        /// 如果既没有批量模式参数也没有单文件模式参数，
        /// 应返回参数错误。
        ///
        /// **Validates: Requirements 6.3**
        #[test]
        fn prop_no_mode_params_returns_error(
            key_file in arb_path(),
            map in arb_optional_path(),
        ) {
            let args = DecryptArgs {
                key_file,
                map,
                in_dir: None,
                out_dir: None,
                input: None,
                out: None,
                key_version: None,
                mirror: false,
            };

            let result = args.validate();
            prop_assert!(
                result.is_err(),
                "没有指定任何模式参数时应返回错误"
            );
        }
    }
}
