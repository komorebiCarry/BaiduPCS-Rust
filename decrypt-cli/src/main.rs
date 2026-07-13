//! decrypt-cli - 独立的命令行解密工具
//!
//! 用于解密 BaiduPCS-Rust 加密的文件，支持批量解密和单文件解密两种模式。
//!
//! # 使用方式
//!
//! ## 批量解密模式
//! ```bash
//! decrypt-cli decrypt --key-file encryption.json --map mapping.json --in-dir ./encrypted --out-dir ./decrypted
//! ```
//!
//! ## 单文件解密模式
//! ```bash
//! decrypt-cli decrypt --key-file encryption.json --in file.age --out file.txt
//! ```

mod cli;
mod decrypt_engine;
mod error;
mod file_parser;
mod key_loader;
mod mapping_loader;
mod types;

use std::process::ExitCode;

use cli::{parse_args, Commands, DecryptMode};
use decrypt_engine::DecryptEngine;
use error::{ErrorFormatter, ResultAnalyzer};
use key_loader::KeyLoader;
use mapping_loader::MappingLoader;
use types::ExitCode as AppExitCode;

fn main() -> ExitCode {
    let cli = parse_args();

    match cli.command {
        Commands::Decrypt(args) => {
            // 验证参数并确定解密模式
            let mode = match args.validate() {
                Ok(mode) => mode,
                Err(e) => {
                    eprintln!("错误: {}", e);
                    return AppExitCode::ArgumentError.to_std();
                }
            };

            // 加载密钥配置
            let key_loader = match KeyLoader::load(args.key_file()) {
                Ok(loader) => loader,
                Err(e) => {
                    eprintln!("错误: 加载密钥文件失败: {}", e);
                    return AppExitCode::IoError.to_std();
                }
            };

            // 检查是否有可用密钥
            if !key_loader.has_keys() {
                eprintln!("错误: 密钥文件中没有可用的密钥");
                return AppExitCode::KeyMismatch.to_std();
            }

            // 根据模式执行解密
            match mode {
                DecryptMode::Batch {
                    in_dir,
                    out_dir,
                    map_file,
                    mirror,
                } => {
                    execute_batch_mode(&key_loader, &in_dir, &out_dir, &map_file, mirror)
                }
                DecryptMode::SingleFile {
                    input,
                    output,
                    ..
                } => {
                    execute_single_file_mode(&key_loader, &input, &output)
                }
            }
        }
    }
}

/// 执行批量解密模式
fn execute_batch_mode(
    key_loader: &KeyLoader,
    in_dir: &std::path::Path,
    out_dir: &std::path::Path,
    map_file: &std::path::Path,
    mirror: bool,
) -> ExitCode {
    println!("批量解密模式");
    println!("  密钥文件: 已加载 {} 个密钥", key_loader.key_count());
    println!("  映射文件: {}", map_file.display());
    println!("  输入目录: {}", in_dir.display());
    println!("  输出目录: {}", out_dir.display());
    println!("  输出模式: {}", if mirror { "镜像（保留输入目录结构）" } else { "原始（恢复原始目录结构）" });
    println!();

    // 检查输入目录是否存在
    if !in_dir.exists() {
        eprintln!("错误: 输入目录 '{}' 不存在", in_dir.display());
        return AppExitCode::IoError.to_std();
    }

    if !in_dir.is_dir() {
        eprintln!("错误: '{}' 不是目录", in_dir.display());
        return AppExitCode::ArgumentError.to_std();
    }

    // 加载映射文件
    let mapping = match MappingLoader::load(map_file) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("错误: 加载映射文件失败: {}", e);
            return AppExitCode::MappingError.to_std();
        }
    };

    println!("已加载 {} 条映射记录", mapping.len());
    println!();
    println!("开始解密...");

    // 执行批量解密
    let engine = DecryptEngine::new();
    let summary = engine.decrypt_directory(in_dir, out_dir, &mapping, key_loader, mirror);

    // 打印结果汇总
    print!("{}", ErrorFormatter::format_summary(&summary));

    // 确定退出码
    let analyzer = ResultAnalyzer::new(&summary);
    let exit_code = analyzer.determine_exit_code();

    println!();
    println!("退出码: {} ({})", exit_code as u8, ErrorFormatter::format_exit_code_description(exit_code));

    exit_code.to_std()
}

/// 执行单文件解密模式
fn execute_single_file_mode(
    key_loader: &KeyLoader,
    input: &std::path::Path,
    output: &std::path::Path,
) -> ExitCode {
    println!("单文件解密模式");
    println!("  age 口令: 已加载");
    println!("  输入文件: {}", input.display());
    println!("  输出文件: {}", output.display());
    println!();

    // 检查输入文件是否存在
    if !input.exists() {
        eprintln!("错误: 输入文件 '{}' 不存在", input.display());
        return AppExitCode::IoError.to_std();
    }

    if !input.is_file() {
        eprintln!("错误: '{}' 不是文件", input.display());
        return AppExitCode::ArgumentError.to_std();
    }

    // 检查输出文件是否已存在
    if output.exists() {
        eprintln!("警告: 输出文件 '{}' 已存在，将被覆盖", output.display());
    }

    let engine = DecryptEngine::new();

    let result = match key_loader.current_key() {
        Some(key) => {
            println!("使用用户提供的 age 口令解密...");
            engine.decrypt_file(input, output, key)
        }
        None => {
            eprintln!("错误: 用户 age 口令未配置");
            return AppExitCode::KeyMismatch.to_std();
        }
    };

    match result {
        Ok(()) => {
            println!();
            println!("✓ 解密成功: {} -> {}", input.display(), output.display());
            println!();
            println!("退出码: 0 (成功)");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!();
            eprintln!("✗ 解密失败: {}", e);
            let exit_code = e.to_exit_code();
            eprintln!();
            eprintln!("退出码: {} ({})", exit_code as u8, ErrorFormatter::format_exit_code_description(exit_code));
            exit_code.to_std()
        }
    }
}
