//! 解密引擎模块
//!
//! 实现文件解密的核心逻辑，使用标准 age 格式 (age-encryption.org/v1)。
//!
//! # 功能
//!
//! - 单文件解密：使用 age 口令模式，自动从 encryption.json 读取密钥
//! - 批量解密：递归遍历目录，根据映射记录恢复原始目录结构
//! - 所有 .age 文件也可通过标准 `age -d` CLI 解密

use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crate::file_parser::is_age_encrypted_file;
use crate::key_loader::KeyLoader;
use crate::mapping_loader::MappingLoader;
use crate::types::{DecryptError, DecryptSummary, EncryptionKeyInfo};

/// 解密引擎
pub struct DecryptEngine;

impl DecryptEngine {
    pub fn new() -> Self { Self }

    /// 解密单个文件（age 口令模式）
    pub fn decrypt_file(
        &self, input: &Path, output: &Path, key: &EncryptionKeyInfo,
    ) -> Result<(), DecryptError> {
        use age::secrecy::Secret;

        if let Some(parent) = output.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        let passphrase = Secret::new(key.master_key.clone());

        let decryptor = age::Decryptor::new(BufReader::new(File::open(input)?))
            .map_err(|e| DecryptError::InvalidFormat(format!("无法读取 age 文件: {}", e)))?;

        let mut reader = match decryptor {
            age::Decryptor::Passphrase(d) => d
                .decrypt(&passphrase, None)
                .map_err(|e| DecryptError::DecryptionFailed(format!("age 解密失败: {}", e)))?,
            _ => return Err(DecryptError::InvalidFormat("不支持的 age 解密模式".to_string())),
        };

        let mut output_file = File::create(output)?;
        let mut buffer = vec![0u8; 65536];
        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 { break; }
            output_file.write_all(&buffer[..bytes_read])?;
        }
        Ok(())
    }

    /// 尝试使用多个密钥解密文件
    pub fn decrypt_file_with_any_key(
        &self, input: &Path, output: &Path, keys: &[&EncryptionKeyInfo],
    ) -> Result<(), DecryptError> {
        if keys.is_empty() {
            return Err(DecryptError::KeyMismatch(0));
        }
        if !is_age_encrypted_file(input) {
            return Err(DecryptError::InvalidFormat("不是 age 加密文件".to_string()));
        }
        let mut last_error = None;
        for key in keys {
            match self.decrypt_file(input, output, key) {
                Ok(()) => return Ok(()),
                Err(e) => match &e {
                    DecryptError::DecryptionFailed(_) => { last_error = Some(e); continue; }
                    _ => return Err(e),
                },
            }
        }
        Err(last_error.unwrap_or_else(|| DecryptError::KeyMismatch(0)))
    }

    /// 批量解密目录
    pub fn decrypt_directory(
        &self, in_dir: &Path, out_dir: &Path,
        mapping: &MappingLoader, key_loader: &KeyLoader, mirror: bool,
    ) -> DecryptSummary {
        let mut summary = DecryptSummary::new();
        let mut used_names = std::collections::HashSet::new();
        if let Err(e) = self.process_directory(in_dir, in_dir, out_dir, mapping, key_loader, &mut summary, mirror, &mut used_names) {
            summary.add_failed(in_dir.display().to_string(), format!("遍历目录失败: {}", e));
        }
        summary
    }

    fn process_directory(
        &self, dir: &Path, in_dir: &Path, out_dir: &Path,
        mapping: &MappingLoader, key_loader: &KeyLoader,
        summary: &mut DecryptSummary, mirror: bool,
        used_names: &mut std::collections::HashSet<String>,
    ) -> Result<(), DecryptError> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                self.process_directory(&path, in_dir, out_dir, mapping, key_loader, summary, mirror, used_names)?;
            } else if path.is_file() {
                self.process_file(&path, in_dir, out_dir, mapping, key_loader, summary, mirror, used_names);
            }
        }
        Ok(())
    }

    fn process_file(
        &self, input: &Path, in_dir: &Path, out_dir: &Path,
        mapping: &MappingLoader, key_loader: &KeyLoader,
        summary: &mut DecryptSummary, mirror: bool,
        used_names: &mut std::collections::HashSet<String>,
    ) {
        let input_str = input.display().to_string();
        let file_name = match input.file_name().and_then(|n| n.to_str()) {
            Some(name) => name,
            None => { summary.add_skipped(input_str, "无法获取文件名".to_string()); return; }
        };

        if !is_age_encrypted_file(input) {
            summary.add_skipped(input_str, "不是 age 加密文件".to_string());
            return;
        }

        let record = match mapping.find_by_encrypted_name(file_name) {
            Some(r) => r,
            None => { summary.add_skipped(input_str, "映射中找不到记录".to_string()); return; }
        };

        let key = match key_loader.get_key(record.key_version) {
            Some(k) => k,
            None => { summary.add_failed(input_str, format!("密钥版本 {} 不存在", record.key_version)); return; }
        };

        let output_path = if mirror {
            self.build_mirror_output_path(input, in_dir, out_dir, &record.original_name, used_names)
        } else {
            self.build_output_path(out_dir, &record.original_path, &record.original_name)
        };

        match self.decrypt_file(input, &output_path, key) {
            Ok(()) => summary.add_success(input_str, output_path.display().to_string()),
            Err(e) => summary.add_failed(input_str, format!("{}", e)),
        }
    }

    fn build_output_path(&self, out_dir: &Path, original_path: &str, original_name: &str) -> PathBuf {
        let clean = original_path.trim_start_matches('/').trim_start_matches('\\');
        if clean.is_empty() { out_dir.join(original_name) }
        else { out_dir.join(clean).join(original_name) }
    }

    fn build_mirror_output_path(
        &self, input: &Path, in_dir: &Path, out_dir: &Path,
        original_name: &str, used_names: &mut std::collections::HashSet<String>,
    ) -> PathBuf {
        let relative_dir = input.parent()
            .and_then(|p| p.strip_prefix(in_dir).ok())
            .unwrap_or(Path::new(""));
        let final_name = self.get_unique_name(original_name, used_names);
        out_dir.join(relative_dir).join(&final_name)
    }

    fn get_unique_name(&self, original_name: &str, used_names: &mut std::collections::HashSet<String>) -> String {
        if !used_names.contains(original_name) {
            used_names.insert(original_name.to_string());
            return original_name.to_string();
        }
        let (stem, ext) = match original_name.rfind('.') {
            Some(pos) => (&original_name[..pos], &original_name[pos..]),
            None => (original_name, ""),
        };
        let mut counter = 1;
        loop {
            let name = format!("{}({}){}", stem, counter, ext);
            if !used_names.contains(&name) {
                used_names.insert(name.clone());
                return name;
            }
            counter += 1;
        }
    }
}

impl Default for DecryptEngine {
    fn default() -> Self { Self::new() }
}
