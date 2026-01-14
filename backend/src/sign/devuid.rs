// DevUID 生成算法
//
// 算法：MD5(BDUSS) + "|0"
// 返回大写16进制字符串，末尾附加 "|0"

/// 生成 DevUID
///
/// DevUID 用于百度网盘 API 请求识别设备
///
/// # 算法
/// 1. 对 BDUSS 进行 MD5 哈希
/// 2. 将结果转换为大写16进制字符串
/// 3. 末尾追加 "|0"
///
/// # 参数
/// * `bduss` - 用户的 BDUSS 凭证
///
/// # 示例
/// ```
/// use baidu_netdisk_rust::sign::devuid::generate_devuid;
/// let devuid = generate_devuid("test_bduss");
/// // 输出类似: "5D41402ABC4B2A76B9719D911017C592|0"
/// ```
pub fn generate_devuid(bduss: &str) -> String {
    // 计算 MD5
    let digest = md5::compute(bduss.as_bytes());

    // 转换为大写16进制字符串
    let hex_string = format!("{:X}", digest);

    // 追加 "|0"
    format!("{}|0", hex_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_devuid() {
        // 测试空字符串
        let devuid = generate_devuid("");
        assert_eq!(devuid, "D41D8CD98F00B204E9800998ECF8427E|0");

        // 测试示例字符串
        let devuid = generate_devuid("test");
        assert_eq!(devuid, "098F6BCD4621D373CADE4E832627B4F6|0");

        // 验证格式
        assert!(devuid.ends_with("|0"));
        assert_eq!(devuid.len(), 34); // 32字符MD5 + 2字符 "|0"
    }

    #[test]
    fn test_devuid_is_uppercase() {
        let devuid = generate_devuid("test_bduss");

        // 验证是大写（排除 "|0" 部分）
        let hex_part = &devuid[..32];
        assert_eq!(hex_part, hex_part.to_uppercase());
    }

    #[test]
    fn test_devuid_consistency() {
        // 相同输入应产生相同输出
        let bduss = "my_test_bduss";
        let devuid1 = generate_devuid(bduss);
        let devuid2 = generate_devuid(bduss);
        assert_eq!(devuid1, devuid2);
    }
}
