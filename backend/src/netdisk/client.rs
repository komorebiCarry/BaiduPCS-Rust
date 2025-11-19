// 网盘客户端实现

use crate::auth::UserAuth;
use crate::netdisk::{FileListResponse, LocateDownloadResponse};
use crate::sign::LocateSign;
use anyhow::{Context, Result};
use reqwest::Client;
use tracing::{debug, error, info, warn};

/// 百度网盘客户端
#[derive(Debug, Clone)]
pub struct NetdiskClient {
    /// HTTP客户端
    client: Client,
    /// 用户认证信息
    user_auth: UserAuth,
    /// 用户User-Agent
    user_agent: String,
}

impl NetdiskClient {
    /// 创建新的网盘客户端
    ///
    /// # 参数
    /// * `user_auth` - 用户认证信息（包含BDUSS）
    pub fn new(user_auth: UserAuth) -> Result<Self> {
        // 配置HTTP客户端
        let client = Client::builder()
            .cookie_store(true)
            .user_agent(Self::default_user_agent())
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            user_auth,
            user_agent: Self::default_user_agent(),
        })
    }

    /// 默认User-Agent（模拟网盘客户端）
    /// 必须使用 Android 客户端的 User-Agent 才能访问 Locate 下载 API
    fn default_user_agent() -> String {
        "netdisk;P2SP;3.0.0.8;netdisk;11.12.3;ANG-AN00;android-android;10.0;JSbridge4.4.0;jointBridge;1.1.0;".to_string()
    }

    /// 获取用户UID
    pub fn uid(&self) -> u64 {
        self.user_auth.uid
    }

    /// 获取用户BDUSS
    pub fn bduss(&self) -> &str {
        &self.user_auth.bduss
    }

    /// 获取文件列表
    ///
    /// # 参数
    /// * `dir` - 目录路径（如 "/" 或 "/test"）
    /// * `page` - 页码（从1开始）
    /// * `page_size` - 每页数量（默认100）
    ///
    /// # 返回
    /// 文件列表响应
    pub async fn get_file_list(
        &self,
        dir: &str,
        page: u32,
        page_size: u32,
    ) -> Result<FileListResponse> {
        info!("获取文件列表: dir={}, page={}", dir, page);

        let url = "https://pan.baidu.com/rest/2.0/xpan/file";

        let response = self
            .client
            .get(url)
            .query(&[
                ("method", "list"),
                ("order", "name"),
                ("desc", "0"),
                ("showempty", "0"),
                ("web", "1"),
                ("page", &page.to_string()),
                ("num", &page_size.to_string()),
                ("dir", dir),
                ("t", &chrono::Utc::now().timestamp_millis().to_string()),
            ])
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.user_agent)
            .send()
            .await
            .context("Failed to fetch file list")?;

        let file_list: FileListResponse = response
            .json()
            .await
            .context("Failed to parse file list response")?;

        if file_list.errno != 0 {
            anyhow::bail!("API error {}: {}", file_list.errno, file_list.errmsg);
        }

        debug!("获取到 {} 个文件/文件夹", file_list.list.len());
        Ok(file_list)
    }

    /// 获取Locate下载链接（通过文件路径）
    ///
    /// # 参数
    /// * `path` - 文件路径（如 "/apps/test/file.zip"）
    ///
    /// # 返回
    /// 下载URL数组
    pub async fn get_locate_download_url(&self, path: &str) -> Result<Vec<String>> {
        info!("获取Locate下载链接: path={}", path);

        // 1. 检查 UID
        if self.uid() == 0 {
            error!("UID 未设置，无法获取下载链接");
            anyhow::bail!("UID 未设置，请先登录");
        }

        // 2. 生成Locate签名
        let sign = LocateSign::new(self.uid(), self.bduss());

        // 3. 构建完整UR
        let url = format!(
            "https://pcs.baidu.com/rest/2.0/pcs/file?\
             ant=1&\
             check_blue=1&\
             es=1&\
             esl=1&\
             app_id=250528&\
             method=locatedownload&\
             path={}&\
             ver=4.0&\
             clienttype=17&\
             channel=0&\
             apn_id=1_0&\
             freeisp=0&\
             queryfree=0&\
             use=0&\
             {}",
            urlencoding::encode(path),
            sign.url_params()
        );

        debug!("Locate 请求 URL: {}", url);
        debug!("UID: {}", self.uid());
        debug!("BDUSS: {}...", &self.bduss()[..20.min(self.bduss().len())]);

        // 4. 发送 POST 请求
        let response = match self
            .client
            .post(&url)
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.user_agent)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                error!("发送 Locate 下载请求失败: path={}, 错误: {}", path, e);
                return Err(e).context("发送 Locate 下载请求失败");
            }
        };

        let status = response.status();
        info!("Locate 请求响应状态: {} (path={})", status, path);

        // 5. 检查响应状态
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            error!(
                "HTTP 请求失败: status={}, path={}, 响应: {}",
                status, path, error_text
            );
            anyhow::bail!("HTTP 请求失败: {} - {}", status, error_text);
        }

        // 6. 解析响应
        let response_text = match response.text().await {
            Ok(text) => text,
            Err(e) => {
                error!("读取响应内容失败: path={}, 错误: {}", path, e);
                return Err(e).context("读取响应内容失败");
            }
        };

        debug!("响应内容: {}", response_text);

        let json: serde_json::Value = match serde_json::from_str(&response_text) {
            Ok(j) => j,
            Err(e) => {
                error!(
                    "解析 JSON 响应失败: path={}, 错误: {}, 响应: {}",
                    path, e, response_text
                );
                return Err(e).context("解析 JSON 响应失败");
            }
        };

        // 7. 检查错误码
        if let Some(errno) = json["errno"].as_i64() {
            if errno != 0 {
                let errmsg = json["errmsg"].as_str().unwrap_or("未知错误");
                error!(
                    "百度 API 返回错误: errno={}, errmsg={}, path={}",
                    errno, errmsg, path
                );
                anyhow::bail!("百度 API 错误 {}: {}", errno, errmsg);
            }
        }

        // 8. 提取下载链接
        let urls = match json["urls"].as_array() {
            Some(urls_array) => {
                urls_array
                    .iter()
                    .filter(|u| u["encrypt"].as_i64() == Some(0)) // 只要非加密链接
                    .filter_map(|u| u["url"].as_str())
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            }
            None => {
                error!(
                    "响应中没有 urls 字段: path={}, JSON: {}",
                    path, response_text
                );
                anyhow::bail!("响应中没有 urls 字段");
            }
        };

        if urls.is_empty() {
            error!(
                "未找到可用的下载链接: path={}, 响应: {}",
                path, response_text
            );
            anyhow::bail!("未找到可用的下载链接");
        }

        info!("成功获取 {} 个下载链接", urls.len());
        Ok(urls)
    }

    /// 获取Locate下载链接（批量，通过文件ID）
    ///
    /// # 参数
    /// * `fs_ids` - 文件服务器ID列表
    ///
    /// # 返回
    /// Locate下载响应
    pub async fn get_locate_download_urls(&self, fs_ids: &[u64]) -> Result<LocateDownloadResponse> {
        info!("获取Locate下载链接: {} 个文件", fs_ids.len());

        // 注意：这个API可能需要不同的处理方式
        // 目前优先使用 get_locate_download_url (通过路径)

        anyhow::bail!("批量下载暂不支持，请使用 get_locate_download_url (通过文件路径)")
    }

    /// 获取单个文件的下载链接（通过文件路径）
    ///
    /// # 参数
    /// * `path` - 文件路径
    /// * `dlink_prefer` - 链接优先级索引（从0开始，默认使用第几个备选下载链接）
    ///
    /// # 返回
    /// 最优下载URL
    ///
    /// # 链接选择逻辑（参考 BaiduPCS-Go）
    /// 1. 根据 dlink_prefer 选择链接索引
    /// 2. 如果索引超出范围，使用最后一个链接
    /// 3. 如果选中的链接是 nb.cache 开头且有更多链接，自动使用下一个链接
    pub async fn get_download_url(&self, path: &str, dlink_prefer: usize) -> Result<String> {
        let urls = self.get_locate_download_url(path).await?;

        if urls.is_empty() {
            anyhow::bail!("未找到可用的下载链接");
        }

        // 1. 边界检查：如果 dlink_prefer 超出范围，使用最后一个链接
        let mut selected_index = if dlink_prefer >= urls.len() {
            urls.len() - 1
        } else {
            dlink_prefer
        };

        // 2. 选择链接
        let mut selected_url = &urls[selected_index];

        // 3. 跳过 nb.cache 链接（如果选中的是 nb.cache 且有更多链接可用）
        if selected_url.starts_with("http://nb.cache")
            || selected_url.starts_with("https://nb.cache")
        {
            if selected_index + 1 < urls.len() {
                // 使用下一个链接
                selected_index += 1;
                selected_url = &urls[selected_index];
                info!(
                    "检测到 nb.cache 链接，自动切换到下一个链接 (索引: {})",
                    selected_index
                );
            } else {
                warn!("所有链接都是 nb.cache，使用当前链接");
            }
        }

        info!(
            "选择下载链接 (索引: {}, 总数: {}): {}",
            selected_index,
            urls.len(),
            selected_url
        );

        Ok(selected_url.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_user_auth() -> UserAuth {
        UserAuth::new(123456789, "test_user".to_string(), "test_bduss".to_string())
    }

    #[test]
    fn test_netdisk_client_creation() {
        let user_auth = create_test_user_auth();
        let client = NetdiskClient::new(user_auth.clone());

        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.uid(), user_auth.uid);
        assert_eq!(client.bduss(), user_auth.bduss);
    }

    #[test]
    fn test_default_user_agent() {
        let ua = NetdiskClient::default_user_agent();
        assert!(ua.contains("netdisk"));
        assert!(ua.contains("android"));
    }
}
