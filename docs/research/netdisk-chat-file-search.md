# 研究笔记：百度网盘「群聊文件搜索」纯 HTTP 接口逆向与实测

> **性质与测试范围声明**
> 这是一份逆向研究笔记，不是功能实现。
> 所有结论来自对一台 Windows 机器上官方客户端的逆向与少量实测，**未做充分测试**：只验证了少量账号与关键词、单文件完整下载。文中不包含任何账号、群组、文件、Cookie 等个人数据（一律用 `<BDUSS>` / `<你的数字uk>` / `<关键词>` 占位）。结论若与你的环境不符，请以实测为准；欢迎在 issue 中补充或纠正。
>
> **本文修订自本仓库早前的一版研究笔记。上一版的结论「群聊文件搜索不走 HTTP、只能 FFI」经进一步逆向证伪，已在下方更正。**

## TL;DR

1. 官方 PC 客户端能搜到「群聊中分享过的文件」，但这**并不是私有协议**——它就是一个未公开的 HTTP GET 接口：
   `https://pan.baidu.com/basembox/group/multisearch`
2. 客户端之所以在调用链上表现为 FFI，是因为 Electron 主进程把请求封装进了 `browserengine.dll`；**DLL 内部最终只是拼一个带 `sign` 的 GET 请求**。可完全脱离 DLL、纯 HTTP 调用，跨平台可用。
3. 签名：`sign = base64( 小写hex( md5( SALT + "_" + uk + 关键词 ) ) )`。`SALT` 是 DLL 内明文常量，`uk` 为登录用户的数字 ID。
4. 实测最小可用请求只需 3 个 query 参数（`key_word` / `type` / `sign`）+ 一个 `BDUSS` Cookie。
5. 顺带确认了一批同源的群聊 H5 接口（群列表 / 群详情 / 群成员 / 会话 / 好友 / 发消息等），见第六节。
6. 检索结果内 `dlink` 的直链下载 UA 组合结论不变（浏览器 UA + Cookie + Referer），见第八节——该部分上一版已验证，仍然有效。

## 一、与上一版笔记的差异（更正）

上一版结论为「群聊文件搜索**不走任何 HTTP 接口**，只能通过 FFI 复用官方 DLL」。经进一步逆向，该结论**不成立**：

- 端点确实存在，并且可以**从 DLL 静态提取**（上一版误判为"URL 运行时拼接、静态字符串里只有参数名"）——URL 模板以明文形式躺在 `.rdata` 段中。
- 无需 FFI、无需 Windows、无需官方客户端进程，纯 HTTP 即可完成检索。

因此本笔记把「FFI 直调」从主结论降级为「一种曾经绕远的替代路径」，主路径改为 HTTP。

## 二、真正的接口

```
GET https://pan.baidu.com/basembox/group/multisearch
```

Query 参数（最小集为前三个）：

| 参数 | 必填 | 说明 |
|---|---|---|
| `key_word` | ✅ | 搜索关键词 |
| `type` | ✅ | 实测用 `2` |
| `sign` | ✅ | 见第三节 |
| `clienttype` | 可选 | 官方客户端用 `8` |
| `channel` / `version` / `devuid` / `win64` / `vip` / `logid` | 可选 | 官方客户端会附带；实测可省略 |

请求头：

```
Cookie: BDUSS=<BDUSS>          # 必需；只需 BDUSS，STOKEN 非必需
User-Agent: <任意 UA 均可>      # 客户端 UA 或浏览器 UA 实测均可
```

即：`key_word` + `type=2` + `sign` + `BDUSS` 四要素即可检索。无 Cookie 时返回 `errno=-6`（账户已过期）。

## 三、sign 算法

```
sign = base64( hex_md5( SALT + "_" + uk + key_word ) )
```

- `SALT` 为客户端 DLL 内明文常量：`D3BA5E6D3B16D9202E10DE5D662CFC15`（32 位大写十六进制字符串，随客户端版本可能变化）。
- `uk` 为登录用户的**数字 ID**。
- 注意拼接细节：`"_"` 只出现一次、紧跟在 `SALT` 之后；**`uk` 与 `key_word` 之间没有分隔符**。这一点很容易踩坑。
- `devuid` 不参与签名（实测空值、任意值均通过）。

参考实现（Node.js）：

```js
const crypto = require('crypto');
const md5 = s => crypto.createHash('md5').update(s, 'utf8').digest('hex'); // 小写 hex

const sign = Buffer.from(md5(`${SALT}_${uk}${keyword}`), 'utf8').toString('base64');
```

### 逆向方法（供复现）

1. 在 `browserengine.dll` 的 `.rdata` 中定位 URL 模板字符串
   （形如 `%sbasembox/group/multisearch?key_word=%s&type=2&sign=%s`）。
2. 在 `.text` 中搜 `lea` 的 RIP 相对引用定位到引用该模板的函数（即 `ImChatSearchFileListProcessor::get_url`），反汇编即可读出拼接顺序与 `SALT` 常量。
3. 捷径：用 FFI 真实触发一次搜索，同时扫描**本进程内存**——拼好的完整 URL 与 md5 的明文输入会残留在堆上，可直接对答案，省去逐条指令还原。

## 四、返回结构

响应为 JSON：`{ errno, all_num, result: [...] }`，`result` 每个元素对应「某条群聊消息中分享的文件」：

> **关于 `all_num` 与结果数量（易踩坑）**：`all_num` **不是命中文件数**。实测对任意关键词
> （包括不存在的词）它都返回同一个数值（疑似「参与检索的群/会话数量」），**不要用它做计数**；
> 真正的命中数应取 `result` 数组的长度。
>
> 该接口**一次性返回全部命中结果**（跨所有群合并成一个列表），**没有分页参数**；
> 但**服务端上限为 500 条**，命中过多时会被截断（换更精确的关键词可减少截断）。

| 字段 | 说明 |
|---|---|
| `fsid` / `md5` / `size` / `category` / `is_dir` / `server_mtime` | 常规文件元数据 |
| `parent_path` | 文件在**分享者**网盘中的路径（URL 编码） |
| `chat_name` / `groupId` | 所在群聊名称与群 ID |
| `msgId` | 分享这条文件的消息 ID（配合 `shareinfo` 使用，见第六节） |
| `uname` / `displayName` | 分享者昵称 |
| `dlink` | **签名直链**（`http://d.pcs.baidu.com/file/...?fid=<所有者uk>-250528-<fsid>&...`，约 8 小时有效） |

同一文件被多个群转存/重复分享时会返回多条（`fsid` 不同、`md5` 可能相同），按 `md5` 或 `(parent_path, size)` 去重即可。

## 五、请求/鉴权细节（实测）

- Cookie 只需 `BDUSS`；带上 `STOKEN` 无副作用。无 Cookie → `errno=-6`。
- `User-Agent` 不敏感（客户端 UA / 浏览器 UA 均可）。
- `type=2` 与 `type=1` 返回集合略有差异（语义未完全确认，默认用 `2`）。
- 返回 `errno` 常见值：`0` 成功；`2157` 校验失败（sign 错）；`2156` 未搜到结果。
- 结果**一次性返回、无分页**，跨所有群合并；服务端**上限 500 条**（命中更多会被截断）。
- `all_num` 不是命中数（见第四节说明），命中数请取 `result.length`。

## 六、周边群聊接口（同源 `pan.baidu.com`，Cookie 鉴权）

检索只是入口。同一套 Cookie 体系下还有一批已验证可用的群聊接口（公共参数 `app_id=118511220&clienttype=0&channel=chunlei&dp-logid=<随机>`）：

| 接口 | 方法 | 关键参数 | 用途 |
|---|---|---|---|
| `/basembox/group/multisearch` | GET | `key_word,type,sign` | 群文件搜索（本文主角） |
| `/mbox/group/list` | GET | `start,limit,type` | 我加入的群列表 |
| `/mbox/group/getinfo` | POST | `gid` | 群详情 |
| `/mbox/group/listuser` | POST | `gid,start,limit` | 群成员 |
| `/mbox/group/status` | POST | `gid_list`(JSON 数组) | 群冻结/禁言状态 |
| `/mbox/msg/historysession` | GET | — | 最近会话 + 最后一条消息 |
| `/mbox/msg/shareinfo` | POST | `gid,from_uk,msg_id,type=2,fs_id` | 某条消息里的文件详情/转存信息 |
| `/mbox/relation/getfollowlist` | GET | `start,limit` | 好友列表 |
| `/mbox/user/checknk` | GET | — | 当前账号资料 |
| `/imbox/msg/send` | POST | `data=JSON` | 发送消息（群 `send_type=4` / 单聊 `3`，`msg_type=1` 文本） |
| `/imbox/msg/pull` | POST | `pulltype,sids,cursors` | 未读快照（见第七节） |

注意 `getinfo` / `listuser` 的参数名是 **`gid`**（不是 `group_id`），这是最常踩的坑。

## 七、历史消息的边界（重要）

`/imbox/msg/pull` **不是通用历史翻页接口**。实测：

- `cursors` 传 `{scursor:0, ecursor:0}`（或省略）时，返回该会话「自上次已读以来的未读消息」（最多若干条），且**不消费已读标记**。
- 传入任意非零窗口（`0→X`、`X→0`、`X→Y`、带 `start/limit/msgid` 锚点等）**一律返回空**。

因此，**任意时间范围的历史消息翻页无法通过该 HTTP 接口获得**；官方客户端的完整历史应来自 IM 长连接（socket）与客户端本地缓存。若只需「未读 / 最近若干条」，HTTP 够用；若要做全量历史导出，需另辟蹊径。

## 八、dlink 直链下载的坑（403 → 可行组合）

这部分结论自上一版沿用，仍然有效。直接 `GET dlink` 得到：

```json
{"error_code":31326,"error_msg":"user is not authorized, hitcode:104"}
```

原因：`fid` 绑定的是**文件所有者**（分享者）的 uk，请求者并不持有该文件权限；但 dlink 带签名，CDN 侧只校验签名与请求头组合。实测四种组合（同一 dlink）：

| 第一跳 UA | Cookie | 第一跳结果 | CDN（302 后）结果 |
|---|---|---|---|
| `netdisk;P2SP;...`（客户端 UA） | 有 | 302 | **403** |
| `netdisk;P2SP;...` | 无 | 302 | **403** |
| 浏览器 UA | 有 | 302 | **200** |
| `netdisk;P2SP;...` + Referer | 有 | 302 | **403** |

结论：**跳到 `*.baidupcs.com` CDN 的那一跳必须用浏览器 UA + BDUSS Cookie**（加 `Referer: https://pan.baidu.com/disk/main` 更稳）。下载完成后流式 MD5 与检索元数据中的 `md5` 一致，文件完整。

### 八之补充：大文件的 UA 规则不一样（后续实测修正）

上面的结论只对**小文件**成立。进一步实测发现能否下载**与文件大小有关**：

| 文件大小 | 不带 UA | 浏览器 UA | 客户端 `netdisk` UA |
|---|---|---|---|
| 小文件（< 约 60MB） | ✅ | ✅ | ✅ |
| **大文件（≥ 约 60MB）** | ✅ | ❌ 第一跳 403（hitcode:125） | ❌ CDN 跳 403（hitcode:104） |

- 大文件用浏览器 UA 时，**第一跳 `d.pcs.baidu.com` 就直接 403**（连 CDN 都到不了）；
  用 `netdisk` UA 能过第一跳，但**在 CDN 那一跳被拒**。
- ✅ **通用最优组合：不带 `User-Agent`**（仅 Cookie + Referer），小文件大文件都能下。
- ⚠️ **两跳必须使用同一个 UA**：中途更换会返回 `31362 sign error`（重定向后的 URL 似与 UA 绑定）。
- 推测原因：服务端按 UA 分流——浏览器 UA 走"网页下载"路线（要求文件在请求者自己网盘内，
  故对他人分享的大文件报"未授权"），非浏览器 UA 走另一条路线。

（此结论来自多文件分档实测：3MB / 52MB / 111MB / 126MB / 214MB / 625MB 各档取样。）


（本仓库 `backend/src/downloader/engine.rs` 已使用浏览器 UA，与该结论一致。）

## 九、对本项目的参考价值

1. **群聊文件搜索可以纯 HTTP 实现**，不再需要 FFI/DLL，也不受平台限制。建议落点：
   - 签名算法放进 `backend/src/sign/`（与现有 `devuid` / `locate` / `share_sign` 并列）；
   - 请求封装为 `NetdiskClient` 的一个方法（参照现有 `search_files`），复用现成的 `uid()` 与 Cookie 头构造。
   - 所需依赖（`md5`、`base64`、`serde`、`reqwest`）仓库已具备。
2. **dlink 下载的 UA 组合**：接入「群聊文件 / 他人分享文件」类 dlink 时务必用浏览器 UA + Cookie + Referer。
3. 第六节的群聊接口矩阵可作为后续「群列表 / 群成员 / 群文件浏览」类功能的参考。

## 十、测试局限（重要）

以下事项**均未充分测试**，请谨慎参考：

- 仅 Windows 10 x64 + 官方客户端 `8.8.x`（BrowserEngine `1.3.6.50`）；未测 macOS / Linux / 其他版本。
- 仅少量账号验证；未测企业版账号、未登录态、多账号切换。
- 检索仅验证少量关键词；未测翻页、超大结果集、并发。
- `SALT` 与算法来自客户端二进制，百度若更换客户端版本可能失效。
- dlink 下载仅验证单文件完整下载与 MD5 一次；未测大文件、断点续传、限速、多线程分片。
- 周边接口（第六节）只做了「单次调用返回 errno=0」级别的验证，参数语义、边界、频控均未系统测试。

---

*本文档由一次实际逆向整理而来，向所有在 issue 里研究百度网盘接口的同行致谢。若百度后续调整客户端架构，本文内容可能随时失效。*
