# 贡献指南

感谢你对本项目的关注！我们欢迎各种形式的贡献。

## 📋 目录

- [行为准则](#行为准则)
- [如何贡献](#如何贡献)
- [开发流程](#开发流程)
- [代码规范](#代码规范)
- [提交规范](#提交规范)
- [测试要求](#测试要求)

---

## 🤝 行为准则

参与本项目即表示你同意遵守我们的行为准则：

- 尊重所有贡献者
- 使用友好和包容的语言
- 接受建设性的批评
- 关注对社区最有利的事情

---

## 💡 如何贡献

### 报告 Bug

如果你发现了 Bug，请在 [GitHub Issues](https://github.com/komorebiCarry/BaiduPCS-Rust/issues) 创建一个 Issue 并包含：

1. **清晰的标题** - 简要描述问题。
2. **复现步骤** - 详细说明如何触发 Bug。
3. **预期行为** - 你期望发生什么。
4. **实际行为** - 实际发生了什么。
5. **环境信息** - 操作系统、Rust 版本、Node 版本等。
6. **截图** - 如果适用的话。

### 建议新功能

如果你有新功能的想法，请在 [GitHub Issues](https://github.com/komorebiCarry/BaiduPCS-Rust/issues) 创建一个 Issue 并包含：

1. **功能描述** - 清晰描述你想要的功能
2. **使用场景** - 为什么需要这个功能
3. **可能的实现** - 如果有想法的话
4. **替代方案** - 是否考虑过其他方案

### 提交代码

1. **Fork 仓库**
   ```bash
   # 在 GitHub 上访问 https://github.com/komorebiCarry/BaiduPCS-Rust
   # 点击右上角的 Fork 按钮，将仓库 Fork 到你的账户
   ```

2. **克隆你的 Fork 到本地**
   ```bash
   # 将 YOUR_USERNAME 替换为你的 GitHub 用户名
   git clone https://github.com/YOUR_USERNAME/BaiduPCS-Rust.git
   cd BaiduPCS-Rust
   ```

3. **添加上游仓库**
   ```bash
   # 添加上游仓库，用于同步最新代码
   git remote add upstream https://github.com/komorebiCarry/BaiduPCS-Rust.git
   
   # 验证远程仓库配置
   git remote -v
   ```

4. **创建分支**
   ```bash
   git checkout -b feature/your-feature-name
   # 或
   git checkout -b fix/your-bug-fix
   ```

5. **进行更改**
   - 遵循代码规范
   - 编写清晰的注释
   - 添加必要的测试

6. **提交更改**
   ```bash
   git add .
   git commit -m "feat: add amazing feature"
   ```

7. **推送到你的 Fork**
   ```bash
   git push origin feature/your-feature-name
   ```

8. **创建 Pull Request**
   - 访问 [GitHub](https://github.com/komorebiCarry/BaiduPCS-Rust/compare) 创建 PR
   - 确保 PR 指向 `komorebiCarry/BaiduPCS-Rust:main` 分支
   - 填写 PR 模板，详细描述你的更改
   - 等待代码审查（Review）

---

## 🔧 开发流程

### 环境设置

#### 后端开发

```bash
cd backend

# 安装依赖
cargo build

# 运行开发服务器
cargo run

# 运行测试
cargo test

# 代码检查
cargo clippy

# 格式化代码
cargo fmt
```

#### 前端开发

```bash
cd frontend

# 安装依赖
npm install

# 运行开发服务器
npm run dev

# 构建生产版本
npm run build

# 类型检查
npm run type-check
```

#### Docker 开发

```bash
# 启动开发环境
./scripts/dev.sh

# 或使用 docker-compose
docker-compose -f docker-compose.dev.yml up
```

### 开发工作流

1. **开始前**
   ```bash
   # 确保你在 main 分支
   git checkout main
   
   # 从上游仓库获取最新更改
   git fetch upstream
   
   # 将上游 main 分支的更改合并到你的 main 分支
   git merge upstream/main
   
   # 推送到你的 Fork（保持同步）
   git push origin main
   
   # 创建新的功能分支
   git checkout -b feature/your-feature-name
   ```

2. **开发中**
   - 频繁提交（每个逻辑单元）
   - 编写有意义的提交信息（遵循提交规范）
   - 及时运行测试确保代码质量
   - 如果上游有更新，可以同步：
     ```bash
     # 切换到 main 分支同步
     git checkout main
     git fetch upstream
     git merge upstream/main
     
     # 回到功能分支并 rebase
     git checkout feature/your-feature-name
     git rebase main
     ```

3. **完成后**
   ```bash
   # 确保所有测试通过
   cargo test  # 后端测试
   npm run test:unit  # 前端测试（如果配置了）
   
   # 代码格式化
   cargo fmt  # Rust 代码
   npm run format  # TypeScript 代码（如果配置了）
   
   # 代码检查
   cargo clippy -- -D warnings  # Rust
   ```
   - 更新文档（如果更改影响了使用方式）
   - 推送到你的 Fork 并创建 Pull Request

---

## 📝 代码规范

### Rust 代码规范

遵循 Rust 官方代码风格：

```bash
# 自动格式化
cargo fmt

# 代码检查
cargo clippy -- -D warnings
```

**要点：**
- 使用 4 空格缩进
- 变量名使用 snake_case
- 类型名使用 PascalCase
- 常量使用 SCREAMING_SNAKE_CASE
- 添加有意义的注释
- 错误处理使用 `Result<T, E>`

**示例：**
```rust
/// 下载文件
/// 
/// # Arguments
/// * `url` - 下载链接
/// * `path` - 保存路径
///
/// # Returns
/// * `Result<(), Error>` - 成功或错误
pub async fn download_file(url: &str, path: &Path) -> Result<(), Error> {
    // 实现...
}
```

### TypeScript 代码规范

遵循 Vue 3 + TypeScript 最佳实践：

```bash
# 类型检查
npm run type-check

# 代码检查（如果配置了 ESLint）
npm run lint
```

**要点：**
- 使用 2 空格缩进
- 变量名使用 camelCase
- 类型名使用 PascalCase
- 组件名使用 PascalCase
- 添加 TypeScript 类型注解
- 使用 Composition API

**示例：**
```typescript
// 定义类型
export interface DownloadTask {
  id: string
  filename: string
  progress: number
}

// 组合式函数
export function useDownload() {
  const tasks = ref<DownloadTask[]>([])
  
  const addTask = (task: DownloadTask) => {
    tasks.value.push(task)
  }
  
  return { tasks, addTask }
}
```

### Vue 组件规范

```vue
<template>
  <!-- 模板内容 -->
</template>

<script setup lang="ts">
// 导入
import { ref, computed } from 'vue'

// 类型定义
interface Props {
  title: string
}

// Props
const props = defineProps<Props>()

// 状态
const count = ref(0)

// 计算属性
const doubled = computed(() => count.value * 2)

// 方法
function increment() {
  count.value++
}
</script>

<style scoped lang="scss">
// 样式（scoped）
</style>
```

---

## 📬 提交规范

使用 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

### 提交类型

- `feat`: 新功能
- `fix`: Bug 修复
- `docs`: 文档更新
- `style`: 代码格式（不影响功能）
- `refactor`: 重构（不是新功能也不是修复）
- `perf`: 性能优化
- `test`: 测试相关
- `chore`: 构建过程或辅助工具的变动

### 提交格式

```
<type>(<scope>): <subject>

<body>

<footer>
```

### 示例

```bash
# 简单提交
git commit -m "feat: add download progress bar"

# 详细提交
git commit -m "feat(downloader): add retry mechanism

- Add automatic retry on failure
- Maximum 3 retries with exponential backoff
- Log retry attempts

Closes #123"
```

---

## 🧪 测试要求

### 后端测试

所有新功能都应该有对应的测试：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_download_file() {
        // 测试逻辑
        let result = download_file("http://example.com", Path::new("/tmp/test")).await;
        assert!(result.is_ok());
    }
}
```

运行测试：
```bash
cargo test
```

### 前端测试

如果配置了测试框架：

```typescript
import { describe, it, expect } from 'vitest'

describe('DownloadManager', () => {
  it('should add task correctly', () => {
    const manager = new DownloadManager()
    manager.addTask({ id: '1', filename: 'test.txt' })
    expect(manager.tasks.length).toBe(1)
  })
})
```

运行测试：
```bash
npm run test:unit
```

### 集成测试

运行完整的集成测试：

```bash
./scripts/test.sh
```

---

## 📚 文档要求

### 代码文档

- 为所有公共 API 添加文档注释
- 解释复杂的算法或逻辑
- 包含使用示例

### README 更新

如果你的更改影响了使用方式：

1. 更新 README.md
2. 更新相关文档
3. 添加使用示例

---

## 🔍 Review 流程

1. **自我检查**
   - 代码符合规范
   - 测试全部通过
   - 文档已更新

2. **提交 PR**
   - 填写 PR 模板
   - 链接相关 Issue
   - 添加截图（如果适用）

3. **等待 Review**
   - 及时响应评论
   - 根据反馈修改
   - 保持沟通

4. **合并**
   - 所有检查通过
   - 至少一个 Approve
   - 解决所有 Conversation

---

## 💬 沟通

- **Issue** - Bug 报告和功能建议
- **Discussions** - 一般性讨论和问题
- **PR Comments** - 代码 Review 和讨论

---

## 🙏 致谢

感谢所有贡献者！你们的贡献让这个项目变得更好。

---

## 📄 许可证

通过贡献代码，你同意你的贡献将在 Apache License 2.0 许可证下发布。

---

**再次感谢你的贡献！** ❤️

