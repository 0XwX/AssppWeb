<div align="center">

# AssppWeb

基于浏览器的 iOS 应用获取与安装工具。登录 Apple ID，搜索应用，获取许可证，下载 IPA 并直接安装到设备。

本分支基于 Cloudflare Workers + Durable Objects + R2 构建，完全兼容免费计划，无需 Container，支持一键部署。

[English](README_EN.md)

<img src="./resources/preview.png" width="600" />

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/0XwX/AssppWeb)

点击按钮自动 Fork 仓库，创建 KV / R2 / Durable Objects 资源，部署到你的 Cloudflare 账号。免费计划即可使用。

</div>

## 安全警告

> **没有任何官方公开实例。** 使用任何他人搭建的实例均需自行承担风险。虽然后端无法解密你的 Apple 通信流量，但恶意运营者可以篡改前端代码在加密前截获凭证。**请勿盲目信任公开实例**，强烈建议自行部署。

**恳请所有转发项目的博主对自己的受众进行网络安全技术科普。要有哪个不拎清的大头儿子搞出事情来都够我们喝一壶的。**

## 架构

AssppWeb 采用零信任设计——**服务端永远不会接触你的 Apple 凭证**。

所有 Apple API 通信通过浏览器内的 WebAssembly（libcurl.js + Mbed TLS 1.3）直接加密完成。服务端仅作为不可读的 TCP 盲转发隧道。

```
┌─ 浏览器 ──────────────────────────────────────────┐
│                                                     │
│  凭证存储：IndexedDB（密码、Cookie、Token 等）       │
│                                                     │
│  Apple 协议：libcurl.js WASM (Mbed TLS 1.3)        │
│    认证 → 购买许可 → 获取下载信息 → 版本查询          │
│                                                     │
│  TLS 1.3 加密，服务端不可解密                         │
│                                                     │
└────────────────┬────────────────────────────────────┘
                 │  Wisp 协议（加密 TCP 隧道，经 WebSocket）
┌─ Cloudflare Workers ─┴──────────────────────────────┐
│                                                      │
│  Wisp 代理：盲转发 TCP 字节流（无法解密）              │
│  下载管理：从 Apple CDN 下载 IPA → 注入 SINF → 存 R2  │
│  认证：PoW 挑战 + 密码保护                            │
│                                                      │
└──────────────────────────────────────────────────────┘
```

**服务端只接收公开的 CDN 下载链接和非敏感元数据用于 IPA 编译，不经手任何 Apple 凭证。**

## 部署

### 环境要求

- Cloudflare 账号（免费计划即可）
- Node.js 22+
- [pnpm](https://pnpm.io/installation)

### 手动部署

```bash
pnpm install
pnpm deploy
```

构建 React 前端并部署到 Cloudflare Workers。首次部署时 Wrangler 会提示创建 KV 和 R2 资源。

### 本地开发

```bash
pnpm install
pnpm dev
```

同时启动 Vite 开发服务器（前端）和 `wrangler dev`（Workers 后端），Vite 自动代理 `/api` 和 `/wisp` 到本地 Workers。

## 配置

### 访问密码

部署完成后首次打开页面会提示设置访问密码，之后所有访问都需要输入密码。密码使用 PBKDF2（100,000 次迭代）哈希存储，登录前需完成 PoW 挑战以防暴力破解。可在 Settings 页面修改密码。

### CDN 加速（推荐）

默认情况下 IPA 文件通过 Worker 流式传输，受 Workers 带宽限制速度较慢。配置 R2 CDN 域名后，下载请求会 302 重定向到 CDN，显著提升下载速度。

**设置步骤：**

1. 进入 Cloudflare Dashboard → R2 → 你的 bucket → Settings → Public access
2. 添加 Custom Domain（如 `cdn.example.com`），等待 DNS 生效
3. 在 Cloudflare Dashboard → Workers & Pages → 你的 Worker → Settings → Variables and Secrets 中添加 `R2_CDN_DOMAIN`，值为你的 CDN 域名（如 `cdn.example.com`）
4. 重新部署：`pnpm deploy`（如果通过 Dashboard 设置则无需重新部署）
5. 打开 Settings 页面，确认 CDN Domain 显示为绿色 "Enabled" 状态

**可选优化：**

- 在 Dashboard → Rules → Cache Rules 中添加规则，匹配 `(ends_with(http.request.uri.path, ".ipa"))` 并启用 Cache Everything，让 `.ipa` 文件被 Cloudflare 边缘缓存（默认不缓存该后缀）
- 在 Dashboard → Speed → Optimization → Content Optimization 中开启 Smart Tiered Cache

### 自动清理

IPA 文件存储在 R2 中，每日 02:00 UTC 自动清理。以下参数可在 `wrangler.jsonc` 或设置页面中配置：

| 变量                  | 默认值 | 说明                                       |
| --------------------- | ------ | ------------------------------------------ |
| `AUTO_CLEANUP_DAYS`   | `2`    | 删除超过 N 天的 IPA 缓存，0 = 禁用         |
| `AUTO_CLEANUP_MAX_MB` | `8192` | 总存储超过 N MB 时删除最旧的文件，0 = 禁用 |

### 其他配置

| 变量             | 默认值 | 说明                                           |
| ---------------- | ------ | ---------------------------------------------- |
| `POW_DIFFICULTY` | `20`   | PoW 挑战难度（16-24 位），越高越安全但登录越慢 |
| `BUILD_COMMIT`   | -      | 部署时自动注入的 Git commit hash               |
| `BUILD_DATE`     | -      | 部署时自动注入的构建时间                       |

## License

MIT License. 详见 [LICENSE](LICENSE)。

## 致谢

重度参考和使用的项目：

- [ipatool](https://github.com/majd/ipatool)
- [Asspp](https://github.com/Lakr233/Asspp)

帮助测试和反馈的朋友：

- [@lbr77](https://github.com/lbr77)
- [@akinazuki](https://github.com/akinazuki)
