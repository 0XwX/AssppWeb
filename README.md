# AssppWeb

A web-based tool for acquiring and installing iOS apps outside the App Store. Authenticate with your Apple ID, search for apps, acquire licenses, and install IPAs directly to your device.

![preview](./resources/preview.png)

## Zero-Trust Architecture

AssppWeb uses a zero-trust design where the server **never sees your Apple credentials**. All Apple API communication happens directly in your browser via WebAssembly (libcurl.js with Mbed TLS 1.3). The server only acts as a blind TCP relay (Wisp protocol) and handles IPA compilation from public CDN downloads.

> **Warning:** There are no official Asspp Web instances. Use any public instance at your own risk. While the backend cannot read your encrypted traffic, a malicious host could serve a modified frontend to capture your credentials before encryption. Therefore, **do not blindly trust public instances**. We strongly recommend self-hosting your own instance or using one provided by a trusted partner. Always verify the SSL certificate and ensure you are connecting to a secure, authentic endpoint.

**恳请所有转发项目的博主对自己的受众进行网络安全技术科普。要有哪个不拎清的大头儿子搞出事情来都够我们喝一壶的。**

## Quick Start

### Deploy to Cloudflare Workers

1. Fork this repository
2. Install dependencies and deploy:

```bash
pnpm install
pnpm deploy
```

This builds the React frontend and deploys to Cloudflare Workers. On first deploy, Wrangler will prompt you to create the required KV namespace and R2 bucket.

**Requirements:**

- Cloudflare account (Free plan works)
- Node.js 22+
- [pnpm](https://pnpm.io/installation)

### Development

```bash
pnpm install
pnpm dev
```

This runs Vite dev server (frontend) and `wrangler dev` (Workers backend) concurrently. The Vite dev server proxies `/api` and `/wisp` to the local Workers dev server.

## Configuration

Configuration is set in `wrangler.jsonc` under `vars`:

| Variable              | Default | Description                                        |
| --------------------- | ------- | -------------------------------------------------- |
| `AUTO_CLEANUP_DAYS`   | `2`     | Automatically delete cached IPAs older than N days |
| `AUTO_CLEANUP_MAX_MB` | `8192`  | Delete oldest cached IPAs when total exceeds N MB  |
| `POW_DIFFICULTY`      | `20`    | Proof-of-Work challenge difficulty (16-24 bits)    |

These can also be overridden per-instance via the Settings page in the UI.

A cron trigger runs daily at 02:00 UTC to clean up expired IPA files from R2 storage.

## Security Recommendations

On first visit, you will be prompted to set a password. This protects your instance from unauthorized access. Login requires solving a Proof-of-Work challenge to prevent brute-force attacks.

## License

MIT License. See [LICENSE](LICENSE) for details.

## Acknowledgments

For projects that was stolen and used heavily:

- [ipatool](https://github.com/majd/ipatool)
- [Asspp](https://github.com/Lakr233/Asspp)

For friends who helped with testing and feedback:

- [@lbr77](https://github.com/lbr77)
- [@akinazuki](https://github.com/akinazuki)
