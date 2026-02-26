<div align="center">

# AssppWeb

A web-based tool for acquiring and installing iOS apps outside the App Store. Authenticate with your Apple ID, search for apps, acquire licenses, and install IPAs directly to your device.

This branch runs on Cloudflare Workers + Durable Objects + R2, fully compatible with the free plan. No Containers required. One-click deploy supported.

[中文](README.md)

<img src="./resources/preview.png" width="600" />

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/0XwX/AssppWeb)

Click the button to automatically fork the repo, provision KV / R2 / Durable Objects, and deploy to your Cloudflare account. The free plan is sufficient.

</div>

## Security Warning

> **There are no official public instances.** Use any third-party instance at your own risk. While the backend cannot decrypt your Apple traffic, a malicious host could serve a modified frontend to capture credentials before encryption. **Do not blindly trust public instances** — self-hosting is strongly recommended.

## Architecture

AssppWeb uses a zero-trust design — **the server never sees your Apple credentials**.

All Apple API communication is encrypted directly in your browser via WebAssembly (libcurl.js with Mbed TLS 1.3). The server only acts as an opaque TCP relay tunnel.

```
┌─ Browser ─────────────────────────────────────────┐
│                                                     │
│  Credentials: IndexedDB (passwords, cookies, etc.)  │
│                                                     │
│  Apple Protocol: libcurl.js WASM (Mbed TLS 1.3)    │
│    Auth → Purchase → Download info → Version query   │
│                                                     │
│  TLS 1.3 encrypted, server cannot decrypt           │
│                                                     │
└────────────────┬────────────────────────────────────┘
                 │  Wisp protocol (encrypted TCP tunnel over WebSocket)
┌─ Cloudflare Workers ─┴──────────────────────────────┐
│                                                      │
│  Wisp Proxy: blind TCP relay (cannot decrypt)        │
│  Download Manager: Apple CDN → inject SINF → R2      │
│  Auth: PoW challenge + password protection           │
│                                                      │
└──────────────────────────────────────────────────────┘
```

**The server only receives public CDN URLs and non-secret metadata for IPA compilation — no Apple credentials ever pass through it.**

## Deployment

### Requirements

- Cloudflare account (free plan works)
- Node.js 22+
- [pnpm](https://pnpm.io/installation)

### Manual Deploy

```bash
pnpm install
pnpm deploy
```

Builds the React frontend and deploys to Cloudflare Workers. On first deploy, Wrangler will prompt you to create the required KV namespace and R2 bucket.

### Development

```bash
pnpm install
pnpm dev
```

Runs Vite dev server (frontend) and `wrangler dev` (Workers backend) concurrently. Vite proxies `/api` and `/wisp` to the local Workers dev server.

## Configuration

### Access Password

After deployment, you'll be prompted to set an access password on first visit. All subsequent access requires this password. The password is hashed with PBKDF2 (100,000 iterations). Login requires solving a PoW challenge to prevent brute-force attacks. You can change the password from the Settings page.

### CDN Acceleration (Recommended)

By default, IPA files are streamed through the Worker, which is limited by Workers bandwidth. Configuring an R2 CDN domain enables 302 redirects to the CDN, significantly improving download speeds.

**Setup:**

1. Go to Cloudflare Dashboard → R2 → your bucket → Settings → Public access
2. Add a Custom Domain (e.g. `cdn.example.com`), wait for DNS to propagate
3. In Cloudflare Dashboard → Workers & Pages → your Worker → Settings → Variables and Secrets, add `R2_CDN_DOMAIN` with your CDN domain (e.g. `cdn.example.com`)
4. Redeploy: `pnpm deploy` (not needed if set via Dashboard)
5. Open the Settings page and verify CDN Domain shows a green "Enabled" badge

**Optional optimizations:**

- Add a Cache Rule in Dashboard → Rules → Cache Rules matching `(ends_with(http.request.uri.path, ".ipa"))` with Cache Everything, so `.ipa` files are cached at Cloudflare's edge (not cached by default)
- Enable Smart Tiered Cache in Dashboard → Speed → Optimization → Content Optimization

### Auto Cleanup

IPA files are stored in R2 and automatically cleaned up daily at 02:00 UTC. These can be configured in `wrangler.jsonc` or the Settings page:

| Variable              | Default | Description                                              |
| --------------------- | ------- | -------------------------------------------------------- |
| `AUTO_CLEANUP_DAYS`   | `2`     | Delete cached IPAs older than N days. 0 = disabled       |
| `AUTO_CLEANUP_MAX_MB` | `8192`  | Delete oldest IPAs when total exceeds N MB. 0 = disabled |

### Other Configuration

| Variable         | Default | Description                                                                  |
| ---------------- | ------- | ---------------------------------------------------------------------------- |
| `POW_DIFFICULTY` | `20`    | PoW challenge difficulty (16-24 bits). Higher = more secure but slower login |
| `BUILD_COMMIT`   | -       | Git commit hash, injected at deploy time                                     |
| `BUILD_DATE`     | -       | Build timestamp, injected at deploy time                                     |

## License

MIT License. See [LICENSE](LICENSE) for details.

## Acknowledgments

Projects heavily referenced and used:

- [ipatool](https://github.com/majd/ipatool)
- [Asspp](https://github.com/Lakr233/Asspp)

Friends who helped with testing and feedback:

- [@lbr77](https://github.com/lbr77)
- [@akinazuki](https://github.com/akinazuki)
