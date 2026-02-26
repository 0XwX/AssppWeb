# Agent Instructions for AssppWeb

## TypeScript Code Style

- **Indentation**: 2 spaces
- **Semicolons**: Required
- **Quotes**: Single quotes for strings
- **Naming**: PascalCase for types/interfaces, camelCase for variables/functions

## Project Structure

- `src/` — Cloudflare Workers backend (Hono, TypeScript, ESM)
  - `src/do/` — Durable Objects (WispProxy, DownloadManager)
  - `src/middleware/` — Auth middleware (PoW, session, password)
  - `src/routes/` — Hono API routes
  - `src/config.ts` — Centralized constants
  - `src/types.ts` — Shared types (Software, DownloadTask, etc.)
  - `src/env.d.ts` — Env interface (Workers bindings)
- `frontend/` — React SPA (TypeScript, Vite, Tailwind CSS)
- `references/ApplePackage/` — Swift reference implementation (source of truth)
- Root `package.json` — unified dependencies for both frontend and workers

## Architecture — Zero-Trust

The server is a blind TCP proxy. It NEVER sees Apple credentials.

```
┌─ Browser (Client) ─────────────────────────────────┐
│  Credentials (IndexedDB): email, password, cookies, │
│    passwordToken, DSID, deviceIdentifier, pod       │
│                                                      │
│  Apple Protocol (libcurl.js WASM + Mbed TLS 1.3):   │
│    1. Bag fetch → backend proxy → resolve auth URL   │
│       (fallback to default auth endpoint if missing)  │
│    2. Authenticate → get token, cookies, pod         │
│    3. Purchase → acquire license                     │
│    4. Download info → get CDN URL + SINFs + metadata │
│    5. Version listing/lookup                         │
│                                                      │
│  TLS 1.3 encrypted via Wisp protocol over WebSocket  │
└──────────────────────┬───────────────────────────────┘
                       │ Wisp-multiplexed TCP (server cannot read)
┌─ Server (Workers) ───┴──────────────────────────────┐
│  Hono app on Cloudflare Workers                      │
│                                                      │
│  Wisp proxy: Durable Object (WispProxy) on /wisp/   │
│  → Each WebSocket session gets its own DO instance   │
│  → multiplexed TCP relay (blind tunnel, no decrypt)  │
│                                                      │
│  Bag proxy: GET /api/bag?guid=<id>                   │
│    - Fetches init.itunes.apple.com/bag.xml via fetch │
│    - Returns public Apple service URLs (no creds)    │
│                                                      │
│  After client obtains download info:                 │
│    Client POSTs: { downloadURL, sinfs, metadata }    │
│    - downloadURL = Apple CDN (public, no auth)       │
│    - sinfs = DRM signatures (base64)                 │
│    - iTunesMetadata = app metadata plist (base64)    │
│                                                      │
│  DownloadManager DO downloads IPA from CDN, injects  │
│  SINFs + iTunesMetadata, stores to R2, serves via    │
│  public install URL (itms-services manifest)         │
│                                                      │
│  Auth: PoW challenge + PBKDF2 password + HMAC token  │
│  Storage: KV (password hash), R2 (IPA files)         │
└──────────────────────────────────────────────────────┘
```

**Key invariant**: The server NEVER sees Apple credentials. All Apple TLS terminates at the browser via libcurl.js WASM (Mbed TLS 1.3). The server only receives public CDN URLs and non-secret metadata for IPA compilation. The bag proxy (`/api/bag`) only returns public Apple service URLs — no credentials pass through it.

## Cloudflare Workers Bindings

Defined in `wrangler.jsonc`, typed in `src/env.d.ts`:

| Binding            | Type                   | Purpose                                 |
| ------------------ | ---------------------- | --------------------------------------- |
| `WISP_PROXY`       | DurableObjectNamespace | Wisp WebSocket proxy (per-session)      |
| `DOWNLOAD_MANAGER` | DurableObjectNamespace | Download tasks + IPA compilation        |
| `AUTH_KV`          | KVNamespace            | Password hash storage                   |
| `IPA_BUCKET`       | R2Bucket               | Compiled IPA file storage               |
| `ASSETS`           | Fetcher                | Frontend static assets (Workers Assets) |

Environment variables (set in `wrangler.jsonc` `vars` or via `wrangler secret`):

| Variable              | Default | Description                                |
| --------------------- | ------- | ------------------------------------------ |
| `AUTO_CLEANUP_DAYS`   | `2`     | Delete cached IPAs older than N days       |
| `AUTO_CLEANUP_MAX_MB` | `8192`  | Delete oldest IPAs when total exceeds N MB |
| `POW_DIFFICULTY`      | `20`    | PoW challenge difficulty (16-24 bits)      |
| `BUILD_COMMIT`        | —       | Git commit hash (set at deploy time)       |
| `BUILD_DATE`          | —       | Build timestamp (set at deploy time)       |

## Backend (Workers)

Entry point: `src/index.ts` — Hono app with:

- HTTPS redirect middleware (via `x-forwarded-proto`)
- Auth middleware on `/api/*` (except `/api/auth/*` and `/api/install/*`) and `/wisp/*`
- Wisp WebSocket proxy → `WispProxy` DO (each session gets a unique DO instance)
- API routes mounted under `/api`
- SPA fallback: Workers Assets binding with `index.html` fallback for 404s
- Scheduled handler: R2 cleanup cron (`0 2 * * *`)

### API Routes (`src/routes/`)

| File           | Endpoints                                                                                | Description                      |
| -------------- | ---------------------------------------------------------------------------------------- | -------------------------------- |
| `auth.ts`      | `/auth/challenge`, `/auth/login`, `/auth/setup`, `/auth/logout`, `/auth/change-password` | PoW + password auth              |
| `bag.ts`       | `/bag?guid=<id>`                                                                         | Apple bag proxy                  |
| `search.ts`    | `/search?term=...&country=...`                                                           | iTunes search proxy              |
| `downloads.ts` | `/downloads` CRUD                                                                        | Download task management         |
| `packages.ts`  | `/packages`                                                                              | Compiled IPA listing             |
| `install.ts`   | `/install/:id/manifest.plist`, `/install/:id/download`                                   | iOS itms-services install        |
| `settings.ts`  | `/settings`                                                                              | Server config + cleanup settings |

### Durable Objects (`src/do/`)

- **WispProxy** (`src/do/WispProxy.ts`) — Wisp protocol server; validates target hosts against Apple whitelist (`auth/buy/init.itunes.apple.com`, pod-based `p{N}-buy.itunes.apple.com`); port 443 only; blocks direct IPs
- **DownloadManager** (`src/do/DownloadManager.ts`) — Singleton (`idFromName('singleton')`); manages download tasks, IPA compilation (SINF + iTunesMetadata injection), R2 storage, auto-cleanup; password hash migration path (DO → KV)

### Auth System (`src/middleware/auth.ts`)

- **Password**: PBKDF2 (100,000 iterations, SHA-256) with random salt; stored in KV (`AUTH_KV`) with DO fallback migration
- **Session**: HMAC-SHA256 token (7-day expiry) in `asspp_session` cookie
- **PoW**: Signed challenge-response; ephemeral HMAC key (non-extractable, regenerated on Worker restart); in-memory replay prevention (`Map<string, number>`, capped at 10,000 entries)
- **Timing-safe comparison**: HMAC-based for password and token verification

### Config (`src/config.ts`)

Centralized constants: `MAX_DOWNLOAD_SIZE` (8 GB), `BAG_TIMEOUT_MS` (15s), `BAG_MAX_BYTES` (1 MB), `MIN_ACCOUNT_HASH_LENGTH` (8), `UPLOAD_PART_SIZE` (25 MB), `CDN_FETCH_TIMEOUT_MS` (30s), `CDN_FETCH_MAX_RETRIES` (3), `MAX_SEARCH_BYTES` (5 MB), `BAG_USER_AGENT`.

## Reference Implementation

The Swift reference at `references/ApplePackage/` is the source of truth for Apple protocol behavior:

- Field mappings (iTunes API → Software type) use Swift `CodingKeys`
- Authentication flow, bag endpoint, pod routing, error codes
- Always consult the reference when making protocol changes

### iTunes API Field Mapping

The backend (`src/routes/search.ts`) maps raw iTunes API fields to our `Software` type, matching the Swift CodingKeys in `references/ApplePackage/Sources/ApplePackage/Models/Software.swift`:

| iTunes Field                | Software Field |
| --------------------------- | -------------- |
| `trackId`                   | `id`           |
| `bundleId`                  | `bundleID`     |
| `trackName`                 | `name`         |
| `artworkUrl512`             | `artworkUrl`   |
| `currentVersionReleaseDate` | `releaseDate`  |

All other fields (`version`, `price`, `artistName`, `sellerName`, `description`, `averageUserRating`, `userRatingCount`, `screenshotUrls`, `minimumOsVersion`, `fileSizeBytes`, `releaseNotes`, `formattedPrice`, `primaryGenreName`) keep their original names.

The backend also extracts the `results` array from the iTunes wrapper `{ resultCount, results }` before sending to the frontend.

## Per-Account Device Identifiers

Device identifiers are **per-account**, not global:

- Generated as 12 random hex chars (6 bytes) at account creation via `generateDeviceId()`
- Editable during login, immutable after authentication
- Stored in IndexedDB on the `Account` object as `deviceIdentifier`
- Passed to all Apple protocol calls (auth, purchase, download, version listing)

## Pod-Based Host Routing

After authentication, Apple returns a `pod` header:

- Store API: `p{pod}-buy.itunes.apple.com` (default: `p25-buy.itunes.apple.com`)
- Purchase API: `p{pod}-buy.itunes.apple.com` (default: `buy.itunes.apple.com`)
- Pod is stored on the Account object and used for all subsequent API calls
- Functions: `storeAPIHost(pod?)` and `purchaseAPIHost(pod?)` in `frontend/src/apple/config.ts`

## Dynamic Host Validation

The WispProxy DO (`src/do/WispProxy.ts`) validates target hosts:

- `auth.itunes.apple.com` — bag-resolved auth endpoint
- `buy.itunes.apple.com` — purchase endpoint
- `init.itunes.apple.com` — bag endpoint
- `/^p\d+-buy\.itunes\.apple\.com$/` — pod-based hosts
- Port restricted to `443` only
- Direct IP targets blocked

## Bag Proxy

The backend proxies the bag endpoint via `GET /api/bag?guid=<deviceId>` using Workers `fetch`. It sends Configurator-compatible request headers (`User-Agent`, `Accept: application/xml`). The bag response is public data (Apple service URLs) — no credentials are involved. See `src/routes/bag.ts`.

## Frontend

- React 19, React Router 7, Zustand for state
- Tailwind CSS 4 for styling
- Vite for build tooling
- IndexedDB for credential storage (via `idb`)
- `libcurl.js` (WASM) for browser-side TLS 1.3 via Mbed TLS — connects through Wisp protocol
- `appleRequest()` in `frontend/src/apple/request.ts` wraps `libcurl.fetch` for all Apple API calls and forces HTTP/1.1 (`_libcurl_http_version: 1.1`)
- Bag endpoint (`frontend/src/apple/bag.ts`) uses backend proxy (`/api/bag`) and falls back to `https://buy.itunes.apple.com/WebObjects/MZFinance.woa/wa/authenticate` when `authenticateAccount` is missing or bag fetch fails
- Authentication (`frontend/src/apple/authenticate.ts`) resolves bag endpoint, then sets `guid` via URL query manipulation to avoid duplicate/malformed query parameters
- Plist build/parse (`frontend/src/apple/plist.ts`) uses native XML builder and browser-native `DOMParser`
- Cookie helper (`frontend/src/apple/cookies.ts`) — `extractAndMergeCookies(rawHeaders, existingCookies)` replaces the repeated extract-and-merge pattern across all Apple protocol files
- PoW solver (`frontend/src/utils/pow.ts`) — client-side SHA-256 brute-force for login/setup challenge

### Frontend Shared Components (`components/common/`)

- **Alert** — `<Alert type="error|success|warning">` for status messages (replaces inline alert divs)
- **Modal** — `<Modal open={bool} onClose={fn} title={string}>` for dialog overlays
- **Spinner** — inline SVG loading spinner for buttons
- **CountrySelect** — optgroup-based country dropdown with "Available Regions" + "All Regions"
- **AppIcon** — 3 sizes (40/56/80px), rounded corners, letter fallback
- **Badge** — color-coded status pill
- **ProgressBar** — gray track, blue fill, percentage label
- **icons** — shared SVG icon components (`HomeIcon`, `AccountsIcon`, `SearchIcon`, `DownloadsIcon`, `SettingsIcon`, `SunIcon`, `MoonIcon`, `SystemIcon`) used by Sidebar, MobileNav, and MobileHeader

### Frontend Shared Utilities (`utils/`)

- `utils/error.ts` — `getErrorMessage(e, fallback)` for standardized catch-block error extraction
- `utils/crypto.ts` — AES-GCM encrypt/decrypt for account export/import
- `utils/account.ts` — `accountHash()`, `accountStoreCountry()`, `firstAccountCountry()`
- `utils/pow.ts` — PoW challenge solver (`solveChallenge`, `fetchAndSolveChallenge`)

### Import Ordering Convention

1. React / library imports (`useState`, `useNavigate`, `useTranslation`)
2. Layout components (`PageContainer`)
3. Common components (`AppIcon`, `Alert`, `Spinner`, `Modal`, `CountrySelect`)
4. Sibling components within the same feature folder (e.g., `DownloadItem` inside `Download/`)
5. Hooks / stores (`useAccounts`, `useSettingsStore`)
6. Apple protocol / API modules (`authenticate`, `purchaseApp`, `apiPost`)
7. Utilities (`accountHash`, `getErrorMessage`)
8. Config (`countryCodeMap`, `storeIdToCountry`)
9. Types (`type Software`)

**Enforcement**: Every PR must verify import ordering. Common mistakes:

- Putting hooks/stores before layout/common components
- Putting config before utilities
- Putting type imports in the middle instead of last

## Security Model

### Password Authentication

The instance is protected by a server-side password (set on first visit). Login requires solving a PoW challenge before submitting credentials. The PoW uses an ephemeral HMAC key that regenerates on Worker restart — no secrets to configure.

### Account Hash Is Public

`accountHash` is a SHA-256 of the account email. It is treated as **public, non-secret data** — it identifies which account owns a download but does not grant any privileged access.

### Trusted Sources

- **Apple API responses** (bag XML, iTunes search results, `customerMessage` fields) are treated as trusted content. No additional sanitization is applied beyond what React's text rendering provides (no `dangerouslySetInnerHTML`).
- **Apple CDN redirects** during IPA download are trusted. The initial URL is validated against `*.apple.com`, and redirect targets from Apple's CDN infrastructure (e.g., Akamai) are followed. The response body is stored to R2 — it is never reflected back to the requester.

### Browser as Security Boundary

Credentials (passwords, `passwordToken`, cookies) stored in IndexedDB are protected by the browser's same-origin policy. Encrypting them at rest would be security theater — the decryption key would also live in JS. The threat model assumes the browser environment is trusted; if an attacker has XSS, they can exfiltrate credentials regardless of at-rest encryption.

### Backend Does Not Reflect Request Headers

The settings endpoint (`/api/settings`) must never reflect request headers (`x-forwarded-host`, `host`, etc.) in its response body. Use server-side values only.

## Error Handling

- Early returns to reduce nesting
- `try/catch` for async operations
- Hono error responses via `c.json({ error: '...' }, status)`
- Type-safe error responses

### Apple Protocol Error Codes

- `2034` / `2042`: Token expired — re-authentication required
- `customerMessage === 'Your password has changed.'`: Password token invalid
- `action.url` ending in `termsPage`: Terms acceptance required (throw with URL)

## Testing

```bash
pnpm test          # vitest run (CI mode, both frontend + workers projects)
pnpm test:watch    # vitest watch mode
pnpm typecheck     # tsc --noEmit for both workers and frontend tsconfigs
```

Two vitest projects defined in `vitest.config.ts`:

- **frontend** — jsdom environment, `frontend/tests/**/*.test.{ts,tsx}`, with `fake-indexeddb` setup
- **workers** — node environment, `src/**/*.test.ts`

## Deployment

```bash
pnpm deploy        # vite build && wrangler deploy
```

This builds the React SPA to `frontend/dist/` then deploys the Workers app with `wrangler deploy`. The Workers Assets binding serves the frontend; SPA routes fall back to `index.html`.

### Development

```bash
pnpm dev           # concurrently "vite" "wrangler dev --port 8787"
```

Vite dev server proxies `/api` and `/wisp` to the local wrangler dev server.

### Required Cloudflare Resources

Created automatically on first `wrangler deploy` or configured in `wrangler.jsonc`:

- **KV namespace** (`AUTH_KV`) — password hash storage
- **R2 bucket** (`IPA_BUCKET`) — compiled IPA files
- **Durable Objects** — WispProxy, DownloadManager (migrations in `wrangler.jsonc`)
- **Cron trigger** — `0 2 * * *` for auto-cleanup

## Interface Design System

### Intent

**Who**: Developers and power users managing Apple app downloads outside the App Store — sideloading IPAs, managing multiple Apple IDs, tracking licenses. Technical audience, likely running this alongside terminals or Xcode.

**Task**: Authenticate Apple accounts → search apps → acquire licenses → download/compile IPAs → install.

**Feel**: A sharp utility. Precise like a package manager, clear like Apple's developer tools. Confident, quiet, functional. Not playful, not corporate.

### Design Tokens

- **Primary accent**: `blue-600` / `blue-700` (hover) — trust + system authority, echoes Apple dev tooling
- **Backgrounds**: `gray-50` (app), `white` (cards/surfaces)
- **Text**: `gray-900` (primary), `gray-600` (secondary), `gray-400` (tertiary)
- **Borders**: `gray-200` (default), `gray-300` (hover) — use sparingly, prefer background tinting for containment
- **Status badges**: Muted tones — `green` (completed), `blue` (downloading), `yellow` (paused), `purple` (injecting), `red` (failed), `gray` (pending)
- **Alerts**: `red-50`/`red-700` (error), `amber-50`/`amber-700` (warning), `green-50`/`green-700` (success)

### Typography

- System font stack (Inter / SF Pro fallback)
- Weight scale: `500` (medium, workhorse), `600` (semibold, page titles and key labels only). Avoid `700` in body.
- Size scale: `xs` (12px), `sm` (14px), `base` (16px), `lg` (18px), `xl` (20px), `2xl` (24px)

### Spacing

- Base unit: `4px`
- Consistent vertical rhythm: `space-y-4` within sections, `space-y-6` between sections
- Page padding: `px-4 sm:px-6`, `py-6`
- Container: `max-w-5xl` (1024px)

### Depth & Surfaces

- Single elevation: white cards on `gray-50` background
- No shadows. Borders only where they serve function (form inputs, dividers, interactive boundaries)
- Rounded corners: `rounded-lg` (8px) for cards, `rounded-md` (6px) for inputs/buttons, `rounded-full` for badges
- Prefer background tinting (`gray-50` → `gray-100`) over borders for visual containment

### Layout

- Desktop: fixed sidebar (240px / `w-60`) + scrollable main content
- Mobile: bottom tab bar with safe-area padding
- Breakpoint: `md:` (768px) for sidebar ↔ bottom nav switch
- Page structure: `PageContainer` with title + optional action button, then content

### Component Patterns

- **Buttons**: Primary (`bg-blue-600 text-white`), Secondary (`border border-gray-300 text-gray-700`), Danger (`text-red-600 border-red-300`)
- **Inputs**: `rounded-md border-gray-300 focus:border-blue-500 focus:ring-1 focus:ring-blue-500`
- **Cards**: White background, `border border-gray-200 rounded-lg`, no shadow
- **Badge**: Color-coded pill (`rounded-full px-2 py-0.5 text-xs font-medium`)
- **ProgressBar**: Gray track, blue fill, percentage label
- **AppIcon**: 3 sizes (40/56/80px), rounded corners, letter fallback
- **Nav active state**: `bg-blue-50 text-blue-700` (sidebar), `text-blue-600` (mobile)

## Frontend Cleanup Rules

These rules prevent the codebase from becoming messy after merging PRs. Enforce them on every change.

### `transition-colors` Usage Policy

**Problem**: `transition-colors` on static containers (cards, sections, alerts, badges) causes visible color flashing when the page loads in dark mode — the element briefly renders in light colors then transitions to dark.

**Rule**: Only use `transition-colors` on **interactive elements** that change color on user interaction:

- Buttons (hover state)
- Links (hover state)
- Form inputs and selects (focus state)
- Nav items (hover/active state)

**Never use `transition-colors` on**:

- Card containers (`bg-white dark:bg-gray-900 rounded-lg border ...`)
- Section wrappers (`<section>` with background)
- Alert/warning banners (use the `<Alert>` component)
- Badge pills
- ProgressBar tracks
- Modal containers
- AppIcon fallback containers
- Empty state placeholder containers

**Exception**: Layout chrome (Sidebar, MobileNav, MobileHeader, PageContainer) may keep `transition-colors duration-200` for smooth theme toggle animation, since these persist across navigations.

### Shared Icons

All navigation and theme icons live in `components/common/icons.tsx`. When Sidebar, MobileNav, or MobileHeader need icons, import from there. Never duplicate icon SVG components inline.

### Import Ordering Verification

Before merging any frontend PR, verify imports follow the convention in every changed file:

```
1. React / library imports
2. Layout components
3. Common components
4. Sibling components (same feature folder)
5. Hooks / stores
6. Apple protocol / API modules
7. Utilities
8. Config
9. Types (always last)
```

### Empty State Containers

Empty states (shown when a list has no items) use a consistent pattern:

- `border-2 border-dashed` (not solid border)
- `bg-gray-50 dark:bg-gray-900/30` background
- No `transition-colors` (removed to prevent dark mode flashing)
- Centered icon in a white circle, title, description, optional CTA button

### Dark Mode Color Pairings

Always pair light and dark variants consistently:

- **Primary text**: `text-gray-900 dark:text-white`
- **Secondary text**: `text-gray-600 dark:text-gray-400` or `text-gray-500 dark:text-gray-400`
- **Tertiary text**: `text-gray-400 dark:text-gray-500`
- **Card background**: `bg-white dark:bg-gray-900`
- **Page background**: `bg-gray-50 dark:bg-gray-950`
- **Card border**: `border-gray-200 dark:border-gray-800`
- **Input border**: `border-gray-300 dark:border-gray-700`

### Code Duplication Prevention

When the same UI pattern appears in 3+ components, extract it to `components/common/`. Current shared components:

- `Alert`, `Modal`, `Spinner`, `CountrySelect`, `AppIcon`, `Badge`, `ProgressBar`, `icons`

When adding new common components, update this CLAUDE.md file accordingly.
