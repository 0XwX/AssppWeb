import type { DownloadManager } from './do/DownloadManager.js';
import type { WispProxy } from './do/WispProxy.js';

declare global {
  interface Env {
    WISP_PROXY: DurableObjectNamespace<WispProxy>;
    DOWNLOAD_MANAGER: DurableObjectNamespace<DownloadManager>;
    AUTH_KV: KVNamespace;
    IPA_BUCKET: R2Bucket;
    ASSETS: Fetcher;
    AUTO_CLEANUP_DAYS: string;
    AUTO_CLEANUP_MAX_MB: string;
    POW_DIFFICULTY?: string;
    BUILD_COMMIT?: string;
    BUILD_DATE?: string;
    R2_CDN_DOMAIN?: string;
  }
}
