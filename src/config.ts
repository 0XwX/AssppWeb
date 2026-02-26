export const MAX_DOWNLOAD_SIZE = 8 * 1024 * 1024 * 1024; // 8 GB
export const BAG_TIMEOUT_MS = 15_000; // 15 seconds
export const BAG_MAX_BYTES = 1024 * 1024; // 1 MB
export const MIN_ACCOUNT_HASH_LENGTH = 8;
export const UPLOAD_PART_SIZE = 25 * 1024 * 1024; // 25 MB (streamToR2 part size)
export const CDN_FETCH_TIMEOUT_MS = 30_000; // 30s connection timeout
export const CDN_STALL_TIMEOUT_MS = 60_000; // 60s no-data stall timeout during streaming
export const CDN_FETCH_MAX_RETRIES = 3;
export const MAX_SEARCH_BYTES = 5 * 1024 * 1024; // 5 MB
export const TOP_CHARTS_MAX_BYTES = 2 * 1024 * 1024; // 2 MB

export const BAG_USER_AGENT =
  'Configurator/2.17 (Macintosh; OS X 15.2; 24C5089c) AppleWebKit/0620.1.16.11.6';
