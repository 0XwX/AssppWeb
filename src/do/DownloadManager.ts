import { DurableObject } from 'cloudflare:workers';
import bplistParser from 'bplist-parser';
import bplistCreator from 'bplist-creator';
import plist from 'plist';
import {
  MAX_DOWNLOAD_SIZE,
  UPLOAD_PART_SIZE,
  CDN_FETCH_TIMEOUT_MS,
  CDN_STALL_TIMEOUT_MS,
  CDN_FETCH_MAX_RETRIES,
  MIN_ACCOUNT_HASH_LENGTH,
} from '../config.js';
import {
  appendToZipTail,
  findEocd,
  parseCentralDirectory,
  readEntryData,
  type CdEntry,
} from '../services/zipAppend.js';
import type { DownloadTask, Software, Sinf } from '../types.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface CreateTaskParams {
  software: Software;
  accountHash: string;
  downloadURL: string;
  sinfs: Sinf[];
  iTunesMetadata?: string;
}

export type SanitizedTask = Omit<
  DownloadTask,
  'downloadURL' | 'sinfs' | 'iTunesMetadata' | 'filePath'
> & { hasFile: boolean; fileSize?: number };

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const ALLOWED_DOWNLOAD_RE = /\.apple\.com$/i;

function validateDownloadURL(url: string): void {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new Error('Invalid download URL');
  }
  if (parsed.protocol !== 'https:') throw new Error('Download URL must use HTTPS');
  if (!ALLOWED_DOWNLOAD_RE.test(parsed.hostname))
    throw new Error('Download URL must be from *.apple.com');
  if (/^\d+\.\d+\.\d+\.\d+$/.test(parsed.hostname))
    throw new Error('Download URL must not use IP addresses');
}

function sanitize(task: DownloadTask, hasFile: boolean, fileSize?: number): SanitizedTask {
  const { downloadURL: _d, sinfs: _s, iTunesMetadata: _m, filePath: _f, ...safe } = task;
  return { ...safe, hasFile, fileSize };
}

function formatSpeed(bps: number): string {
  if (bps < 1024) return `${Math.round(bps)} B/s`;
  if (bps < 1024 * 1024) return `${(bps / 1024).toFixed(1)} KB/s`;
  return `${(bps / (1024 * 1024)).toFixed(1)} MB/s`;
}

/** Take exactly `size` bytes from chunks, return merged + remaining */
function mergeChunks(
  chunks: Uint8Array[],
  size: number,
): { merged: Uint8Array; remaining: Uint8Array[] } {
  const merged = new Uint8Array(size);
  let pos = 0;
  const remaining: Uint8Array[] = [];
  let filled = false;

  for (const chunk of chunks) {
    if (filled) {
      remaining.push(chunk);
      continue;
    }
    const need = size - pos;
    if (chunk.length <= need) {
      merged.set(chunk, pos);
      pos += chunk.length;
      if (pos === size) filled = true;
    } else {
      merged.set(chunk.subarray(0, need), pos);
      remaining.push(chunk.subarray(need));
      filled = true;
    }
  }
  return { merged, remaining };
}

function mergeAll(chunks: Uint8Array[], totalSize: number): Uint8Array {
  const out = new Uint8Array(totalSize);
  let pos = 0;
  for (const c of chunks) {
    out.set(c, pos);
    pos += c.length;
  }
  return out;
}

class HttpError extends Error {
  constructor(
    public readonly status: number,
    statusText: string,
  ) {
    super(`HTTP ${status}: ${statusText}`);
  }
}

async function fetchWithRetry(
  url: string,
  signal: AbortSignal,
  maxRetries: number = CDN_FETCH_MAX_RETRIES,
): Promise<Response> {
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    if (signal.aborted) throw new DOMException('Aborted', 'AbortError');

    const timeoutController = new AbortController();
    const timeout = setTimeout(() => timeoutController.abort(), CDN_FETCH_TIMEOUT_MS);
    const combined = AbortSignal.any([signal, timeoutController.signal]);

    try {
      const response = await fetch(url, {
        signal: combined,
        redirect: 'follow',
      });
      clearTimeout(timeout);

      if (!response.ok) {
        throw new HttpError(response.status, response.statusText);
      }

      return response;
    } catch (err) {
      clearTimeout(timeout);
      if (signal.aborted) throw new DOMException('Aborted', 'AbortError');

      lastError = err instanceof Error ? err : new Error(String(err));

      // 4xx: client error, no retry
      if (lastError instanceof HttpError && lastError.status < 500) throw lastError;
      if (attempt === maxRetries) break;

      const delay = 1000 * Math.pow(2, attempt);
      await new Promise<void>((resolve, reject) => {
        const onAbort = () => {
          clearTimeout(timer);
          reject(new DOMException('Aborted', 'AbortError'));
        };
        const timer = setTimeout(() => {
          signal.removeEventListener('abort', onAbort);
          resolve();
        }, delay);
        signal.addEventListener('abort', onAbort, { once: true });
      });
    }
  }

  throw lastError ?? new Error('Download failed');
}

// ---------------------------------------------------------------------------
// DownloadManager Durable Object
// ---------------------------------------------------------------------------

/**
 * DownloadManager — one singleton DO per deployment.
 * Routes via: env.DOWNLOAD_MANAGER.idFromName('singleton')
 *
 * Storage keys:
 *   task:<id>        → JSON(DownloadTask)  (secrets cleared after completion)
 *   r2key:<id>       → string              (R2 object key for completed IPA)
 *   accounts:<hash>  → JSON(string[])      (task IDs for an account)
 */
export class DownloadManager extends DurableObject<Env> {
  private abortControllers = new Map<string, AbortController>();

  // ---------------------------------------------------------------------------
  // RPC methods (called from HTTP routes via DO stub)
  // ---------------------------------------------------------------------------

  async createTask(params: CreateTaskParams): Promise<SanitizedTask> {
    validateDownloadURL(params.downloadURL);
    if (!params.accountHash || params.accountHash.length < MIN_ACCOUNT_HASH_LENGTH) {
      throw new Error('Invalid accountHash');
    }

    // Deduplicate: reject if same bundleID + version already exists for this account
    const existingIds =
      (await this.ctx.storage.get<string[]>(`accounts:${params.accountHash}`)) ?? [];
    for (const existingId of existingIds) {
      const existing = await this.loadTask(existingId);
      if (
        existing &&
        existing.software.bundleID === params.software.bundleID &&
        existing.software.version === params.software.version &&
        existing.status !== 'failed'
      ) {
        throw new Error(
          `Already downloading: ${existing.software.name} v${existing.software.version}`,
        );
      }
    }

    const task: DownloadTask = {
      id: crypto.randomUUID(),
      software: params.software,
      accountHash: params.accountHash,
      downloadURL: params.downloadURL,
      sinfs: params.sinfs,
      iTunesMetadata: params.iTunesMetadata,
      status: 'pending',
      progress: 0,
      speed: '0 B/s',
      createdAt: new Date().toISOString(),
    };

    await this.saveTask(task);
    await this.addToAccountIndex(params.accountHash, task.id);

    // Start download in background (non-blocking)
    this.ctx.waitUntil(this.startDownload(task));

    return sanitize(task, false);
  }

  async getTask(id: string, accountHash: string): Promise<SanitizedTask | null> {
    const task = await this.loadTask(id);
    if (!task || task.accountHash !== accountHash) return null;
    const r2key = await this.ctx.storage.get<string>(`r2key:${id}`);
    const head = r2key ? await this.env.IPA_BUCKET.head(r2key) : null;
    return sanitize(task, !!head, head?.size);
  }

  async listTasks(accountHashes: string[]): Promise<SanitizedTask[]> {
    const result: SanitizedTask[] = [];
    for (const hash of accountHashes) {
      const ids = (await this.ctx.storage.get<string[]>(`accounts:${hash}`)) ?? [];
      for (const id of ids) {
        const task = await this.loadTask(id);
        if (!task) continue;
        const r2key = await this.ctx.storage.get<string>(`r2key:${id}`);
        const head = r2key ? await this.env.IPA_BUCKET.head(r2key) : null;
        result.push(sanitize(task, !!head, head?.size));
      }
    }
    return result;
  }

  async deleteTask(id: string, accountHash: string): Promise<boolean> {
    const task = await this.loadTask(id);
    if (!task || task.accountHash !== accountHash) return false;

    // Abort if in progress
    this.abortControllers.get(id)?.abort();
    this.abortControllers.delete(id);

    const storedR2Key = (await this.ctx.storage.get<string>(`r2key:${id}`)) ?? null;
    await this.deleteR2Files(id, accountHash, storedR2Key, task);

    // Remove from storage
    await this.ctx.storage.delete(`task:${id}`);
    await this.ctx.storage.delete(`r2key:${id}`);
    await this.removeFromAccountIndex(accountHash, id);
    return true;
  }

  async pauseTask(id: string, accountHash: string): Promise<boolean> {
    const task = await this.loadTask(id);
    if (!task || task.accountHash !== accountHash) return false;
    if (task.status !== 'downloading') return false;

    this.abortControllers.get(id)?.abort();
    this.abortControllers.delete(id);

    task.status = 'paused';
    await this.saveTask(task);
    return true;
  }

  async resumeTask(id: string, accountHash: string): Promise<boolean> {
    const task = await this.loadTask(id);
    if (!task || task.accountHash !== accountHash) return false;
    if (task.status !== 'paused') return false;

    this.ctx.waitUntil(this.startDownload(task));
    return true;
  }

  async getR2Key(id: string, accountHash: string): Promise<string | null> {
    const task = await this.loadTask(id);
    if (!task || task.accountHash !== accountHash) return null;
    if (task.status !== 'completed') return null;
    return (await this.ctx.storage.get<string>(`r2key:${id}`)) ?? null;
  }

  /** Public lookup by task ID only — no accountHash. UUID is the secret. */
  async getTaskPublic(id: string): Promise<{ software: Software; hasFile: boolean } | null> {
    const task = await this.loadTask(id);
    if (!task || task.status !== 'completed') return null;
    const r2key = await this.ctx.storage.get<string>(`r2key:${id}`);
    const hasFile = !!r2key && !!(await this.env.IPA_BUCKET.head(r2key));
    return { software: task.software, hasFile };
  }

  /** Public R2 key lookup by task ID only. */
  async getR2KeyPublic(id: string): Promise<string | null> {
    const task = await this.loadTask(id);
    if (!task || task.status !== 'completed') return null;
    return (await this.ctx.storage.get<string>(`r2key:${id}`)) ?? null;
  }

  async listPackages(accountHashes: string[]): Promise<
    Array<{
      id: string;
      software: Software;
      accountHash: string;
      r2key: string;
      createdAt: string;
    }>
  > {
    const result = [];
    for (const hash of accountHashes) {
      const ids = (await this.ctx.storage.get<string[]>(`accounts:${hash}`)) ?? [];
      for (const id of ids) {
        const task = await this.loadTask(id);
        if (!task || task.status !== 'completed') continue;
        const r2key = await this.ctx.storage.get<string>(`r2key:${id}`);
        if (!r2key) continue;
        result.push({
          id,
          software: task.software,
          accountHash: hash,
          r2key,
          createdAt: task.createdAt,
        });
      }
    }
    return result;
  }

  // ---------------------------------------------------------------------------
  // Config RPC — cleanup settings (overrides env vars)
  // ---------------------------------------------------------------------------

  async getConfig(): Promise<{ autoCleanupDays?: number; autoCleanupMaxMB?: number }> {
    const days = await this.ctx.storage.get<number>('config:autoCleanupDays');
    const maxMB = await this.ctx.storage.get<number>('config:autoCleanupMaxMB');
    return {
      autoCleanupDays: days ?? undefined,
      autoCleanupMaxMB: maxMB ?? undefined,
    };
  }

  async setConfig(config: { autoCleanupDays?: number; autoCleanupMaxMB?: number }): Promise<void> {
    if (config.autoCleanupDays !== undefined) {
      await this.ctx.storage.put('config:autoCleanupDays', config.autoCleanupDays);
    }
    if (config.autoCleanupMaxMB !== undefined) {
      await this.ctx.storage.put('config:autoCleanupMaxMB', config.autoCleanupMaxMB);
    }
  }

  // ---------------------------------------------------------------------------
  // Auth RPC — password management
  // ---------------------------------------------------------------------------

  async getPasswordHash(): Promise<string | null> {
    return (await this.ctx.storage.get<string>('auth:password_hash')) ?? null;
  }

  async setPasswordHash(hash: string): Promise<void> {
    await this.ctx.storage.put('auth:password_hash', hash);
  }

  /** Atomic set-if-not-exists for first-time setup (DO is single-threaded) */
  async setPasswordHashIfNotExists(hash: string): Promise<boolean> {
    const existing = await this.ctx.storage.get<string>('auth:password_hash');
    if (existing) return false;
    await this.ctx.storage.put('auth:password_hash', hash);
    return true;
  }

  // ---------------------------------------------------------------------------
  // Cleanup RPC — called by cron scheduled handler
  // ---------------------------------------------------------------------------

  async cleanupExpired(
    days: number,
    maxMB: number,
  ): Promise<{
    deletedByAge: number;
    deletedBySize: number;
    deletedOrphaned: number;
    totalSizeMB: number;
  }> {
    let deletedByAge = 0;
    let deletedBySize = 0;
    let deletedOrphaned = 0;

    // Collect all tasks
    const allTasks: Array<{ id: string; task: DownloadTask; r2key: string | null }> = [];
    const storageMap = await this.ctx.storage.list<string>({ prefix: 'task:' });
    for (const [key, raw] of storageMap) {
      const id = key.slice('task:'.length);
      let task: DownloadTask;
      try {
        task = JSON.parse(raw) as DownloadTask;
      } catch (e) {
        console.error(`Corrupt task data at ${key}, skipping:`, e);
        continue;
      }
      const r2key = (await this.ctx.storage.get<string>(`r2key:${id}`)) ?? null;
      allTasks.push({ id, task, r2key });
    }

    // Scan R2 upfront (shared by Phase 2 and Phase 3)
    const sizeMap = new Map<string, number>();
    let totalSize = 0;
    let cursor: string | undefined;
    do {
      const listed = await this.env.IPA_BUCKET.list({ cursor, limit: 500 });
      for (const obj of listed.objects) {
        sizeMap.set(obj.key, obj.size);
        totalSize += obj.size;
      }
      cursor = listed.truncated ? listed.cursor : undefined;
    } while (cursor);

    // Phase 1: delete tasks older than N days
    if (days > 0) {
      const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
      for (const entry of [...allTasks]) {
        const createdAt = new Date(entry.task.createdAt).getTime();
        if (createdAt < cutoff) {
          const size = entry.r2key ? (sizeMap.get(entry.r2key) ?? 0) : 0;
          await this.purgeTask(entry.id, entry.task.accountHash, entry.r2key, entry.task);
          if (entry.r2key) sizeMap.delete(entry.r2key);
          totalSize -= size;
          allTasks.splice(allTasks.indexOf(entry), 1);
          deletedByAge++;
        }
      }
    }

    // Phase 2: enforce total size limit
    if (maxMB > 0) {
      const maxBytes = maxMB * 1024 * 1024;
      if (totalSize > maxBytes) {
        allTasks.sort(
          (a, b) => new Date(a.task.createdAt).getTime() - new Date(b.task.createdAt).getTime(),
        );

        for (const entry of allTasks) {
          if (totalSize <= maxBytes) break;
          const size = entry.r2key ? (sizeMap.get(entry.r2key) ?? 0) : 0;
          await this.purgeTask(entry.id, entry.task.accountHash, entry.r2key, entry.task);
          if (entry.r2key) sizeMap.delete(entry.r2key);
          totalSize -= size;
          deletedBySize++;
        }
      }
    }

    // Phase 3: delete orphaned R2 objects (no matching DO record)
    const knownR2Keys = new Set<string>();
    const r2keyMap = await this.ctx.storage.list<string>({ prefix: 'r2key:' });
    for (const [, value] of r2keyMap) {
      knownR2Keys.add(value);
    }

    const orphanKeys: string[] = [];
    for (const [key, size] of sizeMap) {
      if (!knownR2Keys.has(key)) {
        orphanKeys.push(key);
        totalSize -= size;
        deletedOrphaned++;
      }
    }
    if (orphanKeys.length > 0) {
      await this.env.IPA_BUCKET.delete(orphanKeys).catch((e) =>
        console.error('Orphan R2 batch delete failed:', e),
      );
    }

    return {
      deletedByAge,
      deletedBySize,
      deletedOrphaned,
      totalSizeMB: Math.round((totalSize / (1024 * 1024)) * 100) / 100,
    };
  }

  /** Delete a task completely: R2 object + DO storage + account index */
  private async purgeTask(
    id: string,
    accountHash: string,
    r2key: string | null,
    task?: DownloadTask | null,
  ): Promise<void> {
    this.abortControllers.get(id)?.abort();
    this.abortControllers.delete(id);

    await this.deleteR2Files(id, accountHash, r2key, task);
    await this.ctx.storage.delete(`task:${id}`);
    await this.ctx.storage.delete(`r2key:${id}`);
    await this.removeFromAccountIndex(accountHash, id);
  }

  /** Delete R2 files for a task — stored key + computed key + .new temp key */
  private async deleteR2Files(
    id: string,
    accountHash: string,
    r2key: string | null,
    task?: DownloadTask | null,
  ): Promise<void> {
    const keysToDelete = new Set<string>();
    if (r2key) keysToDelete.add(r2key);
    if (task) {
      const computed = `packages/${accountHash}/${task.software.bundleID}/${id}.ipa`;
      keysToDelete.add(computed);
      keysToDelete.add(computed + '.new');
    }
    if (keysToDelete.size > 0) {
      await this.env.IPA_BUCKET.delete([...keysToDelete]).catch((e) =>
        console.error(`R2 delete failed for task=${id}:`, e),
      );
    }
  }

  // ---------------------------------------------------------------------------
  // Download pipeline
  // ---------------------------------------------------------------------------

  private async startDownload(task: DownloadTask): Promise<void> {
    const controller = new AbortController();
    this.abortControllers.set(task.id, controller);

    task.status = 'downloading';
    task.progress = 0;
    task.speed = '0 B/s';
    task.error = undefined;
    await this.saveTask(task);

    const r2key = `packages/${task.accountHash}/${task.software.bundleID}/${task.id}.ipa`;

    try {
      validateDownloadURL(task.downloadURL);

      const response = await fetchWithRetry(task.downloadURL, controller.signal);
      if (!response.body) throw new Error('No response body');

      const contentLength = parseInt(response.headers.get('content-length') ?? '0');
      if (contentLength > MAX_DOWNLOAD_SIZE) {
        throw new Error('File too large');
      }

      // Stream Apple CDN → R2 multipart upload
      await this.streamToR2(task, response.body, r2key, contentLength, controller.signal);

      // SINF injection via R2 CopyPart + appendToZipTail
      if (task.sinfs.length > 0 || task.iTunesMetadata) {
        task.status = 'injecting';
        task.progress = 100;
        await this.saveTask(task);
        await this.injectSinf(task, r2key);
      }

      // Complete
      task.status = 'completed';
      task.progress = 100;
      task.downloadURL = '';
      task.sinfs = [];
      task.iTunesMetadata = undefined;
      await this.saveTask(task);
      await this.ctx.storage.put(`r2key:${task.id}`, r2key);
    } catch (err) {
      this.abortControllers.delete(task.id);
      if (err instanceof Error && err.name === 'AbortError') {
        // pauseTask already set status to 'paused'
        return;
      }
      task.status = 'failed';
      task.error = err instanceof Error ? err.message : 'Download failed';
      await this.saveTask(task);
    }

    this.abortControllers.delete(task.id);
  }

  private async streamToR2(
    task: DownloadTask,
    body: ReadableStream<Uint8Array>,
    r2key: string,
    contentLength: number,
    signal: AbortSignal,
  ): Promise<void> {
    const upload = await this.env.IPA_BUCKET.createMultipartUpload(r2key);
    const parts: R2UploadedPart[] = [];
    let partNum = 1;
    let downloaded = 0;
    let lastTime = Date.now();
    let lastBytes = 0;

    // Collect chunks instead of concat on every read
    let chunks: Uint8Array[] = [];
    let bufferSize = 0;

    // Double-buffering: previous part upload promise
    let pendingUpload: Promise<void> | null = null;

    const flushBuffer = async () => {
      if (pendingUpload) await pendingUpload;
      pendingUpload = null;

      // Synchronously upload all-but-last full parts
      while (bufferSize >= UPLOAD_PART_SIZE * 2) {
        const part = mergeChunks(chunks, UPLOAD_PART_SIZE);
        chunks = part.remaining;
        bufferSize -= UPLOAD_PART_SIZE;
        const uploaded = await upload.uploadPart(partNum++, part.merged);
        parts.push(uploaded);
      }

      // Fire last full part as double-buffer (read continues while it uploads)
      if (bufferSize >= UPLOAD_PART_SIZE) {
        const part = mergeChunks(chunks, UPLOAD_PART_SIZE);
        chunks = part.remaining;
        bufferSize -= UPLOAD_PART_SIZE;
        const num = partNum++;
        const data = part.merged;
        pendingUpload = (async () => {
          const uploaded = await upload.uploadPart(num, data);
          parts.push(uploaded);
        })();
      }
    };

    const reader = body.getReader();
    let stallTimer: ReturnType<typeof setTimeout> | undefined;

    try {
      while (true) {
        if (signal.aborted) throw new DOMException('Aborted', 'AbortError');

        const readResult = await Promise.race([
          reader.read(),
          new Promise<never>((_, reject) => {
            stallTimer = setTimeout(() => {
              const pct = contentLength > 0 ? Math.round((downloaded / contentLength) * 100) : '?';
              reject(
                new Error(`CDN stall: no data for ${CDN_STALL_TIMEOUT_MS / 1000}s at ${pct}%`),
              );
            }, CDN_STALL_TIMEOUT_MS);
          }),
        ]);
        clearTimeout(stallTimer);

        const { done, value } = readResult;
        if (done) break;

        downloaded += value.length;
        if (downloaded > MAX_DOWNLOAD_SIZE) throw new Error('File too large');

        chunks.push(value);
        bufferSize += value.length;

        if (bufferSize >= UPLOAD_PART_SIZE) {
          await flushBuffer();
        }

        const now = Date.now();
        if (now - lastTime >= 2000) {
          // 2s interval to reduce DO storage write overhead
          const bps = ((downloaded - lastBytes) / (now - lastTime)) * 1000;
          task.speed = formatSpeed(bps);
          task.progress = contentLength > 0 ? Math.round((downloaded / contentLength) * 100) : 0;
          lastTime = now;
          lastBytes = downloaded;
          await this.saveTask(task);
        }
      }

      // Wait for pending upload (may still be in-flight after last flushBuffer)
      // TS control-flow narrows pendingUpload incorrectly; async callback sets it
      const pending = pendingUpload as Promise<void> | null;
      if (pending) await pending;

      // Upload remaining full parts
      while (bufferSize >= UPLOAD_PART_SIZE) {
        const part = mergeChunks(chunks, UPLOAD_PART_SIZE);
        chunks = part.remaining;
        bufferSize -= UPLOAD_PART_SIZE;
        const uploaded = await upload.uploadPart(partNum++, part.merged);
        parts.push(uploaded);
      }

      // Upload final partial part
      if (bufferSize > 0) {
        const final = mergeAll(chunks, bufferSize);
        const uploaded = await upload.uploadPart(partNum++, final);
        parts.push(uploaded);
      }

      parts.sort((a, b) => a.partNumber - b.partNumber);
      await upload.complete(parts);
    } catch (err) {
      clearTimeout(stallTimer);
      // pendingUpload may still be in-flight if error occurred during read
      const pending = pendingUpload as Promise<void> | null;
      if (pending)
        await pending.catch((e) => console.error('Pending part upload error during abort:', e));
      await upload.abort().catch((e) => console.error('Multipart upload abort failed:', e));
      throw err;
    } finally {
      reader.releaseLock();
    }
  }

  // ---------------------------------------------------------------------------
  // SINF injection via R2 Range reads + appendToZipTail + R2 CopyPart
  // ---------------------------------------------------------------------------

  private async injectSinf(task: DownloadTask, r2key: string): Promise<void> {
    const meta = await this.env.IPA_BUCKET.head(r2key);
    if (!meta) throw new Error('R2 object not found for SINF injection');
    const archiveSize = meta.size;

    const readRange = async (start: number, end: number): Promise<Uint8Array> => {
      const obj = await this.env.IPA_BUCKET.get(r2key, {
        range: { offset: start, length: end - start },
      });
      if (!obj) throw new Error('R2 range read failed');
      return new Uint8Array(await obj.arrayBuffer());
    };

    // Read EOCD + Central Directory once, reuse for both buildFilesToAppend and appendToZipTail
    const tailSize = Math.min(65536 + 22, archiveSize);
    const tailBuf = await readRange(archiveSize - tailSize, archiveSize);
    const eocd = findEocd(tailBuf, archiveSize);
    const cd = await readRange(eocd.cdOffset, eocd.cdOffset + eocd.cdSize);
    const entries = parseCentralDirectory(cd);

    const filesToAppend = await this.buildFilesToAppend(task, entries, readRange);
    if (filesToAppend.length === 0) return;

    // Compute only the tail (no full-archive read), reusing precomputed CD
    const { cdOffset, tail } = await appendToZipTail(archiveSize, readRange, filesToAppend, {
      eocd,
      entries,
    });

    // Compose new IPA via R2 multipart upload to a temp key, then swap.
    // We must use a separate key because R2 throttles concurrent reads on
    // the same object (error 10058), and readRange() needs to GET the
    // original key while we uploadPart() to the new one.
    const COPY_CHUNK = 50 * 1024 * 1024; // 50 MB per part (DO usable memory ~70-90 MB)
    const newKey = r2key + '.new';
    const upload = await this.env.IPA_BUCKET.createMultipartUpload(newKey);
    try {
      const parts: R2UploadedPart[] = [];
      let partNum = 1;

      // Upload original data in chunks, merging tail into the last chunk
      // (R2 requires all non-trailing parts to have the same size)
      let tailAppended = false;
      for (let offset = 0; offset < cdOffset; offset += COPY_CHUNK) {
        const length = Math.min(COPY_CHUNK, cdOffset - offset);
        const chunk = await readRange(offset, offset + length);
        const isLastChunk = offset + length >= cdOffset;

        if (isLastChunk) {
          const combined = new Uint8Array(chunk.length + tail.length);
          combined.set(chunk, 0);
          combined.set(tail, chunk.length);
          const part = await upload.uploadPart(partNum++, combined);
          parts.push(part);
          tailAppended = true;
        } else {
          const part = await upload.uploadPart(partNum++, chunk);
          parts.push(part);
        }
      }

      // cdOffset === 0: no original data, upload tail only
      if (!tailAppended) {
        const part = await upload.uploadPart(partNum++, tail);
        parts.push(part);
      }

      await upload.complete(parts);
    } catch (err) {
      await upload.abort().catch((e) => console.error('SINF multipart abort failed:', e));
      throw err;
    }

    // Atomic swap: overwrite original with new, then delete temp key
    const newObj = await this.env.IPA_BUCKET.get(newKey);
    if (!newObj) throw new Error('R2 rename step failed: new object missing');
    await this.env.IPA_BUCKET.put(r2key, newObj.body);
    await this.env.IPA_BUCKET.delete(newKey).catch((e) =>
      console.error('R2 temp key cleanup failed:', e),
    );
  }

  private async buildFilesToAppend(
    task: DownloadTask,
    entries: CdEntry[],
    readRange: (start: number, end: number) => Promise<Uint8Array>,
  ): Promise<Array<{ name: string; data: Uint8Array }>> {
    const files: Array<{ name: string; data: Uint8Array }> = [];

    // Find bundle name
    let bundleName: string | null = null;
    for (const e of entries) {
      const m = e.name.match(/^Payload\/([^/]+)\.app\//);
      if (m?.[1] && !e.name.includes('/Watch/')) {
        bundleName = m[1];
        break;
      }
    }
    if (!bundleName) throw new Error('Could not find .app bundle name');

    // Try Manifest.plist first
    const manifestEntry = entries.find(
      (e) => e.name === `Payload/${bundleName}.app/SC_Info/Manifest.plist`,
    );
    let sinfPaths: string[] | null = null;

    if (manifestEntry) {
      const data = await readEntryData(manifestEntry, readRange);
      sinfPaths = this.parseSinfPaths(data);
    }

    if (sinfPaths) {
      // Use manifest-specified paths
      for (let i = 0; i < sinfPaths.length; i++) {
        if (i >= task.sinfs.length) break;
        const sinfPath = sinfPaths[i];
        const entryPath = `Payload/${bundleName}.app/${sinfPath}`;
        files.push({
          name: entryPath,
          data: Buffer.from(task.sinfs[i]!.sinf, 'base64'),
        });
      }
    } else {
      // Fallback: read Info.plist for CFBundleExecutable
      const infoEntry = entries.find(
        (e) => e.name === `Payload/${bundleName}.app/Info.plist` && !e.name.includes('/Watch/'),
      );
      if (!infoEntry) throw new Error('Could not read manifest or info plist');

      const infoData = await readEntryData(infoEntry, readRange);
      const execName = this.parseExecutableName(infoData);
      if (!execName) throw new Error('Could not read CFBundleExecutable');

      if (task.sinfs.length > 0) {
        files.push({
          name: `Payload/${bundleName}.app/SC_Info/${execName}.sinf`,
          data: Buffer.from(task.sinfs[0]!.sinf, 'base64'),
        });
      }
    }

    // iTunesMetadata.plist at archive root
    if (task.iTunesMetadata) {
      const xmlBuffer = Buffer.from(task.iTunesMetadata, 'base64');
      let metaBuffer: Buffer;
      try {
        const parsed = plist.parse(xmlBuffer.toString('utf-8'));
        metaBuffer = Buffer.from(bplistCreator(parsed as Record<string, unknown>));
      } catch (e) {
        console.error(
          `iTunesMetadata XML->bplist conversion failed for task ${task.id}, using raw XML:`,
          e,
        );
        metaBuffer = xmlBuffer;
      }
      files.push({ name: 'iTunesMetadata.plist', data: metaBuffer });
    }

    return files;
  }

  private parseSinfPaths(data: Uint8Array): string[] | null {
    // Try binary plist
    try {
      const parsed = bplistParser.parseBuffer(Buffer.from(data));
      if (parsed?.length) {
        const obj = parsed[0] as Record<string, unknown>;
        const paths = obj['SinfPaths'];
        if (Array.isArray(paths)) return paths as string[];
      }
    } catch {
      // not binary
    }
    // Try XML plist
    try {
      const xml = new TextDecoder().decode(data);
      const parsed = plist.parse(xml) as Record<string, unknown>;
      const paths = parsed['SinfPaths'];
      if (Array.isArray(paths)) return paths as string[];
    } catch {
      // not XML
    }
    return null;
  }

  private parseExecutableName(data: Uint8Array): string | null {
    try {
      const parsed = bplistParser.parseBuffer(Buffer.from(data));
      if (parsed?.length) {
        const obj = parsed[0] as Record<string, unknown>;
        const exe = obj['CFBundleExecutable'];
        if (typeof exe === 'string') return exe;
      }
    } catch {
      // not binary
    }
    try {
      const xml = new TextDecoder().decode(data);
      const parsed = plist.parse(xml) as Record<string, unknown>;
      const exe = parsed['CFBundleExecutable'];
      if (typeof exe === 'string') return exe;
    } catch {
      // not XML
    }
    return null;
  }

  // ---------------------------------------------------------------------------
  // Storage helpers
  // ---------------------------------------------------------------------------

  private async saveTask(task: DownloadTask): Promise<void> {
    await this.ctx.storage.put(`task:${task.id}`, JSON.stringify(task));
  }

  private async loadTask(id: string): Promise<DownloadTask | null> {
    const raw = await this.ctx.storage.get<string>(`task:${id}`);
    if (!raw) return null;
    try {
      return JSON.parse(raw) as DownloadTask;
    } catch {
      return null;
    }
  }

  private async addToAccountIndex(accountHash: string, taskId: string): Promise<void> {
    const existing = (await this.ctx.storage.get<string[]>(`accounts:${accountHash}`)) ?? [];
    if (!existing.includes(taskId)) {
      await this.ctx.storage.put(`accounts:${accountHash}`, [...existing, taskId]);
    }
  }

  private async removeFromAccountIndex(accountHash: string, taskId: string): Promise<void> {
    const existing = (await this.ctx.storage.get<string[]>(`accounts:${accountHash}`)) ?? [];
    await this.ctx.storage.put(
      `accounts:${accountHash}`,
      existing.filter((id) => id !== taskId),
    );
  }
}
