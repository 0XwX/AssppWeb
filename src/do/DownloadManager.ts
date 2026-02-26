import { DurableObject } from 'cloudflare:workers';
import { v4 as uuidv4 } from 'uuid';
import bplistParser from 'bplist-parser';
import bplistCreator from 'bplist-creator';
import plist from 'plist';
import {
  MAX_DOWNLOAD_SIZE,
  MIN_MULTIPART_PART_SIZE,
  MIN_ACCOUNT_HASH_LENGTH,
} from '../config.js';
import {
  appendToZipTail,
  findEocd,
  parseCentralDirectory,
  readEntryData,
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
> & { hasFile: boolean };

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

function sanitize(task: DownloadTask, hasFile: boolean): SanitizedTask {
  const { downloadURL: _d, sinfs: _s, iTunesMetadata: _m, filePath: _f, ...safe } =
    task;
  return { ...safe, hasFile };
}

function formatSpeed(bps: number): string {
  if (bps < 1024) return `${Math.round(bps)} B/s`;
  if (bps < 1024 * 1024) return `${(bps / 1024).toFixed(1)} KB/s`;
  return `${(bps / (1024 * 1024)).toFixed(1)} MB/s`;
}

function concat(...arrays: Uint8Array<ArrayBufferLike>[]): Uint8Array<ArrayBuffer> {
  const total = arrays.reduce((n, a) => n + a.length, 0);
  const out = new Uint8Array(total);
  let pos = 0;
  for (const a of arrays) { out.set(a, pos); pos += a.length; }
  return out;
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
export class DownloadManager extends DurableObject {
  private abortControllers = new Map<string, AbortController>();
  declare env: Env;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.env = env;
  }

  // ---------------------------------------------------------------------------
  // RPC methods (called from HTTP routes via DO stub)
  // ---------------------------------------------------------------------------

  async createTask(params: CreateTaskParams): Promise<SanitizedTask> {
    validateDownloadURL(params.downloadURL);
    if (!params.accountHash || params.accountHash.length < MIN_ACCOUNT_HASH_LENGTH) {
      throw new Error('Invalid accountHash');
    }

    const task: DownloadTask = {
      id: uuidv4(),
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
    const hasFile = !!r2key && !!(await this.env.IPA_BUCKET.head(r2key));
    return sanitize(task, hasFile);
  }

  async listTasks(accountHashes: string[]): Promise<SanitizedTask[]> {
    const result: SanitizedTask[] = [];
    for (const hash of accountHashes) {
      const ids =
        (await this.ctx.storage.get<string[]>(`accounts:${hash}`)) ?? [];
      for (const id of ids) {
        const task = await this.loadTask(id);
        if (!task) continue;
        const r2key = await this.ctx.storage.get<string>(`r2key:${id}`);
        const hasFile = !!r2key && !!(await this.env.IPA_BUCKET.head(r2key));
        result.push(sanitize(task, hasFile));
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

    // Delete R2 file
    const r2key = await this.ctx.storage.get<string>(`r2key:${id}`);
    if (r2key) {
      await this.env.IPA_BUCKET.delete(r2key).catch((e) =>
        console.error('R2 delete failed:', e),
      );
    }

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
  async getTaskPublic(
    id: string,
  ): Promise<{ software: Software; hasFile: boolean } | null> {
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
      const ids =
        (await this.ctx.storage.get<string[]>(`accounts:${hash}`)) ?? [];
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
  ): Promise<{ deletedByAge: number; deletedBySize: number; totalSizeMB: number }> {
    let deletedByAge = 0;
    let deletedBySize = 0;

    // Collect all tasks
    const allTasks: Array<{ id: string; task: DownloadTask; r2key: string | null }> = [];
    const storageMap = await this.ctx.storage.list<string>({ prefix: 'task:' });
    for (const [key, raw] of storageMap) {
      const id = key.slice('task:'.length);
      let task: DownloadTask;
      try {
        task = JSON.parse(raw) as DownloadTask;
      } catch {
        continue;
      }
      const r2key = (await this.ctx.storage.get<string>(`r2key:${id}`)) ?? null;
      allTasks.push({ id, task, r2key });
    }

    // Phase 1: delete tasks older than N days
    if (days > 0) {
      const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
      for (const entry of [...allTasks]) {
        const createdAt = new Date(entry.task.createdAt).getTime();
        if (createdAt < cutoff) {
          await this.purgeTask(entry.id, entry.task.accountHash, entry.r2key);
          allTasks.splice(allTasks.indexOf(entry), 1);
          deletedByAge++;
        }
      }
    }

    // Phase 2: enforce total size limit
    // List R2 to get actual sizes
    const maxBytes = maxMB * 1024 * 1024;
    let totalSize = 0;

    if (maxMB > 0) {
      const sizeMap = new Map<string, number>();
      let cursor: string | undefined;
      do {
        const listed = await this.env.IPA_BUCKET.list({ cursor, limit: 500 });
        for (const obj of listed.objects) {
          sizeMap.set(obj.key, obj.size);
          totalSize += obj.size;
        }
        cursor = listed.truncated ? listed.cursor : undefined;
      } while (cursor);

      if (totalSize > maxBytes) {
        // Sort remaining tasks by createdAt ascending (oldest first)
        allTasks.sort(
          (a, b) =>
            new Date(a.task.createdAt).getTime() -
            new Date(b.task.createdAt).getTime(),
        );

        for (const entry of allTasks) {
          if (totalSize <= maxBytes) break;
          const size = entry.r2key ? (sizeMap.get(entry.r2key) ?? 0) : 0;
          await this.purgeTask(entry.id, entry.task.accountHash, entry.r2key);
          totalSize -= size;
          deletedBySize++;
        }
      }
    }

    return {
      deletedByAge,
      deletedBySize,
      totalSizeMB: Math.round((totalSize / (1024 * 1024)) * 100) / 100,
    };
  }

  /** Delete a task completely: R2 object + DO storage + account index */
  private async purgeTask(
    id: string,
    accountHash: string,
    r2key: string | null,
  ): Promise<void> {
    this.abortControllers.get(id)?.abort();
    this.abortControllers.delete(id);
    if (r2key) {
      await this.env.IPA_BUCKET.delete(r2key).catch((e) =>
        console.error('R2 cleanup delete failed:', e),
      );
    }
    await this.ctx.storage.delete(`task:${id}`);
    await this.ctx.storage.delete(`r2key:${id}`);
    await this.removeFromAccountIndex(accountHash, id);
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

      const response = await fetch(task.downloadURL, {
        signal: controller.signal,
        redirect: 'follow',
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      if (!response.body) throw new Error('No response body');

      const contentLength = parseInt(
        response.headers.get('content-length') ?? '0',
      );
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
    let partBuf = new Uint8Array(0);
    let downloaded = 0;
    let lastTime = Date.now();
    let lastBytes = 0;

    const reader = body.getReader();

    // Upload exactly MIN_MULTIPART_PART_SIZE bytes per part (except the last).
    // R2 requires all non-trailing parts to be the same size.
    const flushFull = async () => {
      while (partBuf.length >= MIN_MULTIPART_PART_SIZE) {
        const slice = partBuf.slice(0, MIN_MULTIPART_PART_SIZE);
        const part = await upload.uploadPart(partNum++, slice);
        parts.push(part);
        partBuf = partBuf.slice(MIN_MULTIPART_PART_SIZE);
      }
    };

    const flushFinal = async () => {
      if (partBuf.length === 0) return;
      const part = await upload.uploadPart(partNum++, partBuf);
      parts.push(part);
      partBuf = new Uint8Array(0);
    };

    try {
      while (true) {
        if (signal.aborted) throw new DOMException('Aborted', 'AbortError');

        const { done, value } = await reader.read();
        if (done) break;

        downloaded += value.length;
        if (downloaded > MAX_DOWNLOAD_SIZE) throw new Error('File too large');

        partBuf = concat(partBuf, value);

        if (partBuf.length >= MIN_MULTIPART_PART_SIZE) {
          await flushFull();
        }

        // Progress + speed every 500ms
        const now = Date.now();
        if (now - lastTime >= 2000) {
          const bps = ((downloaded - lastBytes) / (now - lastTime)) * 1000;
          task.speed = formatSpeed(bps);
          task.progress =
            contentLength > 0
              ? Math.round((downloaded / contentLength) * 100)
              : 0;
          lastTime = now;
          lastBytes = downloaded;
          await this.saveTask(task);
        }
      }

      await flushFull(); // upload any remaining full parts
      await flushFinal(); // upload final partial part
      await upload.complete(parts);
    } catch (err) {
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

    // Parse ZIP metadata to determine sinfPaths
    const filesToAppend = await this.buildFilesToAppend(task, archiveSize, readRange);
    if (filesToAppend.length === 0) return;

    // Compute only the tail (no full-archive read)
    const { cdOffset, tail } = await appendToZipTail(archiveSize, readRange, filesToAppend);

    // Compose new IPA via R2 multipart upload:
    //   Parts 1..N: chunks of original IPA [0, cdOffset) in 500MB slices
    //   Last part:  tail (new local entries + new CD + new EOCD, few KB)
    const COPY_CHUNK = 50 * 1024 * 1024; // 50 MB per part (DO usable memory ~70-90 MB)
    const newKey = r2key + '.new';
    const upload = await this.env.IPA_BUCKET.createMultipartUpload(newKey);
    try {
      const parts: R2UploadedPart[] = [];
      let partNum = 1;

      // Upload original data in chunks (read from R2 in slices)
      for (let offset = 0; offset < cdOffset; offset += COPY_CHUNK) {
        const length = Math.min(COPY_CHUNK, cdOffset - offset);
        const chunk = await readRange(offset, offset + length);
        const part = await upload.uploadPart(partNum++, chunk);
        parts.push(part);
      }

      // Upload tail (few KB)
      const tailPart = await upload.uploadPart(partNum, tail);
      parts.push(tailPart);

      await upload.complete(parts);
    } catch (err) {
      await upload.abort().catch((e) => console.error('SINF multipart abort failed:', e));
      throw err;
    }

    // Swap: overwrite original with new, then delete temp key
    // (put before delete ensures original is preserved if put fails)
    const newObj = await this.env.IPA_BUCKET.get(newKey);
    if (!newObj) throw new Error('R2 rename step failed: new object missing');
    await this.env.IPA_BUCKET.put(r2key, newObj.body);
    await this.env.IPA_BUCKET.delete(newKey);
  }

  private async buildFilesToAppend(
    task: DownloadTask,
    archiveSize: number,
    readRange: (start: number, end: number) => Promise<Uint8Array>,
  ): Promise<Array<{ name: string; data: Uint8Array }>> {
    const files: Array<{ name: string; data: Uint8Array }> = [];

    // Parse Central Directory to find Manifest.plist and Info.plist
    const tailSize = Math.min(65536 + 22, archiveSize);
    const tail = await readRange(archiveSize - tailSize, archiveSize);
    const eocd = findEocd(tail, archiveSize);
    const cd = await readRange(eocd.cdOffset, eocd.cdOffset + eocd.cdSize);
    const entries = parseCentralDirectory(cd);

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
        (e) =>
          e.name === `Payload/${bundleName}.app/Info.plist` &&
          !e.name.includes('/Watch/'),
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
        metaBuffer = Buffer.from(
          bplistCreator(parsed as Record<string, unknown>),
        );
      } catch {
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

  private async addToAccountIndex(
    accountHash: string,
    taskId: string,
  ): Promise<void> {
    const existing =
      (await this.ctx.storage.get<string[]>(`accounts:${accountHash}`)) ?? [];
    if (!existing.includes(taskId)) {
      await this.ctx.storage.put(`accounts:${accountHash}`, [
        ...existing,
        taskId,
      ]);
    }
  }

  private async removeFromAccountIndex(
    accountHash: string,
    taskId: string,
  ): Promise<void> {
    const existing =
      (await this.ctx.storage.get<string[]>(`accounts:${accountHash}`)) ?? [];
    await this.ctx.storage.put(
      `accounts:${accountHash}`,
      existing.filter((id) => id !== taskId),
    );
  }
}
