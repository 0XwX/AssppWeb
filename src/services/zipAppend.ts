/**
 * zipAppend.ts — Pure-buffer ZIP Central Directory parser + file appender.
 *
 * Designed for Cloudflare Workers: all operations work on Uint8Array / Buffer
 * with no filesystem access. In production, the "range read" maps to an R2
 * Range GET; here we expose it as a plain function parameter so tests can pass
 * a Buffer slice instead.
 *
 * ZIP format reference (APPNOTE.TXT):
 *   Local file header  @ offset localHeaderOffset
 *   [file data]
 *   Central directory  @ cdOffset, cdSize bytes
 *   End of central directory record (EOCD)  @ eocdOffset, 22 bytes (no comment)
 *
 * Strategy for appending files without touching the existing data:
 *   1. Range-read the last ~65KB of the archive to locate the EOCD record.
 *   2. Parse EOCD → cdOffset, cdSize, entryCount.
 *   3. Range-read Central Directory → find entries we need (Manifest.plist, Info.plist).
 *   4. For each target entry, Range-read the local file header + data.
 *   5. Build new local headers + data for the files to append.
 *   6. Concatenate: [original archive bytes][new local entries][updated CD][new EOCD].
 *
 * The result is a valid ZIP that any unzipper will accept.
 */

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SIG_LOCAL = 0x04034b50; // Local file header signature
const SIG_CD = 0x02014b50; // Central directory file header signature
const SIG_EOCD = 0x06054b50; // End of central directory record signature
const _SIG_ZIP64_EOCD = 0x06064b50; // ZIP64 end of central directory record
const _SIG_ZIP64_LOCATOR = 0x07064b50; // ZIP64 end of central directory locator

// Compression methods
export const METHOD_STORED = 0; // No compression

// ---------------------------------------------------------------------------
// Low-level readers (little-endian, bounds-checked)
// ---------------------------------------------------------------------------

function u16(buf: Uint8Array, off: number): number {
  if (off + 2 > buf.length) throw new RangeError(`u16 OOB at ${off}`);
  return buf[off]! | (buf[off + 1]! << 8);
}

function u32(buf: Uint8Array, off: number): number {
  if (off + 4 > buf.length) throw new RangeError(`u32 OOB at ${off}`);
  return (buf[off]! | (buf[off + 1]! << 8) | (buf[off + 2]! << 16) | (buf[off + 3]! << 24)) >>> 0;
}

function w16(buf: Uint8Array, off: number, v: number): void {
  buf[off] = v & 0xff;
  buf[off + 1] = (v >>> 8) & 0xff;
}

function w32(buf: Uint8Array, off: number, v: number): void {
  buf[off] = v & 0xff;
  buf[off + 1] = (v >>> 8) & 0xff;
  buf[off + 2] = (v >>> 16) & 0xff;
  buf[off + 3] = (v >>> 24) & 0xff;
}

function concat(...arrays: Uint8Array[]): Uint8Array {
  const total = arrays.reduce((n, a) => n + a.length, 0);
  const out = new Uint8Array(total);
  let pos = 0;
  for (const a of arrays) {
    out.set(a, pos);
    pos += a.length;
  }
  return out;
}

// ---------------------------------------------------------------------------
// CRC-32 (needed for new local headers)
// ---------------------------------------------------------------------------

const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[i] = c;
  }
  return t;
})();

export function crc32(data: Uint8Array): number {
  let crc = 0xffffffff;
  for (let i = 0; i < data.length; i++) {
    crc = CRC_TABLE[(crc ^ data[i]!) & 0xff]! ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

// ---------------------------------------------------------------------------
// EOCD parsing
// ---------------------------------------------------------------------------

export interface Eocd {
  /** Offset of EOCD record within the buffer slice passed in */
  offset: number;
  /** Number of central directory entries */
  entryCount: number;
  /** Size of central directory in bytes */
  cdSize: number;
  /** Offset of central directory from start of archive */
  cdOffset: number;
}

/**
 * Locate and parse the EOCD record from the tail of the archive.
 * @param tail  Last N bytes of the archive (N ≥ 22, typically 65KB).
 * @param archiveSize  Total size of the archive (used to convert relative offsets).
 */
export function findEocd(tail: Uint8Array, archiveSize: number): Eocd {
  // Scan backwards for the EOCD signature (0x06054b50)
  // EOCD is at least 22 bytes; ZIP comment can be up to 65535 bytes after it.
  const maxSearch = Math.min(tail.length, 65535 + 22);
  for (let i = tail.length - 22; i >= tail.length - maxSearch; i--) {
    if (i < 0) break;
    if (u32(tail, i) === SIG_EOCD) {
      const diskEntries = u16(tail, i + 8);
      const totalEntries = u16(tail, i + 10);
      if (diskEntries !== totalEntries) {
        // Multi-disk archive — not supported
        continue;
      }
      const cdSize = u32(tail, i + 12);
      const cdOffsetRaw = u32(tail, i + 16);

      // Check for ZIP64 (fields set to 0xFFFF / 0xFFFFFFFF)
      if (cdOffsetRaw === 0xffffffff || totalEntries === 0xffff) {
        throw new Error('ZIP64 archives are not supported by zipAppend');
      }

      return {
        offset: i + (archiveSize - tail.length),
        entryCount: totalEntries,
        cdSize,
        cdOffset: cdOffsetRaw,
      };
    }
  }
  throw new Error('EOCD signature not found — not a valid ZIP file');
}

// ---------------------------------------------------------------------------
// Central Directory entry
// ---------------------------------------------------------------------------

export interface CdEntry {
  /** File name (UTF-8) */
  name: string;
  /** Compression method (0 = stored) */
  method: number;
  /** CRC-32 of uncompressed data */
  crc32: number;
  /** Compressed size */
  compressedSize: number;
  /** Uncompressed size */
  uncompressedSize: number;
  /** Offset of local file header from start of archive */
  localOffset: number;
  /** Byte offset of this CD entry within the CD buffer */
  cdEntryOffset: number;
  /** Total byte length of this CD entry (including variable fields) */
  cdEntryLength: number;
  /** Extra field bytes */
  extra: Uint8Array;
  /** File comment bytes */
  comment: Uint8Array;
  /** Raw CD entry bytes (for copying into new CD) */
  raw: Uint8Array;
}

/**
 * Parse all entries from the Central Directory block.
 * @param cd  The central directory bytes (exactly cdSize bytes).
 */
export function parseCentralDirectory(cd: Uint8Array): CdEntry[] {
  const entries: CdEntry[] = [];
  let pos = 0;

  while (pos < cd.length) {
    if (pos + 4 > cd.length) break;
    const sig = u32(cd, pos);
    if (sig !== SIG_CD) break;

    if (pos + 46 > cd.length) throw new RangeError('Truncated CD entry');

    const method = u16(cd, pos + 10);
    const crc = u32(cd, pos + 16);
    const compSize = u32(cd, pos + 20);
    const uncompSize = u32(cd, pos + 24);
    const nameLen = u16(cd, pos + 28);
    const extraLen = u16(cd, pos + 30);
    const commentLen = u16(cd, pos + 32);
    const localOffset = u32(cd, pos + 42);

    const entryEnd = pos + 46 + nameLen + extraLen + commentLen;
    if (entryEnd > cd.length) throw new RangeError('Truncated CD entry fields');

    const nameBytes = cd.subarray(pos + 46, pos + 46 + nameLen);
    const name = new TextDecoder().decode(nameBytes);
    const extra = cd.subarray(pos + 46 + nameLen, pos + 46 + nameLen + extraLen);
    const comment = cd.subarray(pos + 46 + nameLen + extraLen, entryEnd);

    entries.push({
      name,
      method,
      crc32: crc,
      compressedSize: compSize,
      uncompressedSize: uncompSize,
      localOffset,
      cdEntryOffset: pos,
      cdEntryLength: entryEnd - pos,
      extra,
      comment,
      raw: cd.subarray(pos, entryEnd),
    });

    pos = entryEnd;
  }

  return entries;
}

// ---------------------------------------------------------------------------
// Local file header reading
// ---------------------------------------------------------------------------

export interface LocalEntry {
  /** Data offset (start of compressed/stored data) */
  dataOffset: number;
  /** Compressed size from local header (may differ from CD for streaming zips) */
  compressedSize: number;
}

/**
 * Parse a local file header to find the actual data start.
 * @param slice  Bytes starting at the local header offset.
 */
export function parseLocalHeader(slice: Uint8Array): LocalEntry {
  if (u32(slice, 0) !== SIG_LOCAL) {
    throw new Error(`Expected local file header signature, got 0x${u32(slice, 0).toString(16)}`);
  }
  const compSize = u32(slice, 18);
  const nameLen = u16(slice, 26);
  const extraLen = u16(slice, 28);
  const dataOffset = 30 + nameLen + extraLen;
  return { dataOffset, compressedSize: compSize };
}

// ---------------------------------------------------------------------------
// Building new local file headers
// ---------------------------------------------------------------------------

/**
 * Build a local file header + data block for a new entry (stored, no compression).
 */
export function buildLocalEntry(name: string, data: Uint8Array): Uint8Array {
  const nameBytes = new TextEncoder().encode(name);
  const crc = crc32(data);
  const size = data.length;

  const header = new Uint8Array(30 + nameBytes.length);
  w32(header, 0, SIG_LOCAL);
  w16(header, 4, 20); // version needed: 2.0
  w16(header, 6, 0); // general purpose flags
  w16(header, 8, METHOD_STORED);
  w16(header, 10, 0); // last mod time
  w16(header, 12, 0); // last mod date
  w32(header, 14, crc);
  w32(header, 18, size); // compressed size
  w32(header, 22, size); // uncompressed size
  w16(header, 26, nameBytes.length);
  w16(header, 28, 0); // extra field length
  header.set(nameBytes, 30);

  return concat(header, data);
}

/**
 * Build a Central Directory entry for a new file.
 */
export function buildCdEntry(name: string, data: Uint8Array, localOffset: number): Uint8Array {
  const nameBytes = new TextEncoder().encode(name);
  const crc = crc32(data);
  const size = data.length;

  const entry = new Uint8Array(46 + nameBytes.length);
  w32(entry, 0, SIG_CD);
  w16(entry, 4, 20); // version made by
  w16(entry, 6, 20); // version needed
  w16(entry, 8, 0); // general purpose flags
  w16(entry, 10, METHOD_STORED);
  w16(entry, 12, 0); // last mod time
  w16(entry, 14, 0); // last mod date
  w32(entry, 16, crc);
  w32(entry, 20, size); // compressed size
  w32(entry, 24, size); // uncompressed size
  w16(entry, 28, nameBytes.length);
  w16(entry, 30, 0); // extra field length
  w16(entry, 32, 0); // comment length
  w16(entry, 34, 0); // disk number start
  w16(entry, 36, 0); // internal attributes
  w32(entry, 38, 0); // external attributes
  w32(entry, 42, localOffset);
  entry.set(nameBytes, 46);
  return entry;
}

// ---------------------------------------------------------------------------
// Main public API
// ---------------------------------------------------------------------------

export interface AppendFile {
  /** Path inside the ZIP (e.g. "Payload/App.app/SC_Info/App.sinf") */
  name: string;
  /** File data */
  data: Uint8Array;
}

export interface ZipReadRange {
  /** Read [start, end) bytes from the archive */
  (start: number, end: number): Promise<Uint8Array>;
}

/**
 * Append files to a ZIP archive without reading the entire archive into memory.
 *
 * In Cloudflare Workers, implement `readRange` as an R2 Range GET:
 *   async (start, end) => {
 *     const obj = await bucket.get(key, { range: { offset: start, length: end - start } });
 *     return new Uint8Array(await obj.arrayBuffer());
 *   }
 *
 * In tests, implement as:
 *   async (start, end) => buf.subarray(start, end)
 *
 * @param archiveSize  Total byte size of the existing archive.
 * @param readRange    Function to read a byte range from the archive.
 * @param files        Files to append.
 * @param readPrefix   Function to read archive bytes from the start (for copying
 *                     the original data into the output). Pass the full buffer in
 *                     tests; in Workers, pipe from R2 directly.
 * @returns            Complete new archive as Uint8Array.
 */
export async function appendToZip(
  archiveSize: number,
  readRange: ZipReadRange,
  files: AppendFile[],
  readPrefix: (end: number) => Promise<Uint8Array>,
): Promise<Uint8Array> {
  if (files.length === 0) {
    return readPrefix(archiveSize);
  }

  // 1. Read tail to find EOCD (max 65KB + 22 bytes)
  const tailSize = Math.min(65536 + 22, archiveSize);
  const tail = await readRange(archiveSize - tailSize, archiveSize);
  const eocd = findEocd(tail, archiveSize);

  // 2. Read Central Directory
  const cd = await readRange(eocd.cdOffset, eocd.cdOffset + eocd.cdSize);
  const existingEntries = parseCentralDirectory(cd);

  // 3. Read original archive up to (but not including) the CD
  const originalData = await readPrefix(eocd.cdOffset);

  // 4. Build new local entries
  const newLocalBlocks: Uint8Array[] = [];
  let appendOffset = eocd.cdOffset; // new data starts where old CD was
  const newCdEntries: { name: string; data: Uint8Array; offset: number }[] = [];

  for (const file of files) {
    const localBlock = buildLocalEntry(file.name, file.data);
    newCdEntries.push({ name: file.name, data: file.data, offset: appendOffset });
    appendOffset += localBlock.length;
    newLocalBlocks.push(localBlock);
  }

  // 5. Build new Central Directory:
  //    existing entries (raw, unchanged) + new entries
  const existingCdRaw = existingEntries.map((e) => e.raw);

  const newCdBlocks = newCdEntries.map(({ name, data, offset }) =>
    buildCdEntry(name, data, offset),
  );

  const newCdSize =
    existingCdRaw.reduce((n, b) => n + b.length, 0) + newCdBlocks.reduce((n, b) => n + b.length, 0);
  const newCdOffset = appendOffset;
  const newTotalEntries = existingEntries.length + files.length;

  // 6. Build new EOCD
  const newEocd = new Uint8Array(22);
  w32(newEocd, 0, SIG_EOCD);
  w16(newEocd, 4, 0); // disk number
  w16(newEocd, 6, 0); // disk with CD start
  w16(newEocd, 8, newTotalEntries);
  w16(newEocd, 10, newTotalEntries);
  w32(newEocd, 12, newCdSize);
  w32(newEocd, 16, newCdOffset);
  w16(newEocd, 20, 0); // comment length

  // 7. Concatenate everything
  return concat(originalData, ...newLocalBlocks, ...existingCdRaw, ...newCdBlocks, newEocd);
}

// ---------------------------------------------------------------------------
// Helper: read a specific file from a ZIP given its CD entry
// ---------------------------------------------------------------------------

/**
 * Extract file data for a given CD entry via range reads.
 * The local header is read first to find the actual data offset.
 */
export async function readEntryData(entry: CdEntry, readRange: ZipReadRange): Promise<Uint8Array> {
  // Local header is 30 bytes + name + extra; read enough to parse
  const headerSlice = await readRange(
    entry.localOffset,
    entry.localOffset + 30 + 512, // 512 is ample for name+extra
  );
  const local = parseLocalHeader(headerSlice);
  const dataStart = entry.localOffset + local.dataOffset;
  const raw = await readRange(dataStart, dataStart + entry.compressedSize);

  // method 0 = stored (no compression)
  if (entry.method === 0) return raw;

  // method 8 = deflate — decompress via DecompressionStream
  if (entry.method === 8) {
    const ds = new DecompressionStream('deflate-raw');
    const writer = ds.writable.getWriter();
    const reader = ds.readable.getReader();

    writer
      .write(raw)
      .then(() => writer.close())
      .catch(() => writer.abort());

    const chunks: Uint8Array[] = [];
    let total = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
      total += value.length;
    }

    const out = new Uint8Array(total);
    let pos = 0;
    for (const chunk of chunks) {
      out.set(chunk, pos);
      pos += chunk.length;
    }
    return out;
  }

  throw new Error(`Unsupported compression method ${entry.method} for "${entry.name}"`);
}

// ---------------------------------------------------------------------------
// R2-optimised variant: only return the bytes to append, not the full archive
// ---------------------------------------------------------------------------

/**
 * Like appendToZip, but does NOT read the original archive data.
 * Returns only the bytes that need to be appended after the original data.
 *
 * Use this in Cloudflare Workers with R2 multipart copy:
 *
 *   const { cdOffset, tail } = await appendToZipTail(size, readRange, files);
 *   const upload = await env.IPA_BUCKET.createMultipartUpload(newKey);
 *   const copyPart = await upload.uploadPartCopy(1, {
 *     sourceKey: originalKey, range: { offset: 0, length: cdOffset }
 *   });
 *   const tailPart = await upload.uploadPart(2, tail);
 *   await upload.complete([copyPart, tailPart]);
 *
 * @returns cdOffset  — copy [0, cdOffset) from original via R2 CopyPart
 * @returns tail      — upload this as the final part
 */
export async function appendToZipTail(
  archiveSize: number,
  readRange: ZipReadRange,
  files: AppendFile[],
  precomputed?: { eocd: Eocd; entries: CdEntry[] },
): Promise<{ cdOffset: number; tail: Uint8Array }> {
  if (files.length === 0) {
    return { cdOffset: archiveSize, tail: new Uint8Array(0) };
  }

  // 1. Locate EOCD and parse Central Directory (reuse if precomputed)
  let eocd: Eocd;
  let existingEntries: CdEntry[];
  if (precomputed) {
    eocd = precomputed.eocd;
    existingEntries = precomputed.entries;
  } else {
    const tailSize = Math.min(65536 + 22, archiveSize);
    const tailBuf = await readRange(archiveSize - tailSize, archiveSize);
    eocd = findEocd(tailBuf, archiveSize);

    const cd = await readRange(eocd.cdOffset, eocd.cdOffset + eocd.cdSize);
    existingEntries = parseCentralDirectory(cd);
  }

  // 2. Build new local entries and CD entries
  let appendOffset = eocd.cdOffset;
  const newLocalBlocks: Uint8Array[] = [];
  const newCdEntries: { name: string; data: Uint8Array; offset: number }[] = [];

  for (const file of files) {
    const localBlock = buildLocalEntry(file.name, file.data);
    newCdEntries.push({ name: file.name, data: file.data, offset: appendOffset });
    appendOffset += localBlock.length;
    newLocalBlocks.push(localBlock);
  }

  // 3. Build new Central Directory
  const existingCdRaw = existingEntries.map((e) => e.raw);
  const newCdBlocks = newCdEntries.map(({ name, data, offset }) =>
    buildCdEntry(name, data, offset),
  );

  const newCdSize =
    existingCdRaw.reduce((n, b) => n + b.length, 0) + newCdBlocks.reduce((n, b) => n + b.length, 0);
  const newCdOffset = appendOffset;
  const newTotalEntries = existingEntries.length + files.length;

  // 4. Build new EOCD
  const newEocd = new Uint8Array(22);
  w32(newEocd, 0, SIG_EOCD);
  w16(newEocd, 4, 0);
  w16(newEocd, 6, 0);
  w16(newEocd, 8, newTotalEntries);
  w16(newEocd, 10, newTotalEntries);
  w32(newEocd, 12, newCdSize);
  w32(newEocd, 16, newCdOffset);
  w16(newEocd, 20, 0);

  const tail = concat(...newLocalBlocks, ...existingCdRaw, ...newCdBlocks, newEocd);

  return { cdOffset: eocd.cdOffset, tail };
}
