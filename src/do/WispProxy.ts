import { DurableObject } from 'cloudflare:workers';
import { connect } from 'cloudflare:sockets';

// Wisp frame types
const TYPE_CONNECT = 0x01;
const TYPE_DATA = 0x02;
const TYPE_CONTINUE = 0x03;
const TYPE_CLOSE = 0x04;

// Close reason codes
const CLOSE_REASON_VOLUNTARY = 0x01;
const CLOSE_REASON_NETWORK_ERROR = 0x02;
const CLOSE_REASON_INVALID_INFO = 0x41;

// Apple host allowlist (matches wsProxy.ts)
const ALLOWED_HOSTS: RegExp[] = [
  /^auth\.itunes\.apple\.com$/,
  /^buy\.itunes\.apple\.com$/,
  /^init\.itunes\.apple\.com$/,
  /^p\d+-buy\.itunes\.apple\.com$/,
];

interface Stream {
  writer: WritableStreamDefaultWriter<Uint8Array>;
}

/**
 * WispProxy — Durable Object implementing a Wisp server.
 *
 * Each WebSocket session gets a fresh DO instance (newUniqueId()),
 * keeping connection state isolated per user session.
 *
 * Wisp protocol reference: https://github.com/MercuryWorkshop/wisp-protocol
 * Frame format (little-endian):
 *   [type: u8][streamId: u32][payload: *]
 *
 * CONNECT payload: [port: u16][streamType: u8][hostname: *]
 * CONTINUE payload: [bufferRemaining: u32]
 * CLOSE payload: [reason: u8]
 */
export class WispProxy extends DurableObject<Env> {
  private streams = new Map<number, Stream>();
  private ws: WebSocket | null = null;

  async fetch(request: Request): Promise<Response> {
    const upgrade = request.headers.get('Upgrade');
    if (upgrade?.toLowerCase() !== 'websocket') {
      return new Response('Expected WebSocket upgrade', { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair) as [WebSocket, WebSocket];

    this.ws = server;
    this.ctx.acceptWebSocket(server);

    // Send initial CONTINUE for stream 0 (connection-level flow control)
    this.sendContinue(0, 128 * 1024);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  async webSocketMessage(_ws: WebSocket, message: ArrayBuffer | string): Promise<void> {
    if (typeof message === 'string') return;

    const buf = new Uint8Array(message);
    if (buf.length < 5) return; // minimum frame size

    const type = buf[0];
    const dv = new DataView(buf.buffer, buf.byteOffset);
    const streamId = dv.getUint32(1, true); // little-endian
    const payload = buf.subarray(5);

    switch (type) {
      case TYPE_CONNECT:
        await this.handleConnect(streamId, payload);
        break;
      case TYPE_DATA:
        await this.handleData(streamId, payload);
        break;
      case TYPE_CLOSE:
        this.handleClose(streamId);
        break;
    }
  }

  async webSocketClose(_ws: WebSocket, _code: number): Promise<void> {
    await this.closeAllStreams();
  }

  async webSocketError(_ws: WebSocket, _error: unknown): Promise<void> {
    await this.closeAllStreams();
  }

  // ---------------------------------------------------------------------------
  // CONNECT handler
  // ---------------------------------------------------------------------------

  private async handleConnect(streamId: number, payload: Uint8Array): Promise<void> {
    if (payload.length < 4) {
      this.sendClose(streamId, CLOSE_REASON_INVALID_INFO);
      return;
    }

    const streamType = payload[0]; // 1=TCP, 2=UDP
    const dv = new DataView(payload.buffer, payload.byteOffset);
    const port = dv.getUint16(1, true);
    const hostname = new TextDecoder().decode(payload.subarray(3));

    // Only TCP (type=1) to port 443, allowlisted hosts
    if (streamType !== 1 || port !== 443 || !ALLOWED_HOSTS.some((r) => r.test(hostname))) {
      this.sendClose(streamId, CLOSE_REASON_INVALID_INFO);
      return;
    }

    try {
      const socket = connect({ hostname, port }, { secureTransport: 'off', allowHalfOpen: false });

      const writer = socket.writable.getWriter();
      this.streams.set(streamId, { writer });

      // Acknowledge CONNECT with a CONTINUE frame
      this.sendContinue(streamId, 128 * 1024);

      // Pipe TCP → WebSocket in background
      this.pipeToWs(streamId, socket.readable).catch(() => {
        this.sendClose(streamId, CLOSE_REASON_NETWORK_ERROR);
        this.streams.delete(streamId);
      });
    } catch (e) {
      console.error(`Wisp CONNECT failed for stream ${streamId} to ${hostname}:${port}:`, e);
      this.sendClose(streamId, CLOSE_REASON_NETWORK_ERROR);
    }
  }

  private async pipeToWs(streamId: number, readable: ReadableStream<Uint8Array>): Promise<void> {
    const reader = readable.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        this.sendData(streamId, value);
      }
    } finally {
      reader.releaseLock();
      this.sendClose(streamId, CLOSE_REASON_VOLUNTARY);
      this.streams.delete(streamId);
    }
  }

  // ---------------------------------------------------------------------------
  // DATA handler
  // ---------------------------------------------------------------------------

  private async handleData(streamId: number, payload: Uint8Array): Promise<void> {
    const stream = this.streams.get(streamId);
    if (!stream) return;
    try {
      await stream.writer.write(payload);
    } catch {
      this.sendClose(streamId, CLOSE_REASON_NETWORK_ERROR);
      this.streams.delete(streamId);
    }
  }

  // ---------------------------------------------------------------------------
  // CLOSE handler
  // ---------------------------------------------------------------------------

  private handleClose(streamId: number): void {
    const stream = this.streams.get(streamId);
    if (!stream) return;
    stream.writer.close().catch(() => {});
    this.streams.delete(streamId);
  }

  private async closeAllStreams(): Promise<void> {
    for (const [, stream] of this.streams) {
      stream.writer.close().catch(() => {});
    }
    this.streams.clear();
  }

  // ---------------------------------------------------------------------------
  // Frame builders
  // ---------------------------------------------------------------------------

  private sendData(streamId: number, data: Uint8Array): void {
    if (!this.ws) return;
    const frame = new Uint8Array(5 + data.length);
    frame[0] = TYPE_DATA;
    new DataView(frame.buffer).setUint32(1, streamId, true);
    frame.set(data, 5);
    this.ws.send(frame);
  }

  private sendContinue(streamId: number, bufferRemaining: number): void {
    if (!this.ws) return;
    const frame = new Uint8Array(9);
    const dv = new DataView(frame.buffer);
    frame[0] = TYPE_CONTINUE;
    dv.setUint32(1, streamId, true);
    dv.setUint32(5, bufferRemaining, true);
    this.ws.send(frame);
  }

  private sendClose(streamId: number, reason: number): void {
    if (!this.ws) return;
    const frame = new Uint8Array(6);
    frame[0] = TYPE_CLOSE;
    new DataView(frame.buffer).setUint32(1, streamId, true);
    frame[5] = reason;
    this.ws.send(frame);
  }
}
