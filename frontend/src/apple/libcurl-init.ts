import { libcurl } from 'libcurl.js/bundled';

let initialized = false;
let initPromise: Promise<void> | null = null;

export async function initLibcurl(): Promise<void> {
  if (initialized) return;
  if (initPromise) return initPromise;

  initPromise = (async () => {
    const wsProto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    libcurl.set_websocket(`${wsProto}//${location.host}/wisp/`);
    await libcurl.load_wasm();
    initialized = true;
  })();

  return initPromise;
}

export { libcurl };
