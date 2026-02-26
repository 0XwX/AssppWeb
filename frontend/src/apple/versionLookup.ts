import type { Account, Software, VersionMetadata } from '../types';
import { appleRequest } from './request';
import type { PlistDict } from './plist';
import { buildPlist, parsePlist } from './plist';
import { extractAndMergeCookies } from './cookies';
import { storeAPIHost } from './config';

export async function getVersionMetadata(
  account: Account,
  app: Software,
  versionId: string,
): Promise<{
  metadata: VersionMetadata;
  updatedCookies: typeof account.cookies;
}> {
  const deviceId = account.deviceIdentifier;

  let requestHost = storeAPIHost(account.pod);
  let requestPath = `/WebObjects/MZFinance.woa/wa/volumeStoreDownloadProduct?guid=${deviceId}`;
  let cookies = [...account.cookies];
  let redirectAttempt = 0;

  while (redirectAttempt <= 3) {
    const payload: PlistDict = {
      creditDisplay: '',
      guid: deviceId,
      salableAdamId: app.id,
      externalVersionId: versionId,
    };

    const plistBody = buildPlist(payload);

    const headers: Record<string, string> = {
      'Content-Type': 'application/x-apple-plist',
      'iCloud-DSID': account.directoryServicesIdentifier,
      'X-Dsid': account.directoryServicesIdentifier,
    };

    const response = await appleRequest({
      method: 'POST',
      host: requestHost,
      path: requestPath,
      headers,
      body: plistBody,
      cookies,
    });

    cookies = extractAndMergeCookies(response.rawHeaders, cookies);

    if (response.status === 302) {
      const location = response.headers['location'];
      if (!location) {
        throw new Error('Failed to retrieve redirect location');
      }
      const url = new URL(location);
      requestHost = url.hostname;
      requestPath = url.pathname + url.search;
      redirectAttempt++;
      continue;
    }

    const dict = parsePlist(response.body) as PlistDict;

    const songList = dict.songList as PlistDict[] | undefined;
    if (!songList || songList.length === 0) {
      throw new Error('No items in response');
    }

    const item = songList[0];
    const itemMetadata = item.metadata as PlistDict | undefined;
    if (!itemMetadata) {
      throw new Error('Missing metadata');
    }

    const bundleShortVersionString = itemMetadata.bundleShortVersionString as string;
    if (!bundleShortVersionString) {
      throw new Error('Missing bundleShortVersionString');
    }

    const assetInfo = item['asset-info'] as PlistDict | undefined;
    const rawSize = assetInfo?.['file-size'];
    const fileSize = rawSize != null ? Number(rawSize) : undefined;

    return {
      metadata: {
        displayVersion: bundleShortVersionString,
        fileSize: fileSize && !isNaN(fileSize) ? fileSize : undefined,
      },
      updatedCookies: cookies,
    };
  }

  throw new Error('Too many redirects');
}
