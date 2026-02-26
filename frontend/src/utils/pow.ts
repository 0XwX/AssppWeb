function hasLeadingZeroBits(hash: Uint8Array, bits: number): boolean {
  let remaining = bits;
  for (let i = 0; i < hash.length && remaining > 0; i++) {
    if (remaining >= 8) {
      if (hash[i] !== 0) return false;
      remaining -= 8;
    } else {
      const mask = 0xff << (8 - remaining);
      if ((hash[i]! & mask) !== 0) return false;
      remaining = 0;
    }
  }
  return true;
}

/**
 * Solve a PoW challenge by finding a nonce where
 * SHA-256(challenge + nonce) has `difficulty` leading zero bits.
 */
export async function solveChallenge(challenge: string, difficulty: number): Promise<string> {
  let nonce = 0;
  const encoder = new TextEncoder();
  while (true) {
    const data = encoder.encode(challenge + nonce);
    const hash = new Uint8Array(await crypto.subtle.digest('SHA-256', data));
    if (hasLeadingZeroBits(hash, difficulty)) return String(nonce);
    nonce++;
  }
}

/** Fetch a challenge from the server and solve it. */
export async function fetchAndSolveChallenge(): Promise<{
  challenge: string;
  nonce: string;
}> {
  const res = await fetch('/api/auth/challenge');
  if (!res.ok) throw new Error('Failed to get challenge');
  const { challenge, difficulty } = (await res.json()) as {
    challenge: string;
    difficulty: number;
  };
  const nonce = await solveChallenge(challenge, difficulty);
  return { challenge, nonce };
}
