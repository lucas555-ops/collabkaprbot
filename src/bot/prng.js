import crypto from 'crypto';

export function sha256Hex(s) {
  return crypto.createHash('sha256').update(String(s)).digest('hex');
}

export function makeSeed({ giveawayId, endsAtIso, eligibleUserIds }) {
  const eligibleCsv = eligibleUserIds.join(',');
  const eligibleHash = sha256Hex(eligibleCsv);
  const seed = sha256Hex(`gw:${giveawayId}|ends:${endsAtIso}|eligible:${eligibleHash}`);
  const seedHash = sha256Hex(seed);
  return { seed, seedHash, eligibleHash };
}

export function makeXorShift32(seedHex) {
  const b = Buffer.from(seedHex, 'hex');
  // 32-bit non-zero
  let x = b.readUInt32BE(0) || 2463534242;
  return function next() {
    // xorshift32
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    // convert to [0,1)
    const u = (x >>> 0);
    return u / 4294967296;
  };
}

export function sampleWithoutReplacement(items, k, rand01) {
  const arr = items.slice();
  const res = [];
  for (let i = 0; i < k && arr.length; i++) {
    const idx = Math.floor(rand01() * arr.length);
    res.push(arr[idx]);
    arr.splice(idx, 1);
  }
  return res;
}
