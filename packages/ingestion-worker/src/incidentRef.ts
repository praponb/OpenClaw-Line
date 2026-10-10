import { createHash } from "node:crypto";

// Crockford base32 (no I, L, O, U): easy to read aloud and type.
const ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
export const INCIDENT_REF_RE = /^INC-[0-9A-HJKMNP-TV-Z]{6}$/;

/**
 * Short human-facing incident reference, e.g. `INC-7F3A2K`. Derived from the
 * incident key (dag_id:run_id) so retries and replays get the same ref.
 */
export function incidentRef(incidentKey: string): string {
  const digest = createHash("sha256").update(incidentKey).digest();
  let bits = digest.readUInt32BE(0) * 2 ** 8 + digest[4]!; // 40 bits
  let out = "";
  for (let i = 0; i < 6; i++) {
    out = ALPHABET[bits % 32] + out;
    bits = Math.floor(bits / 32);
  }
  return `INC-${out}`;
}
