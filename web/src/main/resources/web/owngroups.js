// The reader's own NIP-29 group list (kind 10009), held for the session: the public tags, and
// the private half opened through the NIP-07 extension on first use of `group:`, never on page
// load. Read on the authenticated socket, so it comes back empty for a reader with no scores
// mirrored here (readiness.js explains instead). Cached only when the relay answered.

import { ownGroups, sealed, privateGroups } from "./shared/groups.js";

/**
 * Whether [nostr] (a NIP-07 provider) can open a payload of [scheme]; "unsupported" and
 * "denied" want different words.
 */
export const canDecrypt = (nostr, scheme) => {
  const api = nostr && nostr[scheme];
  return !!(api && typeof api.decrypt === "function");
};

/**
 * One reader's list at a time. [who] is the signed-in pubkey or null, [read] a REQ on the
 * authenticated socket, [nostr] the extension, and [unlocked] runs when a decrypt settles,
 * since the picker drew before it landed.
 */
export class OwnGroups {
  constructor({ who, read, nostr = () => globalThis.window && window.nostr, unlocked = () => {} }) {
    this.who = who;
    this.read = read;
    this.nostr = nostr;
    this.unlocked = unlocked;
    this.forget();
  }

  /** Every cached thing about the reader's own list, dropped when the reader changes. */
  forget() {
    this.list = null;      // the parsed candidates, from the public tags
    this.owner = null;     // whose they are, so signing out drops them
    this.lock = null;      // sealed(): the encrypted half, or null if there is none
    this.secret = null;    // the rows behind that lock, once it has been opened
    this.asking = null;    // the in-flight decrypt, so N keystrokes are one prompt
    this.denied = false;   // the extension said no; do not ask again unasked
  }

  /** The public rows of the reader's list. */
  async candidates() {
    const who = this.who();
    if (!who) { this.forget(); return []; }
    if (this.owner !== who) this.forget();
    if (this.owner === who && this.list) return this.list;
    let evs = [];
    try {
      evs = await this.read({ kinds: [10009], authors: [who], limit: 1 });
    } catch (e) { return this.list && this.owner === who ? this.list : []; }
    if (evs.complete !== true) return [];
    this.list = ownGroups(evs[0]);
    this.lock = sealed(evs[0]);
    this.owner = who;
    return this.list;
  }

  /** The private rows opened so far; empty until the reader has said yes. */
  opened() {
    return this.secret || [];
  }

  /** Whether a lookup should raise the prompt now: a lock, an extension that can open it, and no refusal. */
  shouldAsk() {
    return !!this.lock && canDecrypt(this.nostr(), this.lock.scheme) && !this.denied;
  }

  /**
   * Open the private half: one prompt, ever, and a refusal is final until the reader clicks the
   * retry row. The peer key is the reader's own pubkey.
   */
  async unlock() {
    if (this.secret) return this.secret;
    const who = this.who();
    if (!this.lock || !who) return [];
    if (this.asking) return this.asking;
    const lock = this.lock;
    this.asking = (async () => privateGroups(await this.nostr()[lock.scheme].decrypt(who, lock.content)))()
      .then((rows) => {
        // Cached even when empty, so the reader is not re-prompted to learn that.
        this.secret = rows;
        this.lock = null;
        this.denied = false;
        return rows;
      })
      .catch(() => {
        // Refused, dismissed or failed: the lock stays, and only a click asks again.
        this.denied = true;
        return [];
      })
      .finally(() => {
        this.asking = null;
        this.unlocked();
      });
    return this.asking;
  }

  /** The picker's "unlock" row, clicked: the reader asking for the dialog back. */
  async retry() {
    this.denied = false;
    this.asking = null;
    await this.unlock();
  }

  /** What the picker should say about the locked half, or null; only `denied` a click can fix. */
  lockState() {
    if (!this.lock || !this.who()) return null;
    if (!canDecrypt(this.nostr(), this.lock.scheme)) return { state: "unsupported", scheme: this.lock.scheme };
    if (this.denied) return { state: "denied" };
    return { state: "asking" };
  }
}
