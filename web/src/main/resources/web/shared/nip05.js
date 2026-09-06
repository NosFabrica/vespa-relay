// NIP-05 verification, run when the profile scrolls into view, once per identity per
// session. Three outcomes, not two: a domain that cannot be reached is not a failed claim.
const nip05Cache = new Map();   // "name@domain|pubkey" -> "ok" | "bad" | "unknown"

async function checkNip05(addr, pubkey) {
  const key = `${addr}|${pubkey}`;
  if (nip05Cache.has(key)) return nip05Cache.get(key);
  const at = addr.lastIndexOf("@");
  const local = at < 0 ? "_" : addr.slice(0, at);
  const domain = at < 0 ? addr : addr.slice(at + 1);
  let verdict = "unknown";
  if (/^[a-z0-9._-]+$/i.test(local) && /^[a-z0-9.-]+\.[a-z]{2,}$/i.test(domain)) {
    try {
      const ctl = AbortSignal.timeout ? AbortSignal.timeout(6000) : undefined;
      const r = await fetch(`https://${domain}/.well-known/nostr.json?name=${encodeURIComponent(local)}`,
                            { signal: ctl, referrerPolicy: "no-referrer" });
      if (r.ok) {
        const j = await r.json();
        const claimed = j && j.names && j.names[local];
        // Absent or mismatched are both a failed claim; only a reachable domain can produce one.
        verdict = claimed && claimed.toLowerCase() === pubkey.toLowerCase() ? "ok" : "bad";
      }
    } catch (e) { verdict = "unknown"; }
  }
  nip05Cache.set(key, verdict);
  return verdict;
}

const NIP05_MARK = { ok: ["✓", "verified by the domain"], bad: ["✗", "the domain does not claim this pubkey"], unknown: ["?", "could not reach the domain to check"] };

// One observer for the page.
const nip05Watcher = new IntersectionObserver((entries) => {
  for (const en of entries) {
    if (!en.isIntersecting) continue;
    const el = en.target;
    nip05Watcher.unobserve(el);
    const { addr, pk } = el.dataset;
    checkNip05(addr, pk).then((v) => {
      const [mark, why] = NIP05_MARK[v];
      const chip = el.querySelector(".n5chip");
      if (!chip) return;
      chip.textContent = mark;
      chip.className = `n5chip ${v}`;
      chip.title = why;
    });
  }
}, { rootMargin: "120px" });

/**
 * Hand the rendered nip05 elements to the watcher. Disconnects first: an observer holds
 * its targets strongly, and every caller has just replaced a container's innerHTML.
 */
export function watchNip05() {
  nip05Watcher.disconnect();
  for (const el of document.querySelectorAll(".nip05[data-addr]")) nip05Watcher.observe(el);
}
