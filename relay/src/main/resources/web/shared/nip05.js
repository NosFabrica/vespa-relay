// ---- NIP-05 verification ---------------------------------------------------
//
// A nip05 in a profile is a CLAIM: "this name at this domain is me". It is
// only worth anything once the domain agrees, and until now the page rendered
// the claim as though it were the answer.
//
// Checked lazily, when the profile scrolls into view — one fetch per identity
// per session, no more. Doing it at render time would fire a request per
// result for profiles nobody looks at, against domains that did not ask to be
// polled by a search page.
//
// Three outcomes, not two. A domain that cannot be reached — offline, no CORS
// header, bad TLS — is NOT a failed claim, and marking it invalid would
// accuse people of lying because their web server is down.
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
        // Absent or mismatched are both a failed claim; only a reachable
        // domain can produce one.
        verdict = claimed && claimed.toLowerCase() === pubkey.toLowerCase() ? "ok" : "bad";
      }
    } catch (e) { verdict = "unknown"; }
  }
  nip05Cache.set(key, verdict);
  return verdict;
}

const NIP05_MARK = { ok: ["✓", "verified by the domain"], bad: ["✗", "the domain does not claim this pubkey"], unknown: ["?", "could not reach the domain to check"] };

// One observer for the page: elements register themselves by existing.
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
 * Hand the currently rendered nip05 elements to the watcher.
 *
 * DISCONNECT first, then observe what is on the page now. Every caller of this
 * reaches it just after replacing a container's innerHTML wholesale, so the
 * elements observed on the previous pass are already detached — and an
 * IntersectionObserver holds a STRONG reference to each target until it is
 * unobserved, which only happened here when a target scrolled into view. A
 * session that ran forty searches was keeping every nip05 element of all forty
 * alive, none of them in the document. The old `data-watched` guard existed to
 * avoid re-observing across calls; re-observing is now the whole point, and it
 * costs nothing because a verdict is cached per identity for the session.
 */
export function watchNip05() {
  nip05Watcher.disconnect();
  for (const el of document.querySelectorAll(".nip05[data-addr]")) nip05Watcher.observe(el);
}
