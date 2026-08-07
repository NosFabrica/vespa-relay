// A relay url as somebody wrote it, reduced to the relay it means — or null
// for one this page could not dial anyway.
//
// Lifted out of observer_stats.html, which worked the rules out first against
// the corpus. That page keeps its own inline copy on purpose (it is
// deliberately self-contained, so it works when these modules do not); this is
// the app's, and the two are meant to agree. The rules, and where each came
// from:
//
//  - `wss://nip85.brainstorm.world` and `wss://nip85.brainstorm.world/` are one
//    relay written two ways, and both are in the corpus — 45 services on the
//    first, 2 on the second. Left raw they group apart, so the page dials the
//    same host twice.
//  - a non-ws scheme. `http://localhost:7778` is in the corpus, from somebody's
//    local test leaking a 10040 onto the network.
//  - loopback and private hosts. In a tag those mean the AUTHOR's machine; from
//    this browser they mean the reader's, and asking it questions on their
//    behalf is not something this page should do.
//  - `ws://` from an https page. The browser refuses it before a packet moves,
//    so dialling it would print "the relay did not answer" when the truth is
//    that we were never allowed to ask. The caller distinguishes this one — see
//    [whyNotDialable] — because a reader who TYPED that url deserves the reason
//    rather than a shrug.
export function normalizeRelay(raw) {
  if (!raw) return null;
  const t = String(raw).trim();
  if (!t || /\s/.test(t)) return null;
  // A scheme that is not ws/wss is refused HERE, before the fallback below can
  // paper over it. Prepending `wss://` to a string that already has a scheme
  // produces a url that parses and means nothing: `wss://` + `https://x.com`
  // is `wss://https//x.com`, whose HOST is "https" — it passes every check
  // after this one and gets dialled. Typed into the panel's field it printed
  // "Nothing of yours on https//relay.damus.io"; read out of a tag it is the
  // `http://localhost:7778` in the corpus, which the loopback rule below can
  // no longer see, because the hostname it is checking is "http".
  if (/^[a-z][a-z0-9+.-]*:/i.test(t) && !/^wss?:\/\//i.test(t)) return null;
  let u;
  try { u = new URL(/^wss?:\/\//i.test(t) ? t : "wss://" + t); } catch (e) { return null; }
  if (u.protocol !== "ws:" && u.protocol !== "wss:") return null;
  if (location.protocol === "https:" && u.protocol === "ws:") return null;
  if (/^(localhost|127\.|10\.|192\.168\.|169\.254\.|\[::1\]|0\.0\.0\.0)/i.test(u.hostname) ||
      /^172\.(1[6-9]|2\d|3[01])\./.test(u.hostname)) return null;
  // The URL parser already lowercased scheme and host. A bare "/" path is the
  // same relay as no path at all, which is the duplicate above.
  const path = u.pathname === "/" ? "" : u.pathname.replace(/\/+$/, "");
  return `${u.protocol}//${u.host}${path}`;
}

/**
 * Why [normalizeRelay] refused this one, in words a person can act on.
 *
 * Null when it did not refuse. This exists because the field the reader types
 * into is the ONLY place a rejected url has a human behind it: everywhere else
 * these urls come out of somebody else's tags and dropping them silently is
 * right. "Use wss://" is a fix; a disabled button is not.
 */
export function whyNotDialable(raw) {
  const t = String(raw || "").trim();
  if (!t) return "Type the address of a relay you use, like wss://relay.damus.io";
  if (normalizeRelay(t)) return null;
  if (/\s/.test(t)) return "A relay address has no spaces in it.";
  if (/^https?:\/\//i.test(t)) {
    return `That is a web address. Try wss://${t.replace(/^https?:\/\//i, "").replace(/\/+$/, "")}`;
  }
  if (/^[a-z][a-z0-9+.-]*:/i.test(t) && !/^wss?:\/\//i.test(t)) {
    return "A relay address starts with wss://";
  }
  let u = null;
  try { u = new URL(/^wss?:\/\//i.test(t) ? t : "wss://" + t); } catch (e) { u = null; }
  if (!u) return "That is not an address this page can read. Relays look like wss://relay.damus.io";
  if (location.protocol === "https:" && u.protocol === "ws:") {
    // Not our refusal — the browser's, before anything is sent.
    return `This page is served over https, so the browser blocks plain ws:// connections. Try wss://${u.host}${u.pathname === "/" ? "" : u.pathname}`;
  }
  return "That address points at your own machine, which this page will not dial on your behalf.";
}
