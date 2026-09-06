// A relay url as somebody wrote it, reduced to the relay it means, or null for one this page
// could not dial: a non-ws scheme, a loopback or private host, or `ws://` from an https
// page. observer_stats.html keeps its own inline copy; the two must agree.
export function normalizeRelay(raw) {
  if (!raw) return null;
  const t = String(raw).trim();
  if (!t || /\s/.test(t)) return null;
  // A foreign scheme is refused before the fallback: `wss://` prepended to `https://x.com`
  // parses with host "https" and passes every later check.
  if (/^[a-z][a-z0-9+.-]*:/i.test(t) && !/^wss?:\/\//i.test(t)) return null;
  let u;
  try { u = new URL(/^wss?:\/\//i.test(t) ? t : "wss://" + t); } catch (e) { return null; }
  if (u.protocol !== "ws:" && u.protocol !== "wss:") return null;
  if (location.protocol === "https:" && u.protocol === "ws:") return null;
  if (/^(localhost|127\.|10\.|192\.168\.|169\.254\.|\[::1\]|0\.0\.0\.0)/i.test(u.hostname) ||
      /^172\.(1[6-9]|2\d|3[01])\./.test(u.hostname)) return null;
  // A bare "/" path is the same relay as no path at all.
  const path = u.pathname === "/" ? "" : u.pathname.replace(/\/+$/, "");
  return `${u.protocol}//${u.host}${path}`;
}

/** Why [normalizeRelay] refused this one, in words a person can act on; null when it did not. */
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
    // The browser's refusal, not ours.
    return `This page is served over https, so the browser blocks plain ws:// connections. Try wss://${u.host}${u.pathname === "/" ? "" : u.pathname}`;
  }
  return "That address points at your own machine, which this page will not dial on your behalf.";
}
