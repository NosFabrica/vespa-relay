// Signing in to an admin page with a NIP-07 extension. A NIP-98 token is single-use, so a
// polling page exchanges one signature for a short-lived HttpOnly session cookie and the
// polls ride that. Nothing here holds a credential; the cookie is invisible to this script.

export const NIP98_KIND = 27235;

/** Refused for want of credentials, and told exactly what to sign. */
export class SignInRequired extends Error {
  constructor(body) {
    super(body?.error || "administrator sign-in required");
    this.name = "SignInRequired";
    /** `{ url, method, kind }` for opening this route directly, one token per request. */
    this.sign = body?.sign || null;
    /** `{ url, method, kind }` for opening a session: a different url from [sign]. */
    this.session = body?.session || null;
  }
}

/** Verified, and not on the administrator list. */
export class NotAnAdmin extends Error {
  constructor(body) {
    super(body?.error || "not an administrator of this relay");
    this.name = "NotAnAdmin";
    this.pubkey = body?.pubkey || null;
  }
}

/** Whether this browser can sign at all. */
export const canSign = () => !!(globalThis.window && window.nostr && window.nostr.signEvent);

/**
 * A NIP-98 `Authorization` value. [url] must be the url the relay named in its refusal,
 * never `location.href`: the `u` tag is checked against the relay's configured origin,
 * which behind a tunnel or a proxy is not the address this browser dialled.
 */
export async function nip98Token(url, method) {
  if (!canSign()) throw new Error("No Nostr extension found (window.nostr / NIP-07)");
  const signed = await window.nostr.signEvent({
    kind: NIP98_KIND,
    created_at: Math.floor(Date.now() / 1000),
    content: "",
    tags: [["u", url], ["method", method]],
  });
  // `unescape(encodeURIComponent(…))` makes btoa safe for non-ASCII in the event json.
  return "Nostr " + btoa(unescape(encodeURIComponent(JSON.stringify(signed))));
}

/** A 401 is "sign in"; a 403 is "you signed in and you are not on the list". */
async function refusalOf(res) {
  let body = null;
  try { body = await res.json(); } catch (e) { body = null; }
  if (res.status === 403) return new NotAnAdmin(body);
  return new SignInRequired(body);
}

/**
 * Fetch an admin document with the session cookie, throwing the refusal when there is
 * none. `same-origin` is the default and is stated so nobody widens it to `include`.
 */
export async function fetchGuarded(url) {
  const res = await fetch(url, { credentials: "same-origin" });
  if (res.status === 401 || res.status === 403) throw await refusalOf(res);
  if (!res.ok && res.status !== 503) throw new Error(`GET ${url} — ${res.status} ${res.statusText}`);
  return res;
}

/**
 * One signature, exchanged for a session; returns the administrator's pubkey. [session]
 * is the `session` block from the refusal, and the caller should pass it: the fallback
 * is right only where the relay's origin and this browser's agree.
 */
export async function signIn(session) {
  const method = session?.method || "POST";
  // Two urls on purpose: the token is signed over the url the relay expects, the request
  // goes to the address this browser can reach. Document-relative, so a path prefix survives.
  const here = new URL("./pulse/session", location.href).href;
  const token = await nip98Token(session?.url || here, method);
  const res = await fetch(here, {
    method,
    headers: { Authorization: token },
    credentials: "same-origin",
  });
  if (res.status === 401 || res.status === 403) throw await refusalOf(res);
  if (!res.ok) throw new Error(`sign-in failed — ${res.status} ${res.statusText}`);
  return (await res.json()).pubkey;
}

/** End the session. Best effort: a failed logout must still clear the page. */
export async function signOut() {
  try {
    await fetch(new URL("./pulse/logout", location.href).href, { method: "POST", credentials: "same-origin" });
  } catch (e) { /* the cookie expires on its own */ }
}
