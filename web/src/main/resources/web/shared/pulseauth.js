// Signing in to an admin page, with a NIP-07 extension.
//
// The document this page reads is administrators-only, proven with NIP-98: a
// kind-27235 event signed over the exact url and method of the request. Those
// tokens are SINGLE-USE — the relay remembers what it has seen — so a page
// that polls every two seconds cannot carry one, and does not try: one
// signature is exchanged for a short-lived HttpOnly session cookie, and the
// polls ride that.
//
// Nothing here holds a credential. The cookie is set by the relay and is
// invisible to this script, which is the point: an XSS on this page still
// cannot walk away with the session.

/** The kind a NIP-98 token is. */
export const NIP98_KIND = 27235;

/** Refused for want of credentials, and told exactly what to sign. */
export class SignInRequired extends Error {
  constructor(body) {
    super(body?.error || "administrator sign-in required");
    this.name = "SignInRequired";
    /**
     * `{ url, method, kind }` for opening THIS route directly — one token per
     * request, which is the path a script takes.
     */
    this.sign = body?.sign || null;
    /**
     * `{ url, method, kind }` for opening a SESSION — one token, then a cookie,
     * which is the path a browser takes. A different url from [sign], and
     * signing the wrong one of the two is the mistake this pair prevents.
     */
    this.session = body?.session || null;
  }
}

/** Verified, and not on the administrator list. A different problem with a different fix. */
export class NotAnAdmin extends Error {
  constructor(body) {
    super(body?.error || "not an administrator of this relay");
    this.name = "NotAnAdmin";
    this.pubkey = body?.pubkey || null;
  }
}

/** Whether this browser can sign at all. Checked before anything is asked of the operator. */
export const canSign = () => !!(globalThis.window && window.nostr && window.nostr.signEvent);

/**
 * A NIP-98 `Authorization` value for [url] and [method].
 *
 * SIGNED OVER THE URL THE RELAY NAMED, never over `location.href`. The `u` tag
 * is what stops a token spent here from being spent elsewhere, so the server
 * checks it against its own configured origin; behind a tunnel or a proxy that
 * is not the address this browser dialled, and signing what the browser sees
 * would fail every time with nothing on screen to explain it. The refusal
 * carries the expected url, and this signs that.
 */
export async function nip98Token(url, method) {
  if (!canSign()) throw new Error("No Nostr extension found (window.nostr / NIP-07)");
  const signed = await window.nostr.signEvent({
    kind: NIP98_KIND,
    created_at: Math.floor(Date.now() / 1000),
    content: "",
    tags: [["u", url], ["method", method]],
  });
  // base64 of the event json, per NIP-98. `unescape(encodeURIComponent(…))` is
  // the one-liner that makes btoa safe for non-ASCII; a relay url or a name
  // with an accent in it would otherwise throw here.
  return "Nostr " + btoa(unescape(encodeURIComponent(JSON.stringify(signed))));
}

/**
 * Read a refusal into the error it is. A 401 and a 403 are different problems:
 * one is "sign in", the other is "you signed in and you are not on the list",
 * and a page that shows the same prompt for both sends an operator looking in
 * the wrong place.
 */
async function refusalOf(res) {
  let body = null;
  try { body = await res.json(); } catch (e) { body = null; }
  if (res.status === 403) return new NotAnAdmin(body);
  return new SignInRequired(body);
}

/**
 * Fetch an admin document with the session cookie, throwing the refusal when
 * there is none. `credentials: "same-origin"` is the default, and stated so
 * nobody "fixes" it to `include`: this page is served from the origin it reads.
 */
export async function fetchGuarded(url) {
  const res = await fetch(url, { credentials: "same-origin" });
  if (res.status === 401 || res.status === 403) throw await refusalOf(res);
  if (!res.ok && res.status !== 503) throw new Error(`GET ${url} — ${res.status} ${res.statusText}`);
  return res;
}

/**
 * One signature, exchanged for a session. Returns the administrator's pubkey.
 *
 * [session] is the `session` block from the refusal that sent us here — the
 * relay's own url and method for THIS route, not for the document. Without one
 * this falls back to the session path on this origin, which is right for the
 * ordinary case (a tunnel where the two agree) and wrong in exactly the case
 * the block exists to fix, so the caller should pass it.
 */
export async function signIn(session) {
  const method = session?.method || "POST";
  // TWO URLS, ON PURPOSE. The token is signed over the url the RELAY expects
  // (its configured origin, which behind a tunnel is not the one this browser
  // dialled); the request goes to the address this browser can actually reach.
  // Signing one and sending to the other is what makes a tunnelled or proxied
  // deployment work without the operator having to make the two agree.
  const here = new URL("/pulse/session", location.href).href;
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

/** End the session. Best effort: a logout that fails must still clear the page. */
export async function signOut() {
  try {
    await fetch(new URL("/pulse/logout", location.href).href, { method: "POST", credentials: "same-origin" });
  } catch (e) { /* the cookie expires on its own */ }
}
