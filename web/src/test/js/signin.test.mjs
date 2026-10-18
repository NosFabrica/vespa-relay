// Sign-in: the remembered choice, the AUTH event, the remembered face, and that a reader who
// chose to stay signed out is never prompted by the extension.
import assert from "node:assert/strict";

globalThis.location = { protocol: "https:", host: "relay.test" };
globalThis.window = { addEventListener: () => {} };
globalThis.document = { cookie: "" };
class QuietWS { constructor() { this.readyState = 0; } send() {} close() {} }
globalThis.WebSocket = QuietWS;

const S = await import(new URL("../../main/resources/web/signin.js", import.meta.url));
const ok = (name) => console.log(`  ✓ ${name}`);
const PK = "a".repeat(64);

{
  assert.equal(S.signInWanted(""), true, "unset defaults to signed in");
  assert.equal(S.signInWanted("x=1; sot_signedin=0"), false);
  assert.equal(S.signInWanted("sot_signedin=1; x=1"), true);
  assert.equal(S.signInWanted("not_sot_signedin=0"), true, "another cookie's name is not this one");
  assert.equal(S.signInWanted(S.signInCookie(false).split(";")[0]), false, "the cookie written reads back as written");
  assert.match(S.signInCookie(true), /; path=\/; max-age=31536000; SameSite=Lax$/, "every page the relay serves reads it, for a year");
  ok("the signed-out choice is remembered across the relay's pages");
}

{
  const e = S.authEvent("chal", "wss://relay.test/", 1700000000);
  assert.deepEqual(e, { kind: 22242, created_at: 1700000000, content: "", tags: [["relay", "wss://relay.test/"], ["challenge", "chal"]] });
  ok("the AUTH answers this relay's challenge, NIP-42's shape");
}

{
  assert.deepEqual(S.parseFace(JSON.stringify({ pubkey: PK, picture: "p", name: "n" })), { pubkey: PK, picture: "p", name: "n" });
  assert.equal(S.parseFace("{bad json"), null);
  assert.equal(S.parseFace(JSON.stringify({ pubkey: "npub1nope" })), null, "only a hex key is somebody");
  assert.equal(S.parseFace(null), null);
  const remembered = { pubkey: PK, picture: "old.png", name: "Old" };
  assert.deepEqual(S.faceOf(PK, { known: false, profile: null, remembered }), { picture: "old.png", name: "Old" },
    "the remembered face stands in until the relay answers");
  assert.deepEqual(S.faceOf(PK, { known: true, profile: null, remembered }), { picture: "", name: "" },
    "…and an answer of no profile is believed over it");
  assert.deepEqual(S.faceOf(PK, { known: true, profile: { picture: "new.png", name: "New" }, remembered }), { picture: "new.png", name: "New" });
  assert.deepEqual(S.faceOf("b".repeat(64), { known: false, profile: null, remembered }), { picture: "", name: "" },
    "another account never wears it");
  ok("the remembered face is drawn only for its own account, and only until the relay answers");
}

{
  document.cookie = S.signInCookie(false).split(";")[0];
  let prompted = false;
  window.nostr = { signEvent: async () => { prompted = true; }, getPublicKey: async () => { prompted = true; return PK; } };
  const heard = [];
  S.onSignIn({ unproved: (e) => heard.push(["unproved", e]), settled: (pk) => heard.push(["settled", pk]) });
  await S.ensureLogin();
  await S.ensureLogin();
  assert.equal(prompted, false, "a remembered sign-out is a decision: the extension is never asked");
  assert.deepEqual(heard, [["unproved", null], ["settled", null], ["settled", null]], "one attempt, settled as nobody each time it is awaited");
  assert.equal(S.me(), null);
  assert.equal(S.loginSettled(), true);
  ok("a reader who chose to stay signed out is never prompted");
}

console.log("signin: the choice, the proof's shape and the face, held by the module that owns them");
