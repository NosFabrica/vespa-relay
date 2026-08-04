// What each kind is called and which family tints its badge — shared by every
// renderer so a mixed list stays scannable.

// Badge names for the kinds the index carries; anything else says "kind N".
const KIND_LABELS = {
  0: "profile", 1: "note", 3: "follows", 11: "thread", 20: "picture", 21: "video",
  22: "short video", 40: "channel", 41: "channel", 1063: "file", 1337: "code",
  1617: "patch", 1621: "issue", 1986: "audio", 9041: "goal", 9802: "highlight",
  30000: "follow set", 30002: "relay set", 30004: "curation", 30005: "video set",
  30009: "badge", 30017: "stall", 30018: "product", 30023: "article", 30024: "draft",
  30030: "emoji pack", 30063: "release", 30311: "live", 30402: "listing",
  30617: "repository", 30818: "wiki", 31890: "feed",
  31922: "date", 31923: "event", 31924: "calendar", 31989: "app", 31990: "app",
  32267: "app", 34235: "video", 34236: "short video",
};
export const kindLabel = (k) => KIND_LABELS[k] || `kind ${k}`;

// A badge tint per family, so one mixed list of every kind stays scannable.
const KIND_TONES = {
  people: [0, 3, 30000, 30002],
  note: [1, 11, 40, 41, 9802],
  article: [30023, 30024, 30818, 30004],
  media: [20, 21, 22, 1063, 1986, 30005, 30030, 34235, 34236],
  code: [1337, 1617, 1621, 30063, 30617],
  live: [30311, 31922, 31923, 31924],
  market: [9041, 30009, 30017, 30018, 30402],
};
const TONE_OF = {};
for (const [tone, kinds] of Object.entries(KIND_TONES)) for (const k of kinds) TONE_OF[k] = tone;
export const kindTone = (k) => TONE_OF[k] || "";
