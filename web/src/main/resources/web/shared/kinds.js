// What each kind is called and which family tints its badge, shared by every
// renderer so a mixed list stays scannable.

// Badge names; anything else reads "kind N". Every kind with a renderer is named here.
const KIND_LABELS = {
  0: "profile", 1: "note", 3: "follows", 5: "deletion", 6: "repost", 7: "reaction",
  8: "badge award", 9: "chat", 11: "thread", 16: "repost", 17: "reaction",
  20: "picture", 21: "video", 22: "short video", 40: "channel", 41: "channel",
  42: "chat", 1018: "poll vote", 1063: "file", 1068: "poll", 1111: "comment",
  1222: "voice", 1244: "voice reply", 1311: "live chat", 1337: "code",
  1617: "patch", 1618: "pull request", 1619: "pr update", 1621: "issue",
  1622: "git reply", 1630: "git status", 1631: "git status", 1632: "git status",
  1633: "git status", 1984: "report", 1985: "label", 1986: "audio",
  4550: "post approval", 9041: "goal", 9734: "zap request", 9735: "zap",
  9802: "highlight",
  // NIP-51 standard lists, one per person, so they read as possessions.
  10000: "mute list", 10001: "pins", 10002: "relay list", 10003: "bookmarks",
  10004: "communities", 10005: "public chats", 10006: "blocked relays",
  10007: "search relays", 10008: "profile badges", 10009: "groups",
  10011: "favorite sets", 10012: "relay feeds", 10013: "private relays",
  10015: "interests", 10017: "git authors", 10018: "git repos",
  10020: "media follows", 10030: "emoji list", 10040: "observer",
  10050: "dm relays", 10054: "podcasts", 10063: "media servers",
  10064: "podcasts", 10096: "file servers", 10101: "wiki authors",
  10102: "wiki relays", 10166: "relay monitor",
  // NIP-51 sets, many per person, each with its own `d`.
  30000: "follow set", 30001: "list", 30002: "relay set", 30003: "bookmark set",
  30004: "curation", 30005: "video set", 30006: "picture set", 30007: "mute set",
  30008: "badge set", 30009: "badge", 30015: "interest set", 30017: "stall",
  30018: "product", 30020: "auction", 30023: "article", 30024: "draft",
  30030: "emoji pack", 30040: "publication", 30041: "section", 30063: "release",
  30166: "relay", 30267: "app set", 30311: "live", 30312: "room",
  30313: "conference", 30315: "status", 30382: "score", 30383: "event score",
  30384: "entry score", 30402: "listing", 30403: "draft listing",
  // The Tapestry Trusted Lists, named by what they hold.
  30392: "trusted people", 30393: "trusted events", 30394: "trusted articles",
  30395: "trusted identifiers",
  30617: "repository", 30618: "repo state", 30818: "wiki", 31890: "feed",
  // The NIP-29 group itself; its siblings (39001-39005) have no renderer, so no name here.
  39000: "group",
  31922: "date", 31923: "event", 31924: "calendar", 31925: "rsvp",
  31989: "app", 31990: "app", 32267: "app", 34235: "video", 34236: "short video",
  34550: "community", 39089: "starter pack", 39092: "starter pack",
  39701: "web bookmark",
};
export const kindLabel = (k) => KIND_LABELS[k] || `kind ${k}`;

/** Every kind the UI can render, ascending; kind_stats.html reads it so a new card gains a row there. */
export const KNOWN_KINDS = Object.keys(KIND_LABELS).map(Number).sort((a, b) => a - b);

// A badge tint per family, following the renderer rather than the kind number.
const KIND_TONES = {
  people: [0, 3, 10002, 10040, 10166, 30000, 30002, 30166, 30382, 30383, 30384, 30392, 30393, 30394, 30395, 39089, 39092],
  note: [1, 9, 11, 40, 41, 42, 1111, 1311, 9802, 34550, 39000],
  social: [5, 6, 7, 8, 16, 17, 1018, 1068, 1984, 1985, 4550, 9734, 9735, 30315],
  article: [30004, 30023, 30024, 30040, 30041, 30818],
  media: [20, 21, 22, 1063, 1222, 1244, 1986, 30005, 30006, 30030, 34235, 34236],
  code: [1337, 1617, 1618, 1619, 1621, 1622, 1630, 1631, 1632, 1633, 30063, 30617, 30618],
  live: [30311, 30312, 30313, 31922, 31923, 31924, 31925],
  market: [9041, 30009, 30017, 30018, 30020, 30402, 30403],
  apps: [31890, 31989, 31990, 32267],
  list: [10000, 10001, 10003, 10004, 10005, 10006, 10007, 10008, 10009, 10011,
    10012, 10013, 10015, 10017, 10018, 10020, 10030, 10050, 10054, 10063, 10064,
    10096, 10101, 10102, 30001, 30003, 30007, 30008, 30015, 30267, 39701],
};
const TONE_OF = {};
for (const [tone, kinds] of Object.entries(KIND_TONES)) for (const k of kinds) TONE_OF[k] = tone;
export const kindTone = (k) => TONE_OF[k] || "";
