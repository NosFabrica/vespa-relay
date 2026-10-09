// What lands after a list is drawn: a rank read is not asked again while it is out, and the
// lookups that land together repaint the list once. Lifted out of app.js and run on stubs.
import assert from "assert";
import { readFileSync } from "node:fs";

const app = readFileSync(new URL("../../main/resources/web/app.js", import.meta.url), "utf8");

function lift(head) {
  const start = app.indexOf(head);
  assert.notStrictEqual(start, -1, `app.js has no ${head}`);
  return app.slice(start, app.indexOf("\n}", start) + 2);
}

const pk = (c) => c.repeat(64);
let chips = [];
const frames = [];
const asked = [];
const answers = [];
const env = {
  viewingAs: null, me: pk("f"), scores: new Map(), scoring: new Set(), scoreLensKey: null,
  rankServicesOf: async () => [pk("s")],
  refConn: async () => ({
    req: (filter) => {
      asked.push(...filter["#d"]);
      return new Promise((resolve) => answers.push(() => {
        const evs = filter["#d"].map((d) => ({ pubkey: pk("s"), tags: [["d", d], ["rank", "7"]] }));
        evs.complete = true;
        resolve(evs);
      }));
    },
  }),
  document: { querySelectorAll: () => chips, querySelector: () => null },
  paintChips: (cs) => { for (const c of cs) c.textContent = env.scores.get(c.dataset.pk) ?? ""; },
  requestAnimationFrame: (f) => frames.push(f),
  field: { repaint: () => {} },
};
const src = ["async function paintScores()", "async function readScores(", "function paintLate(", "function repaintSoon("]
  .map(lift).join("\n") + "\nconst lateRepaints = new Map();";
const { paintScores, paintLate } = new Function(...Object.keys(env),
  `${src}\nreturn { paintScores, paintLate };`)(...Object.values(env));

const chip = (p) => ({ dataset: { pk: p }, textContent: "" });
const flush = () => new Promise((r) => setTimeout(r, 0));

// Every paintList calls paintScores; three in a row while the first read is out ask once.
chips = [chip(pk("a")), chip(pk("b"))];
const first = paintScores();
await flush();
chips = [chip(pk("a")), chip(pk("b"))];   // the list re-rendered under the read
const second = paintScores(), third = paintScores();
await flush();
assert.deepStrictEqual(asked.sort(), [pk("a"), pk("b")], "a pubkey whose rank is being read is not asked again");
answers.splice(0).forEach((f) => f());
await Promise.all([first, second, third]);
assert.deepStrictEqual(chips.map((c) => c.textContent), [7, 7], "the chips drawn while the read was out are the ones painted");
asked.length = 0;
chips = [chip(pk("a")), chip(pk("c"))];
const fourth = paintScores();
await flush();
assert.deepStrictEqual(asked, [pk("c")], "a landed read is not in flight any more, and a known score is not re-read");
answers.splice(0).forEach((f) => f());
await fourth;

// The lookups that land in one frame are one repaint.
const st = { requestId: 1 };
let renders = 0;
const draw = () => renders++;
paintLate(st, 1, [Promise.resolve(2), Promise.resolve(1), Promise.resolve(0), Promise.resolve(5)], draw);
await flush();
assert.strictEqual(frames.length, 1, "one repaint is queued for the frame");
frames.splice(0).forEach((f) => f());
assert.strictEqual(renders, 1);

// A newer answer drops the old one's repaint, and its own landing still paints.
paintLate(st, 1, [Promise.resolve(1)], draw);
await flush();
st.requestId = 2;
paintLate(st, 2, [Promise.resolve(1)], draw);
await flush();
frames.splice(0).forEach((f) => f());
assert.strictEqual(renders, 2, "the landing for the current answer is drawn");

console.log("late: one rank read per pubkey in flight, and one repaint per frame of landings");
