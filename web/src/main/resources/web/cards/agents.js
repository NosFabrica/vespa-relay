// Agents and the little apps they run: a Buzz workspace's agent profiles, personas, teams and
// managed agents, its workflow definitions, and the NIP-5D napplet manifests.
//
// | kind | | |
// |---|---|---|
// | 10100 | an agent's own profile | one per key, in place of its `kind:0` |
// | 30175 | a persona | a definition the workspace owner publishes, keyed by a slug |
// | 30176 | a team | a grouping of personas |
// | 30177 | a managed agent | keyed by the AGENT's pubkey, so the `d` is a person |
// | 30620 | a workflow | its `d` is a uuid and its content is YAML |
// | 5129 / 15129 / 35129 | a napplet | a pinned build, an author's default, a named one |
//
// Five of these carry JSON in `content`, and it is a stranger's JSON from a schema its own
// authors call loose: every field read here goes through [text] or [list], so a name that
// arrives as an object contributes nothing rather than "[object Object]".

import { esc, clip, titleOf, summaryOf } from "../shared/format.js";
import {
  register, registerRow, registerNamedPeople, shell, titleHtml, bodyHtml, chipRow, relayRows,
  personLink, extLink, jsonContent, tagOf, tagsOf, oneLine, plural,
} from "./base.js";
import { codeBlock } from "./code.js";

const HEX64 = /^[0-9a-f]{64}$/;

/** A field off a stranger's JSON as one line of text, or "". */
const text = (v) => oneLine(v);
/** A list off a stranger's JSON: a non-array, and any entry that is not text, contributes nothing. */
const list = (v) => (Array.isArray(v) ? v.map(oneLine).filter(Boolean) : []);
/** A props value from that same JSON, already escaped, or null. */
const fact = (v) => (text(v) ? esc(clip(text(v), 80)) : null);

/** 10100 — an agent's own profile: what it is, what it can do, and who may add it to a room. */
function agentProfileCard(ev, opts) {
  const c = jsonContent(ev);
  const name = text(c.display_name) || text(c.name);
  const kindOfAgent = [text(c.agent_type), text(c.status)].filter(Boolean).join(" · ");
  const channels = list(c.channel_ids).length;
  const inner =
    titleHtml(opts, name, 140) +
    (kindOfAgent ? `<div class="result-body muted">${esc(clip(kindOfAgent, 120))}</div>` : "") +
    (channels ? `<div class="result-body">${esc(`in ${plural(channels, "room")}`)}</div>` : "") +
    chipRow(list(c.capabilities), opts);
  return shell(ev, opts, inner, [["adds to rooms", fact(c.channel_add_policy)]]);
}

/**
 * The lines that say how an agent is wired: which model answers, through which provider, and
 * when it speaks. Shared by a persona and by the managed agent that instantiates one.
 */
const wiringOf = (c) => [
  ["model", fact(c.model)],
  ["provider", fact(c.provider)],
  ["runtime", fact(c.runtime)],
  ["responds to", fact(c.respond_to)],
  ["at once", fact(c.parallelism)],
];

/** 30175 — a persona: a named way for an agent to behave, which is mostly its system prompt. */
function personaCard(ev, opts) {
  const c = jsonContent(ev);
  const inner =
    titleHtml(opts, text(c.display_name) || tagOf(ev, "d"), 140) +
    bodyHtml(opts, text(c.system_prompt), 400, true) +
    chipRow(list(c.name_pool), opts);
  return shell(ev, opts, inner, wiringOf(c));
}

/** 30176 — a team: personas grouped under one name, with the instructions they share. */
function teamCard(ev, opts) {
  const c = jsonContent(ev);
  const personas = list(c.persona_ids);
  const inner =
    titleHtml(opts, text(c.name) || tagOf(ev, "d"), 140) +
    bodyHtml(opts, text(c.description), 300, true) +
    `<div class="result-body">${esc(plural(personas.length, "persona"))}</div>` +
    bodyHtml(opts, opts && opts.full ? text(c.instructions) : "", 0) +
    chipRow(personas, opts);
  return shell(ev, opts, inner);
}

/** A managed agent's `d` is the agent's own pubkey, so the card can name the agent itself. */
const agentKey = (ev) => (HEX64.test(tagOf(ev, "d") || "") ? tagOf(ev, "d") : null);

/** 30177 — a managed agent: one persona, instantiated under a key its owner runs. */
function managedAgentCard(ev, opts) {
  const c = jsonContent(ev);
  const key = agentKey(ev);
  const inner =
    titleHtml(opts, text(c.name), 140) +
    (key ? `<div class="result-body">runs as ${personLink(key)}</div>` : "") +
    bodyHtml(opts, text(c.system_prompt), 400, true) +
    chipRow(list(c.respond_to_allowlist), opts);
  return shell(ev, opts, inner, [["persona", fact(c.persona_id)], ...wiringOf(c)]);
}

/** 30620 — a workflow: its source is YAML, so it is read as code rather than as prose. */
function workflowCard(ev, opts) {
  const inner =
    titleHtml(opts, tagOf(ev, "name") || tagOf(ev, "d"), 140) +
    codeBlock(opts, ev.content, { lang: "yaml" });
  return shell(ev, opts, inner);
}

// ---- the napplets ----------------------------------------------------------

/** What a manifest ships: one `["path", <path>, <hash>]` per file. */
const pathsOf = (ev) => tagsOf(ev, "path").filter((t) => t[1]).map((t) => t[1]);

/** Which of the three a manifest is, in the words its NIP uses. */
const NAPPLET_ROLE = {
  5129: "one pinned build",
  15129: "the default napplet for this key",
  35129: "a named napplet",
};

/** 5129 / 15129 / 35129 — a napplet manifest: the files it is made of, and where they are served. */
function nappletCard(ev, opts) {
  const paths = pathsOf(ev);
  const servers = tagsOf(ev, "server").map((t) => t[1]).filter(Boolean);
  const inner =
    titleHtml(opts, titleOf(ev), 140) +
    `<div class="result-body muted">${esc(NAPPLET_ROLE[ev.kind] || "a napplet")}</div>` +
    bodyHtml(opts, summaryOf(ev) || ev.content, 300, true) +
    `<div class="result-body">${esc(plural(paths.length, "file"))}</div>` +
    (opts && opts.full ? relayRows(servers.map((url) => ({ url })), opts) : "") +
    chipRow(tagsOf(ev, "requires").map((t) => t[1]).filter(Boolean), opts);
  return shell(ev, opts, inner, [
    ["source", extLink(tagOf(ev, "source"))],
    // The aggregate hash over every path: what "the same napplet" means between two manifests.
    ["hash", tagOf(ev, "x") ? `<span class="mono">${esc(clip(tagOf(ev, "x"), 20))}</span>` : null],
  ]);
}

register([10100], agentProfileCard);
register([30175], personaCard);
register([30176], teamCard);
register([30177], managedAgentCard);
register([30620], workflowCard);
register([5129, 15129, 35129], nappletCard);
// The agent a 30177 runs is named by its `d`, which no scan of `p` tags reaches.
registerNamedPeople([30177], (ev) => [agentKey(ev)].filter(Boolean));

registerRow([10100], (ev) => {
  const c = jsonContent(ev);
  return {
    name: text(c.display_name) || text(c.name),
    sub: [text(c.agent_type), text(c.status), list(c.capabilities).join(", ")].filter(Boolean).join(" · "),
  };
});
registerRow([30175], (ev) => {
  const c = jsonContent(ev);
  return {
    name: text(c.display_name) || tagOf(ev, "d"),
    sub: [text(c.model), text(c.system_prompt)].filter(Boolean).join(" · "),
  };
});
registerRow([30176], (ev) => {
  const c = jsonContent(ev);
  return {
    name: text(c.name) || tagOf(ev, "d"),
    sub: [plural(list(c.persona_ids).length, "persona"), text(c.description)].filter(Boolean).join(" · "),
  };
});
registerRow([30177], (ev) => {
  const c = jsonContent(ev);
  return { name: text(c.name), sub: [text(c.model), text(c.system_prompt)].filter(Boolean).join(" · ") };
});
// A workflow's body is YAML: the first line of it says nothing, so the count does.
registerRow([30620], (ev) => ({
  name: tagOf(ev, "name") || tagOf(ev, "d"),
  sub: plural(String(ev.content || "").split("\n").filter((l) => l.trim()).length, "line"),
}));
registerRow([5129, 15129, 35129], (ev) => ({
  name: titleOf(ev),
  sub: [NAPPLET_ROLE[ev.kind], plural(pathsOf(ev).length, "file"), summaryOf(ev)].filter(Boolean).join(" · "),
}));
