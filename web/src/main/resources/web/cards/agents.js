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
// | 15128 / 35128 | a static website | the same manifest, for a site rather than an applet |
// | 30178 | a team catalog | a team shared whole: its members' prompts inline, not by id |
// | 43001…43006 | a Buzz job | a request in a room, then its accept, progress, result, cancel or error |
// | 11316 | a ContextVM server | an MCP server announcing itself over Nostr (CEP-6) |
//
// Five of these carry JSON in `content`, and it is a stranger's JSON from a schema its own
// authors call loose: every field read here goes through [text] or [list], so a name that
// arrives as an object contributes nothing rather than "[object Object]".

import { esc, clip, titleOf, summaryOf } from "../shared/format.js";
import {
  register, registerRow, registerNamedPeople, shell, titleHtml, bodyHtml, chipRow, relayRows,
  personLink, extLink, jsonContent, tagOf, tagsOf, oneLine, plural, noteHref, coverThumb, safeUrl,
} from "./base.js";
import { shortNote } from "../shared/nip19.js";
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

/**
 * 30178 — a team catalog: the shareable form of a team, every member carried inline (name,
 * model, system prompt) so a reader can adopt the team without the personas it was built from.
 */
function teamCatalogCard(ev, opts) {
  const c = jsonContent(ev);
  const members = Array.isArray(c.members) ? c.members.filter((m) => m && typeof m === "object") : [];
  const names = members.map((m) => text(m.display_name)).filter(Boolean);
  const full = opts && opts.full;
  const inner =
    titleHtml(opts, text(c.name) || tagOf(ev, "d"), 140) +
    bodyHtml(opts, text(c.description), 300, true) +
    `<div class="result-body">${esc(plural(members.length, "member"))}</div>` +
    chipRow(names, opts) +
    (full ? bodyHtml(opts, text(c.instructions), 0) : "") +
    (full && members.length
      ? `<dl class="props">${members.map((m) => `<dt>${esc(clip(text(m.display_name) || text(m.member_key), 60))}</dt>` +
          `<dd>${esc(clip([text(m.model), text(m.provider)].filter(Boolean).join(" · ") || text(m.runtime) || "—", 80))}</dd>`).join("")}</dl>`
      : "");
  return shell(ev, opts, inner);
}

// ---- a Buzz job, from ask to answer ----------------------------------------

/** What each step of a job says it does, in the order a job goes through them. */
const JOB_STEP = {
  43001: "asks for a job",
  43002: "accepts a job",
  43003: "reports progress on a job",
  43004: "delivers a job",
  43005: "cancels a job",
  43006: "fails a job",
};

/** A job event's counterpart: the one `p` — the worker asked, or the requester answered. */
const jobPerson = (ev) => tagsOf(ev, "p").map((t) => t[1]).find((pk) => HEX64.test(pk || "")) || null;

/**
 * 43001…43006 — one step of a Buzz job. The request (43001) names the worker it asks; every
 * later step points at that request with an `e`, names the requester with a `p`, and carries
 * its text in `content`: the ask, the result, the reason or the error.
 */
function jobCard(ev, opts) {
  const request = ev.kind === 43001 ? null : tagsOf(ev, "e").map((t) => t[1]).find((id) => HEX64.test(id || ""));
  const who = jobPerson(ev);
  const status = oneLine(tagOf(ev, "status"));
  const step = JOB_STEP[ev.kind] || "a job";
  const verb = esc(step.replace(/ a job$/, ""));
  const line = ev.kind === 43001
    ? (who ? `asks ${personLink(who)} for a job` : esc(step))
    : `${verb} ${request ? `job <a class="mono" href="${noteHref(request)}">${esc(shortNote(request))}</a>` : "a job"}` +
      `${who ? ` for ${personLink(who)}` : ""}`;
  const inner =
    `<div class="result-body">${line}</div>` +
    (status ? `<div class="pill-row"><span class="status-pill">${esc(clip(status, 24))}</span></div>` : "") +
    bodyHtml(opts, ev.content, 500, ev.kind === 43005);
  return shell(ev, opts, inner);
}

/**
 * 11316 — a ContextVM server's announcement: its name and blurb in tags, the transport features
 * it supports as bare flag tags, and the MCP `initialize` result as JSON content.
 */
const CVM_FLAGS = {
  support_encryption: "encrypted",
  support_encryption_ephemeral: "ephemeral encryption",
  support_oversized_transfer: "oversized transfer",
  support_open_stream: "open stream",
};

function cvmServerCard(ev, opts) {
  const c = jsonContent(ev);
  const info = c.serverInfo && typeof c.serverInfo === "object" ? c.serverInfo : {};
  const caps = c.capabilities && typeof c.capabilities === "object" && !Array.isArray(c.capabilities)
    ? Object.keys(c.capabilities).map(oneLine).filter(Boolean) : [];
  const flags = Object.entries(CVM_FLAGS).filter(([tag]) => tagsOf(ev, tag).length).map(([, label]) => label);
  const pic = safeUrl(tagOf(ev, "picture"));
  const inner =
    `<div class="result-main"><div class="text">` +
    titleHtml(opts, tagOf(ev, "name") || text(info.name), 140) +
    bodyHtml(opts, tagOf(ev, "about"), 300) +
    `</div>${pic ? coverThumb(pic) : ""}</div>` +
    chipRow([...caps, ...flags], opts) +
    (opts && opts.full ? bodyHtml(opts, text(c.instructions), 0, true) : "");
  return shell(ev, opts, inner, [
    ["website", tagOf(ev, "website") ? extLink(tagOf(ev, "website")) : null],
    ["version", fact(info.version)],
    ["protocol", fact(c.protocolVersion)],
  ]);
}

// ---- the napplets ----------------------------------------------------------

/** What a manifest ships: one `["path", <path>, <hash>]` per file. */
const pathsOf = (ev) => tagsOf(ev, "path").filter((t) => t[1]).map((t) => t[1]);

/** Which of the three a manifest is, in the words its NIP uses. */
const NAPPLET_ROLE = {
  5129: "one pinned build",
  15129: "the default napplet for this key",
  35129: "a named napplet",
  15128: "the default website for this key",
  35128: "a named website",
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
register([5129, 15129, 35129, 15128, 35128], nappletCard);
register([30178], teamCatalogCard);
register(Object.keys(JOB_STEP).map(Number), jobCard);
register([11316], cvmServerCard);
// A job's counterpart is its one `p`, read here so the name the card writes is loaded.
registerNamedPeople(Object.keys(JOB_STEP).map(Number), (ev) => [jobPerson(ev)].filter(Boolean));
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
registerRow([5129, 15129, 35129, 15128, 35128], (ev) => ({
  name: titleOf(ev),
  sub: [NAPPLET_ROLE[ev.kind], plural(pathsOf(ev).length, "file"), summaryOf(ev)].filter(Boolean).join(" · "),
}));
registerRow([30178], (ev) => {
  const c = jsonContent(ev);
  const members = Array.isArray(c.members) ? c.members.filter((m) => m && typeof m === "object") : [];
  return {
    name: text(c.name) || tagOf(ev, "d"),
    sub: [plural(members.length, "member"), members.map((m) => text(m.display_name)).filter(Boolean).join(", "), text(c.description)]
      .filter(Boolean).join(" · "),
  };
});
// A job's line is the step, then what it said: the ask, the answer or the error.
registerRow(Object.keys(JOB_STEP).map(Number), (ev) => ({
  name: JOB_STEP[ev.kind],
  sub: [oneLine(tagOf(ev, "status")), ev.content].filter(Boolean).join(" · "),
}));
registerRow([11316], (ev) => ({
  name: tagOf(ev, "name") || text((jsonContent(ev).serverInfo || {}).name) || "a ContextVM server",
  sub: tagOf(ev, "about") || "",
}));
