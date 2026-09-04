# Card renderer decisions

The history behind `web/src/main/resources/web/cards/*.js`, moved out of the
source so the comments can stay short. One paragraph per decision; `git log
-L` on the function finds the commit.

**A reactive kind leads with the relation, not its payload.** Reactions,
reposts, zaps, labels and deletions came back by the thousand from any search
over the whole corpus, each as a "kind 7" badge over an empty body. Their
content is a fragment ("+", "", a bolt11 invoice); the type-ahead row had the
same problem, printing "+" or an invoice as its one line. Card and row both say
"liked a note", "zapped 1,000 sats", "asks to delete 3 events".

**A row's target noun follows the tag, not a guess.** An `a` tag is "an entry",
never "a note": it names an article or a live stream as often as a note, and
"liked a note" under a reaction to an article is the row inventing a kind.

**A comment's parent is a person, its root a row only when different.** The
1111 card once printed the parent and root as two bech32 ids side by side, and
on a direct comment they were the same event, so the card said nothing twice.

**Media is drawn at both depths.** Every other family previews small on the
principle that a results list is a list. For kind 22 that rendered nothing: no
`image` tag, so an empty 92px poster slot beside a title ladder that fell
through to a UUID `d`, with the caption in `content` never reaching the page.

**One imeta, read whole.** A NIP-71 video may list an mp4 and a webm, each with
its own url, dim and duration. Reading "the first tag that carries this field"
across all of them shaped the frame from one file and played another.

**The video frame is sized before a byte loads.** `dim` reserves the box, so
nothing below moves when metadata lands; `#t=0.1` asks for the first frame
when there is no poster, because Safari paints black otherwise; `preload`
drops to `none` behind a poster; and the url sits in `data-src` for app.js to
promote near the viewport, because `preload="metadata"` on sixty short videos
is sixty range requests before the reader has passed the second card.

**`frameStyle` is the one place an event's numbers reach a `style` attribute.**
The parse is a strict full-string match on two short digit runs; anything else
yields no attribute rather than a sanitised one. The width floor is for `dim
100x4000`, a legal tag whose ratio implies a nine-pixel hairline.

**A picture post is an album.** Kind 20 carries one imeta per image; reading
only the first showed one photo of nine, at 92px in the search preview.

**The NIP-51 table covers the whole NIP, not the kind that was missing.** Six
list kinds had bespoke cards because something else in the relay needed them;
the other thirty fell to the generic floor, where a bookmark set with twelve
saved articles rendered as a title and a "kind 30003" badge. Closing it for
30003 alone would have let the next set kind arrive the same way.

**A `group` tag is a triple, and only the id is required.** The tag once went
through `relayRows` over `t[1]` like `relay` and `server`, printing group ids
in a list of relay urls and throwing the host and name away. A card draws what
somebody else's list says, so an entry with no host is still an entry;
dropping those rendered a list with entries as "nothing public here". The
picker (shared/groups.js) demands the host because it is choosing a group to
filter by. The dedupe key is (id, host) and excludes the name, as quartz's
`GroupTag.equals` does, so a group listed once with a cached name and once
without stays one row.

**List values are deduped, and `p` counts what it draws.** Clients append
without checking and merges stack the same entries; a follow list with the
same person twice drew two cells and counted two members. A `p` value that is
not hex is not a person, so "7 people" over six faces was two answers to one
question.

**`sectionsOf` runs once per card.** It dedupes every tag on the event, and a
follow list is thousands; calling it again to write the counts line built a
Set of eight thousand keys for nothing.

**An assertion's subject is resolved by kind, not by shape.** 30382's `d` is a
pubkey, 30383's an event id, 30384's an address. The pubkey-only version of
scoreCard turned an event id into a link to a person who does not exist. The
Trusted List family keys its member tag by kind for the same reason: a 30393
carries `p` tags (its `observer`) that are not members.

**A member score outside 0..100 is unscored, not clamped.** Quartz reads index
3 as a percentage and drops anything outside that range, so a publisher on
another scale reads back as unscored there; the card mirrors it rather than
drawing a number the scale cannot express.

**The Trusted List row carries `metric`.** Two lists can share a title and
differ only in what they measured; "Verified Human" was published under two
observers on staging, and a row that said only the title offered two identical
lines.

**Rows for counting kinds lead with the count.** Every people.js card counts
something and none has a title, so each row led with the author's name and
repeated it underneath. The count is the row's line and cards.js fills the
author in. A named set's description follows the count, because the count is
short and every list has one, so clipping takes words off the sentence rather
than the number.

**A description reaches the card, not only the row.** A starter pack called
"Test Group" and described as "Test Group for Amethyst" said the second half
only in the type-ahead popup and lost it on the page the popup opened.

**A channel's row reads fields, never the document.** Kind 40's content is
profile-shaped JSON, so "the content" as a row printed
`{"about":"","name":"Test group","picture":""}` beside a card that had drawn
the channel properly all along. NIP-15 products had the same row. This is the
row the type-ahead rework started from.

**The `note:`/`id` prop rows are gone.** They existed because a title-less
card had no other link to its own page; the card frame (`data-href`, the
byline date) carries that now, and the id is under "json".

**The profile card's hand-rolled frame carries `data-href` and provenance.**
Both were missed once. Its click target is the person's page, not the kind
0's id, which names one revision; and a profile is what a Trusted List of
pubkeys splices, so it is the card most likely to arrive with a provenance
row to draw.

**The floor's row keeps the content fallback on both lines.** Registered
kinds avoid falling back to raw content (it printed JSON payloads). For an
unknown kind the content is all anybody knows, so with a title and no summary
it stays as the second line rather than dropping to the author's name.

**Markdown is not rendered.** A hand-rolled markdown renderer is the surface
where an escaping mistake becomes an XSS in a page that renders strangers'
events. `mdExcerpt` reduces markdown to text for the preview line, never to
HTML, so what the card interpolates is still a string it escapes. Without it a
row of search results led with `## Somebody is paying for this`, marks and all.

**One summary slot, one voice.** The article preview showed the `summary` tag
muted and the body-excerpt fallback in body text; whichever it came from, the
line stands in for the article and reads the same.

**A price part is tested after `oneLine`, not before.** `{"price": {}}` is a
legal NIP-15 document, and a period that is not text passed a bare `period ?`
check and rendered as "250 USD / ".

**A draft is its published twin's card.** 30403 carries a 30402's tags and
30020 a product's JSON plus a starting bid; a draft rendering as "kind 30403"
beside a listing was the registry's gap, not the event's.
