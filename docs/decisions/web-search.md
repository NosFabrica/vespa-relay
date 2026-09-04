# Search page decisions

The history behind `web/src/main/resources/web/searchfield.js`,
`readiness.js` and `provenance.js`, moved out of the source so the code
reads on its own. One paragraph per decision; `git log -L` on the function
finds the commit.

## The search field

**The picker's face is the card's face.** The field once drew its own
`<img>`, so the one place that asks "who do you mean by this person" was the
one place that did not show what the lens thinks of them. It calls
`avatarHtml` now, score chip and all. When the date arithmetic was lifted into
`shared/calendar.js`, the move deleted `faceHtml` with it, one `const` line in
a diff that was otherwise all move; both call sites kept calling it, and the
two symptoms are the next paragraph.

**Rows are drawn outside the lookup's catch.** The catch exists so a relay
that does not answer is not read as "nobody matches". With the draw inside
it, a throw in `rowHtml()` was swallowed the same way and the picker sat on
"Searching people…" having been handed twelve answers; the next keystroke
then threw before `++reqId`, so no further lookup was ever sent. A lookup
may fail; drawing what it returned may not.

**The chip's staleness key is written as `\u0000`, never as the byte.** A
literal NUL made git treat the module as binary, and four commits landed as
`Bin 23897 -> 23932 bytes` with nothing to review, which is how the
`faceHtml` deletion shipped. A space would not do: ("Alice B", "") and
("Alice", "B") must not share a key.

**A pending token is derived from the text, not from the open list.** Blur
closes the picker while `from:al` stays half-written. With `picking` keyed
on the open list, returning to the field opened the results popup over that
token, the one thing the field promises will not happen.

**The last rows stay up while the next answer is fetched.** Blanking to
"Searching people…" between characters made the list flicker rows, note,
rows at typing speed and moved the row out from under the pointer. Rows are
dropped only for a different token or an emptied partial, when `hits` would
otherwise answer Enter with somebody no longer offered.

**Highlights are class flips, not renders.** The people list re-rendered
whole per arrow press, rebuilding every row's `<img>` to change one class,
and told a screen reader nothing because nothing wrote
`aria-activedescendant`. `markActive` and the calendar's `markDay` flip
classes on nodes that exist, and `scrollIntoView` runs only when the
keyboard moved the highlight.

**The unlock action is a row in the walkable list.** It was a
`tabindex="-1"` button wired to mousedown only: outside the arrow walk, the
tab order and every key the field handles, so a reader who dismissed the
permission dialog with the keyboard had no way to ask for it back. Being
last keeps its index equal to its place among the rendered `.popup-item`s.

**`regroup` is read and cleared above every return.** Cleared below the
token check, an unlock landing after the caret had left the token returned
with the flag armed, and it was spent on the next `group:` typed, re-asking
and resetting that list's highlight.

**Enter over nothing highlighted is the page's Enter.** The picker is a
suggestion over the text, not a gate: a partial date with no day chosen, or
a pasted group id this relay has never seen, must search rather than do
nothing. `shared/groups.js`'s exact-id band is the other half, putting an id
typed in full at row 0 so Enter over it picks itself.

**A phone's Enter is a line break, caught on `beforeinput`.** Gboard and
the iOS keyboard report the action key as an IME edit: the keydown carries
`Unidentified` (keyCode 229) or never arrives, so `e.key === "Enter"` never
matched and the div grew a second line. The first version refused any
insertion containing a newline, which dropped `hello` when an IME committed
`hello\n` as one insertion; only the break is refused now.
`insertCompositionText` is left alone for the reason the input handler
leaves a composition alone. A desktop cannot double-submit through this
door: there Enter is a keydown, and `app.js`'s `preventDefault` stops the
input from ever being attempted.

**A touch outside the field blurs it, on `pointerdown`.** An input loses
focus when a pointer goes down outside it; a contenteditable gets that from
a mouse and not from a finger, since nothing under the tap can take focus,
so the caret kept blinking until the browser put the keyboard away seconds
later. It is invisible in desktop mobile-emulation, which synthesises the
tap but has no keyboard to put away, so this one is checked on a phone.

**Leaving the page releases the caret, on three events.** A page frozen with
the field focused thawed with the keyboard gone and the caret still
blinking; tapping the box did nothing, because it was already focused and
there was no focus change to raise a keyboard for. `pagehide`,
`visibilitychange` and `pageshow` each fire where the others do not, and
only where `softKeyboard()` is true, since a desktop caret is not a keyboard.

**A contenteditable, not an input with an overlay.** An input's value is a
string of characters and cannot hold a picture. A transparent input over a
mirrored overlay holds only while every rendered token is exactly as wide as
the text it hides, which a name and a face are not.

**The date token is the ISO day.** `06/08/2026` means two different days to
two readers; the token is `since:2026-08-06` and the pill is the reader's own
spelling of it.

## The readiness panel

**The generation is the only concurrency control.** A `running` flag beside
it was a hole: "Check again" during a pass returned early on the flag while
the old pass dropped its own paint for being superseded, so the click did
nothing and said nothing. The newer pass runs and the older finishes into a
dropped paint, closing its sockets on the way out.

**The asks are guarded, not only the paint.** The posts stage once followed
the verdict with nothing awaited between them. Reading the mirror scope
opened a window: sign out across it and four asks, two of them to somebody
else's relay, still went out on behalf of an account that had left.

**The local and remote counts run together.** In series the local count,
milliseconds on Vespa, waited on a stranger's relay that `observer_stats`
measured at up to 47.6 s for a single COUNT.

**`seen` is a separate fact from `writeRelays.length`.** A list naming only
`ws://` relays loses every one on an https page, and a list naming only
loopback loses them too; both left an empty array, and the panel told a
reader who had published a relay list that it had never seen one.

**The posts figure says "about".** The denominator is the other relay's
NIP-45 COUNT, and those do not agree with themselves: on one author, kind 1
alone came back as 126,426 against 89,485 for every kind at once. The 35%
reading on a mirror missing nothing is in `relay-maintenance.md`.

**A waiting link reports nothing.** Every case in `chainHtml` reads
`detail`, and a link below the break carries none, so each fell through to
its own healthy branch: "Ranked search … returns results" under a headline
saying search cannot rank at all.

**A dismissal is keyed on the state.** It was not, and only a comment said
it was: one click on "importing — 43%" silenced the panel for six hours,
including for the reader whose import then stopped dead or whose provider
list went away.

**The ready verdict is a cookie; the dismissal is localStorage.** The
cookie's week is enforced by the browser rather than by arithmetic, it
matches `app.js`'s signed-in preference, and it tells the relay nothing a
NIP-42 login did not. `Secure` is set only on https: on an http page the
browser drops the cookie silently and the check ran on every load.

**`whyNotDialable()`'s headline is escaped.** It quotes the field's own
text back. A reader's own typing attacks only themselves, but
`observer_stats.html` accepted that argument once before a stranger's
display name went into `innerHTML` raw.

**The three list events are published together.** In series they were
three full round trips for three independent OKs; NIP-01 never required
waiting for one before sending the next, and the client keys its OK waiters
by event id.

## The provenance row

**The gate is applied here, over whatever filled the array.** "It arrived,
so the reader asked for it" was never a fact: the expansion's companion is
gated but plain recall is not, and a search naming no kinds recalls every
kind, so a stranger's list whose title matched landed beside the delegated
publisher's and collapsed by value into one pill with a count of 2. Anyone
could inflate a publisher's corroboration by signing a list with the same
title.

**Pointers are read by index, not adjacency.** A subject used to arrive
directly behind its pointer. The store now places a spliced member by the
confidence its list expressed, so a doubted member sinks past organic hits
and lands anywhere on the page. Both passes index the whole page first,
which is why that change cost this file nothing.

**The row is filled twice.** The expansion once sent a pointer and
everything it named on one subscription. A search for `kinds:[0]` is now
answered with the profiles found through the lists and not the pointers, so
`app.js` seeds off the answer and again once `shared/pointers.js` has
fetched what the answer left out. Each event is walked once however many
times the array holds it: the second copy, walked as a second pointer,
landed on `count`, and "Verified Human 2" over one list is the false claim
the count exists to avoid.

**Labels now say what has been said, not why a card is here.** A label that
did not match the search never arrived, so the row could only explain
presence. Asked by target, every label the relay holds about it comes back.
That drift is what is left when the relay stops saying which labels matched;
`pointers.js`'s `LABEL_LIMIT` and the collapse bound it, and neither
restores the old meaning. The expansion's own budgets (100 per event, 1,000
per request) and the one batch of 100 targets per filter are the other two
ways the row is partial.

**Collapse is by value, never by event.** Two lists titled "Verified Human"
are one pill with a 2; 66 labels saying "zapped" are one pill with a 66. The
rule took the worst real card from 139 pills to 2.

**`ISO-639-1` and `pub.ditto.trends` are furniture.** `ISO-639-1` is 87% of
the labels on staging. Ditto's trending feed is NIP-32 with
`["l","#p","pub.ditto.trends"]` and forty `p` tags, so the pill reads `#p`
and one post puts it on forty cards. Measured on 2026-09-01 over one page of
40 profiles: 500 events, 2.4 MB, all this namespace, one distinct pill; the
same people have no labels in any other namespace.

**Contributions are emitted, not returned.** A Trusted List carries
thousands of members and a page at most a hundred results; building an array
per list allocated five thousand entries, three times over (filter, map,
filter), to find five matches.

**The NIP-51 people kinds are spliced under the same gate.** Since store
`2bc79f5f40` a list called `Verified Human` answers the query "verified
human" with its members. Anyone may title a list `bitcoin` and name a
thousand accounts, so a list unpacks only for a reader who delegated its
signer, and a reader is always their own.

**A block list draws no pill.** Quartz's `PeopleListEvent.BLOCK_LIST_D_TAG`
is `"mute"`, and `SearchReferences` reads every public `p` off one. An
untitled mute list has no indexed text, so the splice never sends it, but
`shared/pointers.js` asks by member and does. Of 400 kind-30000s sampled on
staging (2026-09-01), 41 carry a block `d`, 9 name people, the largest names
3,980, and two are titled `Mute`, which is why this is a check of its own.

**`d` is the fallback for a Trusted List and not for a NIP-51 one.** A `d`
is never indexed, so never a word anybody searched. For a 30392 an untitled
list is rare. Of the same 400 sampled 30000s, 183 name a person and 53
(29%) carry a title or name; the other 130 would have drawn
`intent-bloom-r0s63o3y-isPlaying`, `chats/null/lastOpened` and
`communities` as provenance on up to 10,934 cards.

**A score is not a reason.** The assertion pill fell back to `rank 92`, or
"scored". A number out of its scale invites a comparison the pill cannot
support, and `shared/avatar.js`'s chip already carries the rank with its
lens; the second spelling crowded the row that explains the other reason a
card is here. All three assertion kinds read `t`, not only 30382.

**Gated pills carry a face only past one publisher.** On staging this
reader's Map names one publisher for lists and one for scores; a face on
every gated pill was the same face forty times down a results list.

**The epoch lives beside its writers.** It was a counter in `app.js`, which
`forgetProvenance` (called from `entity.js`) could not reach: a permalink
opened during a search's second pass cleared the row on the way in and had
it written straight back.
