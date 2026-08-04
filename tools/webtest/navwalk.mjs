// playwright resolves normally when installed; the fallback is the global
// install this repo's cloud sessions carry.
let chromium;
try { ({ chromium } = await import("playwright")); }
catch { ({ chromium } = await import("/opt/node22/lib/node_modules/playwright/index.mjs")); }
import http from 'http';
import fs from 'fs';

const DIR = new URL('../../relay/src/main/resources', import.meta.url).pathname;
// Serve like the relay does: / -> index.html, plus the two stats pages.
const server = http.createServer((req, res) => {
  const path = req.url.split('?')[0];
  // Mirror the relay's routing: NIP-19-shaped paths serve the landing page.
  const file = path === '/' ? 'index.html'
    : /^\/(npub|nprofile|note|nevent|naddr)1[02-9ac-hj-np-z]+$/.test(path) ? 'index.html'
    : path.slice(1);
  try {
    const body = fs.readFileSync(`${DIR}/${file}`);
    // ES modules are MIME-checked by the browser: text/html on a .js file
    // makes every import fail, so serve real content types like the relay does.
    const type = file.endsWith('.js') ? 'text/javascript' : 'text/html';
    res.writeHead(200, { 'content-type': type });
    res.end(body);
  } catch { res.writeHead(404); res.end(); }
});
await new Promise(r => server.listen(7787, r));

const browser = await chromium.launch({});
const page = await browser.newPage();
let pass = 0, fail = 0;
const check = (name, cond) => { cond ? pass++ : fail++; console.log(`${cond ? 'PASS' : 'FAIL'}  ${name}`); };
const url = () => new URL(page.url()).pathname + new URL(page.url()).search;

// 1. Plain load: hero, clean URL.
await page.goto('http://localhost:7787/');
check('load: clean URL', url() === '/');
check('load: hero shown', await page.locator('#results').isHidden());

// Marker to detect any full page reload later.
await page.evaluate(() => { window.__marker = 42; });

// 2. Type + Enter -> full view, ?q= pushed.
await page.fill('#q', 'hello world');
await page.press('#q', 'Enter');
await page.waitForTimeout(300);
check('search: URL carries q', url() === '/?q=hello+world' || url() === '/?q=hello%20world');
check('search: results view shown', await page.locator('#results').isVisible());

// 3. Chip -> tab param pushed.
await page.click('.chip:has-text("Notes")');
await page.waitForTimeout(200);
check('chip: URL carries tab', url().includes('tab=notes') && url().includes('q=hello'));

// 4. Sort + spam -> params pushed.
await page.selectOption('#sort', 'rank');
await page.waitForTimeout(200);
await page.evaluate(() => document.getElementById('spam').click());
await page.waitForTimeout(200);
check('sort+spam in URL', url().includes('sort=rank') && url().includes('spam=1'));

// 5. Back walks the states in order.
await page.goBack(); await page.waitForTimeout(200);
check('back 1: spam undone', !url().includes('spam=1') && url().includes('sort=rank'));
check('back 1: spam checkbox restored', !(await page.isChecked('#spam')));
await page.goBack(); await page.waitForTimeout(200);
check('back 2: sort undone', !url().includes('sort=rank'));
check('back 2: sort select restored', (await page.inputValue('#sort')) === '');
await page.goBack(); await page.waitForTimeout(200);
check('back 3: tab undone', !url().includes('tab='));
check('back 3: chip restored', (await page.locator('.chip.on').textContent()) === 'Everything');
await page.goBack(); await page.waitForTimeout(200);
check('back 4: hero floor, clean URL', url() === '/');
check('back 4: hero shown again', await page.locator('#results').isHidden());
check('back 4: query box emptied', (await page.inputValue('#q')) === '');

// 6. Forward restores the search.
await page.goForward(); await page.waitForTimeout(300);
check('forward: q restored in URL', url().includes('q=hello'));
check('forward: results view back', await page.locator('#results').isVisible());
check('forward: query box refilled', (await page.inputValue('#q')) === 'hello world');

// 7. Clear button is a navigation: hero pushed, Back returns to the search.
await page.click('#clear');
await page.waitForTimeout(200);
check('clear: clean URL pushed', url() === '/');
check('clear: hero shown', await page.locator('#results').isHidden());
await page.goBack(); await page.waitForTimeout(300);
check('back after clear: search restored', url().includes('q=hello') && await page.locator('#results').isVisible());

// 8. Brand click resets in place — no reload (marker survives).
await page.click('.brand');
await page.waitForTimeout(200);
check('brand: clean URL, hero', url() === '/' && await page.locator('#results').isHidden());
check('brand: no page reload', (await page.evaluate(() => window.__marker)) === 42);

// 9. Deep link restores every control.
const as = 'npub1sg6plzptd64u62a878hep2kev88swjh3tw00gjsfl8f237lmu63q0uf63m';
await page.goto(`http://localhost:7787/?q=alice&tab=people&sort=followers&spam=1&as=${as}`);
await page.waitForTimeout(300);
check('deep link: query box', (await page.inputValue('#q')) === 'alice');
check('deep link: chip', (await page.locator('.chip.on').textContent()) === 'People');
check('deep link: sort', (await page.inputValue('#sort')) === 'followers');
check('deep link: spam', await page.isChecked('#spam'));
check('deep link: results view', await page.locator('#results').isVisible());
check('deep link: lens active', await page.evaluate(() => document.getElementById('obsbox').classList.contains('active')));
check('deep link: lens label npub not hex', (await page.textContent('#obscurrent')).startsWith('npub1'));

// 10. Bad ?as= falls back to "me" instead of a dead lens.
await page.goto('http://localhost:7787/?q=x&as=npub1garbagegarbagegarbage');
await page.waitForTimeout(200);
check('bad as: ignored', (await page.textContent('#obscurrent')) === 'me');

// 11. Typing (popup path) must NOT touch the URL.
await page.goto('http://localhost:7787/');
await page.fill('#q', 'typed only');
await page.waitForTimeout(400);
check('popup typing: URL untouched', url() === '/');

// 12. Unknown tab/sort params degrade to defaults.
await page.goto('http://localhost:7787/?q=x&tab=bogus&sort=bogus');
await page.waitForTimeout(200);
check('unknown tab -> Everything', (await page.locator('.chip.on').textContent()) === 'Everything');
check('unknown sort -> best match', (await page.inputValue('#sort')) === '');

// 13. kind_stats: ?kinds= pre-fills, run canonicalises via replaceState.
await page.goto('http://localhost:7787/kind_stats.html?kinds=1063,9735');
await page.waitForTimeout(400);
check('kind_stats: box pre-filled', (await page.inputValue('#extra')) === '1063,9735');
check('kind_stats: URL kept', url() === '/kind_stats.html?kinds=1063,9735');
await page.fill('#extra', ' 7 , 9735, banana, 0 ');   // 0 is already in the fixed list
await page.click('#run');
await page.waitForTimeout(400);
check('kind_stats: URL canonicalised on run', url() === '/kind_stats.html?kinds=7,9735');
const histLen = await page.evaluate(() => history.length);
await page.fill('#extra', '');
await page.click('#run');
await page.waitForTimeout(400);
check('kind_stats: empty box cleans URL', url() === '/kind_stats.html');
check('kind_stats: replace, not push', (await page.evaluate(() => history.length)) === histLen);

// 14. Entity pages: a valid note1 URL renders the entity view.
// Build a REAL identifier with the page's own encoder, so the checksum passes.
await page.goto('http://localhost:7787/');
const realNote = await page.evaluate(async () => {
  const m = await import('/web/shared/nip19.js');
  return m.noteId('ab'.repeat(32));
});
await page.goto(`http://localhost:7787/${realNote}`);
await page.waitForSelector('#results .error', { timeout: 10000 }).catch(() => {});
check('entity: head rendered', await page.locator('.entity-head').isVisible());
check('entity: njump escape hatch', (await page.locator('.entity-head a[href*="njump.me"]').count()) === 1);
check('entity: relay unreachable -> honest error', (await page.locator('#results .error').count()) === 1);
check('entity: URL untouched', url() === `/${realNote}`);

// 15. Invalid checksum -> "not a valid identifier", not a blank page.
await page.goto('http://localhost:7787/note1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq');
await page.waitForTimeout(300);
check('entity: bad checksum says so', (await page.textContent('#results')).includes('Not a valid NIP-19 identifier'));

// 16. Internal link click: search -> entity -> Back restores the search.
await page.goto('http://localhost:7787/?q=hello&tab=notes');
await page.waitForTimeout(300);
await page.evaluate((href) => {
  const a = document.createElement('a');
  a.href = '/' + href;
  a.textContent = 'x';
  document.querySelector('#results').appendChild(a);
  a.click();
}, realNote);
await page.waitForTimeout(300);
check('ilink: URL is the entity', url() === `/${realNote}`);
check('ilink: entity view shown', await page.locator('.entity-head').isVisible());
check('ilink: no reload', (await page.evaluate(() => history.length)) > 1);
await page.goBack();
await page.waitForTimeout(300);
check('ilink back: search restored with state', url() === '/?q=hello&tab=notes' && (await page.inputValue('#q')) === 'hello');
check('ilink back: results view', await page.locator('#results').isVisible() && (await page.locator('.entity-head').count()) === 0);

// 17. "← Search" out of an entity page pushes the hero.
await page.goto(`http://localhost:7787/${realNote}`);
await page.waitForTimeout(300);
await page.click('.entity-head .back-home');
await page.waitForTimeout(200);
check('entity home: clean URL', url() === '/');
check('entity home: hero shown', await page.locator('#results').isHidden());
await page.goBack();
await page.waitForTimeout(300);
check('entity home back: entity again', url() === `/${realNote}` && await page.locator('.entity-head').isVisible());

// 18. Searching FROM an entity page lands the search at the root.
await page.goto(`http://localhost:7787/${realNote}`);
await page.waitForTimeout(300);
await page.fill('#q', 'from entity');
await page.press('#q', 'Enter');
await page.waitForTimeout(300);
check('search from entity: rooted URL', url() === '/?q=from+entity');
check('search from entity: results view', (await page.locator('.entity-head').count()) === 0);

console.log(`\n${pass} passed, ${fail} failed`);
await browser.close();
server.close();
process.exit(fail ? 1 : 0);
