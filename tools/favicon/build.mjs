// The relay's favicon, generated from the same geometry the page's brand mark
// draws — plain node, no dependencies, matching tools/webtest.
//
//     node tools/favicon/build.mjs              # rewrite the two artifacts
//     node tools/favicon/build.mjs --preview D  # …and 8x blow-ups into D
//
// Writes relay/src/main/resources/web/favicon.svg and favicon.ico. Both are
// COMMITTED: the resources directory is the whole deployment (no build step for
// the web side at all), and a Gradle task that shelled out to node would put a
// node on the build machine's critical path to serve a 3KB icon.
//
// Two things make this more than "export the mark as an .ico", and both are
// visible at 16 pixels:
//
//   - The mark is drawn in `currentColor` at 1.7-unit strokes in a 24-unit box.
//     A tab strip is not the page, so there is no `currentColor` there to
//     inherit — and a transparent icon has to survive a light strip and a dark
//     one with the same ink, which no single blue does. So it sits on an accent
//     TILE and the ink is white: the background is ours in both themes.
//   - 1.7 units is 0.9 device pixels at 16px, which antialiases into a smudge,
//     so the favicon's stroke is 2.6 and its margin is smaller than the page
//     mark's. That is a deliberate divergence, not drift — see STROKE.
//
// Each ICO size is rendered at its own resolution rather than downsampled from
// the largest, which is the other thing a single exported bitmap cannot do: the
// coverage at 16px is computed against the geometry, not against 48px of
// already-antialiased pixels blurred a second time.
//
// The geometry below is index.html's `.mark` verbatim. Keep it that way —
// RelayFaviconTest reads both and fails if a circle or a link moves in one and
// not the other.
import { deflateSync } from "node:zlib";
import { writeFileSync, mkdirSync } from "node:fs";
import path from "node:path";

const OUT = "relay/src/main/resources/web";

// index.html's `.mark`, in its own 24-unit box: a filled hub, three ringed
// nodes, three links. `links` are the `M6.1 7.5 9.4 10.2`-style paths as
// endpoint pairs — every one of them is a single straight segment with round
// caps, which is why they can be numbers here and still be compared to the
// markup as strings by the test.
const MARK = {
  hub: { cx: 12, cy: 12, r: 3.2 },
  nodes: [
    { cx: 4.2, cy: 6.4, r: 2.1 },
    { cx: 4.2, cy: 17.6, r: 2.1 },
    { cx: 19.8, cy: 12, r: 2.1 },
  ],
  links: [
    [6.1, 7.5, 9.4, 10.2],
    [6.1, 16.5, 9.4, 13.8],
    [14.9, 12, 17.7, 12],
  ],
};

// --accent from index.html's light palette, and the tile is why it can be the
// light one: the ink is white on that tile in EVERY theme, so the page's dark
// --accent (#7aa2ff, picked to sit on a dark page) has nothing to do here.
const ACCENT = [0x25, 0x63, 0xeb];
const INK = [0xff, 0xff, 0xff];

// The glyph inside the tile: scaled about the centre, leaving a margin. The
// page mark breathes because it sits beside a wordmark; a favicon has 16 pixels
// and nothing beside it, so the margin is the first thing to spend. 0.72 (an
// app-icon-ish inset) loses the ring holes at 16px; 0.84 gets them back but
// walks the left-hand node's ring into the tile's rounded corner. 0.80 is the
// one that keeps both.
const GLYPH_SCALE = 0.8;
// A full-bleed rounded square, 21.7% radius — the proportion iOS and every
// launcher grid already round app icons to, so the icon reads as an icon rather
// than as a screenshot of a blue box.
const TILE_RADIUS = 5.2;

// Stroke weight, in the mark's own 24-unit space, for BOTH artifacts — so the
// SVG a browser scales and the ICO it picks a size from are the same drawing.
//
// A stroke of s units lands at s * GLYPH_SCALE * size / 24 device pixels, so
// the mark's own 1.7 would be 0.91px at 16 — under the ~1.4px floor where a
// stroke stops being a line and becomes a grey suggestion. 2.6 buys 1.39px at
// 16 and 4.16px at 48, which is heavy for the mark and right for a badge. The
// nodes' rings survive it at 16 by one pixel of hole, which is what fixed
// GLYPH_SCALE at 0.80 above.
const STROKE = 2.6;
// The ICO's sizes. 16 and 32 are the tab strip at 1x and 2x; 48 is what Windows
// puts on a desktop shortcut and what Chrome reaches for in its bookmark
// manager. Nothing larger — an .ico is a fallback, and the SVG beside it is
// what a browser that can pick will pick.
const ICO_SIZES = [16, 32, 48];

// ---- rasteriser ----------------------------------------------------------
// Analytic coverage by supersampling: 8x8 samples per pixel, each either inside
// a shape or not. Shapes are unioned rather than painted in order, so the links
// meeting the hub do not double-darken their overlap — every sample answers one
// boolean per layer and the layer answers a coverage fraction.
const SS = 8;

const dist = (ax, ay, bx, by) => Math.hypot(ax - bx, ay - by);

/** Distance from (px,py) to the segment (x0,y0)-(x1,y1) — the round-capped link. */
function distToSegment(px, py, x0, y0, x1, y1) {
  const dx = x1 - x0;
  const dy = y1 - y0;
  const len2 = dx * dx + dy * dy;
  const t = len2 === 0 ? 0 : Math.max(0, Math.min(1, ((px - x0) * dx + (py - y0) * dy) / len2));
  return dist(px, py, x0 + t * dx, y0 + t * dy);
}

/** Is (x,y) — in tile units — inside the full-bleed rounded square? */
function inTile(x, y) {
  const r = TILE_RADIUS;
  if (x < 0 || y < 0 || x > 24 || y > 24) return false;
  // Only the four corner squares can be outside; everywhere else the rounded
  // rect and the rect agree.
  const cx = x < r ? r : x > 24 - r ? 24 - r : x;
  const cy = y < r ? r : y > 24 - r ? 24 - r : y;
  return dist(x, y, cx, cy) <= r;
}

/** Is (x,y) — in the MARK's own units — ink, at this stroke weight? */
function inGlyph(x, y, stroke) {
  const half = stroke / 2;
  if (dist(x, y, MARK.hub.cx, MARK.hub.cy) <= MARK.hub.r) return true;
  for (const n of MARK.nodes) {
    // A ring, not a disc: r=2.1 with a 2.6 stroke leaves 0.8 units of hole,
    // which is the last thing to go at 16px and the reason it is still a
    // network of nodes there rather than three dots.
    if (Math.abs(dist(x, y, n.cx, n.cy) - n.r) <= half) return true;
  }
  for (const [x0, y0, x1, y1] of MARK.links) {
    if (distToSegment(x, y, x0, y0, x1, y1) <= half) return true;
  }
  return false;
}

/** The icon at `size`, as straight-alpha RGBA bytes. */
function render(size, stroke) {
  const px = new Uint8Array(size * size * 4);
  const toTile = 24 / size;
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      let tile = 0;
      let ink = 0;
      for (let sy = 0; sy < SS; sy++) {
        for (let sx = 0; sx < SS; sx++) {
          const tx = (x + (sx + 0.5) / SS) * toTile;
          const ty = (y + (sy + 0.5) / SS) * toTile;
          if (!inTile(tx, ty)) continue;
          tile++;
          // Tile units -> the mark's own units: the glyph is scaled about the
          // centre of the tile, so the inverse is too.
          const gx = 12 + (tx - 12) / GLYPH_SCALE;
          const gy = 12 + (ty - 12) / GLYPH_SCALE;
          if (inGlyph(gx, gy, stroke)) ink++;
        }
      }
      const n = SS * SS;
      const a = tile / n;
      const k = ink / n;
      const i = (y * size + x) * 4;
      // The glyph never reaches the tile's antialiased edge, so alpha is the
      // tile's coverage alone and the ink only mixes the colour. Written as a
      // mix rather than a second composite because straight alpha over a
      // partially transparent pixel is where premultiplication bugs live.
      for (let c = 0; c < 3; c++) px[i + c] = Math.round(ACCENT[c] + (INK[c] - ACCENT[c]) * (a === 0 ? 0 : k / a));
      px[i + 3] = Math.round(a * 255);
    }
  }
  return px;
}

// ---- PNG -----------------------------------------------------------------
function crc32(buf) {
  let c = ~0;
  for (const b of buf) {
    c ^= b;
    for (let k = 0; k < 8; k++) c = (c >>> 1) ^ (0xedb88320 & -(c & 1));
  }
  return ~c >>> 0;
}

function chunk(type, data) {
  const head = Buffer.alloc(8);
  head.writeUInt32BE(data.length, 0);
  head.write(type, 4, "ascii");
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(Buffer.concat([head.subarray(4), data])), 0);
  return Buffer.concat([head, data, crc]);
}

/** 8-bit RGBA, one filter byte per row, all zero — these are 48 pixels wide. */
function png(size, rgba) {
  const raw = Buffer.alloc(size * (size * 4 + 1));
  for (let y = 0; y < size; y++) {
    raw[y * (size * 4 + 1)] = 0;
    Buffer.from(rgba.buffer, y * size * 4, size * 4).copy(raw, y * (size * 4 + 1) + 1);
  }
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(size, 0);
  ihdr.writeUInt32BE(size, 4);
  ihdr[8] = 8; // bit depth
  ihdr[9] = 6; // colour type: RGBA
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    chunk("IHDR", ihdr),
    // Level 9 and no timestamp anywhere in the format, so the file is a pure
    // function of the geometry — re-running this must not produce a diff.
    chunk("IDAT", deflateSync(raw, { level: 9 })),
    chunk("IEND", Buffer.alloc(0)),
  ]);
}

// ---- ICO -----------------------------------------------------------------
// PNG-compressed entries rather than the BMP/DIB ones the format was born with:
// every browser since IE11 reads them, and the alternative is ~15KB of
// uncompressed 32bpp bitmaps plus an AND mask that has to be right for the
// transparency to survive.
function ico(images) {
  const dir = Buffer.alloc(6 + 16 * images.length);
  dir.writeUInt16LE(0, 0);
  dir.writeUInt16LE(1, 2); // 1 = icon
  dir.writeUInt16LE(images.length, 4);
  let offset = dir.length;
  for (const [i, img] of images.entries()) {
    const e = 6 + 16 * i;
    dir[e] = img.size === 256 ? 0 : img.size; // 0 means 256 in a one-byte field
    dir[e + 1] = img.size === 256 ? 0 : img.size;
    dir[e + 2] = 0; // palette size: none
    dir[e + 3] = 0;
    dir.writeUInt16LE(1, e + 4); // colour planes
    dir.writeUInt16LE(32, e + 6); // bits per pixel
    dir.writeUInt32LE(img.png.length, e + 8);
    dir.writeUInt32LE(offset, e + 12);
    offset += img.png.length;
  }
  return Buffer.concat([dir, ...images.map((i) => i.png)]);
}

// ---- SVG -----------------------------------------------------------------
// The mark's numbers verbatim inside a transform, rather than pre-multiplied
// coordinates: this file has to stay diffable against index.html by eye, and
// RelayFaviconTest compares the two element by element.
function svg() {
  const circle = (o, extra = "") => `    <circle cx="${o.cx}" cy="${o.cy}" r="${o.r}"${extra} />`;
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" role="img" aria-label="SearchOverTrust">
  <!-- Generated by tools/favicon/build.mjs from index.html's brand mark. Do not
       hand-edit: the generator also writes favicon.ico, and a test pins the two
       against the mark. -->
  <rect width="24" height="24" rx="${TILE_RADIUS}" fill="#2563eb" />
  <g transform="translate(12 12) scale(${GLYPH_SCALE}) translate(-12 -12)"
     fill="none" stroke="#fff" stroke-width="${STROKE}">
${circle(MARK.hub, ' fill="#fff" stroke="none"')}
${MARK.nodes.map((n) => circle(n)).join("\n")}
    <path d="${MARK.links.map(([x0, y0, x1, y1]) => `M${x0} ${y0} ${x1} ${y1}`).join("")}" stroke-linecap="round" />
  </g>
</svg>
`;
}

// ---- write ---------------------------------------------------------------
const images = ICO_SIZES.map((size) => ({ size, png: png(size, render(size, STROKE)) }));
const icoBytes = ico(images);
writeFileSync(path.join(OUT, "favicon.ico"), icoBytes);
writeFileSync(path.join(OUT, "favicon.svg"), svg());
console.log(`favicon.ico  ${icoBytes.length} bytes  (${ICO_SIZES.join(", ")})`);
console.log(`favicon.svg  ${svg().length} bytes`);

// A 16px icon cannot be judged at 16px. `--preview D` writes each size blown up
// 8x with nearest-neighbour sampling, so what lands on the tab strip is what is
// being looked at rather than something a viewer resampled.
const previewAt = process.argv.indexOf("--preview");
if (previewAt !== -1 && process.argv[previewAt + 1]) {
  const dir = process.argv[previewAt + 1];
  mkdirSync(dir, { recursive: true });
  for (const { size } of images) {
    const src = render(size, STROKE);
    const z = 8;
    const big = new Uint8Array(size * z * size * z * 4);
    for (let y = 0; y < size * z; y++) {
      for (let x = 0; x < size * z; x++) {
        const s = (Math.floor(y / z) * size + Math.floor(x / z)) * 4;
        const d = (y * size * z + x) * 4;
        for (let c = 0; c < 4; c++) big[d + c] = src[s + c];
      }
    }
    const out = path.join(dir, `favicon-${size}@8x.png`);
    writeFileSync(out, png(size * z, big));
    console.log(`preview      ${out}`);
  }
}
