// A websocket client that completes the handshake, subscribes, then never reads again, so the relay's
// outbound queue for it only grows; optionally publishes big events from a second, healthy socket.
//   node stalled-client.mjs <ws-url> <publish-count> <content-bytes> <rate-per-second>
// Prints when the stalled socket was opened and when its TCP connection ended, as seen from here.
import net from "node:net";
import { event, keypair } from "../nostr-sign.mjs";

const [url, countS = "0", sizeS = "60000", rateS = "10"] = process.argv.slice(2);
const { hostname, port } = new URL(url);
const t0 = Date.now();
const at = () => ((Date.now() - t0) / 1000).toFixed(1) + "s";

const frame = (text) => {
  const payload = Buffer.from(text), mask = Buffer.from([1, 2, 3, 4]);
  const len = payload.length;
  const head = len < 126 ? Buffer.from([0x81, 0x80 | len]) : len < 65536 ? Buffer.from([0x81, 0x80 | 126, len >> 8, len & 255]) : null;
  if (!head) throw new Error("too big for this client");
  return Buffer.concat([head, mask, Buffer.from(payload.map((b, i) => b ^ mask[i % 4]))]);
};

const stalled = net.connect(Number(port), hostname, () => {
  stalled.write(`GET / HTTP/1.1\r\nHost: ${hostname}:${port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n`);
  stalled.write(frame(JSON.stringify(["REQ", "s", { kinds: [1], limit: 1, search: "include:spam" }])));
  console.log(`${at()} stalled socket open and subscribed; it reads nothing from here on`);
  setTimeout(() => stalled.pause(), 200);
});
stalled.setNoDelay(true);
// A paused socket hears no FIN, but a reset or a write after the close surfaces as an error or close.
stalled.on("error", (e) => console.log(`${at()} stalled socket error: ${e.code}`));
stalled.on("close", () => { console.log(`${at()} stalled socket closed`); process.exit(0); });

const count = Number(countS);
if (count > 0) {
  const key = keypair();
  const content = "x".repeat(Number(sizeS));
  const pub = new WebSocket(url);
  pub.onopen = async () => {
    for (let i = 0; i < count; i++) {
      pub.send(JSON.stringify(["EVENT", event(key, 1, [], `${i} ${content}`)]));
      await new Promise((r) => setTimeout(r, 1000 / Number(rateS)));
    }
    console.log(`${at()} published ${count} events of ${sizeS} bytes`);
  };
}
setTimeout(() => { console.log(`${at()} still open after 150 s`); process.exit(0); }, 150_000);
