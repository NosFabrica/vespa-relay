// A Vespa that answers everything except the monitor's own-verdict walk: any
// /search/ whose body names kind 30166 is held open forever. Staging's shape
// (#167: deadline-less store reads ahead of the retire walk), reproduced on
// the one query that matters. Speaks h2c prior-knowledge (the feed client)
// and HTTP/1.1 (the query client), forwarding each in kind.
import http2 from "node:http2";
import http from "node:http";
import net from "node:net";
import { Duplex } from "node:stream";
const UP = "127.0.0.1:8080";
let held = 0;
const h2 = http2.connect(`http://${UP}`);
h2.on("error", (e) => console.error("upstream h2 error", e.message));
const drop = (h) => Object.fromEntries(Object.entries(h).filter(([k]) => !k.startsWith(":") && !["connection", "host", "transfer-encoding", "keep-alive", "upgrade", "http2-settings"].includes(k)));
const handler = (req, res) => {
  const chunks = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", () => {
    const body = Buffer.concat(chunks);
    if (req.url.startsWith("/search/") && body.includes("30166")) {
      held++;
      console.error(`tarpit: holding ${req.method} ${req.url} (#${held}, http/${req.httpVersion}) — ${body.toString().slice(0, 160)}`);
      return; // never answered
    }
    if (req.httpVersion === "2.0") {
      const s = h2.request({ ":method": req.method, ":path": req.url, ...drop(req.headers) }, { endStream: body.length === 0 });
      s.on("response", (h) => res.writeHead(Number(h[":status"]), drop(h)));
      s.on("error", (e) => { try { res.writeHead(502); res.end(String(e)); } catch {} });
      s.pipe(res);
      if (body.length) s.end(body);
    } else {
      const [host, port] = UP.split(":");
      const u = http.request({ host, port, method: req.method, path: req.url, headers: drop(req.headers) }, (r) => { res.writeHead(r.statusCode, drop(r.headers)); r.pipe(res); });
      u.on("error", (e) => { res.writeHead(502); res.end(String(e)); });
      u.end(body);
    }
  });
};
const h2srv = http2.createServer(handler);
const h1srv = http.createServer(handler);
// Sniff the preface: node's cleartext http2 server does not, so h2c prior
// knowledge and HTTP/1.1 each get their own server behind one port.
net.createServer((socket) => {
  socket.on("error", (e) => console.error("socket error", e.message));
  socket.once("data", (buf) => {
    if (!buf.toString("latin1", 0, 24).startsWith("PRI * HTTP/2.0")) {
      socket.pause();
      socket.unshift(buf);
      h1srv.emit("connection", socket);
      socket.resume();
      return;
    }
    // http2 takes over a net.Socket's native handle, so bytes already read
    // are lost to it — hand it a plain Duplex instead, with the preface first.
    const wrap = new Duplex({
      read() {},
      write(c, _e, cb) { socket.write(c, cb); },
      final(cb) { socket.end(cb); },
    });
    wrap.push(buf);
    socket.on("data", (d) => wrap.push(d));
    socket.on("end", () => wrap.push(null));
    socket.on("close", () => wrap.destroy());
    wrap.on("close", () => socket.destroy());
    h2srv.emit("connection", wrap);
  });
}).listen(18080, () => console.error("tarpit: listening on 18080 (h2c + http/1.1), upstream " + UP));
