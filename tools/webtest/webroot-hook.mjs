// The resolver half of `webroot.mjs`, in its own file because Node runs a
// registered hook on a separate thread and loads it by specifier.
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";

let roots = [];

export function initialize(data) {
  roots = data || [];
}

export function resolve(specifier, context, next) {
  // Only the server-absolute form. A relative import inside one module
  // resolves normally, and a bare specifier is a package.
  if (specifier.startsWith("/web/")) {
    for (const root of roots) {
      const url = new URL(specifier.slice(1), root);
      if (existsSync(fileURLToPath(url))) return { url: url.href, shortCircuit: true };
    }
  }
  // …and the case that made this necessary: a RELATIVE import that leaves its
  // own module. `web/shared/conn.js` ships in :relay and imports `./relay.js`,
  // which ships in :web — same url to a browser, two directories on disk.
  if (specifier.startsWith(".")) {
    const target = new URL(specifier, context.parentURL);
    if (!existsSync(fileURLToPath(target))) {
      for (const root of roots) {
        if (!target.href.includes("/src/main/resources/")) break;
        const rel = target.href.split("/src/main/resources/")[1];
        const url = new URL(rel, root);
        if (existsSync(fileURLToPath(url))) return { url: url.href, shortCircuit: true };
      }
    }
  }
  return next(specifier, context);
}
