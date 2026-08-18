// RESOLVE `/web/…` THE WAY THE SERVER DOES — off the classpath, not off one
// module's resource directory.
//
// The pages import their modules by absolute url (`/web/shared/page.js`), and
// `WebAssets` answers those out of whichever jar ships the file: the shared
// ones from :web, each service's own from its own module. That is a url
// question, not a module question — which is the whole reason the asset route
// looks things up on the classpath.
//
// Node resolves an import against the FILE it appears in, so a module in
// :relay that imports one in :web is a path that does not exist on disk. This
// hook does for the test what the classpath does for the server: try each
// module's resource root in turn, first hit wins. Registered by `run.mjs` for
// every suite; a suite run directly needs
//
//     node --import ./tools/webtest/webroot.mjs tools/webtest/<suite>.test.mjs
//
// The roots are ordered most-specific-first so a service's own asset shadows a
// shared one of the same name, exactly as the classpath would.
import { register } from "node:module";
import { pathToFileURL } from "node:url";

const HERE = new URL("../../", import.meta.url);
const ROOTS = ["relay", "sync", "monitor", "web"].map((m) => new URL(`${m}/src/main/resources/`, HERE));

register("./webroot-hook.mjs", import.meta.url, { data: ROOTS.map((u) => u.href) });
