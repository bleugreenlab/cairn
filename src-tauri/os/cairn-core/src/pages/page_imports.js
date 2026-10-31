// How a page's relative imports become `page:<path>` specifiers: the one
// implementation both runtimes that load a page share. The frame's shell
// document inlines this script ahead of its bootstrap (`pages_shell.js`), whose
// import map points each `page:` specifier at that file's blob; the headless
// host a rendered read runs (`packages/page-kit/headless/host.ts`) evaluates the
// same text and serves `page:` specifiers from the page's files.
//
// A classic script, not a module, so the shell can inline it under its nonce.
// The rewrite stays on the line it rewrites, so a stack's file:line is the
// author's own.
const PAGE_IMPORTS = (() => {
  "use strict";

  const PAGE = "page:";

  // `./a.js` and `../b/c.js` resolve against the importing file's directory;
  // `/a.js` against the page root. A path that climbs above the root is left
  // as a `page:` specifier with no mapping, which fails with a clear error.
  const resolvePagePath = (fromPath, specifier) => {
    const segments = specifier.startsWith("/") ? [] : fromPath.split("/").slice(0, -1);
    for (const segment of specifier.split("/")) {
      if (segment === "" || segment === ".") continue;
      if (segment === "..") {
        if (segments.length === 0) return `../${specifier}`;
        segments.pop();
        continue;
      }
      segments.push(segment);
    }
    return segments.join("/");
  };

  const RELATIVE_SPECIFIER = /(\bfrom\s*|\bimport\s*\(\s*|\bimport\s+)(["'])(\.{1,2}\/[^"'\n]*|\/(?!\/)[^"'\n]*)\2/g;

  const rewritePageImports = (source, fromPath) =>
    source.replace(
      RELATIVE_SPECIFIER,
      (_match, lead, quote, specifier) => `${lead}${quote}${PAGE}${resolvePagePath(fromPath, specifier)}${quote}`,
    );

  // What a page reads instead of the kit's retired arbitrary-resource reads
  // (tenet #1312). A page still importing one fails at mount, and both
  // loaders turn the engine's bare missing-export error into this.
  const ROWS_ONLY =
    "a page reads only database rows. Read the thread's tables with useRows(table, {where, order, limit, offset}) and useRow(table, pk), the app database read-only with useRows(table, options, {store: \"app\"}), and SQL over either with useQuery(sql, {store}). Anything else (an https API, vcs history, transcripts) reaches a page as rows a service writes: a service method, called on demand or on the service's schedule, writes rows to the thread's database, and the page shows them. See the pages skill.";

  // `subscribe` is a common word, so it is matched only as a named import.
  const RETIRED = [
    { name: "useRead", pattern: /\buseRead\b/ },
    { name: "useSubscribe", pattern: /\buseSubscribe\b/ },
    { name: "subscribe", pattern: /['"`]subscribe['"`]/ },
  ];
  const MISSING = /export|binding|not a function|not found|not defined|undefined is not/i;

  // The instructive message for a load or render failure that names a
  // retired kit export, or null for any other failure.
  const explainRetiredImport = (message) => {
    const text = String(message);
    if (!MISSING.test(text)) return null;
    const retired = RETIRED.find(({ pattern }) => pattern.test(text));
    return retired ? `\`${retired.name}\` was retired from @cairn/page-kit: ${ROWS_ONLY} (${text})` : null;
  };

  return Object.freeze({ PAGE, resolvePagePath, rewritePageImports, explainRetiredImport });
})();
