#!/usr/bin/env node
// sync-docs.mjs — import the repo's canonical markdown into website/docs/.
//
// The repo's docs/*.md and the component/demo README.md files are the SOURCE OF
// TRUTH; the site is a projection of them. This script copies each into
// website/docs/ (the Docusaurus content root) and rewrites its relative links so
// they resolve in the site's routing model:
//
//   - a link to another IMPORTED doc  -> the Docusaurus doc route (/docs/<id>)
//   - a link to a repo file NOT on the site (source code, a Dockerfile, a fixture,
//     the schema) -> an absolute GitHub blob/tree URL on the public repo
//
// It also injects Docusaurus frontmatter (title + sidebar slug) so the first H1
// isn't duplicated and the URL is stable. Run it via `npm run sync` (wired into
// prebuild), so a docs edit in the repo flows to the site with one command and
// never drifts.
//
// This is a build-time projector, not a watcher: re-run it whenever the source
// docs change (the prebuild hook does this automatically before every build).

import { promises as fs, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const websiteDir = path.resolve(__dirname, '..');
const repoRoot = path.resolve(websiteDir, '..');
const outDir = path.join(websiteDir, 'docs');

// The public repo, for rewriting off-site file links to absolute GitHub URLs.
const GH = 'https://github.com/salesforce/converge/blob/main';
const GH_TREE = 'https://github.com/salesforce/converge/tree/main';

// Repo assets (docs/assets/*) are copied into the site under static/img/ (served
// at /img/<name>), so an image/link pointing at one is rewritten to that on-site
// path — NOT a GitHub blob URL, which would render an HTML page, not the raw
// image, and show a broken <img> on the site. Keep this list in sync with the
// `cp docs/assets/* website/static/img/` set (the assets the site actually ships).
const SITE_ASSETS = new Set([
  'docs/assets/logo.png',
  'docs/assets/demo.gif',
  'docs/assets/topology.svg',
  'docs/assets/ha-dr.svg',
  'docs/assets/reconcile-flow.gif',
]);

// The projection map: each entry copies one source markdown file to one site doc.
//   src   — path relative to the repo root
//   id    — the Docusaurus doc id (its path under website/docs, minus .md); this
//           is also the sidebar item id and the URL slug (/docs/<id>)
//   title — the sidebar/nav label + page <title>
// The order here mirrors sidebars.ts.
const DOCS = [
  // Getting started — intro is the README, reframed as the docs landing page.
  { src: 'README.md', id: 'intro', title: 'Introduction' },
  { src: 'docs/features.md', id: 'features', title: 'Features at a glance' },
  { src: 'docs/install.md', id: 'install', title: 'Install & run' },

  // Guides
  { src: 'docs/why-code-not-yaml.md', id: 'why-code-not-yaml', title: 'Why code, not YAML' },
  { src: 'docs/implementing-a-provider.md', id: 'implementing-a-provider', title: 'Implementing a provider' },
  { src: 'docs/tls.md', id: 'tls', title: 'TLS, mTLS & SPIFFE' },
  { src: 'docs/ha-dr.md', id: 'ha-dr', title: 'High availability & DR' },

  // Reference
  { src: 'docs/architecture.md', id: 'architecture', title: 'Architecture & internals' },
  { src: 'docs/how-converge-compares.md', id: 'how-converge-compares', title: 'How Converge compares' },
  { src: 'docs/spec/sdk-spec.md', id: 'spec/sdk-spec', title: 'Worker SDK parity spec' },

  // Components
  { src: 'cmd/converge/README.md', id: 'components/converge', title: 'converge (server)' },
  { src: 'cmd/conctl/README.md', id: 'components/conctl', title: 'conctl (CLI)' },
  { src: 'cmd/stdworker/README.md', id: 'components/stdworker', title: 'stdworker' },
  { src: 'sdk-go/converge/README.md', id: 'components/sdk-go', title: 'Go worker SDK' },
  { src: 'sdk-ts/README.md', id: 'components/sdk-ts', title: 'TypeScript worker SDK' },
  { src: 'deploy/helm/converge/README.md', id: 'components/helm', title: 'Helm chart' },

  // Demos
  { src: 'examples/demos/classic/README.md', id: 'demos/classic', title: 'Classic composition' },
  { src: 'examples/demos/datadriven/README.md', id: 'demos/datadriven', title: 'Data-driven composition' },
  { src: 'examples/demos/gitops/README.md', id: 'demos/gitops', title: 'GitOps sync' },
  { src: 'examples/demos/stdio/README.md', id: 'demos/stdio', title: 'stdio (any language)' },
  { src: 'examples/demos/stdshell/README.md', id: 'demos/stdshell', title: 'Shell' },
  { src: 'examples/demos/stdterraform/README.md', id: 'demos/stdterraform', title: 'Terraform' },
  { src: 'examples/demos/typescript/README.md', id: 'demos/typescript', title: 'TypeScript worker' },
];

// Build a lookup from a repo-root-relative source path -> its site doc id, so a
// link pointing at another imported doc can be rewritten to /docs/<id>.
const srcToId = new Map(DOCS.map((d) => [normalize(d.src), d.id]));

function normalize(p) {
  return path.normalize(p).replace(/\\/g, '/');
}

// resolveLink turns one relative link target (as written in `srcAbsPath`) into
// its site-correct form. Returns { href } — either a /docs/... route (imported
// doc), an absolute GitHub URL (off-site repo file), or the original (already
// absolute / anchor-only / mailto).
function resolveLink(target, srcAbsPath) {
  // Leave absolute URLs, anchors, and mailto untouched.
  if (/^(https?:)?\/\//.test(target) || target.startsWith('#') || target.startsWith('mailto:')) {
    return target;
  }

  // Split off any #fragment so we can preserve it after rewriting the path.
  const hashIdx = target.indexOf('#');
  const rawPath = hashIdx >= 0 ? target.slice(0, hashIdx) : target;
  const frag = hashIdx >= 0 ? target.slice(hashIdx) : '';

  // A pure fragment (no path) — same-page anchor.
  if (rawPath === '') return target;

  // Resolve the link relative to the SOURCE file's directory, then make it
  // repo-root-relative — that's the key we match against the projection map.
  const srcDir = path.dirname(srcAbsPath);
  const absTarget = path.resolve(srcDir, rawPath);
  const repoRel = normalize(path.relative(repoRoot, absTarget));

  // If it climbed above the repo root, we can't rewrite it — leave as-is.
  if (repoRel.startsWith('..')) return target;

  // 0) Points at a bundled asset (a diagram/gif) -> the on-site /img/ copy, so
  //    the image actually renders (a GitHub blob URL would be an HTML page).
  //    baseUrl is prepended by Docusaurus at render time, so a root-absolute
  //    /img/... path is correct under the /converge/ project sub-path.
  if (SITE_ASSETS.has(repoRel)) {
    return `/img/${path.basename(repoRel)}${frag}`;
  }
  // 1) Points at another IMPORTED doc -> the Docusaurus route.
  if (srcToId.has(repoRel)) {
    return `/docs/${srcToId.get(repoRel)}${frag}`;
  }
  // A link to a directory that holds an imported README (e.g. ../conctl/ or
  // ../classic/) -> that README's doc route.
  const asReadme = normalize(path.join(repoRel, 'README.md'));
  if (srcToId.has(asReadme)) {
    return `/docs/${srcToId.get(asReadme)}${frag}`;
  }

  // 2) Points at a repo file/dir NOT on the site -> absolute GitHub URL. GitHub
  //    serves a FILE under /blob/ and a DIRECTORY under /tree/; the wrong one is
  //    a broken link (a file at /tree/ 404s). Decide by STATTING the real path on
  //    disk — the repo is present at build time — because an extension is not a
  //    reliable proxy: extensionless FILES exist (LICENSE, Dockerfile, justfile)
  //    and would be mis-tagged as directories. Fall back to the trailing-slash
  //    hint only when the path can't be statted (a link to something not checked
  //    out), defaulting to /blob/ otherwise.
  let isDir;
  try {
    isDir = statSync(absTarget).isDirectory();
  } catch {
    isDir = rawPath.endsWith('/');
  }
  const base = isDir ? GH_TREE : GH;
  return `${base}/${repoRel}${frag}`;
}

// rewriteLinks walks every markdown link/image AND raw-HTML <img src> in `body`
// and rewrites its target via resolveLink. Covers [text](target), ![alt](target),
// and <img … src="target" …> (the README's centered logo header uses raw HTML,
// which the markdown-link regex doesn't see — without the second pass its
// docs/assets/logo.png would leak through unrewritten and 404 on the site).
function rewriteLinks(body, srcAbsPath) {
  // Matches [label](target) and ![alt](target). The target is everything up to
  // the closing paren that isn't itself a paren — good enough for our links,
  // none of which contain parens in the path.
  body = body.replace(/(!?)\[([^\]]*)\]\(([^)\s]+)(\s+"[^"]*")?\)/g, (m, bang, label, target, title) => {
    const href = resolveLink(target, srcAbsPath);
    return `${bang}[${label}](${href}${title || ''})`;
  });
  // Handle raw HTML <img …> tags whose src is a bundled asset. A raw <img> is
  // emitted VERBATIM by Docusaurus — it is NOT baseUrl-prefixed — so a rewritten
  // src="/img/logo.png" would 404 on a sub-path deploy (…/converge/ fetches
  // salesforce.github.io/img/logo.png → the SPA 404 HTML page → a blank image).
  // A MARKDOWN image IS baseUrl-prefixed (Docusaurus require()s it), so CONVERT
  // the whole <img> to ![alt](/img/name) for a SITE_ASSETS target. The alt text
  // (from the tag's alt=/title=) is preserved so CSS can size it (see custom.css).
  // A non-asset <img> (external URL, etc.) just has its src resolved in place.
  body = body.replace(/<img\b([^>]*?)\/?>/gi, (m, attrs) => {
    const srcM = attrs.match(/\bsrc=(["'])([^"']+)\1/i);
    if (!srcM) return m;
    const target = srcM[2];
    const resolved = resolveLink(target, srcAbsPath);
    // If it resolved to an on-site /img/ asset, emit a markdown image so baseUrl
    // is applied. Use the alt or title attribute as the markdown alt text.
    if (resolved.startsWith('/img/')) {
      const altM = attrs.match(/\b(?:alt|title)=(["'])([^"']*)\1/i);
      const alt = altM ? altM[2] : '';
      return `![${alt}](${resolved})`;
    }
    // Otherwise keep the <img> but rewrite its src.
    return m.replace(srcM[0], `src=${srcM[1]}${resolved}${srcM[1]}`);
  });
  return body;
}

// stripFirstH1 removes the leading "# Title" line: Docusaurus renders the page
// title from frontmatter, so keeping the H1 would double it. Only strips the
// FIRST h1 and only if it's at the very top (after optional blank lines).
function stripFirstH1(body) {
  const lines = body.split('\n');
  let i = 0;
  while (i < lines.length && lines[i].trim() === '') i++;
  if (i < lines.length && /^#\s+/.test(lines[i])) {
    lines.splice(i, 1);
    // Drop one trailing blank line left behind, for tidiness.
    if (lines[i] !== undefined && lines[i].trim() === '') lines.splice(i, 1);
  }
  return lines.join('\n');
}

// escapeYaml quotes a frontmatter string value safely (titles contain ':' etc.).
function escapeYaml(s) {
  return `'${s.replace(/'/g, "''")}'`;
}

async function run() {
  // Clean the generated tree so a removed source doc doesn't linger on the site.
  await fs.rm(outDir, { recursive: true, force: true });

  for (const doc of DOCS) {
    const srcAbs = path.join(repoRoot, doc.src);
    let body;
    try {
      body = await fs.readFile(srcAbs, 'utf8');
    } catch (err) {
      throw new Error(`sync-docs: cannot read source ${doc.src}: ${err.message}`);
    }

    body = stripFirstH1(body);
    body = rewriteLinks(body, srcAbs);

    // Frontmatter: title drives the page + sidebar label; slug pins the URL;
    // custom_edit_url points "Edit this page" at the CANONICAL committed source
    // (doc.src on the public repo), NOT the generated copy under website/docs
    // (which is gitignored — a preset editUrl would 404). Each source lives at a
    // different repo path, so a single editUrl prefix can't express them; the
    // per-doc custom_edit_url is the correct mechanism.
    const frontmatter =
      `---\n` +
      `title: ${escapeYaml(doc.title)}\n` +
      `slug: /${doc.id}\n` +
      `custom_edit_url: ${GH}/${doc.src}\n` +
      `# This page is GENERATED from ${doc.src} by website/scripts/sync-docs.mjs.\n` +
      `# Edit the source file, not this copy.\n` +
      `---\n\n`;

    const outPath = path.join(outDir, `${doc.id}.md`);
    await fs.mkdir(path.dirname(outPath), { recursive: true });
    await fs.writeFile(outPath, frontmatter + body, 'utf8');
  }

  // Copy the generated OpenAPI spec into static/ so Redocusaurus (configured in
  // docusaurus.config.ts) can render it at /api. The golden spec is the SOURCE OF
  // TRUTH (regenerated by `just golden-openapi` from the Huma routes); the site
  // serves a build-time copy of it, so the API reference never drifts from the code.
  const openapiSrc = path.join(repoRoot, 'internal/api/openapi.golden.yaml');
  const openapiDst = path.join(websiteDir, 'static', 'openapi.yaml');
  try {
    await fs.mkdir(path.dirname(openapiDst), { recursive: true });
    await fs.copyFile(openapiSrc, openapiDst);
  } catch (err) {
    throw new Error(`sync-docs: cannot copy OpenAPI spec ${openapiSrc}: ${err.message}`);
  }

  console.log(`sync-docs: projected ${DOCS.length} docs + the OpenAPI spec into ${path.relative(repoRoot, websiteDir)}/`);
}

run().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
