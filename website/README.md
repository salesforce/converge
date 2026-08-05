# Converge documentation site

The [Converge](https://github.com/salesforce/converge) documentation site, built with
[Docusaurus](https://docusaurus.io/). It is published to GitHub Pages at
**https://salesforce.github.io/converge/** by the [`docs` workflow](../.github/workflows/docs.yml)
on every push to `main` that touches the site or the docs it projects.

## The docs are projected, not authored here

The repository's markdown is the **source of truth** — the site is a projection of it.
[`scripts/sync-docs.mjs`](scripts/sync-docs.mjs) copies each source file into `docs/`
(the Docusaurus content root, git-ignored and regenerated) and rewrites its relative
links so they resolve on the site:

- a link to another imported doc → the Docusaurus route (`/docs/<id>`),
- a link to a repo file that is **not** on the site (source code, a Dockerfile, the
  schema) → an absolute GitHub URL on the public repo,
- a bundled diagram/gif (`docs/assets/*`) → the on-site `static/img/` copy so the image
  actually renders.

To add or move a page, edit the `DOCS` map in `sync-docs.mjs` and the matching entry in
[`sidebars.ts`](sidebars.ts). **Do not edit `docs/` by hand** — the sync overwrites it.

The projection runs automatically via the `presync`/`prebuild` npm hooks, so you never
call it directly.

## Local development

```bash
cd website
npm install          # once
npm start            # sync + dev server with hot reload at http://localhost:3000/converge/
```

## Build & preview the production site

```bash
npm run build        # sync + static build into ./build (fails on a broken internal link)
npm run serve        # serve ./build locally to preview exactly what deploys
```

## Deploy

Deployment is automatic (the `docs` workflow → GitHub Pages). The one-time repo setting
is **Settings → Pages → Build and deployment → Source = "GitHub Actions"**; there is no
`gh-pages` branch. Nothing built is committed — only the site's source (config, `src/`,
`static/`, `scripts/`, `package.json` + `package-lock.json`) is tracked.
