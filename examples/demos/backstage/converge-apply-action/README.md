# `converge:apply` — Backstage Scaffolder custom action

A small custom action that provisions a Converge resource by POSTing it to the
control-plane REST API (`POST /api/v1/resources`). It is the entire Backstage↔Converge
integration for the direct-apply golden path (see the [demo README](../README.md)). For the
demo it expands platform knobs (deployment name, # functional domains, teams per domain, TGW
on/off) into a `classicbom` BOM spec; it also accepts a raw `spec` pass-through for any other
kind.

## Files

- [`index.ts`](index.ts) — the action (`createConvergeApplyAction`).

## Install into your Backstage backend

Backstage's new backend system (v1.x) registers scaffolder actions via a backend module.

1. Copy [`index.ts`](index.ts) into your backend, e.g.
   `packages/backend/src/plugins/scaffolder/convergeApply.ts`, alongside a small module:

   ```ts
   // packages/backend/src/plugins/scaffolder/module.ts
   import { createBackendModule } from '@backstage/backend-plugin-api';
   import { scaffolderActionsExtensionPoint } from '@backstage/plugin-scaffolder-node/alpha';
   import { coreServices } from '@backstage/backend-plugin-api';
   import { createConvergeApplyAction } from './convergeApply';

   export const scaffolderModuleConverge = createBackendModule({
     pluginId: 'scaffolder',
     moduleId: 'converge-apply',
     register(env) {
       env.registerInit({
         deps: {
           scaffolder: scaffolderActionsExtensionPoint,
           config: coreServices.rootConfig,
         },
         async init({ scaffolder, config }) {
           scaffolder.addActions(createConvergeApplyAction({ config }));
         },
       });
     },
   });
   ```

2. Wire the module into your backend:

   ```ts
   // packages/backend/src/index.ts
   backend.add(import('@backstage/plugin-scaffolder-backend'));
   backend.add(import('./plugins/scaffolder/module'));
   ```

3. Point the action at Converge — merge [`../app-config.demo.yaml`](../app-config.demo.yaml)
   into your `app-config.yaml`:

   ```yaml
   converge:
     baseUrl: http://localhost:8080
   ```

## Dependencies

The action uses only Backstage packages already present in a scaffolder backend:

- `@backstage/plugin-scaffolder-node` (`createTemplateAction`)
- `@backstage/config` (`Config`)

`fetch` is the runtime global (Node 18+); no HTTP client dependency is added. For an
`https://` Converge server behind mTLS, extend the `fetch` call with a TLS agent using the
same cert/key/CA scheme as [`conctl`](../../../../cmd/conctl/README.md)'s `--tls-*` flags —
out of scope for this local demo.

## Contract

Encodes exactly one call — keep it in sync with the server
(`internal/api/handlers_objects.go: applyResourceManifest`) and, for the BOM expansion, the
classicbom spec_schema (`examples/demos/classic/testfixtures/classicbom.kind.json`):

| | |
|---|---|
| method + path | `POST /api/v1/resources` |
| body | `{ type:"resource", kind, kind_version, name, labels, spec }` |
| classicbom spec | `{ deployment_instance: { name, functional_domains: [ { name, aws_transit_gateway:{enabled}, service_teams:[{name}] } ] } }` |
| success | `201 Created` (first apply) / `200 OK` (update) |
| applied verb | `X-Apply-Result` header: `created` \| `configured` \| `unchanged` |
| errors | RFC-7807 `application/problem+json` (surfaced verbatim in the step log) |
