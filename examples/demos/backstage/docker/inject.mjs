// Image-build injection: wire the Converge custom action + golden-path template into a
// freshly-scaffolded Backstage app (Dockerfile.backstage stage 2). Idempotent text edits —
// each guarded by a contains-check so re-running is a no-op. Run from the app root
// (/work/app). Mirrors the hand-verified wiring against Backstage 1.53 / scaffolder-node
// 0.13.x; if you bump the pinned create-app version and the layout shifts, re-verify these
// three edits.
import { readFileSync, writeFileSync } from 'node:fs';

// ── 1. Register the backend module in packages/backend/src/index.ts. ──
// Add the import + backend.add right after the scaffolder-backend plugin line.
{
  const f = 'packages/backend/src/index.ts';
  let s = readFileSync(f, 'utf8');
  if (!s.includes('convergeModule')) {
    const anchor = "backend.add(import('@backstage/plugin-scaffolder-backend'));";
    if (!s.includes(anchor)) {
      throw new Error('inject: scaffolder-backend anchor not found in backend index.ts');
    }
    s = s.replace(
      anchor,
      anchor +
        "\n// Converge golden-path: register the converge:apply custom action.\n" +
        "import { scaffolderModuleConverge } from './plugins/scaffolder/convergeModule';\n" +
        'backend.add(scaffolderModuleConverge);',
    );
    writeFileSync(f, s);
    console.log('inject: wired convergeModule into backend index.ts');
  } else {
    console.log('inject: convergeModule already wired');
  }
}

// ── 2. app-config.yaml: converge.baseUrl (from env) + register the template location. ──
{
  const f = 'app-config.yaml';
  let s = readFileSync(f, 'utf8');

  // Bind the frontend dev server to 0.0.0.0 so it is reachable from the host when the app
  // runs in a container (default bind is localhost, unreachable through a port mapping).
  if (!/^app:\n {2}listen:/m.test(s)) {
    s = s.replace(
      'app:\n  title:',
      'app:\n  listen:\n    host: 0.0.0.0\n    port: 3000\n  title:',
    );
    console.log('inject: bound app frontend to 0.0.0.0:3000');
  }

  if (!s.includes('\nconverge:')) {
    // Backstage env substitution is ${VAR} only — it does NOT support a ${VAR:default}
    // fallback (the whole expression would be left unsubstituted). CONVERGE_BASE_URL is
    // always set by docker-compose (http://control:8080), so a bare ${CONVERGE_BASE_URL}
    // is correct; the action itself falls back to http://localhost:8080 if it's ever unset.
    s +=
      '\n\n# Converge control-plane API the converge:apply scaffolder action POSTs to.\n' +
      'converge:\n  baseUrl: ${CONVERGE_BASE_URL}\n';
    console.log('inject: added converge.baseUrl to app-config');
  }

  if (!s.includes('converge/template.yaml')) {
    const anchor = '  locations:';
    if (!s.includes(anchor)) {
      throw new Error('inject: catalog.locations anchor not found in app-config.yaml');
    }
    s = s.replace(
      anchor,
      anchor +
        '\n    # Converge golden-path template (relative to the backend process, packages/backend).\n' +
        '    - type: file\n' +
        '      target: ../../converge/template.yaml\n' +
        '      rules:\n' +
        '        - allow: [Template]',
    );
    console.log('inject: registered converge/template.yaml location');
  }

  writeFileSync(f, s);
}

// ── 3. .yarnrc.yml: use the public npm registry + node-modules linker. ──
// A corporate mirror may lack some Backstage transitive deps (e.g. @asyncapi/specs); the
// public registry has them. node-modules linker keeps the runtime simple.
{
  const f = '.yarnrc.yml';
  writeFileSync(
    f,
    'nodeLinker: node-modules\n' +
      'npmRegistryServer: "https://registry.npmjs.org/"\n' +
      'enableGlobalCache: true\n',
  );
  console.log('inject: pinned .yarnrc.yml to public npm registry');
}
