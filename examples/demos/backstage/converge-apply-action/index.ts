/**
 * `converge:apply` — a Backstage Scaffolder custom action that provisions a Converge
 * resource by POSTing it to the control-plane REST API. This is the ENTIRE Backstage↔Converge
 * integration seam: one HTTP call, the same `POST /api/v1/resources` envelope that `conctl apply`
 * and the GitOps `conctl sync` loop send.
 *
 * For the golden-path demo it takes platform-level KNOBS (deployment name, # functional
 * domains, teams per domain, TGW on/off) and expands them into a `classicbom` BOM spec —
 * so the developer never writes nested JSON. It also accepts a raw `spec` pass-through for
 * any other kind.
 *
 * Register it on your scaffolder backend (see README.md in this directory):
 *
 *     import { createConvergeApplyAction } from './converge-apply-action';
 *     scaffolder.addActions(createConvergeApplyAction({ config }));
 *
 * The Converge base URL is read from app-config (`converge.baseUrl`) — see
 * app-config.demo.yaml. There are NO wire-compatibility guarantees on the Converge REST API
 * before its first stable release, so keep the envelope + the classicbom spec shape in sync
 * with the server (internal/api/handlers_objects.go: applyResourceManifest;
 * examples/demos/classic/testfixtures/classicbom.kind.json: the spec_schema).
 *
 * Written against @backstage/plugin-scaffolder-node's v2 (zod) action API — the schema is
 * given as `(z) => z.object(...)` and the handler reads `ctx.input` / writes `ctx.output`.
 */
import { createTemplateAction } from '@backstage/plugin-scaffolder-node';
import { Config } from '@backstage/config';

/** Options: the action needs Backstage config to resolve `converge.baseUrl`. */
export interface ConvergeApplyActionOptions {
  config: Config;
}

/** The platform-level knobs the "Provision a deployment" template collects. */
interface BomKnobs {
  deployment_name: string;
  functional_domains: number;
  teams_per_domain: number;
  enable_tgw: boolean;
}

/**
 * Expand the form knobs into a classicbom spec. Mirrors the shape in
 * examples/demos/classic/testfixtures/classicbom.kind.json (spec_schema):
 * deployment_instance{ name, functional_domains[ { name, aws_transit_gateway{enabled},
 * service_teams[ {name} ] } ] }. Deterministic names (fd-<i>, <deployment>-fd<i>-team<j>) so
 * a re-apply with the same knobs is idempotent — the composer diffs children by name.
 */
function buildClassicbomSpec(k: BomKnobs): Record<string, unknown> {
  const functional_domains = Array.from({ length: k.functional_domains }, (_, i) => ({
    name: `fd-${i}`,
    aws_transit_gateway: { enabled: k.enable_tgw },
    service_teams: Array.from({ length: k.teams_per_domain }, (_, j) => ({
      name: `${k.deployment_name}-fd${i}-team${j}`,
    })),
  }));
  return {
    deployment_instance: {
      name: k.deployment_name,
      functional_domains,
    },
  };
}

export const createConvergeApplyAction = (options: ConvergeApplyActionOptions) => {
  const { config } = options;

  return createTemplateAction({
    id: 'converge:apply',
    description:
      'Apply (create-or-update) a Converge resource via POST /api/v1/resources — the golden-path provisioning seam. Expands classicbom knobs into a BOM, or passes a raw spec through.',
    schema: {
      // Exactly one of `spec` (raw pass-through) or `bom` (classicbom knobs) is provided.
      input: z =>
        z.object({
          kind: z.string().describe('Resource kind (e.g. classicbom)'),
          kindVersion: z
            .number()
            .describe('Kind version (>=1, REQUIRED — the API has no implicit v1 default)'),
          name: z.string().describe('Resource name; identity together with kind'),
          labels: z
            .record(z.string())
            .optional()
            .describe('Label set (string→string)'),
          spec: z
            .record(z.any())
            .optional()
            .describe('Raw per-kind spec (pass-through; mutually exclusive with bom)'),
          bom: z
            .object({
              deployment_name: z.string(),
              functional_domains: z.number(),
              teams_per_domain: z.number(),
              enable_tgw: z.boolean(),
            })
            .optional()
            .describe('classicbom knobs — expanded into a deployment_instance BOM'),
        }),
      output: z =>
        z.object({
          applyResult: z
            .string()
            .describe('Applied verb from X-Apply-Result (created | configured | unchanged)'),
          childCount: z
            .number()
            .describe('Approx. leaf resources the composer will fan out (domains × teams)'),
        }),
    },

    async handler(ctx) {
      // Resolve the Converge control-plane base URL (default matches the demo).
      const baseUrl =
        config.getOptionalString('converge.baseUrl') ?? 'http://localhost:8080';

      // Build the spec: either expand classicbom knobs, or use the raw pass-through.
      let spec: Record<string, unknown>;
      let childCount = 0;
      if (ctx.input.bom) {
        spec = buildClassicbomSpec(ctx.input.bom as BomKnobs);
        childCount = ctx.input.bom.functional_domains * ctx.input.bom.teams_per_domain;
      } else if (ctx.input.spec) {
        spec = ctx.input.spec as Record<string, unknown>;
      } else {
        throw new Error('converge:apply requires either `bom` (knobs) or `spec` (raw)');
      }

      // The self-describing resource envelope. `type: "resource"` is accepted + validated by
      // the API (it never affects routing on this endpoint — the URL already selects it), so
      // the body is identical to a `conctl sync` resource document.
      const body = {
        type: 'resource',
        kind: ctx.input.kind,
        kind_version: ctx.input.kindVersion,
        name: ctx.input.name,
        labels: ctx.input.labels ?? {},
        spec,
      };

      ctx.logger.info(
        `Applying ${ctx.input.kind}/${ctx.input.name} (v${ctx.input.kindVersion}) to Converge at ${baseUrl}` +
          (childCount ? ` — composer will fan out ~${childCount} team subtrees` : ''),
      );

      const res = await fetch(`${baseUrl}/api/v1/resources`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        // The API returns RFC-7807 application/problem+json on error — surface it verbatim so
        // the developer sees WHY (e.g. 422 schema violation, 422 retired kind_version).
        const detail = await res.text();
        throw new Error(
          `Converge apply failed: ${res.status} ${res.statusText} — ${detail}`,
        );
      }

      // The applied verb (created | configured | unchanged) rides the X-Apply-Result header.
      const applyResult = res.headers.get('x-apply-result') ?? 'unknown';
      ctx.logger.info(
        `Converge apply ${applyResult} (HTTP ${res.status}) for ${ctx.input.kind}/${ctx.input.name}`,
      );
      ctx.output('applyResult', applyResult);
      ctx.output('childCount', childCount);
    },
  });
};
