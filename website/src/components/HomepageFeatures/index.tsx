import type { ReactNode } from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

// A feature card. `to` deep-links the card into the doc that goes deep, so the
// grid doubles as navigation.
type Feature = {
  title: string;
  to: string;
  description: ReactNode;
};

// SPOTLIGHTS are the three capabilities that make a first-time visitor stop and
// look — the differentiators, stated as an outcome, not a mechanism. These render
// large across the top. No wire/DB internals here: a newcomer cares what it does
// for them, not how the queue is claimed.
const SPOTLIGHTS: Feature[] = [
  {
    title: 'One spec fans out into a whole graph',
    to: '/docs/how-converge-compares',
    description: (
      <>
        Declare a high-level resource once; Converge expands it into a graph of children,
        brings them up <strong>in dependency order</strong>, and flows each one's outputs —
        an ID, an ARN, a URL — into the specs that depend on it. A single{' '}
        <code>landingzone</code> spec becomes a hundred accounts, each with its VPCs, routes,
        and guardrails, wired together with no glue code.
      </>
    ),
  },
  {
    title: 'Never drifts, never lies about status',
    to: '/docs/architecture',
    description: (
      <>
        Every resource is <strong>continuously reconciled</strong> — anything that drifts is
        detected and healed, across millions of resources. A composite is green only when
        everything beneath it is healthy, and when it isn't, it{' '}
        <strong>names the exact child at fault</strong> — status you can actually trust.
      </>
    ),
  },
  {
    title: 'Logic is data — edit it live, no redeploy',
    to: '/docs/demos/datadriven',
    description: (
      <>
        Composition rules, dependency edges, and value flows are{' '}
        <strong>first-class declarative data</strong>, not controller code. Write them in Go,
        CEL, or Starlark — or ship them in a config bundle and edit them on a running cluster.
        Change how a kind composes and every resource re-converges. No rebuild, no rollout.
      </>
    ),
  },
];

// FEATURES are the rest of the major surface — still benefit-led, kept to a tidy
// six so the grid reads at a glance rather than becoming a wall of text.
const FEATURES: Feature[] = [
  {
    title: 'Provider-agnostic core',
    to: '/docs/implementing-a-provider',
    description: (
      <>
        The engine knows no concrete resource type. Teach it a <em>kind</em> with a
        Kubernetes-CRD-style manifest and a handler that does anything — Terraform, a
        Kubernetes Job, or any cloud API call.
      </>
    ),
  },
  {
    title: 'Any language — or none',
    to: '/docs/components/sdk-go',
    description: (
      <>
        Write providers in Go or TypeScript, or make <em>any</em> external program a kind
        over <code>stdio</code> — a bash script, a Python file, a binary — with no SDK import
        or compile step.
      </>
    ),
  },
  {
    title: 'One binary, one database',
    to: '/docs/architecture',
    description: (
      <>
        No etcd, Redis, Kafka, or message broker. A single binary over Postgres does it all —
        leaderless, self-sharding, and colocated or split into control and worker tiers as you
        grow.
      </>
    ),
  },
  {
    title: 'Reactive at scale',
    to: '/docs/architecture',
    description: (
      <>
        Thousands of reconciliations per second, tuned for millions of resources. Work is{' '}
        <strong>pushed, not polled</strong>, so a change propagates immediately — and
        at-least-once, idempotent delivery makes it safe to redeliver.
      </>
    ),
  },
  {
    title: 'Live lifecycle & migrations',
    to: '/docs/features',
    description: (
      <>
        Version a kind and migrate a resource live (<code>vpc/v1</code> → <code>vpc/v2</code>
        ). Throttle a kind cluster-wide with concurrency caps, roll back to any earlier spec,
        and fire durable follow-up <strong>reactors</strong> on any transition.
      </>
    ),
  },
  {
    title: 'GitOps, CLI & live UI',
    to: '/docs/demos/gitops',
    description: (
      <>
        <code>conctl sync</code> reconciles a manifest directory Argo-CD-style with ownership
        tracking and prune. A kubectl-style CLI and a rich web UI — topology, dependency
        graph, fleet view, live events — ship in the box.
      </>
    ),
  },
];

function SpotlightCard({ title, description, to }: Feature) {
  return (
    <div className={clsx('col col--4', styles.spotlightCol)}>
      <Link to={to} className={clsx(styles.card, styles.spotlightCard)}>
        <Heading as="h3" className={styles.cardTitle}>
          {title}
        </Heading>
        <p className={styles.cardDesc}>{description}</p>
      </Link>
    </div>
  );
}

function FeatureCard({ title, description, to }: Feature) {
  return (
    <div className={clsx('col col--4', styles.featureCol)}>
      <Link to={to} className={styles.card}>
        <Heading as="h3" className={styles.cardTitle}>
          {title}
        </Heading>
        <p className={styles.cardDesc}>{description}</p>
      </Link>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className={styles.sectionHead}>
          <Heading as="h2" className={styles.sectionTitle}>
            Describe what you want. Converge makes it so.
          </Heading>
          <p className={styles.sectionLede}>
            Like Crossplane or kro, it composes a resource into a graph and rolls readiness up
            — but dependency edges and value flows are declarative data you can edit live, in
            the language of your choice.
          </p>
        </div>

        <div className={clsx('row', styles.spotlightRow)}>
          {SPOTLIGHTS.map((f) => (
            <SpotlightCard key={f.title} {...f} />
          ))}
        </div>

        <div className="row">
          {FEATURES.map((f) => (
            <FeatureCard key={f.title} {...f} />
          ))}
        </div>

        <div className={styles.moreRow}>
          <Link className="button button--primary button--lg" to="/docs/features">
            See every feature at a glance →
          </Link>
        </div>
      </div>
    </section>
  );
}
