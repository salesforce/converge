import type { ReactNode } from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

// FeatureItem is one card in the "why Converge" grid. `to` makes the card's
// title deep-link into the relevant doc so the grid doubles as navigation.
type FeatureItem = {
  title: string;
  to: string;
  description: ReactNode;
};

// The features are the project's actual differentiators, phrased for a first
// read — each links to the doc that goes deep. Kept to six so the grid reads at
// a glance rather than turning into a wall.
const FEATURES: FeatureItem[] = [
  {
    title: 'Provider-agnostic core',
    to: '/docs/implementing-a-provider',
    description: (
      <>
        The engine knows no concrete resource type. Teach it a <em>kind</em> with a
        Kubernetes-CRD-style manifest plus a handler — Terraform, a shell script, a
        Kubernetes Job, or any cloud API call.
      </>
    ),
  },
  {
    title: 'Declarative composition',
    to: '/docs/how-converge-compares',
    description: (
      <>
        Compose a resource into a graph of children and roll their readiness up into
        the composite's status. Dependency edges and value flows are first-class
        declarative data, not imperative controller code.
      </>
    ),
  },
  {
    title: 'Built for scale',
    to: '/docs/architecture',
    description: (
      <>
        Millions of resources, thousands of reconciliations per second. Graph, rollup,
        and value-flow logic lives in Postgres; workers claim work with{' '}
        <code>FOR UPDATE SKIP LOCKED</code>.
      </>
    ),
  },
  {
    title: 'Reactive, not polled',
    to: '/docs/architecture',
    description: (
      <>
        Work is pushed to workers over a broker mesh, not polled. A control plane and a
        broker mesh that any-language workers dial to pull work — a single binary over
        Postgres.
      </>
    ),
  },
  {
    title: 'Any language',
    to: '/docs/components/sdk-go',
    description: (
      <>
        Write providers in Go or TypeScript, or drive any external program over{' '}
        <code>stdio</code>. The only tissue between a worker and the broker is one public
        worker proto.
      </>
    ),
  },
  {
    title: 'GitOps & CLI',
    to: '/docs/demos/gitops',
    description: (
      <>
        <code>conctl sync</code> applies a manifest directory GitOps-style with Argo-CD-like
        ownership tracking and prune. A kubectl-style CLI and a live web UI ship in the box.
      </>
    ),
  },
];

function Feature({ title, description, to }: FeatureItem) {
  return (
    <div className={clsx('col col--4', styles.featureCol)}>
      <Link to={to} className={styles.featureCard}>
        <Heading as="h3" className={styles.featureTitle}>
          {title}
        </Heading>
        <p className={styles.featureDesc}>{description}</p>
      </Link>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FEATURES.map((props) => (
            <Feature key={props.title} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
