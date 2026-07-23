import type { ReactNode } from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';

import styles from './index.module.css';

// HomepageHero is the top-of-page banner: the one-line pitch, the two primary
// CTAs (get started / see the demos), and the animated demo GIF so a first-time
// visitor immediately sees a resource graph converging.
function HomepageHero() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <div className="container">
        <h1 className={styles.heroTitle}>{siteConfig.title}</h1>
        <p className={styles.heroTagline}>
          A declarative orchestration engine for platform teams. Describe the
          resources you want as specs; the engine reconciles the world to match —
          composing, ordering, healing, and rolling up status.
        </p>
        <p className={styles.heroSub}>
          Built for <strong>scale</strong> (millions of resources), <strong>simplicity</strong>{' '}
          (a single binary + Postgres), and <strong>reactivity</strong> (work is pushed, not polled).
        </p>
        <div className={styles.buttons}>
          <Link className="button button--primary button--lg" to="/docs/intro">
            Get started
          </Link>
          <Link className="button button--secondary button--lg" to="/docs/demos/classic">
            Explore the demos
          </Link>
          <Link
            className="button button--outline button--secondary button--lg"
            href="https://github.com/salesforce/converge"
          >
            GitHub
          </Link>
        </div>
        <img
          className={styles.heroDemo}
          src={useBaseUrl('/img/demo.gif')}
          alt="Converge converging a resource graph in the UI"
          loading="eager"
        />
      </div>
    </header>
  );
}

export default function Home(): ReactNode {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title} — declarative orchestration`}
      description="A declarative, provider-agnostic orchestration engine for platform teams — describe resources as specs; the engine reconciles the world to match."
    >
      <HomepageHero />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
