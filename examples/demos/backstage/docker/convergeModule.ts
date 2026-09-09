import { createBackendModule, coreServices } from '@backstage/backend-plugin-api';
import { scaffolderActionsExtensionPoint } from '@backstage/plugin-scaffolder-node';
import { createConvergeApplyAction } from './convergeApply';

/**
 * Backend module that registers the `converge:apply` scaffolder action, wiring the app
 * config in so the action can resolve `converge.baseUrl`. Wired into the backend in
 * packages/backend/src/index.ts (by docker/inject.mjs at image-build time).
 *
 * scaffolderActionsExtensionPoint is exported from the MAIN @backstage/plugin-scaffolder-node
 * entry (not /alpha) as of scaffolder-node 0.13.x (Backstage 1.53).
 */
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
