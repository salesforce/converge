// convert.ts translates between the SDK's ergonomic types (types.ts) and the generated
// proto (gen/…/worker_pb): requestFromTask decodes a StageTask a provider's work() reads;
// completeFromOutcome / failComplete encode its result back. The exported names match
// sdk-go/converge function-for-function (see docs/spec/sdk-spec.md).
import { create } from "@bufbuild/protobuf";
import {
  type StageTask,
  type StageComplete,
  StageCompleteSchema,
  ComposeResultSchema,
  type Resource as PbResource,
  type Operation as PbOperation,
  type ChildSpec as PbChildSpec,
  type DepEdge as PbDepEdge,
  type ProviderConfigSpec as PbProviderConfigSpec,
  type Condition as PbCondition,
  ConditionSchema,
  ChildSpecSchema,
  DepEdgeSchema,
  ValueFlowSchema,
  ResourceRefSchema,
  ProviderConfigSpecSchema,
  Stage,
  Transition as PbTransition,
} from "../gen/converge/worker/v1/worker_pb.js";
import type {
  ChildSpec,
  Condition,
  DepEdge,
  Operation,
  Outcome,
  ProviderConfigSpec,
  ReactionRequest,
  Resource,
  Transition,
  Trigger,
} from "./types.js";

// ─────────────────────────────────────────────────────────────────────────
// Stage ⇄ Trigger + Transition mapping.
// ─────────────────────────────────────────────────────────────────────────

// stageToTrigger maps the wire Stage enum to the SDK's ergonomic Trigger. The broker
// selects the reaction and ships its NAME; the trigger tells the handler WHY it ran (a
// kind that declares several reactions switches on req.reaction, using trigger as context).
function stageToTrigger(stage: Stage): Trigger {
  switch (stage) {
    case Stage.COMPOSE:
    case Stage.WORK:
      return "specChange";
    case Stage.ROLLUP:
      return "childrenSettled";
    case Stage.DELETE:
      return "deleteRequested";
    case Stage.OPERATE:
      return "operation";
    case Stage.REACT:
      return "reactor";
    default:
      return "specChange";
  }
}

// transitionFromProto maps the wire Transition enum to the SDK's ergonomic string union;
// UNSPECIFIED (0, a garbled/absent value) maps to undefined so a handler sees "no
// transition" rather than a bogus one.
function transitionFromProto(t: PbTransition): Transition | undefined {
  switch (t) {
    case PbTransition.CREATED:
      return "created";
    case PbTransition.SYNCED:
      return "synced";
    case PbTransition.DEGRADED:
      return "degraded";
    case PbTransition.FAILED:
      return "failed";
    case PbTransition.DELETED:
      return "deleted";
    default:
      return undefined;
  }
}

// ─────────────────────────────────────────────────────────────────────────
// proto → ergonomic decoders (used by requestFromTask).
// ─────────────────────────────────────────────────────────────────────────

const EMPTY = new Uint8Array();

// resourceFromProto decodes a proto Resource (or undefined) into the SDK's ergonomic
// Resource. A missing message yields a zero-value Resource (empty id/spec/status).
function resourceFromProto(r: PbResource | undefined): Resource {
  return {
    id: r?.id ?? EMPTY,
    kind: r?.kind ?? "",
    name: r?.name ?? "",
    spec: r?.spec ?? EMPTY,
    status: r?.status ?? EMPTY,
    generation: r?.generation ?? 0n,
    isReady: r?.isReady ?? false,
    labels: r?.labels ?? {},
  };
}

function resourcesFromProto(rs: PbResource[]): Resource[] {
  return rs.map((r) => resourceFromProto(r));
}

function operationFromProto(o: PbOperation | undefined): Operation | undefined {
  if (!o) return undefined;
  return {
    resourceId: o.resourceId,
    verb: o.verb,
    input: o.input,
    attempts: o.attempts,
    requestedBy: o.requestedBy,
  };
}

// requestFromTask decodes a proto StageTask into a ReactionRequest, filling only the fields
// the task's stage carries. Every stage sets the base fields (reaction, kindVersion,
// trigger, resource, env override); each stage's typed request (composeReq/workReq/…)
// supplies the stage-specific extras (observed/status/descendants/operation/transition).
export function requestFromTask(task: StageTask): ReactionRequest {
  const base: ReactionRequest = {
    reaction: task.reaction,
    kindVersion: task.kindVersion,
    trigger: stageToTrigger(task.stage),
    resource: { id: EMPTY, kind: task.kind, name: "", spec: EMPTY, status: EMPTY, generation: task.generation, isReady: false, labels: {} },
    status: EMPTY,
    observed: [],
    descendants: [],
    dedupToken: "",
    generation: task.generation,
    env: {
      providerConfig: task.providerConfig,
      providerBundle: task.providerBundle,
    },
  };

  switch (task.stage) {
    case Stage.COMPOSE: {
      const cr = task.composeReq;
      base.resource = resourceFromProto(cr?.root);
      base.observed = resourcesFromProto(cr?.observed ?? []);
      break;
    }
    case Stage.WORK: {
      const wr = task.workReq;
      base.resource = resourceFromProto(wr?.resource);
      base.status = wr?.status ?? EMPTY;
      break;
    }
    case Stage.ROLLUP: {
      const rr = task.rollupReq;
      base.resource = resourceFromProto(rr?.root);
      base.descendants = resourcesFromProto(rr?.descendants ?? []);
      base.status = rr?.status ?? EMPTY;
      break;
    }
    case Stage.DELETE: {
      const dr = task.deleteReq;
      base.resource = resourceFromProto(dr?.resource);
      break;
    }
    case Stage.OPERATE: {
      const or = task.operateReq;
      base.resource = resourceFromProto(or?.resource);
      base.operation = operationFromProto(or?.operation);
      break;
    }
    case Stage.REACT: {
      const rr = task.reactReq;
      base.resource = resourceFromProto(rr?.resource);
      base.transition = rr ? transitionFromProto(rr.transition) : undefined;
      base.dedupToken = rr?.dedupToken ?? "";
      break;
    }
    default:
      break;
  }
  return base;
}

// ─────────────────────────────────────────────────────────────────────────
// ergonomic → proto encoders (used by completeFromOutcome).
// ─────────────────────────────────────────────────────────────────────────

function conditionToProto(c: Condition): PbCondition {
  return create(ConditionSchema, {
    type: c.type,
    status: c.status,
    reason: c.reason ?? "",
    message: c.message ?? "",
  });
}

function childSpecToProto(c: ChildSpec): PbChildSpec {
  return create(ChildSpecSchema, {
    kind: c.kind,
    kindVersion: c.kindVersion,
    name: c.name,
    spec: c.spec,
    labels: c.labels,
  });
}

function depEdgeToProto(e: DepEdge): PbDepEdge {
  return create(DepEdgeSchema, {
    from: create(ResourceRefSchema, { kind: e.from.kind, name: e.from.name }),
    to: create(ResourceRefSchema, { kind: e.to.kind, name: e.to.name }),
    values: e.values.map((v) => create(ValueFlowSchema, { dependentField: v.dependentField, sourceField: v.sourceField })),
  });
}

function configSpecToProto(c: ProviderConfigSpec): PbProviderConfigSpec {
  return create(ProviderConfigSpecSchema, {
    name: c.name,
    kind: c.kind,
    kindVersion: c.kindVersion,
    isDefault: c.isDefault,
    spec: c.spec,
  });
}

// completeFromOutcome encodes an Outcome back into a proto StageComplete, setting the
// result fields PER STAGE (the exact mirror of Go convert.go completeFromOutcome): WORK/
// ROLLUP/REACT carry status + conditions; COMPOSE carries the typed ComposeResult; OPERATE
// carries the verb output; DELETE reports only the bare completion. leaseToken, claimEpoch,
// and stage are echoed so any broker fences the write on the epoch and routes the result.
export function completeFromOutcome(task: StageTask, out: Outcome): StageComplete {
  const sc = create(StageCompleteSchema, { leaseToken: task.leaseToken, claimEpoch: task.claimEpoch, stage: task.stage });
  switch (task.stage) {
    case Stage.WORK:
    case Stage.ROLLUP:
    case Stage.REACT:
      sc.status = out.status ?? EMPTY;
      sc.conditions = (out.conditions ?? []).map(conditionToProto);
      break;
    case Stage.COMPOSE:
      sc.compose = create(ComposeResultSchema, {
        desired: (out.children ?? []).map(childSpecToProto),
        edges: (out.edges ?? []).map(depEdgeToProto),
        configs: (out.configs ?? []).map(configSpecToProto),
        status: out.status ?? EMPTY,
        conditions: (out.conditions ?? []).map(conditionToProto),
      });
      break;
    case Stage.OPERATE:
      sc.output = out.operationOutput ?? EMPTY;
      break;
    case Stage.DELETE:
      // DELETE reports only the bare completion.
      break;
    default:
      break;
  }
  return sc;
}

// failComplete encodes a failed stage: error_message set, terminal flag per the caller
// (terminal = non-retryable). leaseToken, claimEpoch, and stage are echoed like a success
// so the fenced write lands on the claiming row.
export function failComplete(task: StageTask, message: string, isTerminal: boolean): StageComplete {
  return create(StageCompleteSchema, {
    leaseToken: task.leaseToken,
    claimEpoch: task.claimEpoch,
    stage: task.stage,
    errorMessage: message,
    terminal: isTerminal,
  });
}
