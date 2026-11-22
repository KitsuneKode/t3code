import {
  effectiveSnoozed,
  type ThreadSnoozeShell,
} from "@t3tools/client-runtime/state/thread-settled";
import type { OrchestrationV2ThreadShell } from "@t3tools/contracts";
import {
  backgroundWorkHoldsCompletion,
  monitorHoldsWakeCompletion,
} from "@t3tools/shared/orchestrationV2PendingBackgroundWork";

export type ThreadNotificationKind = "input" | "completion";

export interface ThreadNotificationObservation {
  /** Status and request key, or status and run without a request, while the thread needs attention. */
  readonly attention: string | null;
  /** The pending request's id, when it has one; null whenever attention is. */
  readonly requestId: string | null;
  /** The thread is idle and its latest run completed. */
  readonly settled: boolean;
  /** When the latest run completed, if it did. */
  readonly completedAt: number | null;
  /** When the latest completed run finished while still held by background work or a monitor wake. */
  readonly heldCompletionAt: number | null;
  /** The latest run answers a background-work progress wake, like a Monitor line. */
  readonly backgroundWake: boolean;
}

/** Matches the push relay's completion confirmation: a wake run can start right after background work releases a completion. */
export const RELEASED_COMPLETION_GRACE_MS = 5_000;

export function resolveThreadCompletionObservation(input: {
  readonly status: string;
  readonly latestRun: { readonly status: string; readonly completedAt: string | null } | null;
  readonly pendingBackgroundTasks: NonNullable<
    OrchestrationV2ThreadShell["pendingBackgroundTasks"]
  >;
  readonly rawThread: Pick<
    OrchestrationV2ThreadShell,
    "latestRunBackgroundWake" | "pendingBackgroundTasks"
  >;
}): Pick<
  ThreadNotificationObservation,
  "settled" | "completedAt" | "heldCompletionAt" | "backgroundWake"
> {
  const completed = input.status === "ready" && input.latestRun?.status === "completed";
  const monitorHeld = monitorHoldsWakeCompletion(input.rawThread);
  const settled = completed && !monitorHeld;
  const parsedCompletedAt = Date.parse(input.latestRun?.completedAt ?? "");
  const completedAt = Number.isFinite(parsedCompletedAt) ? parsedCompletedAt : null;
  const heldCompletion =
    !settled &&
    input.latestRun?.status === "completed" &&
    (backgroundWorkHoldsCompletion(input.pendingBackgroundTasks) || monitorHeld);
  return {
    settled,
    completedAt: settled ? completedAt : null,
    heldCompletionAt: heldCompletion ? completedAt : null,
    backgroundWake: input.rawThread.latestRunBackgroundWake === true,
  };
}

/**
 * Whether completion notifications are suppressed by snooze. A completion that landed
 * during the snooze stays suppressed even when its update is processed after the wake.
 */
export function isSnoozedForNotification(
  thread: ThreadSnoozeShell,
  completedAt: number | null,
  now: number,
): boolean {
  if (effectiveSnoozed(thread, { now: new Date(now).toISOString() })) return true;
  if (completedAt === null) return false;
  if (thread.snoozedAt != null && completedAt < Date.parse(thread.snoozedAt)) return false;
  return effectiveSnoozed(thread, { now: new Date(completedAt).toISOString() });
}

export function createThreadNotificationGate<Key>(
  notify: (key: Key, kind: ThreadNotificationKind) => void,
) {
  // The newest completion already notified, or deliberately skipped.
  const handled = new Map<Key, number>();
  // Wake completions waiting out the grace, by completion time; no timer while paused.
  const releases = new Map<
    Key,
    { readonly at: number; readonly timer: ReturnType<typeof setTimeout> | undefined }
  >();
  const release = (key: Key, at: number) => {
    const timer = setTimeout(() => {
      releases.delete(key);
      handled.set(key, at);
      notify(key, "completion");
    }, RELEASED_COMPLETION_GRACE_MS);
    releases.set(key, { at, timer });
  };
  const skip = (key: Key, at: number | null) => {
    if (at !== null && at > (handled.get(key) ?? -Infinity)) handled.set(key, at);
  };
  const cancel = (key: Key) => {
    clearTimeout(releases.get(key)?.timer);
    releases.delete(key);
  };
  // Attention keys with a request id seen since the thread last stopped needing attention.
  const attended = new Map<Key, Set<string>>();
  const attend = (
    key: Key,
    prior: ThreadNotificationObservation | undefined,
    next: ThreadNotificationObservation,
  ): boolean => {
    if (next.attention === null) {
      attended.delete(key);
      return false;
    }
    if (next.requestId === null) return next.attention !== prior?.attention;
    const seen = attended.get(key);
    if (seen?.has(next.attention)) return false;
    if (seen) seen.add(next.attention);
    else attended.set(key, new Set([next.attention]));
    return true;
  };
  return {
    observe(
      key: Key,
      prior: ThreadNotificationObservation | undefined,
      next: ThreadNotificationObservation,
      options: { readonly archived: boolean; readonly quiet: boolean; readonly snoozed: boolean },
    ): void {
      // The grace survives updates that keep the wake's completion; anything else cancels it.
      const pending = releases.get(key);
      const silenced = options.archived || options.quiet || options.snoozed;
      if (pending?.at === next.completedAt && !silenced) {
        if (pending.timer === undefined) release(key, pending.at);
        return;
      }
      cancel(key);
      // The first sighting, archived or snoozed threads, and stopped or silent runs never notify completion.
      if (!prior || silenced) skip(key, next.completedAt);
      const fresh = attend(key, prior, next);
      if (!prior || options.archived) return;
      if (fresh) {
        notify(key, "input");
        return;
      }
      const at = next.completedAt;
      if (!next.settled || at === null || at <= (handled.get(key) ?? -Infinity)) return;
      if (next.backgroundWake || prior.heldCompletionAt === at) {
        release(key, at);
        return;
      }
      handled.set(key, at);
      notify(key, "completion");
    },
    /** Stops grace timers while the snapshot is stale; the next observation restarts or drops them. */
    pause(): void {
      for (const [key, { at, timer }] of releases) {
        clearTimeout(timer);
        releases.set(key, { at, timer: undefined });
      }
    },
    /** Forgets threads that left the snapshot. */
    retain(keys: ReadonlySet<Key>): void {
      for (const key of handled.keys()) if (!keys.has(key)) handled.delete(key);
      for (const key of releases.keys()) if (!keys.has(key)) cancel(key);
      for (const key of attended.keys()) if (!keys.has(key)) attended.delete(key);
    },
    clear(): void {
      for (const key of releases.keys()) cancel(key);
      handled.clear();
      attended.clear();
    },
  };
}
