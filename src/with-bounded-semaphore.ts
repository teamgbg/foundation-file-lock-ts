/**
 * @system file-lock
 * @status handwritten
 * @edit a host-wide bounded-concurrency gate (a counting semaphore over K lockfile "slots") for frequent-but-heavy work where K=1 over-serialises (violating iteration-speed-budget) yet N concurrent lane pushes saturate the host (the 2026-07-18 load-42 publish-build storm); the K=1 case is exactly withFileLock, and this is the bounded-K generalisation for the build-storm arm of no-uncontrolled-repetition-or-cascade
 *
 * Mechanism: K slot lockfiles (`<basePath>.0.lock` … `<basePath>.K-1.lock`), each a standard PID-lockfile,
 * so withFileLock's stale-PID reclaim returns a crashed holder's slot for free. Slots rather than a counter
 * file because a counter would need its own crash-consistent decrement — the "incremented then crashed →
 * count permanently inflated → semaphore drained" class. A sweeper polls the slots with non-blocking tries
 * and, if all are busy past `timeoutMs`, FAIL-OPENS: this is a thundering-herd OPTIMISATION, never a
 * correctness gate, so a stuck slot can never permanently stall a publish.
 */
import { getAppLogger } from "@teamscala/logger/app-loggers";
import { withFileLock } from "./with-lock.ts";
import type { LockOptions } from "./types.ts";

export interface BoundedSemaphoreOptions {
	/** Max concurrent holders host-wide (K). <1 disables gating (fn runs free). */
	concurrency: number;
	/** Total budget to wait for a free slot before fail-open. */
	timeoutMs: number;
	/** Poll interval between full slot sweeps when all slots are busy. */
	pollIntervalMs?: number;
	/** If true (default), run fn WITHOUT a slot once the budget is exceeded
	 * (thundering-herd optimisation, never a correctness gate). If false, throw. */
	failOpen?: boolean;
	/** Forwarded to each slot's withFileLock (stale-reclaim control). */
	stalePidCheck?: boolean;
}

/** Run `fn` under a host-wide bounded-concurrency gate. At most `concurrency` callers run fn concurrently across the whole host; the rest wait (polled) up to `timeoutMs`, then fail-open (or throw if `failOpen:false`). */
export async function withBoundedSemaphore<T>(
	basePath: string,
	opts: BoundedSemaphoreOptions,
	fn: () => Promise<T>,
): Promise<T> {
	const {
		concurrency,
		timeoutMs,
		pollIntervalMs = 50,
		failOpen = true,
		stalePidCheck = true,
	} = opts;
	if (concurrency < 1) return fn();
	const deadline = Date.now() + timeoutMs;
	const slotOpts: LockOptions = { timeoutMs: 0, pollIntervalMs, stalePidCheck };
	for (;;) {
		for (let slot = 0; slot < concurrency; slot++) {
			const slotPath = `${basePath}.${slot}.lock`;
			try {
				// timeoutMs:0 = one non-blocking try: withFileLock acquires a FREE slot, runs fn and releases in finally, or throws "timed out acquiring" immediately when BUSY.
				return await withFileLock(slotPath, slotOpts, fn);
			} catch (err) {
				const msg = err instanceof Error ? err.message : String(err);
				if (!msg.includes("timed out acquiring")) throw err;
				// slot busy → try the next slot
			}
		}
		if (Date.now() >= deadline) {
			if (failOpen) {
				getAppLogger().warn(
					`@teamscala/file-lock: withBoundedSemaphore("${basePath}") all ${concurrency} slot(s) busy after ${timeoutMs}ms — fail-open (thundering-herd optimisation, not a correctness gate)`,
				);
				return fn();
			}
			throw new Error(
				`@teamscala/file-lock: withBoundedSemaphore("${basePath}") timed out acquiring a slot after ${timeoutMs}ms (${concurrency} slot(s) busy)`,
			);
		}
		await Bun.sleep(pollIntervalMs);
	}
}
