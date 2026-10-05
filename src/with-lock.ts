/**
 * @system file-lock
 * @status handwritten
 * @edit the single sanctioned file-locking surface (constitution `file-lock-is-the-only-file-lock`) — a lock is an atomic lockfile holding the holder PID (not flock(2), see doctrine), written to a process-unique temp file and `link()`ed into place, so the lockfile NEVER exists in a PID-less state
 *
 * Released in a finally and on graceful process exit; unexpected deaths are reclaimed by the next
 * caller's stale check.
 */

import { linkSync } from "node:fs";
import { rm, unlink, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileLockRegistry } from "./registry.ts";
import type { LockOptions } from "./types.ts";

function isAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (err) {
		// ESRCH = no such process (dead). EPERM = exists but not signalable (alive).
		return (err as NodeJS.ErrnoException).code === "EPERM";
	}
}

async function readHolderPid(lockPath: string): Promise<number | null> {
	try {
		const raw = await Bun.file(lockPath).text();
		const pid = Number.parseInt(raw.trim(), 10);
		return Number.isFinite(pid) && pid > 0 ? pid : null;
	} catch {
		return null;
	}
}

export async function withFileLock<T>(
	path: string,
	opts: LockOptions,
	fn: () => Promise<T>,
): Promise<T> {
	const { timeoutMs, pollIntervalMs = 50, stalePidCheck = true } = opts;

	// Disabled locks (registry/config override) run the critical section without serialising — transparent fall-through, mirroring cache/spawn disable.
	if (fileLockRegistry.isDisabled(path)) return fn();

	// Temp lives in the lock's own dir so link()'s source and target share one filesystem (hardlinks cannot cross mount boundaries); the name is unique per invocation (pid + nanos + random), so concurrent acquirers never collide.
	const temp = join(
		dirname(path),
		`.scala-lock-tmp.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2, 8)}`,
	);

	const deadline = Date.now() + timeoutMs;
	try {
		for (;;) {
			// Write the PID to the temp file FIRST (exclusively), then atomically link it into place: on success the lockfile appears with the PID already inside; on EEXIST it is held and we fall through to the stale check.
			await writeFile(temp, String(process.pid), { flag: "wx" });
			let acquired = false;
			try {
				linkSync(temp, path); // atomic: EEXIST iff path already exists
				acquired = true;
			} catch (err) {
				if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
			} finally {
				// path (if linked) is an independent hardlink now; removing temp never disturbs a linked lockfile's contents.
				await unlink(temp).catch(() => {});
			}
			if (acquired) break;

			// Lock is held. Break it if the holder is dead OR PID-less: under link-from-temp a live holder ALWAYS links a PID in, so a PID-less lockfile is definitively a stale orphan.
			if (stalePidCheck) {
				const holder = await readHolderPid(path);
				if (holder === null || !isAlive(holder)) {
					await unlink(path).catch(() => {});
					continue; // retry immediately
				}
			}
			if (Date.now() >= deadline) {
				throw new Error(
					`@teamscala/file-lock: timed out acquiring "${path}" after ${timeoutMs}ms`,
				);
			}
			await Bun.sleep(pollIntervalMs);
		}
	} catch (err) {
		// Never leak a temp across a throw (e.g. write/link failure).
		await unlink(temp).catch(() => {});
		throw err;
	}

	fileLockRegistry.register(path, process.pid);
	// Release on graceful exit. SIGKILL/crash leaves the file → reclaimed by the next caller's stale check. (No signal-shutdown phase: sibling-primitive horizontal dep is forbidden.)
	const onExit = async (): Promise<void> => {
		try {
			await rm(path);
		} catch {
			/* already gone */
		}
	};
	process.once("exit", onExit);
	try {
		return await fn();
	} finally {
		process.removeListener("exit", onExit);
		await unlink(path).catch(() => {});
		fileLockRegistry.unregister(path);
	}
}
