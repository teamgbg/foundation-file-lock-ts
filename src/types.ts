/**
 * @system file-lock
 * @status handwritten
 */

export interface LockOptions {
	timeoutMs: number;
	pollIntervalMs?: number;
	stalePidCheck?: boolean;
}

export interface LockConfig {
	defaultTimeoutMs?: number;
	overrides?: Record<string, { enabled?: boolean }>;
}

export interface ActiveLockInfo {
	path: string;
	pid: number;
	heldSinceMs: number;
	ageMs: number;
}
