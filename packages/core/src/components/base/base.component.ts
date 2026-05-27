/**
 * Base Component - Abstract base class for all test components.
 *
 * Three-phase execution model:
 * 1. registerHook(step) - Register hooks before execution
 * 2. executeStep(step) - Execute each step
 * 3. clearHooks() - Cleanup after execution
 */

import type {InteractionRecorder} from "../../recording";
import {createDeferred, generateId} from "../../utils";
import type {Component, ComponentState, ITestCaseContext} from "./base.types";
import type {Hook} from "./hook.types";
import type {Handler, Step} from "./step.types";

export abstract class BaseComponent<TStepBuilder = unknown> implements Component<TStepBuilder> {
	readonly name: string;
	protected state: ComponentState = "created";
	protected hooks: Hook[] = [];
	protected unhandledErrors: Error[] = [];
	protected recorder?: InteractionRecorder;

	constructor(name: string) {
		this.name = name;
	}

	/**
	 * Attach an InteractionRecorder so the component reports request/response
	 * pairs into `result.interactions`. Set by TestScenario when `recording: true`.
	 */
	setRecorder(recorder: InteractionRecorder | undefined): void {
		this.recorder = recorder;
	}

	// =========================================================================
	// Abstract Methods - Must be implemented by subclasses
	// =========================================================================

	/**
	 * Create step builder for this component.
	 * Called by TestCaseBuilder.use() to get typed builder API.
	 *
	 * Implementation: Return new XxxStepBuilder(context, this)
	 */
	abstract createStepBuilder(context: ITestCaseContext): TStepBuilder;

	/**
	 * Execute a step based on its mode.
	 *
	 * Implementation should handle step.mode:
	 * - "action": Execute the action (send request, publish message, etc.)
	 * - "hook": Usually no-op (hook already registered in Phase 1)
	 * - "wait": Wait for hook to be triggered, then execute handlers
	 */
	abstract executeStep(step: Step): Promise<void>;

	/**
	 * Create a matcher function for hook registration.
	 * Called by registerHook() to create hook.isMatch predicate.
	 *
	 * Implementation: Return a function that tests if incoming message
	 * matches the step's criteria (messageType, traceId, custom matcher, etc.)
	 */
	protected abstract createHookMatcher(step: Step): (message: unknown) => boolean;

	/**
	 * Execute a single handler.
	 *
	 * Implementation should switch on handler.type and execute accordingly.
	 * Common handler types: assert, transform, delay, drop, mockResponse, proxy
	 *
	 * @returns New value to chain, null to terminate, undefined to keep current
	 */
	protected abstract executeHandler<TContext = unknown>(
		handler: Handler,
		payload: unknown,
		context?: TContext
	): Promise<unknown>;

	/**
	 * Start the component (connect, listen, etc.)
	 * Called by start() after state validation.
	 */
	protected abstract doStart(): Promise<void>;

	/**
	 * Stop the component (disconnect, close, cleanup).
	 * Called by stop() after state validation.
	 */
	protected abstract doStop(): Promise<void>;

	// =========================================================================
	// State
	// =========================================================================

	getState(): ComponentState {
		return this.state;
	}

	isStarted(): boolean {
		return this.state === "started";
	}

	isStopped(): boolean {
		return this.state === "stopped";
	}

	// =========================================================================
	// Error Tracking
	// =========================================================================

	protected trackUnhandledError(error: Error): void {
		this.unhandledErrors.push(error);
	}

	getUnhandledErrors(): Error[] {
		return [...this.unhandledErrors];
	}

	clearUnhandledErrors(): void {
		this.unhandledErrors = [];
	}

	// =========================================================================
	// Phase 1: Hook Registration
	// =========================================================================

	/**
	 * Register a hook for a step.
	 * @param step - The step to create a hook for
	 * @param withPending - If true, creates a pending Deferred for wait steps
	 */
	async registerHook(step: Step, withPending?: boolean): Promise<Hook> {
		const hook: Hook = {
			id: generateId("hook_"),
			stepId: step.id,
			testCaseId: step.testCaseId,
			isMatch: this.createHookMatcher(step),
			step: step,
			persistent: step.testCaseId === undefined,
			pending: withPending ? createDeferred() : undefined,
		};
		this.hooks.push(hook);
		return hook;
	}

	// =========================================================================
	// Phase 3: Cleanup
	// =========================================================================

	/**
	 * Clear hooks.
	 * @param testCaseId - If provided, clears only non-persistent hooks for this testCaseId.
	 *                     If empty, clears all hooks.
	 */
	clearHooks(testCaseId?: string): void {
		if (testCaseId) {
			this.hooks = this.hooks.filter((hook) => hook.persistent || hook.testCaseId !== testCaseId);
		} else {
			this.hooks = [];
		}
	}

	getAllHooks(): Hook[] {
		return [...this.hooks];
	}

	getHookById(id: string): Hook | undefined {
		return this.hooks.find((hook) => hook.id === id);
	}

	// =========================================================================
	// Handler Execution
	// =========================================================================

	/**
	 * Execute handlers for a step with result chaining.
	 *
	 * Return value semantics:
	 * - null = terminate chain (message dropped)
	 * - undefined = keep current value, continue
	 * - other = use as new value, continue
	 */
	protected async executeHandlers<TMessage, TContext = unknown>(
		step: Step,
		message: TMessage,
		context?: TContext
	): Promise<TMessage | null> {
		let current = message;

		for (const handler of step.handlers) {
			const result = await this.executeHandler(handler, current, context);

			if (result === null) {
				return null;
			}

			if (result !== undefined) {
				current = result as TMessage;
			}
		}

		return current;
	}

	// =========================================================================
	// Hook Utilities
	// =========================================================================

	protected findMatchingHook(message: unknown): Hook | null {
		for (const hook of this.hooks) {
			if (hook.resolved) continue;
			try {
				if (hook.isMatch(message)) {
					return hook;
				}
			} catch {
				// Matcher error = no match
			}
		}
		return null;
	}

	protected removeHook(hookId: string): void {
		this.hooks = this.hooks.filter((h) => h.id !== hookId);
	}

	// =========================================================================
	// Unified Wait Pattern
	// =========================================================================

	/**
	 * Resolve a hook's pending with a value.
	 * Marks the hook as resolved so it's skipped in future matching.
	 * Note: Does NOT remove the hook - cleanup happens after step execution.
	 */
	protected resolveHook(hook: Hook, value: unknown): void {
		if (hook.pending) {
			hook.pending.resolve(value);
			hook.resolved = true;
		}
	}

	/**
	 * Reject a hook's pending with an error.
	 * Removes non-persistent hooks automatically.
	 */
	protected rejectHook(hook: Hook, error: Error): void {
		if (hook.pending) {
			hook.pending.reject(error);
		}
		if (!hook.persistent) {
			this.removeHook(hook.id);
		}
	}

	/**
	 * Await a hook's pending with timeout.
	 * Returns the resolved value or throws on timeout.
	 */
	protected async awaitHook(hook: Hook, timeout: number): Promise<unknown> {
		if (!hook.pending) {
			throw new Error(`Hook ${hook.id} has no pending`);
		}

		return Promise.race([
			hook.pending.promise,
			new Promise<never>((_, reject) => {
				setTimeout(() => {
					reject(new Error(`Timeout waiting for hook ${hook.id} (${timeout}ms)`));
				}, timeout);
			}),
		]);
	}

	/**
	 * Find hook by step ID.
	 */
	protected findHookByStepId(stepId: string): Hook | undefined {
		return this.hooks.find((h) => h.step?.id === stepId);
	}

	// =========================================================================
	// Lifecycle
	// =========================================================================

	async start(): Promise<void> {
		if (this.state !== "created" && this.state !== "stopped") {
			throw new Error(`Cannot start component ${this.name} in state ${this.state}`);
		}

		this.state = "starting";

		try {
			await this.doStart();
			this.state = "started";
		} catch (error) {
			this.state = "error";
			throw error;
		}
	}

	async stop(): Promise<void> {
		if (this.state === "stopped") {
			return;
		}

		if (this.state !== "started" && this.state !== "error") {
			throw new Error(`Cannot stop component ${this.name} in state ${this.state}`);
		}

		this.state = "stopping";

		try {
			await this.doStop();
			this.state = "stopped";
		} catch (error) {
			this.state = "error";
			throw error;
		}
	}
}
