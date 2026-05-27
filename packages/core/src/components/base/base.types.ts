/**
 * Base Component Types
 *
 * Common types for all components - interfaces that allow
 * TestScenario and TestCaseBuilder to work with any component type.
 */

import type {Hook} from "./hook.types";
import type {Step} from "./step.types";

// =============================================================================
// Component Interface
// =============================================================================

/**
 * Common component interface shared by all components.
 *
 * This interface allows TestScenario and TestCaseBuilder to work with
 * any component type uniformly.
 *
 * Implements three-phase execution model:
 * - Phase 1: registerHook(step) - Register hooks before execution
 * - Phase 2: executeStep(step) - Execute each step
 * - Phase 3: clearHooks() - Cleanup after execution
 */
export interface Component<TStepBuilder = unknown> {
	/** Component name (must be unique within scenario) */
	readonly name: string;

	/** Get current component state */
	getState(): string;

	/** Check if component is started */
	isStarted(): boolean;

	/** Check if component is stopped */
	isStopped(): boolean;

	/** Start the component */
	start(): Promise<void>;

	/** Stop the component */
	stop(): Promise<void>;

	/**
	 * Create a step builder for use in testCase.
	 * @param context - Test case builder context
	 */
	createStepBuilder(context: ITestCaseContext): TStepBuilder;

	// =========================================================================
	// Three-Phase Execution
	// =========================================================================

	/**
	 * Phase 1: Register a hook for a step.
	 * Called by executor for steps with mode="hook" or mode="wait".
	 * Creates Hook from Step and stores it for message matching.
	 * May perform async setup (e.g., subscriptions) before returning.
	 * @param step - The step to create a hook for
	 * @param withPending - If true, creates a pending Deferred for wait steps
	 * @returns The created Hook
	 */
	registerHook(step: Step, withPending?: boolean): Promise<Hook>;

	/**
	 * Phase 2: Execute a step.
	 * Called by executor for ALL steps.
	 * Behavior depends on step.mode:
	 *   - "action": Execute the action (send message, make request, etc.)
	 *   - "hook": No-op (hook already registered in Phase 1)
	 *   - "wait": Wait for hook to be triggered
	 */
	executeStep(step: Step): Promise<void>;

	/**
	 * Phase 3: Clear hooks.
	 * Called by executor after step execution (success or error).
	 * @param testCaseId - If provided, removes non-persistent hooks with matching testCaseId.
	 *                     If empty/undefined, clears all hooks.
	 */
	clearHooks(testCaseId?: string): void;

	// =========================================================================
	// Error Tracking
	// =========================================================================

	/** Get unhandled errors */
	getUnhandledErrors(): Error[];

	/** Clear unhandled errors */
	clearUnhandledErrors(): void;

	/**
	 * Attach an InteractionRecorder for request/response recording.
	 * Called by TestScenario when `recording: true`. Optional for components
	 * that don't produce recordable interactions.
	 */
	setRecorder?(recorder: import("../../recording").InteractionRecorder | undefined): void;
}

// =============================================================================
// Test Case Builder Interface
// =============================================================================

/**
 * Test case builder context for step registration.
 *
 * Passed to StepBuilder constructor - allows builders to register steps.
 * This is a minimal interface that works with any TestCaseBuilder implementation.
 */
export interface ITestCaseContext {
	/** Current test phase (accepts any string for compatibility) */
	phase: string;

	/** Test case ID for hook isolation (optional) */
	testCaseId?: string;

	/**
	 * Register a step in the test case.
	 * Accepts Step object (pure data, no action function).
	 */
	registerStep(step: Step): void;
}

// =============================================================================
// Component State
// =============================================================================

/**
 * Component lifecycle state
 */
export type ComponentState = "created" | "starting" | "started" | "stopping" | "stopped" | "error";

// =============================================================================
// Component Options
// =============================================================================

/**
 * Options for dynamic component creation
 */
export interface CreateComponentOptions {
	scope?: "scenario" | "testCase";
}

// =============================================================================
// Payload Matchers
// =============================================================================

/**
 * Match by trace ID (for correlating request/response)
 */
export interface TraceIdMatcher {
	type: "traceId";
	value: string;
}

/**
 * Match by custom function
 */
export interface FunctionMatcher<T = unknown> {
	type: "function";
	fn: (payload: T) => boolean;
}

/**
 * Payload matcher - matches by traceId or custom function
 */
export type PayloadMatcher<T = unknown> = TraceIdMatcher | FunctionMatcher<T>;

// Re-export Hook type for convenience
export type { Hook };
