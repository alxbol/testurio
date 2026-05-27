/**
 * TestScenario Class
 *
 * Main orchestrator for test execution with component lifecycle management.
 * Component management is inlined for simplicity.
 */

import type {Component} from "../components";
// TODO: AsyncClient and AsyncServer not yet migrated to new execution model
// import { AsyncClient, AsyncServer, Client, Server } from "../components";
import {Client, Server} from "../components";
import type {Interaction, TestReporter} from "../recording";
import {ConsoleReporter, InteractionRecorder} from "../recording";
import type {TestCaseResult, TestResult, TestStepResult} from "./execution.types";
import type {TestCase} from "./test-case";
import {TestCaseBuilder} from "./test-case-builder";

/**
 * Test scenario configuration
 */
export interface TestScenarioConfig {
	name: string;
	description?: string;
	components: Component[];
	timeout?: number;
	recording?: boolean;
	metadata?: Record<string, unknown>;
}

/**
 * TestScenario - orchestrates test execution
 */
export class TestScenario {
	// ========== Instance Members ==========

	private config: TestScenarioConfig;
	private components = new Map<string, Component>();
	private initHandler?: (test: TestCaseBuilder) => void;
	private stopHandler?: (test: TestCaseBuilder) => void;
	private interactions: Interaction[] = [];
	private initialized = false;
	private recorder: InteractionRecorder;
	private reporters: TestReporter[] = [];

	constructor(config: TestScenarioConfig) {
		this.config = config;
		this.recorder = new InteractionRecorder();

		// Enable recording if configured
		if (!this.config.recording) {
			this.recorder.disable();
		}

		// Register components (already created externally)
		for (const component of config.components) {
			this.registerComponent(component);
		}
	}

	// ========== Component Management ==========

	/**
	 * Register a component
	 */
	private registerComponent(component: Component): void {
		if (this.components.has(component.name)) {
			throw new Error(`Component ${component.name} already exists`);
		}
		this.components.set(component.name, component);
		if (this.config.recording) {
			component.setRecorder?.(this.recorder);
		}
	}

	/**
	 * Check if a component is a network component (Server, Client, AsyncServer, AsyncClient)
	 * TODO: Add AsyncServer/AsyncClient back when migrated
	 */
	private isNetworkComponent(component: Component): boolean {
		return component instanceof Server || component instanceof Client;
	}

	/**
	 * Start all components
	 * Order: non-network components first, then servers, then clients
	 *
	 * Non-network components (like DataSource) start first because:
	 * - They may be dependencies for network components (e.g., database for API server)
	 *
	 * Servers are started sequentially in the order they were defined in config.components because:
	 * - Proxy servers need their target backend to be ready before accepting connections
	 * - The user defines components in dependency order (backend before proxy)
	 */
	private async startComponents(): Promise<void> {
		// Use original config order to preserve dependency order
		const all = this.config.components;

		// Categorize components
		const nonNetwork = all.filter((c) => !this.isNetworkComponent(c));
		// TODO: Add AsyncServer/AsyncClient back when migrated
		const servers = all.filter((c) => c instanceof Server);
		const clients = all.filter((c) => c instanceof Client);

		// Start non-network components first (e.g., DataSource) - they may be dependencies
		for (const component of nonNetwork) {
			await component.start();
		}

		// Start servers sequentially (in config order) to handle dependencies
		for (const server of servers) {
			await server.start();
		}

		// Clients can start in parallel (they connect to already-running servers)
		await Promise.all(clients.map((c) => c.start()));
	}

	/**
	 * Stop all components
	 * Order: clients first, then servers in reverse order, then non-network components
	 *
	 * Servers are stopped in reverse config order because:
	 * - Components are defined in dependency order (backend before proxy)
	 * - Stopping in reverse order ensures proxies stop before backends
	 * - This prevents ECONNREFUSED errors from in-flight proxy connections
	 *
	 * Non-network components (like DataSource) stop last because:
	 * - They may still be needed by cleanup handlers
	 */
	private async stopComponents(): Promise<void> {
		// Use original config order to get correct dependency order
		const all = this.config.components;

		// Categorize components
		// TODO: Add AsyncServer/AsyncClient back when migrated
		const clients = all.filter((c) => c instanceof Client);
		const servers = all.filter((c) => c instanceof Server);
		const nonNetwork = all.filter((c) => !this.isNetworkComponent(c));

		// Helper to stop with timeout (1 second)
		const stopWithTimeout = async (component: Component): Promise<void> => {
			const stopTimeout = 1000;
			try {
				await Promise.race([
					component.stop(),
					new Promise<void>((_, reject) =>
						setTimeout(() => reject(new Error(`Stop timeout for ${component.name}`)), stopTimeout)
					),
				]);
			} catch {
				// Ignore stop errors and timeouts
			}
		};

		// Stop clients in parallel
		await Promise.all(clients.map((c) => stopWithTimeout(c)));

		// Stop servers sequentially in reverse order (proxies before backends)
		for (let i = servers.length - 1; i >= 0; i--) {
			await stopWithTimeout(servers[i]);
		}

		// Stop non-network components last (in reverse order)
		for (let i = nonNetwork.length - 1; i >= 0; i--) {
			await stopWithTimeout(nonNetwork[i]);
		}
	}

	/**
	 * Define init handler
	 */
	init(handler: (test: TestCaseBuilder) => void): this {
		this.initHandler = handler;
		return this;
	}

	/**
	 * Define stop handler
	 */
	stop(handler: (test: TestCaseBuilder) => void): this {
		this.stopHandler = handler;
		return this;
	}

	/**
	 * Get interaction recorder
	 */
	getRecorder(): InteractionRecorder {
		return this.recorder;
	}

	/**
	 * Add a reporter
	 */
	addReporter(reporter: TestReporter): this {
		this.reporters.push(reporter);
		return this;
	}

	/**
	 * Use console reporter
	 */
	useConsoleReporter(options?: { verbose?: boolean }): this {
		this.reporters.push(new ConsoleReporter(options));
		return this;
	}

	/**
	 * Run initialization
	 *
	 * Lifecycle order:
	 * 1. Run init handler (collect steps)
	 * 2. Register init hooks BEFORE starting components (routes must be registered for HTTP)
	 * 3. Start components (servers start with all routes already registered)
	 * 4. Process any dynamically created components
	 */
	private async runInit(): Promise<void> {
		if (this.initialized) return;

		let builder: ReturnType<typeof this.createBuilder> | undefined;

		// Run init handler FIRST to collect steps
		if (this.initHandler) {
			builder = this.createBuilder();
			builder.setPhase("init");
			this.initHandler(builder);
		}

		// Register init hooks BEFORE starting components
		// This is critical for HTTP where routes must be registered before server starts
		if (builder) {
			const steps = builder.getSteps();
			for (const step of steps) {
				if (step.mode === "hook" || step.mode === "wait") {
					step.component.registerHook(step);
				}
			}
		}

		// Start initial components (from constructor config)
		// Routes are already registered from init handler
		await this.startComponents();

		// Process any dynamically created components (start them)
		if (builder) {
			await this.processPendingComponents(builder);
		}

		this.initialized = true;
	}

	/**
	 * Run cleanup
	 */
	private async runStop(): Promise<void> {
		// Run stop handler if defined
		if (this.stopHandler) {
			const builder = this.createBuilder();
			builder.setPhase("stop");
			this.stopHandler(builder);

			// Execute stop steps
			const steps = builder.getSteps();
			for (const step of steps) {
				try {
					await step.component.executeStep(step);
				} catch (_error) {
					// Continue with cleanup even if stop steps fail
				}
			}
		}

		// Stop all components (components dispose their own protocols and clear their hooks)
		await this.stopComponents();

		this.initialized = false;
	}

	/**
	 * Create a test case builder
	 */
	private createBuilder(): TestCaseBuilder {
		const builder = new TestCaseBuilder(this.components);
		builder.setComponentRegistry(this.components);
		return builder;
	}

	/**
	 * Process pending components from a builder
	 * Starts components (already registered during builder phase), returns list of test-case-scoped components for cleanup
	 */
	private async processPendingComponents(builder: TestCaseBuilder): Promise<Component[]> {
		const pending = builder.getPendingComponents();
		const testCaseComponents: Component[] = [];

		for (const { component, options } of pending) {
			// Start the component
			await component.start();

			// Track test-case-scoped components for cleanup
			if (options.scope === "testCase") {
				testCaseComponents.push(component);
			}
		}

		builder.clearPendingComponents();
		return testCaseComponents;
	}

	/**
	 * Stop and remove test-case-scoped components
	 */
	private async cleanupTestCaseComponents(components: Component[]): Promise<void> {
		for (const component of components) {
			try {
				await component.stop();
				this.components.delete(component.name);
			} catch {
				// Continue cleanup even if stop fails
			}
		}
	}

	/**
	 * Run test cases
	 *
	 * Execution modes:
	 * - Single TestCase: runs alone
	 * - Array of TestCase[]: runs sequentially within the array
	 * - Multiple args: runs in parallel across args
	 *
	 * Example:
	 *   run(test1, test2, test3)           // all 3 run in parallel
	 *   run([test1, test2], test3)         // test1 & test2 sequential, test3 in parallel with them
	 *   run([test1, test2], [test3, test4]) // two sequential groups running in parallel
	 */
	async run(...testCases: (TestCase | TestCase[])[]): Promise<TestResult> {
		const startTime = Date.now();
		const results: TestCaseResult[] = [];
		let initError: Error | undefined;

		// Count expected test cases for validation
		const expectedTestCount = testCases.reduce((sum, tc) => sum + (Array.isArray(tc) ? tc.length : 1), 0);

		// Notify reporters of start
		for (const reporter of this.reporters) {
			reporter.onStart?.({ name: this.config.name, startTime });
		}

		try {
			// Initialize scenario
			await this.runInit();

			// Normalize: each arg becomes a group (array runs sequentially, single item is a group of 1)
			const parallelGroups = testCases.map((tc) => (Array.isArray(tc) ? tc : [tc]));

			// Execute all groups in parallel, within each group run sequentially
			const executeGroup = async (group: TestCase[]): Promise<TestCaseResult[]> => {
				const results: TestCaseResult[] = [];
				for (const testCase of group) {
					const result = await this.executeTestCase(testCase);
					results.push(result);
				}
				return results;
			};

			// Create promises but don't await yet - this starts all groups in parallel
			const groupResults = await Promise.all(parallelGroups.map(executeGroup));

			// Flatten results
			for (const group of groupResults) {
				results.push(...group);
			}
		} catch (error) {
			// Capture initialization/execution error instead of swallowing it
			initError = error instanceof Error ? error : new Error(String(error));
		} finally {
			// Always run cleanup
			try {
				await this.runStop();
			} catch (_error) {
				// Cleanup failed - this is OK to swallow as it's just cleanup
			}
		}

		const endTime = Date.now();

		const result = this.createTestResult(results, startTime, endTime, expectedTestCount, initError);

		// Notify reporters of completion
		for (const reporter of this.reporters) {
			reporter.onComplete(result);
		}

		return result;
	}

	/**
	 * Execute a single test case
	 */
	private async executeTestCase(testCase: TestCase): Promise<TestCaseResult> {
		// Notify reporters
		for (const reporter of this.reporters) {
			reporter.onTestCaseStart?.({ name: testCase.name });
		}

		const builder = this.createBuilder();
		let testCaseComponents: Component[] = [];

		// Clear unhandled errors before test case
		for (const component of this.components.values()) {
			component.clearUnhandledErrors();
		}

		// Snapshot recorder so we can slice off interactions captured during this test case
		const recordedBefore = this.config.recording ? this.recorder.getInteractions().length : 0;

		const result = await testCase.execute(builder, {
			failFast: true,
			onBeforeExecute: async () => {
				// Process any dynamically created components before executing steps
				testCaseComponents = await this.processPendingComponents(builder);
			},
			onStepComplete: (stepResult: TestStepResult) => {
				// Notify reporters
				for (const reporter of this.reporters) {
					reporter.onStepComplete?.(stepResult);
				}
			},
		});

		// Check for unhandled errors from components (e.g., server-side assertion failures)
		const unhandledErrors: Error[] = [];
		for (const component of this.components.values()) {
			unhandledErrors.push(...component.getUnhandledErrors());
		}

		// If test passed but there are unhandled errors, mark as failed
		let finalResult = result;
		if (result.passed && unhandledErrors.length > 0) {
			const firstError = unhandledErrors[0];
			finalResult = {
				...result,
				passed: false,
				error: firstError.message,
				stackTrace: firstError.stack,
				failedSteps: result.failedSteps + 1,
			};
		}

		// Cleanup test-case-scoped components
		if (testCaseComponents.length > 0) {
			await this.cleanupTestCaseComponents(testCaseComponents);
		}

		// Clear test case hooks from all components (only for this test case)
		for (const component of this.components.values()) {
			component.clearHooks(testCase.testCaseId);
		}

		// Record interactions if enabled — pull what the recorder captured for this case
		if (this.config.recording) {
			const captured = this.recorder.getInteractions().slice(recordedBefore);
			if (captured.length > 0) {
				finalResult = {...finalResult, interactions: captured};
				this.interactions.push(...captured);
			}
		}

		// Notify reporters
		for (const reporter of this.reporters) {
			reporter.onTestCaseComplete?.(finalResult);
		}

		return finalResult;
	}

	/**
	 * Create final test result
	 *
	 * @param testCases - Array of test case results
	 * @param startTime - Scenario start time
	 * @param endTime - Scenario end time
	 * @param expectedTestCount - Number of test cases that were expected to run
	 * @param initError - Error that occurred during initialization (if any)
	 */
	private createTestResult(
		testCases: TestCaseResult[],
		startTime: number,
		endTime: number,
		expectedTestCount: number,
		initError?: Error
	): TestResult {
		const passedTests = testCases.filter((tc) => tc.passed).length;
		const failedTests = testCases.filter((tc) => !tc.passed).length;

		// Scenario fails if:
		// 1. Any test case failed
		// 2. There was an initialization error
		// 3. Expected test cases didn't run (e.g., init failed before execution)
		const hasInitError = initError !== undefined;
		const missingTests = expectedTestCount > 0 && testCases.length < expectedTestCount;
		const passed = failedTests === 0 && !hasInitError && !missingTests;

		return {
			name: this.config.name,
			passed,
			duration: endTime - startTime,
			startTime,
			endTime,
			testCases,
			passedTests,
			failedTests,
			totalTests: testCases.length,
			interactions: this.config.recording ? this.interactions : undefined,
			error: initError?.message,
			stackTrace: initError?.stack,
			summary: {
				totalTestCases: testCases.length,
				passedTestCases: passedTests,
				failedTestCases: failedTests,
				totalSteps: testCases.reduce((sum, tc) => sum + tc.totalSteps, 0),
				passedSteps: testCases.reduce((sum, tc) => sum + tc.passedSteps, 0),
				failedSteps: testCases.reduce((sum, tc) => sum + tc.failedSteps, 0),
				totalDuration: endTime - startTime,
				averageDuration: testCases.length > 0 ? Math.round((endTime - startTime) / testCases.length) : 0,
				totalInteractions: this.interactions.length,
				passRate: testCases.length > 0 ? passedTests / testCases.length : 0,
			},
		};
	}
}

/**
 * Factory function for creating test scenarios
 */
export function scenario(config: TestScenarioConfig): TestScenario {
	return new TestScenario(config);
}
