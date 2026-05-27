/**
 * Client Component - Sync client for request/response protocols (HTTP, gRPC unary).
 *
 * Response handling modes:
 * - onResponse (non-strict): Works regardless of timing
 * - waitResponse (strict): Error if response arrives before step starts
 */

import type {Address, ISyncClientAdapter, ISyncProtocol, SyncValidationOptions, TlsConfig,} from "../../protocols/base";
import type {SchemaLike, SyncSchemaInput} from "../../validation";
import {ValidationError} from "../../validation";
import type {Handler, Hook, ITestCaseContext, Step, ValueOrFactory} from "../base";
import {resolveValue, ServiceComponent} from "../base";
import {SyncClientStepBuilder} from "./sync-client.step-builder";

interface ResponseMessage {
	type: string;
	payload: unknown;
}

export interface ClientOptions<P extends ISyncProtocol = ISyncProtocol> {
	protocol: P;
	targetAddress: Address;
	tls?: TlsConfig;
	/** Control auto-validation behavior */
	validation?: SyncValidationOptions;
}

export class Client<P extends ISyncProtocol = ISyncProtocol> extends ServiceComponent<P, SyncClientStepBuilder<P>> {
	private readonly _targetAddress: Address;
	private readonly _tls?: TlsConfig;
	private readonly _schema?: SyncSchemaInput;
	private readonly _validation?: SyncValidationOptions;
	private _clientAdapter?: ISyncClientAdapter;

	constructor(name: string, options: ClientOptions<P>) {
		super(name, options.protocol);
		this._targetAddress = options.targetAddress;
		this._tls = options.tls;
		this._schema = options.protocol.schema as SyncSchemaInput | undefined;
		this._validation = options.validation;
	}

	static create<P extends ISyncProtocol>(name: string, options: ClientOptions<P>): Client<P> {
		return new Client<P>(name, options);
	}

	createStepBuilder(context: ITestCaseContext): SyncClientStepBuilder<P> {
		return new SyncClientStepBuilder<P>(context, this);
	}

	get targetAddress(): Address {
		return this._targetAddress;
	}

	get validationOptions(): SyncValidationOptions | undefined {
		return this._validation;
	}

	// =========================================================================
	// Hook Registration
	// =========================================================================

	async registerHook(step: Step): Promise<Hook> {
		const withPending = step.type === "onResponse" || step.type === "waitResponse";
		return super.registerHook(step, withPending);
	}

	// =========================================================================
	// Step Execution
	// =========================================================================

	async executeStep(step: Step): Promise<void> {
		switch (step.type) {
			case "request":
				await this.executeRequest(step);
				break;
			case "onResponse":
			case "waitResponse":
				await this.executeResponseStep(step);
				break;
			default:
				throw new Error(`Unknown step type: ${step.type} for Client ${this.name}`);
		}
	}

	private async executeRequest(step: Step): Promise<void> {
		const params = step.params as {
			messageType: string;
			data?: ValueOrFactory<P["$request"]>;
			traceId?: string;
		};

		const data = resolveValue(params.data);

		// Auto-validate outgoing request
		if (this._validation?.validateRequests !== false) {
			this.autoValidate(params.messageType, "request", data);
		}

		// Find matching response hooks (already registered with pending in Phase 1)
		const preMatchMessage: ResponseMessage = { type: params.messageType, payload: undefined };
		const matchingHooks = this.findAllMatchingHooks(preMatchMessage);

		// Begin recording (no-op if no recorder attached)
		const interactionId = this.recorder?.startInteraction({
			serviceName: this.name,
			direction: "downstream",
			protocol: this.protocol.type,
			messageType: params.messageType,
			traceId: params.traceId,
			requestPayload: data,
		});

		// Start request (don't await)
		const requestPromise = this.request(params.messageType, data);

		if (interactionId) {
			requestPromise
				.then((response) => {
					this.recorder?.completeInteraction(interactionId, {responsePayload: response});
				})
				.catch((error) => {
					this.recorder?.failInteraction(
						interactionId,
						error instanceof Error ? error.message : String(error),
					);
				});
		}

		// Resolve hooks when response arrives
		for (const hook of matchingHooks) {
			if (!hook.step) continue;

			requestPromise
				.then((response) => {
					this.resolveHook(hook, response);
				})
				.catch((error) => {
					this.rejectHook(hook, error instanceof Error ? error : new Error(String(error)));
				});
		}
	}

	private async executeResponseStep(step: Step): Promise<void> {
		const params = step.params as {
			messageType: string;
			timeout?: number;
		};
		const timeout = params.timeout ?? 5000;

		const hook = this.findHookByStepId(step.id);
		if (!hook) {
			throw new Error(
				`No matching request for ${step.type}: ${params.messageType}. ` +
					`Make sure request() is called before ${step.type}().`
			);
		}

		// Strict ordering check for waitResponse
		if (step.type === "waitResponse" && hook.resolved) {
			throw new Error(
				`Strict ordering violation: Response arrived before waitResponse started. ` +
					`Step: ${step.id}, messageType: ${params.messageType}. ` +
					`Use onResponse() if ordering doesn't matter.`
			);
		}

		try {
			const response = await this.awaitHook(hook, timeout);

			// Auto-validate incoming response
			if (this._validation?.validateResponses !== false) {
				this.autoValidate(params.messageType, "response", response);
			}

			await this.executeHandlers(step, response);
		} finally {
			if (!hook.persistent) {
				this.removeHook(hook.id);
			}
		}
	}

	// =========================================================================
	// Hook Matching
	// =========================================================================

	protected createHookMatcher(step: Step): (message: unknown) => boolean {
		if (step.type !== "onResponse" && step.type !== "waitResponse") {
			return () => false;
		}

		const params = step.params as { messageType: string };

		return (message: unknown): boolean => {
			const msg = message as ResponseMessage;
			return msg.type === params.messageType;
		};
	}

	// =========================================================================
	// Handler Execution
	// =========================================================================

	protected async executeHandler<TContext = unknown>(
		handler: Handler,
		payload: unknown,
		_context?: TContext
	): Promise<unknown> {
		const params = handler.params as Record<string, unknown>;

		switch (handler.type) {
			case "assert": {
				const predicate = params.predicate as (p: unknown) => boolean | Promise<boolean>;
				const result = await predicate(payload);
				if (!result) {
					const errorMsg = handler.description ? `Assertion failed: ${handler.description}` : "Assertion failed";
					throw new Error(errorMsg);
				}
				return undefined;
			}

			case "transform": {
				const transformFn = params.handler as (p: unknown) => unknown | Promise<unknown>;
				return await transformFn(payload);
			}

			case "validate": {
				const explicitSchema = params.schema as SchemaLike | undefined;
				const lookupKey = params.lookupKey as string;
				const lookupDirection = params.lookupDirection as string;

				const schema = explicitSchema ?? this.lookupSchema(lookupKey, lookupDirection);
				if (!schema) {
					throw new ValidationError(`No schema registered for '${lookupKey}' (${lookupDirection})`, {
						componentName: this.name,
						operationId: lookupKey,
						direction: lookupDirection,
					});
				}

				try {
					return schema.parse(payload);
				} catch (cause) {
					if (cause instanceof ValidationError) throw cause;
					throw new ValidationError(`Validation failed for ${this.name} '${lookupKey}' (${lookupDirection})`, {
						componentName: this.name,
						operationId: lookupKey,
						direction: lookupDirection,
						cause,
					});
				}
			}

			default:
				return undefined;
		}
	}

	// =========================================================================
	// Schema Lookup
	// =========================================================================

	/**
	 * Look up schema for an operation by key and direction.
	 * Used by validate handler and auto-validation.
	 */
	lookupSchema(key: string, direction: string): SchemaLike | undefined {
		if (!this._schema) return undefined;
		const opSchema = this._schema[key];
		if (!opSchema) return undefined;
		if (direction === "request") return opSchema.request;
		if (direction === "response") return opSchema.response;
		return undefined;
	}

	/**
	 * Auto-validate data against registered schema.
	 * No-op if no schema is registered for the given key/direction.
	 */
	private autoValidate(key: string, direction: string, data: unknown): void {
		const schema = this.lookupSchema(key, direction);
		if (!schema) return;

		try {
			schema.parse(data);
		} catch (cause) {
			throw new ValidationError(`Auto-validation failed for ${this.name} '${key}' (${direction})`, {
				componentName: this.name,
				operationId: key,
				direction,
				cause,
			});
		}
	}

	// =========================================================================
	// Protocol Operations
	// =========================================================================

	async request(messageType: string, data?: P["$request"]): Promise<P["$response"]> {
		if (!this.isStarted()) {
			throw new Error(`Client ${this.name} is not started`);
		}

		if (!this._clientAdapter) {
			throw new Error(`Client ${this.name} has no client adapter`);
		}

		return this._clientAdapter.request(messageType, data);
	}

	// =========================================================================
	// Lifecycle
	// =========================================================================

	protected async doStart(): Promise<void> {
		this._clientAdapter = await this.protocol.createClient({
			targetAddress: this._targetAddress,
			tls: this._tls,
		});
	}

	protected async doStop(): Promise<void> {
		if (this._clientAdapter) {
			await this._clientAdapter.close();
			this._clientAdapter = undefined;
		}
		this.clearHooks();
	}
}
