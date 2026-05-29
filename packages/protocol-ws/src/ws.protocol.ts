/**
 * WebSocket Protocol
 *
 * Implements async bidirectional messaging over WebSocket.
 *
 * v2 Design:
 * - Protocol handles transport only (sockets, framing, encoding)
 * - Connection wrappers handle handler registration and matching
 * - Components handle hooks, sessions, and business logic
 *
 * @template S - Service definition type with clientMessages/serverMessages
 */

import type {
	AsyncSchemaInput,
	ClientProtocolConfig,
	IAsyncClientAdapter,
	IAsyncProtocol,
	IAsyncServerAdapter,
	InferAsyncMessages,
	SchemaDefinition,
	ServerProtocolConfig,
} from "testurio";
import {BaseAsyncProtocol} from "testurio";
import type {WsConnectParams, WsProtocolOptions, WsServiceDefinition} from "./types";
import {WsClientAdapter, WsServerAdapter} from "./ws.adapters";

/**
 * Resolve WebSocket protocol type from generic parameter.
 *
 * Three cases:
 * 1. S = never → WsServiceDefinition (loose mode with index signatures)
 * 2. S = AsyncSchemaInput → InferAsyncMessages<S> (schema inference)
 * 3. S = explicit type → S as-is (backward compat)
 */
type ResolveWsType<S> = [S] extends [never]
	? WsServiceDefinition
	: S extends AsyncSchemaInput
		? InferAsyncMessages<S>
		: S extends WsServiceDefinition
			? S
			: WsServiceDefinition;

/**
 * WebSocket Protocol
 *
 * Provides WebSocket client and server functionality for testing.
 * Uses real WebSocket servers and connections for actual network communication.
 *
 * @template S - Schema input, explicit service type, or never (loose mode)
 *
 * @example
 * ```typescript
 * interface MyWsService {
 *   clientMessages: {
 *     ping: { seq: number };
 *     subscribe: { channel: string };
 *   };
 *   serverMessages: {
 *     pong: { seq: number };
 *     subscribed: { channel: string; success: boolean };
 *   };
 * }
 *
 * const protocol = new WebSocketProtocol<MyWsService>();
 * ```
 */
export class WebSocketProtocol<S = never>
	extends BaseAsyncProtocol<ResolveWsType<S>, WsConnectParams>
	implements IAsyncProtocol<ResolveWsType<S>, WsConnectParams>
{
	readonly type = "websocket";
	override readonly schema?: AsyncSchemaInput;

	constructor(private _options: WsProtocolOptions<S> = {} as WsProtocolOptions<S>) {
		super();
		this.schema = _options.schema as AsyncSchemaInput | undefined;
	}

	/**
	 * Get protocol options
	 */
	getOptions(): WsProtocolOptions<S> {
		return this._options;
	}

	/**
	 * Load JSON schema (optional for WebSocket)
	 */
	async loadSchema(schemaPath: string | string[]): Promise<SchemaDefinition> {
		const paths = Array.isArray(schemaPath) ? schemaPath : [schemaPath];
		return {
			type: "json-schema",
			content: { paths: paths.join(",") },
			validate: true,
		};
	}

	/**
	 * Create and start WebSocket server adapter (v3 API)
	 * Component owns the returned adapter
	 */
	async createServer(config: ServerProtocolConfig): Promise<IAsyncServerAdapter> {
		return WsServerAdapter.create(config.listenAddress.host, config.listenAddress.port, this._options.codec);
	}

	/**
	 * Create WebSocket client adapter (v3 API)
	 * Component owns the returned adapter
	 */
	async createClient(config: ClientProtocolConfig): Promise<IAsyncClientAdapter> {
		// Connection timeout: config overrides protocol options
		const connectionTimeout = config.timeouts?.connectionTimeout ?? this._options.timeout;
		const wsParams = config.connectParams as WsConnectParams | undefined;

		// connectParams.path overrides targetAddress.path
		const path = wsParams?.path ?? config.targetAddress.path;

		return WsClientAdapter.create(
			config.targetAddress.host,
			config.targetAddress.port,
			path,
			config.tls?.enabled,
			connectionTimeout,
			this._options.codec,
			wsParams
		);
	}
}

/**
 * Create WebSocket protocol factory
 */
export function createWebSocketProtocol<S = never>(options?: WsProtocolOptions<S>): WebSocketProtocol<S> {
	return new WebSocketProtocol<S>(options);
}
