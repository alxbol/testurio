/**
 * TCP Protocol
 *
 * Implements async bidirectional messaging over TCP.
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
	ServerProtocolConfig,
} from "testurio";
import {BaseAsyncProtocol} from "testurio";
import {TcpClientAdapter, TcpServerAdapter} from "./tcp.adapters";
import type {TcpProtocolOptions, TcpServiceDefinition} from "./types";

/**
 * Resolve TCP protocol type from generic parameter.
 *
 * Three cases:
 * 1. S = never → TcpServiceDefinition (loose mode with index signatures)
 * 2. S = AsyncSchemaInput → InferAsyncMessages<S> (schema inference)
 * 3. S = explicit type with clientMessages/serverMessages → S as-is (backward compat)
 */
type ResolveTcpType<S> = [S] extends [never]
	? TcpServiceDefinition
	: S extends AsyncSchemaInput
		? InferAsyncMessages<S>
		: S extends TcpServiceDefinition
			? S
			: TcpServiceDefinition;

/**
 * TCP Protocol
 *
 * Provides TCP client and server functionality for testing.
 * Uses real TCP servers and sockets for actual network communication.
 *
 * @template S - Schema input, explicit service type, or never (loose mode)
 *
 * @example
 * ```typescript
 * interface MyTcpService {
 *   clientMessages: {
 *     OrderRequest: { orderId: string; quantity: number };
 *   };
 *   serverMessages: {
 *     OrderResponse: { orderId: string; status: string };
 *   };
 * }
 *
 * const protocol = new TcpProtocol<MyTcpService>();
 * ```
 */
export class TcpProtocol<S = never>
	extends BaseAsyncProtocol<ResolveTcpType<S>>
	implements IAsyncProtocol<ResolveTcpType<S>>
{
	readonly type = "tcp";
	override readonly schema?: AsyncSchemaInput;

	/** Protocol options */
	private protocolOptions: TcpProtocolOptions<S>;

	constructor(options: TcpProtocolOptions<S> = {} as TcpProtocolOptions<S>) {
		super();
		this.protocolOptions = options;
		this.schema = options.schema as AsyncSchemaInput | undefined;
	}

	/**
	 * Get protocol options
	 */
	getOptions(): TcpProtocolOptions<S> {
		return this.protocolOptions;
	}

	/** Extract transport options (exclude schema which is only for validation) */
	private getTransportOptions(): TcpProtocolOptions {
		const { schema: _schema, ...transport } = this.protocolOptions;
		return transport;
	}

	/**
	 * Create and start TCP server adapter (v3 API)
	 * Component owns the returned adapter
	 */
	async createServer(config: ServerProtocolConfig): Promise<IAsyncServerAdapter> {
		return TcpServerAdapter.create(
			config.listenAddress.host,
			config.listenAddress.port,
			this.getTransportOptions(),
			config.tls
		);
	}

	/**
	 * Create TCP client adapter (v3 API)
	 * Component owns the returned adapter
	 */
	async createClient(config: ClientProtocolConfig): Promise<IAsyncClientAdapter> {
		// Connection timeout: config overrides protocol options
		const connectionTimeout = config.timeouts?.connectionTimeout ?? this.protocolOptions.timeout;

		return TcpClientAdapter.create(config.targetAddress.host, config.targetAddress.port, {
			...this.getTransportOptions(),
			timeout: connectionTimeout,
			tls: config.tls?.enabled ?? this.protocolOptions.tls,
		});
	}
}

/**
 * Create TCP protocol factory
 */
export function createTcpProtocol<S = never>(options?: TcpProtocolOptions<S>): TcpProtocol<S> {
	return new TcpProtocol<S>(options);
}
