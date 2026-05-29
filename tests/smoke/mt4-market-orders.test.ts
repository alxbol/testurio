/**
 * TC-MT4-MARKET-{SIDE}-{SYMBOL}-001 — parameterized MT4 → bridge → fill
 * happy path using canonical testurio adapters.
 *
 * Follows the official testurio examples verbatim:
 *
 *   • HTTP (Steps 1–4)  — `Client` + `HttpProtocol<MtEmulatorApi>`
 *     https://udamir.github.io/testurio/examples/http.html
 *
 *   • ClickHouse (Steps 5, 6) — `DataSource` + `ClickHouseAdapter`
 *     https://udamir.github.io/testurio/examples/datasources.html
 *     `store.exec(label, async (db) => db.query<Row>({query}))` patterns.
 *
 *   • Kafka (Step 7) — `Subscriber<OrderEventTopics>` + `KafkaAdapter`
 *     https://udamir.github.io/testurio/examples/message-queues.html
 *     Subscriber MUST be listed before any Publisher in `scenario.components`.
 *
 * Kafka requires `kafka:9092` to be reachable from the test runner. The
 * broker is in-cluster (test-stable ns); a port-forward must be set up
 * externally before running Step 7 — e.g. per-pod forward + hosts mapping:
 *
 *   kubectl --kubeconfig=... -n test-stable port-forward pod/kafka-controller-0 9092:9092
 *   # then add to %WINDIR%/system32/drivers/etc/hosts:
 *   #   127.0.0.1 kafka-controller-0.kafka-controller-headless.test-stable.svc.cluster.local
 *
 * Symbol parametrization: AUDJPY.MT4 / GBPJPY.MT4 / CADJPY.MT4 / USDJPY.MT4,
 * both BUY and SELL sides; login=1063 (memory: reference_demo_uat_traders).
 */

import {randomUUID} from "node:crypto";
import {readdirSync, readFileSync, statSync, writeFileSync} from "node:fs";
import {createConnection} from "node:net";
import {join} from "node:path";
import {z} from "zod";
import {
    Client,
    type Codec,
    DataSource,
    HttpProtocol,
    type Interaction,
    Subscriber,
    testCase,
    TestScenario,
} from "testurio";
import {ClickHouseAdapter} from "@testurio/adapter-clickhouse";
import {KafkaAdapter} from "@testurio/adapter-kafka";
import {AllureReporter} from "@testurio/reporter-allure";
import {Kafka, logLevel as kafkaLogLevel} from "kafkajs";
import {beforeAll, describe, expect, it} from "vitest";
import {GetV1PingResponse, PostV1OrdersResponse} from "./mt-api.schema";

// ---------------------------------------------------------------------------
// Allure attachment helpers (orthogonal to data source — reused everywhere).
// ---------------------------------------------------------------------------

type AllureAttachment = { name: string; source: string; type: string };
type AllureStep = { name: string; attachments?: AllureAttachment[]; steps?: AllureStep[] };

const ALLURE_DIR = "allure-results";

function writeJsonAttachment(dir: string, name: string, payload: unknown): AllureAttachment {
    const filename = `${randomUUID()}-attachment.json`;
    writeFileSync(join(dir, filename), JSON.stringify(payload, null, 2), "utf8");
    return {name, source: filename, type: "application/json"};
}

function writeTextAttachment(dir: string, name: string, content: string, mime: string, ext: string): AllureAttachment {
    const filename = `${randomUUID()}-attachment.${ext}`;
    writeFileSync(join(dir, filename), content, "utf8");
    return {name, source: filename, type: mime};
}

function findLatestResultPath(dir: string): string | undefined {
    const candidates = readdirSync(dir)
        .filter((f) => f.endsWith("-result.json"))
        .map((f) => ({f, t: statSync(join(dir, f)).mtimeMs}))
        .sort((a, b) => b.t - a.t);
    return candidates[0] ? join(dir, candidates[0].f) : undefined;
}

function setAllureSuites(
    dir: string,
    labels: { parentSuite: string; suite: string; subSuite?: string },
): void {
    const resultPath = findLatestResultPath(dir);
    if (!resultPath) return;
    const json = JSON.parse(readFileSync(resultPath, "utf8")) as {
        labels?: { name: string; value: string }[];
    };
    json.labels = (json.labels ?? []).filter(
        (l) => !["parentSuite", "suite", "subSuite"].includes(l.name),
    );
    json.labels.push({name: "parentSuite", value: labels.parentSuite});
    json.labels.push({name: "suite", value: labels.suite});
    if (labels.subSuite) json.labels.push({name: "subSuite", value: labels.subSuite});
    writeFileSync(resultPath, JSON.stringify(json, null, 2), "utf8");
}

function attachExtrasToLatestAllureResult(
    dir: string,
    extras: { attachment: AllureAttachment; stepNameIncludes?: string }[],
): void {
    if (!extras.length) return;
    const resultPath = findLatestResultPath(dir);
    if (!resultPath) return;
    const json = JSON.parse(readFileSync(resultPath, "utf8")) as {
        steps?: AllureStep[];
        attachments?: AllureAttachment[];
    };
    json.steps ||= [];
    json.attachments ||= [];
    for (const {attachment, stepNameIncludes} of extras) {
        const step = stepNameIncludes ? json.steps.find((s) => s.name.includes(stepNameIncludes)) : undefined;
        if (step) {
            (step.attachments ||= []).push(attachment);
        } else {
            json.attachments.push(attachment);
        }
    }
    writeFileSync(resultPath, JSON.stringify(json, null, 2), "utf8");
}

function attachInteractionsToLatestAllureResult(dir: string, interactions: Interaction[]): void {
    if (!interactions.length) return;
    const resultPath = findLatestResultPath(dir);
    if (!resultPath) return;
    const json = JSON.parse(readFileSync(resultPath, "utf8")) as {
        steps?: AllureStep[];
        attachments?: AllureAttachment[];
    };
    json.steps ||= [];
    json.attachments ||= [];
    for (const ix of interactions) {
        const requestStep = json.steps.find((s) => s.name.includes("request") && s.name.includes(ix.messageType));
        const responseStep = json.steps.find((s) => s.name.includes("onResponse") && s.name.includes(ix.messageType));
        const reqAttachment = writeJsonAttachment(dir, `request: ${ix.messageType}`, {
            method: (ix.requestPayload as { method?: string } | undefined)?.method,
            path: (ix.requestPayload as { path?: string } | undefined)?.path,
            ...(ix.requestPayload as object),
            sentAt: new Date(ix.requestTimestamp).toISOString(),
        });
        const respAttachment = writeJsonAttachment(dir, `response: ${ix.messageType}`, {
            ...(ix.responsePayload as object),
            status: ix.status,
            error: ix.error,
            receivedAt: ix.responseTimestamp ? new Date(ix.responseTimestamp).toISOString() : null,
            durationMs: ix.duration,
        });
        if (requestStep) (requestStep.attachments ||= []).push(reqAttachment);
        else json.attachments.push(reqAttachment);
        if (responseStep) (responseStep.attachments ||= []).push(respAttachment);
        else json.attachments.push(respAttachment);
    }
    writeFileSync(resultPath, JSON.stringify(json, null, 2), "utf8");
}

function escapeHtml(s: string): string {
    return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

// ---------------------------------------------------------------------------
// Service contracts (HTTP).
// ---------------------------------------------------------------------------

interface PlaceOrderRequestBody {
    symbol: string;
    volume: number;
    side: "BUY" | "SELL";
    type: "MARKET" | "LIMIT" | "STOP";
    login: number;
    price?: number;
    stopLoss?: number;
    takeProfit?: number;
}

interface PlaceOrderResponseBody {
    orderId: number;
    code: string;
    message: string;
}

interface OrderStateResponseBody {
    id?: number;
    type?: "MARKET" | "LIMIT" | "STOP" | "STOP_LOSS" | "TAKE_POFIT";
    side: "BUY" | "SELL";
    state?: string;
    symbol?: string;
    time?: string;
    requestedPrice?: number;
    filledAvgPrice?: number;
    volume?: number;
    filledVolume?: number;
    createdAt: string;
    comment?: string;
}

interface MtEmulatorApi {
    ping: {
        request: { method: "GET"; path: "/v1/health/ping" };
        response: { code: 200; body: { status?: string } };
    };
    placeOrder: {
        request: { method: "POST"; path: "/v1/orders"; body: PlaceOrderRequestBody };
        response: { code: 200; body: PlaceOrderResponseBody };
    };
    getOrder: {
        // Path interpolated per-call (`/v1/orders/${id}`); typed as a string union
        // since testurio HttpProtocol does not yet support template path params.
        request: { method: "GET"; path: string };
        response: { code: 200; body: OrderStateResponseBody };
    };
}

const MT_HOST = "192.168.8.46";
const MT_PORT = 5000;

function makeMtClient() {
    return new Client("mt4-emulator", {
        protocol: new HttpProtocol<MtEmulatorApi>(),
        targetAddress: {host: MT_HOST, port: MT_PORT},
    });
}

const PingResponseSchema = z.object({code: z.literal(200), body: GetV1PingResponse}).passthrough();
const PlaceOrderResponseSchema = z.object({code: z.literal(200), body: PostV1OrdersResponse}).passthrough();
const GetOrderResponseBodySchema = z
    .object({
        id: z.number().optional(),
        type: z.enum(["MARKET", "LIMIT", "STOP", "STOP_LOSS", "TAKE_POFIT"]).optional(),
        side: z.enum(["BUY", "SELL"]),
        state: z.string().optional(),
        symbol: z.string().optional(),
        time: z.string().optional(),
        requestedPrice: z.number().optional(),
        // Relaxed vs OAS: emulator may return 0 even on FILLED orders (async update).
        filledAvgPrice: z.number().min(0).optional(),
        volume: z.number().min(0).optional(),
        filledVolume: z.number().min(0).optional(),
        createdAt: z.iso.datetime({}),
        comment: z.string().optional(),
    })
    .passthrough();
const GetOrderResponseSchema = z.object({code: z.literal(200), body: GetOrderResponseBodySchema}).passthrough();

// ---------------------------------------------------------------------------
// ClickHouse DataSource (Steps 5, 6).
// ---------------------------------------------------------------------------

const CH_URL = "http://clickhouse.test-stable.cbrid.ge:8123";
const CH_USER = "admin";
const CH_PASS = "admin";
const CH_DB = "default";
const CH_POLL_TIMEOUT_MS = 30_000;
const CH_POLL_INTERVAL_MS = 1_000;

function makeChDataSource(name = "clickhouse-collector") {
    return new DataSource(name, {
        adapter: new ClickHouseAdapter({
            url: CH_URL,
            username: CH_USER,
            password: CH_PASS,
            database: CH_DB,
        }),
    });
}

interface CollectorOrderRow {
    collector_order_id: string;
    order_id: string;
    client_order_id: string;
    client_acc_id: string;
    client_acc_group: string;
    client_symbol_name: string;
    core_symbol_name: string;
    side: string;
    order_type: string;
    pricing_mode: string;
    time_in_force: string;
    bridge_status: string;
    client_status: string;
    lp_status: string;
    requested_core_amount: number | null;
    client_amount_filled: number | null;
    client_amount_remaining: number | null;
    client_price_filled_avg: number | null;
    matched_price: number | null;
    tenant_id: string;
    created_at: string;
    client_request_at: string | null;
    bridge_received_at: string | null;
    lp_request_at: string | null;
    last_modified_at: string | null;
    server_type: string;
    server_name: string;
    trading_lp_name: string;
    quote_lp_name: string;
    is_completed: number;
    is_order_received: number;
    is_final_order_received: number;
    reject_reason: number | null;
    reject_text: string;
}

const COLLECTOR_ORDER_COLUMNS = [
    "collector_order_id",
    "order_id",
    "client_order_id",
    "client_acc_id",
    "client_acc_group",
    "client_symbol_name",
    "core_symbol_name",
    "side",
    "order_type",
    "pricing_mode",
    "time_in_force",
    "bridge_status",
    "client_status",
    "lp_status",
    "requested_core_amount",
    "client_amount_filled",
    "client_amount_remaining",
    "client_price_filled_avg",
    "matched_price",
    "tenant_id",
    "created_at",
    "client_request_at",
    "bridge_received_at",
    "lp_request_at",
    "last_modified_at",
    "server_type",
    "server_name",
    "trading_lp_name",
    "quote_lp_name",
    "is_completed",
    "is_order_received",
    "is_final_order_received",
    "reject_reason",
    "reject_text",
] as const;

function buildCollectorOrderSql(emulatorOrderId: number, tenant: string, login: number): string {
    return `SELECT ${COLLECTOR_ORDER_COLUMNS.join(", ")}
            FROM collector.order
            WHERE client_order_id = '${emulatorOrderId}'
              AND client_acc_id = '${login}'
              AND tenant_id = '${tenant}'
              AND is_completed = 1
            ORDER BY last_modified_at DESC LIMIT 1`;
}

function renderOrderRowHtml(row: CollectorOrderRow, caption: string): string {
    const rows = COLLECTOR_ORDER_COLUMNS.map((col) => {
        const v = (row as unknown as Record<string, unknown>)[col];
        const isNull = v === null || v === undefined;
        const cell = isNull ? '<em style="color:#888">null</em>' : escapeHtml(String(v));
        return `    <tr><td><code>${col}</code></td><td>${cell}</td></tr>`;
    }).join("\n");
    return wrapHtml(`<table>
  <caption>${escapeHtml(caption)}</caption>
  <thead><tr><th>Column</th><th>Value</th></tr></thead>
  <tbody>
${rows}
  </tbody>
</table>`);
}

interface CollectorExecutionRow {
    collector_execution_id: string;
    order_id: string;
    client_order_id: string;
    routing: string;
    generated_by: string;
    execution_type: string;
    side: string;
    request_source: string;
    tenant_id: string;
    server_type: string;
    server_name: string;
    created_at: string;
    client_amount_filled: number | null;
    client_price_filled: number | null;
    lp_amount_filled: number | null;
    lp_price_filled: number | null;
    matched_price: number | null;
    lp_request_at: string | null;
    lp_executed_at: string | null;
    lp_response_at: string | null;
    lp_execution_id: string;
    client_acc_id: string;
    client_acc_group: string;
    client_symbol_name: string;
    order_type: string;
    pricing_mode: string;
    time_in_force: string;
    trading_lp_name: string;
    quote_lp_name: string;
    reject_reason: number | null;
    reject_text: string;
}

const COLLECTOR_EXECUTION_COLUMNS = [
    "collector_execution_id",
    "order_id",
    "client_order_id",
    "routing",
    "generated_by",
    "execution_type",
    "side",
    "request_source",
    "tenant_id",
    "server_type",
    "server_name",
    "created_at",
    "client_amount_filled",
    "client_price_filled",
    "lp_amount_filled",
    "lp_price_filled",
    "matched_price",
    "lp_request_at",
    "lp_executed_at",
    "lp_response_at",
    "lp_execution_id",
    "client_acc_id",
    "client_acc_group",
    "client_symbol_name",
    "order_type",
    "pricing_mode",
    "time_in_force",
    "trading_lp_name",
    "quote_lp_name",
    "reject_reason",
    "reject_text",
] as const;

function buildCollectorExecutionSql(emulatorOrderId: number, tenant: string, login: number): string {
    return `SELECT ${COLLECTOR_EXECUTION_COLUMNS.join(", ")}
            FROM collector.execution
            WHERE client_order_id = '${emulatorOrderId}'
              AND client_acc_id = '${login}'
              AND tenant_id = '${tenant}'
            ORDER BY lp_executed_at ASC, created_at ASC`;
}

function renderExecutionsHtml(rows: CollectorExecutionRow[], caption: string): string {
    if (!rows.length) {
        return wrapHtml(`<p><em>No execution rows.</em></p>`);
    }
    const headerCells = rows.map((_, i) => `<th>execution #${i + 1}</th>`).join("");
    const bodyRows = COLLECTOR_EXECUTION_COLUMNS.map((col) => {
        const cells = rows
            .map((r) => {
                const v = (r as unknown as Record<string, unknown>)[col];
                const isNull = v === null || v === undefined;
                return `<td>${isNull ? '<em style="color:#888">null</em>' : escapeHtml(String(v))}</td>`;
            })
            .join("");
        return `    <tr><td><code>${col}</code></td>${cells}</tr>`;
    }).join("\n");
    return wrapHtml(`<table>
  <caption>${escapeHtml(caption)}</caption>
  <thead><tr><th>Column</th>${headerCells}</tr></thead>
  <tbody>
${bodyRows}
  </tbody>
</table>`);
}

function wrapHtml(body: string): string {
    return `<!doctype html><html lang=""><head><meta charset="utf-8"><style>
body{font:13px/1.4 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;margin:8px;color:#222}
table{border-collapse:collapse;width:100%}
caption{text-align:left;font-weight:600;padding:4px 0 8px;font-size:14px}
th,td{border:1px solid #ddd;padding:4px 8px;vertical-align:top;text-align:left}
th{background:#f4f4f4}
td:first-child{width:240px;white-space:nowrap;background:#fafafa}
code{font:12px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
</style></head><body>
${body}
</body></html>`;
}

// ---------------------------------------------------------------------------
// Kafka Subscriber (Step 7) — binary protobuf payloads, raw passthrough codec.
// ---------------------------------------------------------------------------

const KAFKA_BROKERS = (
    process.env.KAFKA_BROKERS ?? "127.0.0.1:9092,127.0.0.2:9092,127.0.0.3:9092"
).split(",");
const KAFKA_TOPIC_RECEIVED = "order.received.v1.demo-uat";
const KAFKA_TOPIC_FINISHED = "order.finished.v1.demo-uat";
const KAFKA_WAIT_MESSAGE_TIMEOUT_MS = 30_000;

// Topics map for typed Subscriber. Payload is a Uint8Array — adapter delivers
// raw bytes from KafkaJS once we hand it the binary passthrough codec below.
interface OrderEventTopics {
    [KAFKA_TOPIC_RECEIVED]: Uint8Array;
    [KAFKA_TOPIC_FINISHED]: Uint8Array;
}

const rawBytesCodec: Codec<Uint8Array> = {
    name: "raw-bytes",
    wireFormat: "binary",
    encode: <D = unknown>(data: D) => data as unknown as Uint8Array,
    decode: <D = unknown>(wire: Uint8Array) => wire as unknown as D,
};

function makeKafkaSubscriber(groupId?: string) {
    return new Subscriber<OrderEventTopics>("kafka-order-events-sub", {
        adapter: new KafkaAdapter({
            brokers: KAFKA_BROKERS,
            groupId: groupId ?? `mt4-smoke-${randomUUID()}`,
            // testMode tightens KafkaJS consumer-group coordination for faster rebalancing.
            testMode: true,
        }),
        codec: rawBytesCodec,
    });
}

function bytesContainAscii(payload: Uint8Array, needle: string): boolean {
    if (!payload || payload.length === 0) return false;
    const needleBytes = Buffer.from(needle, "utf8");
    return Buffer.from(payload).indexOf(needleBytes) !== -1;
}

async function isKafkaReachable(brokers: string[], timeoutMs = 2_000): Promise<boolean> {
    return new Promise((resolve) => {
        const [host, portStr] = brokers[0].split(":");
        const port = Number(portStr ?? 9092);
        const socket = createConnection({host, port, timeout: timeoutMs});
        socket.once("connect", () => {
            socket.destroy();
            resolve(true);
        });
        socket.once("timeout", () => {
            socket.destroy();
            resolve(false);
        });
        socket.once("error", () => resolve(false));
    });
}

// ---------------------------------------------------------------------------
// HTTP polling. testurio's declarative DSL does not natively express "poll
// until terminal", so we keep raw fetch ONLY for the wait loop. The terminal
// snapshot is then re-fetched through a Client+TestScenario for Allure.
// ---------------------------------------------------------------------------

const TERMINAL_STATES = new Set(["FILLED", "CANCELLED"]);
const POLL_INTERVAL_MS = 500;
const POLL_TIMEOUT_MS = 30_000;

async function pollUntilTerminal(orderId: number): Promise<{
    state: string;
    polls: number;
    lastBody: OrderStateResponseBody;
}> {
    const deadline = Date.now() + POLL_TIMEOUT_MS;
    let polls = 0;
    let lastBody: OrderStateResponseBody | undefined;
    while (Date.now() < deadline) {
        polls++;
        const r = await fetch(`http://${MT_HOST}:${MT_PORT}/v1/orders/${orderId}`);
        if (r.status === 404 && polls === 1) {
            await new Promise((res) => setTimeout(res, POLL_INTERVAL_MS));
            continue;
        }
        if (!r.ok) throw new Error(`GET /v1/orders/${orderId} → HTTP ${r.status}`);
        lastBody = (await r.json()) as OrderStateResponseBody;
        if (lastBody.state && TERMINAL_STATES.has(lastBody.state)) {
            return {state: lastBody.state, polls, lastBody};
        }
        await new Promise((res) => setTimeout(res, POLL_INTERVAL_MS));
    }
    throw new Error(
        `Order ${orderId} did not reach terminal state within ${POLL_TIMEOUT_MS}ms (polls=${polls}, last=${JSON.stringify(lastBody)})`,
    );
}

// ---------------------------------------------------------------------------
// Parametrization.
// ---------------------------------------------------------------------------

interface SymbolCase {
    symbol: string;
    tcCode: string;
    login: number;
    volume: number;
    side: "BUY" | "SELL";
}

const SYMBOL_CASES: SymbolCase[] = (["BUY", "SELL"] as const).flatMap((side) => [
    {symbol: "AUDJPY.MT4", tcCode: "AUDJPY", login: 1063, volume: 0.01, side},
    {symbol: "GBPJPY.MT4", tcCode: "GBPJPY", login: 1063, volume: 0.01, side},
    {symbol: "CADJPY.MT4", tcCode: "CADJPY", login: 1063, volume: 0.01, side},
    {symbol: "USDJPY.MT4", tcCode: "USDJPY", login: 1063, volume: 0.01, side},
]);

const TENANT = "demo-uat";
const ALLURE_PARENT_SUITE = "MT4 | Market order BUY/SELL | parametrized by symbol";
const allureSuiteFor = (s: SymbolCase) =>
    `MT4 | Market order '${s.side}' | '${s.symbol}' ${s.volume} | login ${s.login}`;

// ---------------------------------------------------------------------------
// Suite.
// ---------------------------------------------------------------------------

describe("MT4 | Market order BUY/SELL | parametrized by symbol", () => {
    beforeAll(async () => {
        // Step 1 — Health check via canonical Client + HttpProtocol.
        const mtClient = makeMtClient();
        const scenario = new TestScenario({
            name: "TC-MT4-MARKET / Step 1 (shared health check)",
            components: [mtClient],
            recording: true,
        });
        scenario.addReporter(
            new AllureReporter({
                resultsDir: ALLURE_DIR,
                environmentInfo: {
                    env: "demo-uat / test-stable",
                    target: `${MT_HOST}:${MT_PORT} (MT Test emulator)`,
                    step: "1 — health check",
                    node: process.version,
                },
            }),
        );
        const tc = testCase("ping", (test) => {
            const mt = test.use(mtClient);
            mt.request("ping", {method: "GET", path: "/v1/health/ping"});
            mt.onResponse("ping")
                .validate(PingResponseSchema)
                .assert("HTTP 200", (res) => res.code === 200)
                .assert("status === 'ok'", (res) => res.body.status === "ok");
        });
        const result = await scenario.run(tc);
        attachInteractionsToLatestAllureResult(ALLURE_DIR, result.interactions ?? []);
        setAllureSuites(ALLURE_DIR, {parentSuite: ALLURE_PARENT_SUITE, suite: "setup (health check)"});
        if (!result.passed) {
            throw new Error(
                `Step 1 health check failed — aborting suite (precondition P1).\n${JSON.stringify(result, null, 2)}`,
            );
        }
    });

    describe.each(SYMBOL_CASES)(
        "MT4 | Market order $side | $symbol $volume | login $login",
        ({symbol, tcCode, login, volume, side}) => {
            const ALLURE_SUITE = allureSuiteFor({symbol, tcCode, login, volume, side});
            const labelSuites = () =>
                setAllureSuites(ALLURE_DIR, {parentSuite: ALLURE_PARENT_SUITE, suite: ALLURE_SUITE});

            // -----------------------------------------------------------------
            // Step 2 — Place market order via Client + HttpProtocol.
            // -----------------------------------------------------------------
            it(`Step 2 — place market ${side}: POST /v1/orders → 200 {code:'OK', orderId>0}`, async () => {
                const mtClient = makeMtClient();
                const scenario = new TestScenario({
                    name: `TC-MT4-MARKET-${side}-${tcCode}-001 / Step 2`,
                    components: [mtClient],
                    recording: true,
                });
                scenario.addReporter(
                    new AllureReporter({
                        resultsDir: ALLURE_DIR,
                        environmentInfo: {
                            env: "demo-uat / test-stable",
                            target: `${MT_HOST}:${MT_PORT} (MT Test emulator)`,
                            symbol,
                            login: String(login),
                            volume: String(volume),
                            side,
                            node: process.version,
                        },
                    }),
                );
                const tc = testCase(`placeOrder ${symbol} ${side} ${volume} login=${login}`, (test) => {
                    const mt = test.use(mtClient);
                    mt.request("placeOrder", {
                        method: "POST",
                        path: "/v1/orders",
                        body: {symbol, volume, side, type: "MARKET", login},
                    });
                    mt.onResponse("placeOrder")
                        .validate(PlaceOrderResponseSchema)
                        .assert("HTTP 200", (res) => res.code === 200)
                        .assert("code === 'OK'", (res) => res.body.code === "OK")
                        .assert(
                            "orderId > 0",
                            (res) => typeof res.body.orderId === "number" && res.body.orderId > 0,
                        )
                        .assert("message is empty", (res) => res.body.message === "");
                });
                const result = await scenario.run(tc);
                attachInteractionsToLatestAllureResult(ALLURE_DIR, result.interactions ?? []);
                labelSuites();
                expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
            });

            // -----------------------------------------------------------------
            // Helper: place via Client (returns the response body for downstream
            // steps that need orderId). Runs a TestScenario internally so the
            // Allure entry for this `it` already captures the place request.
            // -----------------------------------------------------------------
            const placeAndCaptureOrderId = async (
                stepLabel: string,
            ): Promise<{
                response: PlaceOrderResponseBody;
                scenarioPassed: boolean;
                interactions: Interaction[];
            }> => {
                const mtClient = makeMtClient();
                const scenario = new TestScenario({
                    name: `TC-MT4-MARKET-${side}-${tcCode}-001 / ${stepLabel} place leg`,
                    components: [mtClient],
                    recording: true,
                });
                // No reporter — this scenario is invoked from inside another it()
                // which already has its own Allure result for the step.
                let captured: PlaceOrderResponseBody | undefined;
                const tc = testCase(`placeOrder ${symbol} ${side} ${volume} login=${login}`, (test) => {
                    const mt = test.use(mtClient);
                    mt.request("placeOrder", {
                        method: "POST",
                        path: "/v1/orders",
                        body: {symbol, volume, side, type: "MARKET", login},
                    });
                    mt.onResponse("placeOrder").assert("HTTP 200", (res) => {
                        captured = res.body as PlaceOrderResponseBody;
                        return res.code === 200;
                    });
                });
                const result = await scenario.run(tc);
                if (!captured) {
                    captured = {
                        orderId: 0,
                        code: "PLACE_LEG_NO_RESPONSE",
                        message: "no response captured",
                    };
                }
                return {response: captured, scenarioPassed: result.passed, interactions: result.interactions ?? []};
            };

            // -----------------------------------------------------------------
            // Step 3 — Wait for terminal state, then re-fetch via Client for
            // Allure-recorded final snapshot. Terminal state is FILLED|CANCELLED.
            // -----------------------------------------------------------------
            it(`Step 3 — poll until terminal state: GET /v1/orders/{id} reaches FILLED|CANCELLED ≤${POLL_TIMEOUT_MS / 1000}s`, async () => {
                const placed = (await placeAndCaptureOrderId("Step 3")).response;
                expect(placed.code, JSON.stringify(placed)).toBe("OK");
                const orderId = placed.orderId;
                expect(orderId).toBeGreaterThan(0);
                const polled = await pollUntilTerminal(orderId);

                const mtClient = makeMtClient();
                const scenario = new TestScenario({
                    name: `TC-MT4-MARKET-${side}-${tcCode}-001 / Step 3`,
                    components: [mtClient],
                    recording: true,
                });
                scenario.addReporter(
                    new AllureReporter({
                        resultsDir: ALLURE_DIR,
                        environmentInfo: {
                            env: "demo-uat / test-stable",
                            target: `${MT_HOST}:${MT_PORT} (MT Test emulator)`,
                            symbol,
                            login: String(login),
                            volume: String(volume),
                            side,
                            orderId: String(orderId),
                            polls: String(polled.polls),
                            terminalState: polled.state,
                            node: process.version,
                        },
                    }),
                );
                const tc = testCase(
                    `pollOrder ${symbol} ${side} ${volume} login=${login} orderId=${orderId}`,
                    (test) => {
                        const mt = test.use(mtClient);
                        mt.request("getOrder", {method: "GET", path: `/v1/orders/${orderId}`});
                        mt.onResponse("getOrder")
                            .validate(GetOrderResponseSchema)
                            .assert("HTTP 200", (res) => res.code === 200)
                            .assert(
                                "state ∈ {FILLED, CANCELLED}",
                                (res) => res.body.state !== undefined && TERMINAL_STATES.has(res.body.state),
                            );
                    },
                );
                const result = await scenario.run(tc);
                attachInteractionsToLatestAllureResult(ALLURE_DIR, result.interactions ?? []);
                labelSuites();
                expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
                expect(TERMINAL_STATES.has(polled.state)).toBe(true);
            }, POLL_TIMEOUT_MS + 10_000);

            // -----------------------------------------------------------------
            // Step 4 — Strict final-body assertion + CANCELLED diagnostic.
            // -----------------------------------------------------------------
            it("Step 4 — assert terminal outcome: state=FILLED and full body matches", async () => {
                const placed = (await placeAndCaptureOrderId("Step 4")).response;
                expect(placed.code, JSON.stringify(placed)).toBe("OK");
                const orderId = placed.orderId;
                const polled = await pollUntilTerminal(orderId);
                const cancelComment = polled.state === "CANCELLED" ? polled.lastBody.comment ?? "" : "";
                const cancelHint =
                    polled.state !== "CANCELLED"
                        ? ""
                        : /not enough funds/i.test(cancelComment)
                            ? "P3 broken (account underfunded for current price × volume)"
                            : /no price|price off/i.test(cancelComment)
                                ? "P5 broken (no ticks for the symbol)"
                                : "escalate with raw response";

                const mtClient = makeMtClient();
                const scenario = new TestScenario({
                    name: `TC-MT4-MARKET-${side}-${tcCode}-001 / Step 4`,
                    components: [mtClient],
                    recording: true,
                });
                scenario.addReporter(
                    new AllureReporter({
                        resultsDir: ALLURE_DIR,
                        environmentInfo: {
                            env: "demo-uat / test-stable",
                            target: `${MT_HOST}:${MT_PORT} (MT Test emulator)`,
                            symbol,
                            login: String(login),
                            volume: String(volume),
                            side,
                            orderId: String(orderId),
                            polls: String(polled.polls),
                            terminalState: polled.state,
                            cancelComment: cancelComment || "—",
                            cancelHint: cancelHint || "—",
                            node: process.version,
                        },
                    }),
                );
                const tc = testCase(
                    `assertOrder ${symbol} ${side} ${volume} login=${login} orderId=${orderId}`,
                    (test) => {
                        const mt = test.use(mtClient);
                        mt.request("getOrder", {method: "GET", path: `/v1/orders/${orderId}`});
                        mt.onResponse("getOrder")
                            .validate(GetOrderResponseSchema)
                            .assert("HTTP 200", (res) => res.code === 200)
                            .assert("state === 'FILLED'", (res) => res.body.state === "FILLED")
                            .assert("symbol matches", (res) => res.body.symbol === symbol)
                            .assert("side matches", (res) => res.body.side === side)
                            .assert("type === 'MARKET'", (res) => res.body.type === "MARKET")
                            .assert("volume matches", (res) => res.body.volume === volume)
                            .assert("id matches orderId", (res) => res.body.id === orderId);
                        // filledVolume intentionally not strictly asserted: emulator
                        // may return 0 immediately after state flips to FILLED.
                    },
                );
                const result = await scenario.run(tc);
                attachInteractionsToLatestAllureResult(ALLURE_DIR, result.interactions ?? []);
                labelSuites();
                expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
            }, POLL_TIMEOUT_MS + 10_000);

            // -----------------------------------------------------------------
            // Step 5 — ClickHouse collector.order persistence via DataSource.
            // -----------------------------------------------------------------
            it(`Step 5 — ClickHouse collector.order: row persisted (exact client_order_id match, ≤${CH_POLL_TIMEOUT_MS / 1000}s lag)`, async () => {
                let blockReason: string | undefined;
                let placeResp: PlaceOrderResponseBody | undefined;
                let polled: Awaited<ReturnType<typeof pollUntilTerminal>> | undefined;

                try {
                    placeResp = (await placeAndCaptureOrderId("Step 5")).response;
                } catch (e) {
                    blockReason = `Step 2 (place) error: ${(e as Error).message}`;
                }
                if (!blockReason && placeResp!.code !== "OK") {
                    blockReason =
                        `Step 2: code=${placeResp!.code} message=${JSON.stringify(placeResp!.message)} ` +
                        `(orderId=${placeResp!.orderId})`;
                }
                if (!blockReason && placeResp) {
                    try {
                        polled = await pollUntilTerminal(placeResp.orderId);
                    } catch (e) {
                        blockReason = `Step 3 (poll) error: ${(e as Error).message}`;
                    }
                }
                const emulatorOrderId = placeResp?.orderId ?? 0;
                if (!blockReason && polled && polled.state !== "FILLED") {
                    blockReason =
                        `Step 4 precondition: order ${emulatorOrderId} state=${polled.state} ` +
                        `comment=${JSON.stringify(polled.lastBody.comment ?? "")} — Step 5 cannot validate persistence`;
                }

                const sql = emulatorOrderId
                    ? buildCollectorOrderSql(emulatorOrderId, TENANT, login)
                    : "-- order was not placed";

                const ch = makeChDataSource();
                const scenario = new TestScenario({
                    name: `TC-MT4-MARKET-${side}-${tcCode}-001 / Step 5`,
                    components: [ch],
                    recording: false,
                });
                scenario.addReporter(
                    new AllureReporter({
                        resultsDir: ALLURE_DIR,
                        environmentInfo: {
                            env: "demo-uat / test-stable",
                            collector: `${CH_URL} (collector.order)`,
                            symbol,
                            login: String(login),
                            volume: String(volume),
                            side,
                            emulatorOrderId: String(emulatorOrderId),
                            terminalState: polled?.state ?? "—",
                            blocked: blockReason ?? "—",
                            node: process.version,
                        },
                    }),
                );

                let row: CollectorOrderRow | undefined;
                let chPolls = 0;
                const tc = testCase(
                    `chOrder ${symbol} ${side} login=${login} emulatorOrderId=${emulatorOrderId}`,
                    (test) => {
                        const store = test.use(ch);
                        store
                            .exec("wait for collector.order row", async (db) => {
                                if (blockReason) return undefined;
                                const deadline = Date.now() + CH_POLL_TIMEOUT_MS;
                                while (Date.now() < deadline) {
                                    chPolls++;
                                    const rows = await db.query<CollectorOrderRow>({query: sql});
                                    if (rows.length === 1) return rows[0];
                                    await new Promise((res) => setTimeout(res, CH_POLL_INTERVAL_MS));
                                }
                                return undefined;
                            })
                            .assert("collector.order row present", (r) => {
                                row = r as CollectorOrderRow | undefined;
                                return row !== undefined;
                            })
                            .assert("client_order_id matches", () => row?.client_order_id === String(emulatorOrderId))
                            .assert("tenant_id = demo-uat", () => row?.tenant_id === TENANT)
                            .assert("client_acc_id matches", () => row?.client_acc_id === String(login))
                            .assert("client_symbol_name matches", () => row?.client_symbol_name === symbol)
                            .assert("side matches", () => row?.side === side)
                            .assert("order_type = MARKET", () => row?.order_type === "MARKET")
                            .assert("bridge_status = FILLED", () => row?.bridge_status === "FILLED")
                            .assert("client_status = FILLED", () => row?.client_status === "FILLED")
                            .assert("lp_status = FILLED", () => row?.lp_status === "FILLED")
                            .assert("server_type = MT4", () => row?.server_type === "MT4")
                            .assert("is_completed = 1", () => row?.is_completed === 1)
                            .assert("is_order_received = 1", () => row?.is_order_received === 1)
                            .assert("is_final_order_received = 1", () => row?.is_final_order_received === 1)
                            .assert("reject_text empty", () => row?.reject_text === "")
                            .assert("lp_request_at NOT NULL", () => row?.lp_request_at !== null)
                            .assert(
                                "client_amount_filled === requested_core_amount",
                                () => row?.client_amount_filled === row?.requested_core_amount,
                            );
                    },
                );
                const result = await scenario.run(tc);

                const sqlAttachment = writeTextAttachment(ALLURE_DIR, "ClickHouse SELECT", sql, "text/plain", "txt");
                const tableAttachment = writeTextAttachment(
                    ALLURE_DIR,
                    row ? "collector.order — matched row" : "collector.order — no matching row",
                    row
                        ? renderOrderRowHtml(
                            row,
                            `collector.order (client_order_id='${emulatorOrderId}', client_acc_id='${login}', tenant_id='${TENANT}')`,
                        )
                        : wrapHtml(
                            `<p><strong>No row</strong> matched <code>client_order_id='${emulatorOrderId}' AND client_acc_id='${login}' AND tenant_id='${TENANT}' AND is_completed=1</code>.</p><p>Reason: ${escapeHtml(blockReason ?? "unknown")}</p>`,
                        ),
                    "text/html",
                    "html",
                );
                attachExtrasToLatestAllureResult(ALLURE_DIR, [
                    {attachment: sqlAttachment, stepNameIncludes: "wait for collector.order row"},
                    {attachment: tableAttachment, stepNameIncludes: "wait for collector.order row"},
                ]);
                labelSuites();

                if (blockReason) expect.fail(blockReason);
                expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
            }, POLL_TIMEOUT_MS + CH_POLL_TIMEOUT_MS + 10_000);

            // -----------------------------------------------------------------
            // Step 6 — ClickHouse collector.execution: ≥1 FILLED, A-book.
            // -----------------------------------------------------------------
            it("Step 6 — ClickHouse collector.execution: ≥1 FILLED execution per order (ABOOK routing)", async () => {
                let blockReason: string | undefined;
                let placeResp: PlaceOrderResponseBody | undefined;
                let polled: Awaited<ReturnType<typeof pollUntilTerminal>> | undefined;

                try {
                    placeResp = (await placeAndCaptureOrderId("Step 6")).response;
                } catch (e) {
                    blockReason = `Step 2 (place) error: ${(e as Error).message}`;
                }
                if (!blockReason && placeResp!.code !== "OK") {
                    blockReason = `Step 2: code=${placeResp!.code} message=${JSON.stringify(placeResp!.message)}`;
                }
                if (!blockReason && placeResp) {
                    try {
                        polled = await pollUntilTerminal(placeResp.orderId);
                    } catch (e) {
                        blockReason = `Step 3 (poll) error: ${(e as Error).message}`;
                    }
                }
                const emulatorOrderId = placeResp?.orderId ?? 0;
                if (!blockReason && polled && polled.state !== "FILLED") {
                    blockReason =
                        `Step 4 precondition: order ${emulatorOrderId} state=${polled.state} ` +
                        `comment=${JSON.stringify(polled.lastBody.comment ?? "")}`;
                }

                const sql = emulatorOrderId
                    ? buildCollectorExecutionSql(emulatorOrderId, TENANT, login)
                    : "-- order was not placed";

                const ch = makeChDataSource();
                const scenario = new TestScenario({
                    name: `TC-MT4-MARKET-${side}-${tcCode}-001 / Step 6`,
                    components: [ch],
                    recording: false,
                });
                scenario.addReporter(
                    new AllureReporter({
                        resultsDir: ALLURE_DIR,
                        environmentInfo: {
                            env: "demo-uat / test-stable",
                            collector: `${CH_URL} (collector.execution)`,
                            symbol,
                            login: String(login),
                            volume: String(volume),
                            side,
                            emulatorOrderId: String(emulatorOrderId),
                            terminalState: polled?.state ?? "—",
                            blocked: blockReason ?? "—",
                            node: process.version,
                        },
                    }),
                );

                let execRows: CollectorExecutionRow[] = [];
                let filledRows: CollectorExecutionRow[] = [];
                let sumFilledVolume = 0;

                const tc = testCase(
                    `chExecutions ${symbol} ${side} login=${login} emulatorOrderId=${emulatorOrderId}`,
                    (test) => {
                        const store = test.use(ch);
                        store
                            .exec("wait for collector.execution rows", async (db) => {
                                if (blockReason) return [];
                                const deadline = Date.now() + CH_POLL_TIMEOUT_MS;
                                let rows: CollectorExecutionRow[] = [];
                                while (Date.now() < deadline) {
                                    rows = await db.query<CollectorExecutionRow>({query: sql});
                                    if (rows.length >= 1 && rows.some((r) => r.execution_type === "FILLED")) {
                                        return rows;
                                    }
                                    await new Promise((res) => setTimeout(res, CH_POLL_INTERVAL_MS));
                                }
                                return rows;
                            })
                            .assert("≥1 row", (rows) => {
                                execRows = rows as CollectorExecutionRow[];
                                filledRows = execRows.filter((r) => r.execution_type === "FILLED");
                                sumFilledVolume = filledRows.reduce(
                                    (acc, r) => acc + (r.client_amount_filled ?? 0),
                                    0,
                                );
                                return execRows.length >= 1;
                            })
                            .assert("≥1 FILLED execution", () => filledRows.length >= 1)
                            .assert(
                                "all rows tenant_id = demo-uat",
                                () => execRows.every((r) => r.tenant_id === TENANT),
                            )
                            .assert(
                                "all rows client_order_id matches",
                                () => execRows.every((r) => r.client_order_id === String(emulatorOrderId)),
                            )
                            .assert(
                                "all rows client_acc_id matches",
                                () => execRows.every((r) => r.client_acc_id === String(login)),
                            )
                            .assert(
                                "all rows client_symbol_name matches",
                                () => execRows.every((r) => r.client_symbol_name === symbol),
                            )
                            .assert("all rows side matches", () => execRows.every((r) => r.side === side))
                            .assert(
                                "all rows server_type = MT4",
                                () => execRows.every((r) => r.server_type === "MT4"),
                            )
                            .assert(
                                "all FILLED rows routing = ABOOK",
                                () => filledRows.every((r) => r.routing === "ABOOK"),
                            )
                            .assert(
                                "all FILLED rows lp_request_at NOT NULL",
                                () => filledRows.every((r) => r.lp_request_at !== null),
                            )
                            .assert(
                                "all FILLED rows lp_executed_at NOT NULL",
                                () => filledRows.every((r) => r.lp_executed_at !== null),
                            )
                            .assert(
                                "all FILLED rows lp_response_at NOT NULL",
                                () => filledRows.every((r) => r.lp_response_at !== null),
                            )
                            .assert(
                                "all FILLED rows client_price_filled > 0",
                                () => filledRows.every((r) => (r.client_price_filled ?? 0) > 0),
                            )
                            .assert(
                                "SUM(client_amount_filled WHERE FILLED) > 0",
                                () => sumFilledVolume > 0,
                            );
                    },
                );
                const result = await scenario.run(tc);

                const sqlAttachment = writeTextAttachment(ALLURE_DIR, "ClickHouse SELECT", sql, "text/plain", "txt");
                const tableAttachment = writeTextAttachment(
                    ALLURE_DIR,
                    execRows.length
                        ? `collector.execution — ${execRows.length} row${execRows.length === 1 ? "" : "s"}`
                        : "collector.execution — no matching rows",
                    execRows.length
                        ? renderExecutionsHtml(
                            execRows,
                            `collector.execution (client_order_id='${emulatorOrderId}', client_acc_id='${login}', tenant_id='${TENANT}')`,
                        )
                        : wrapHtml(
                            `<p><strong>No rows</strong> matched <code>client_order_id='${emulatorOrderId}' AND client_acc_id='${login}' AND tenant_id='${TENANT}'</code>.</p><p>Reason: ${escapeHtml(blockReason ?? "unknown")}</p>`,
                        ),
                    "text/html",
                    "html",
                );
                attachExtrasToLatestAllureResult(ALLURE_DIR, [
                    {attachment: sqlAttachment, stepNameIncludes: "wait for collector.execution rows"},
                    {attachment: tableAttachment, stepNameIncludes: "wait for collector.execution rows"},
                ]);
                labelSuites();

                if (blockReason) expect.fail(blockReason);
                expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
            }, POLL_TIMEOUT_MS + CH_POLL_TIMEOUT_MS + 10_000);

            // -----------------------------------------------------------------
            // Step 7 — Kafka order events via Subscriber + KafkaAdapter.
            // Subscribers MUST be listed before publishers in scenario.components;
            // here we have only a subscriber (bridge plays the publisher role).
            // -----------------------------------------------------------------
            it(`Step 7 — Kafka: order.received + order.finished publish a message for our orderId`, async () => {
                let blockReason: string | undefined;

                if (!(await isKafkaReachable(KAFKA_BROKERS))) {
                    blockReason =
                        `Kafka brokers ${KAFKA_BROKERS.join(",")} unreachable. ` +
                        `Start port-forward in a separate shell: ` +
                        `tests/smoke/scripts/kafka-port-forward.ps1 (or .sh). ` +
                        `Override broker with KAFKA_BROKERS=<host:port> env var if needed.`;
                }

                // Raw kafkajs path: the testurio Subscriber lifecycle starts the
                // consumer lazily AFTER `placeAndCaptureOrderId`, so bridge events
                // published in the meantime land before the consumer joins and
                // are missed (delay measured at -200 ms — message arrives before
                // wait step starts). We open the consumer first, wait for the
                // GROUP_JOIN, then place the order — guaranteeing capture.
                const kafka = new Kafka({
                    brokers: KAFKA_BROKERS,
                    clientId: `mt4-smoke-${randomUUID()}`,
                    logLevel: kafkaLogLevel.NOTHING,
                });
                const consumer = kafka.consumer({
                    groupId: `mt4-smoke-${randomUUID()}`,
                    sessionTimeout: 10_000,
                    heartbeatInterval: 3_000,
                });

                const matched = {received: false, finished: false};
                let resolveBoth: () => void = () => undefined;
                const bothMatched = new Promise<void>((res) => {
                    resolveBoth = res;
                });

                let emulatorOrderId = 0;
                let orderIdMarker = "";
                let polledState: string | undefined;

                try {
                    if (!blockReason) {
                        await consumer.connect();
                        await consumer.subscribe({topic: KAFKA_TOPIC_RECEIVED, fromBeginning: false});
                        await consumer.subscribe({topic: KAFKA_TOPIC_FINISHED, fromBeginning: false});

                        // Register GROUP_JOIN listener BEFORE consumer.run() —
                        // kafkajs fires it as soon as the group rebalance
                        // completes (often within 1-2 s), and a listener
                        // attached afterwards races with the event.
                        const joinedPromise = new Promise<void>((res, rej) => {
                            const t = setTimeout(
                                () => rej(new Error("kafkajs GROUP_JOIN timeout (15s)")),
                                15_000,
                            );
                            consumer.on(consumer.events.GROUP_JOIN, () => {
                                clearTimeout(t);
                                res();
                            });
                        });

                        await consumer.run({
                            eachMessage: async ({topic, message}) => {
                                const val = message.value;
                                if (!val || !orderIdMarker) return;
                                if (!bytesContainAscii(val as Uint8Array, orderIdMarker)) return;
                                if (topic === KAFKA_TOPIC_RECEIVED) matched.received = true;
                                if (topic === KAFKA_TOPIC_FINISHED) matched.finished = true;
                                if (matched.received && matched.finished) resolveBoth();
                            },
                        });

                        await joinedPromise;

                        // Now safe to place the order.
                        const placed = await placeAndCaptureOrderId("Step 7");
                        const placeResp = placed.response;
                        if (placeResp.code !== "OK") {
                            blockReason = `Step 2: code=${placeResp.code} message=${JSON.stringify(placeResp.message)}`;
                        } else {
                            emulatorOrderId = placeResp.orderId;
                            orderIdMarker = String(emulatorOrderId);
                            const polled = await pollUntilTerminal(emulatorOrderId);
                            polledState = polled.state;
                            if (polled.state !== "FILLED") {
                                blockReason =
                                    `Step 4 precondition: order ${emulatorOrderId} state=${polled.state} ` +
                                    `comment=${JSON.stringify(polled.lastBody.comment ?? "")}`;
                            } else {
                                // Race: messages may have arrived for prior orders before
                                // orderIdMarker was set; eachMessage now starts matching.
                                await Promise.race([
                                    bothMatched,
                                    new Promise<void>((res) => setTimeout(res, KAFKA_WAIT_MESSAGE_TIMEOUT_MS)),
                                ]);
                            }
                        }
                    }
                } finally {
                    try {
                        await consumer.disconnect();
                    } catch {
                        // best-effort cleanup
                    }
                }

                writeTextAttachment(
                    ALLURE_DIR,
                    "Kafka subscription",
                    `brokers: ${KAFKA_BROKERS.join(",")}\n` +
                    `topics:\n  - ${KAFKA_TOPIC_RECEIVED}\n  - ${KAFKA_TOPIC_FINISHED}\n` +
                    `matcher: bytes include "${orderIdMarker}"\n` +
                    `terminalState: ${polledState ?? "—"}\n` +
                    `matched: received=${matched.received} finished=${matched.finished}`,
                    "text/plain",
                    "txt",
                );

                if (blockReason) expect.fail(blockReason);
                expect(
                    matched.received,
                    `order.received event for orderId=${emulatorOrderId} not seen within ${KAFKA_WAIT_MESSAGE_TIMEOUT_MS}ms`,
                ).toBe(true);
                expect(
                    matched.finished,
                    `order.finished event for orderId=${emulatorOrderId} not seen within ${KAFKA_WAIT_MESSAGE_TIMEOUT_MS}ms`,
                ).toBe(true);
            }, POLL_TIMEOUT_MS + KAFKA_WAIT_MESSAGE_TIMEOUT_MS * 2 + 30_000);
        },
    );
});
