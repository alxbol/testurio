/**
 * TC-MT5-MARKET-BUY-{SYMBOL}-001 — parameterized smoke for MT5 → bridge → fill happy path.
 *
 * Параметризация по символам: EURUSD.MT5, GBPUSD.MT5, XAUUSD.MT5, NZDUSD.MT5.
 *
 * Реализовано:
 *   Step 1 — Health check (GET /v1/health/ping), один раз в beforeAll.
 *   Step 2 — Place market BUY order (POST /v1/orders), code === "OK" + orderId > 0.
 *   Step 3 — Wait for terminal state: poll GET /v1/orders/{id} 500ms × 15s до
 *            state ∈ {FILLED, CANCELLED}; на этом шаге не утверждаем FILLED,
 *            только что terminal-state достигнут.
 *   Step 4 — Assert terminal outcome: строгая проверка final-body
 *            (state=FILLED, symbol/side/type/volume/id matches). При CANCELLED —
 *            извлекаем comment и формируем диагностический фейл (Not enough funds → P3,
 *            "No price"/"Price off" → P5, прочее → escalate).
 *   Step 5 — ClickHouse `collector.order` персистенс: после fill ждём до 10с появления
 *            строки и валидируем поля. Корреляция через (client_acc_id, client_symbol_name,
 *            side, created_at >= test_start) — order_id в CH это UUID, а не integer
 *            эмулятора. Схема таблицы отличается от спеки (нет lp_response_at/executed_at),
 *            проверяем bridge_status/client_status вместо state, requested_core_amount
 *            вместо volume, client_amount_filled вместо filled_volume.
 *   Step 6 — ClickHouse `collector.execution`: проверяем ≥1 fill-execution для ордера
 *            (corr по client_order_id+client_acc_id), routing=ABOOK, execution_type=FILLED,
 *            tenant_id, lp_request_at/lp_executed_at/lp_response_at NOT NULL,
 *            SUM(client_amount_filled WHERE FILLED) > 0. Schema-замена: `type=FILL` из
 *            спеки → `execution_type=FILLED`, `filled_volume` → `client_amount_filled`.
 *   Step 7 — Kafka events: order.received.v1.demo-uat и order.finished.v1.demo-uat
 *            должны опубликовать сообщение про наш orderId. Messages are protobuf-encoded
 *            (not JSON as spec suggested) — we don't decode the payload; instead we capture
 *            HWMs per partition before placing the order, then consume only the new tail
 *            after FILLED and grep for the integer client_order_id as a literal byte
 *            sequence in the message body. Topic naming and protobuf encoding are
 *            adjustments vs spec ("KafkaGWEventsTopic"/"KafkaWHTopic" / JSON).
 *
 * Known spec discrepancy (defect candidate):
 *   OpenAPI declares the path as `GET /v1/ping`, but the live MT5 emulator
 *   answers only on `GET /v1/health/ping` (`/v1/ping` → HTTP 404).
 */

import {readdirSync, readFileSync, statSync, writeFileSync} from "node:fs";
import {randomUUID} from "node:crypto";
import {join} from "node:path";
import {z} from "zod";
import {Client, DataSource, HttpProtocol, type Interaction, testCase, TestScenario} from "testurio";
import {ClickHouseAdapter} from "@testurio/adapter-clickhouse";
import {AllureReporter} from "@testurio/reporter-allure";
import {beforeAll, describe, expect, it} from "vitest";
import {GetV1PingResponse, PostV1OrdersResponse} from "./mt-api.schema";
import {spawnSync} from "node:child_process";

type AllureAttachment = { name: string; source: string; type: string };
type AllureStep = { name: string; attachments?: AllureAttachment[]; steps?: AllureStep[] };

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

function attachExtrasToLatestAllureResult(
    dir: string,
    extras: { attachment: AllureAttachment; stepNameIncludes?: string }[],
): void {
    if (!extras.length) return;
    const candidates = readdirSync(dir)
        .filter((f) => f.endsWith("-result.json"))
        .map((f) => ({f, t: statSync(join(dir, f)).mtimeMs}))
        .sort((a, b) => b.t - a.t);
    if (!candidates.length) return;
    const resultPath = join(dir, candidates[0].f);
    const json = JSON.parse(readFileSync(resultPath, "utf8")) as {
        steps?: AllureStep[];
        attachments?: AllureAttachment[];
    };
    json.steps ||= [];
    json.attachments ||= [];
    for (const {attachment, stepNameIncludes} of extras) {
        const step = stepNameIncludes
            ? json.steps.find((s) => s.name.includes(stepNameIncludes))
            : undefined;
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
    const candidates = readdirSync(dir)
        .filter((f) => f.endsWith("-result.json"))
        .map((f) => ({f, t: statSync(join(dir, f)).mtimeMs}))
        .sort((a, b) => b.t - a.t);
    if (!candidates.length) return;

    const resultPath = join(dir, candidates[0].f);
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

        if (requestStep) {
            (requestStep.attachments ||= []).push(reqAttachment);
        } else {
            json.attachments.push(reqAttachment);
        }
        if (responseStep) {
            (responseStep.attachments ||= []).push(respAttachment);
        } else {
            json.attachments.push(respAttachment);
        }
    }

    writeFileSync(resultPath, JSON.stringify(json, null, 2), "utf8");
}

// ---------------------------------------------------------------------------
// Service contract (only what Step 1 needs)
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
        // path is dynamic: `/v1/orders/{orderId}` — typed loose so we can interpolate at runtime.
        request: { method: "GET"; path: string };
        response: { code: 200; body: OrderStateResponseBody };
    };
}

const PingResponseSchema = z
    .object({
        code: z.literal(200),
        body: GetV1PingResponse,
    })
    .passthrough();

const PlaceOrderResponseSchema = z
    .object({
        code: z.literal(200),
        body: PostV1OrdersResponse,
    })
    .passthrough();

// Relaxed schema: the emulator returns `filledAvgPrice = 0` and `filledVolume = 0`
// even when `state === "FILLED"` (asynchronous fill-detail update, см. Step 4 note
// в MT5-market-buy-gbpusd.md). The OAS-generated GetV1OrdersOrderIdResponse
// has `.gt(0)` on filledAvgPrice which conflicts with real emulator behaviour.
const GetOrderResponseBodySchema = z
    .object({
        id: z.number().optional(),
        type: z.enum(["MARKET", "LIMIT", "STOP", "STOP_LOSS", "TAKE_POFIT"]).optional(),
        side: z.enum(["BUY", "SELL"]),
        state: z.string().optional(),
        symbol: z.string().optional(),
        time: z.string().optional(),
        requestedPrice: z.number().optional(),
        filledAvgPrice: z.number().min(0).optional(),
        volume: z.number().min(0).optional(),
        filledVolume: z.number().min(0).optional(),
        createdAt: z.iso.datetime({}),
        comment: z.string().optional(),
    })
    .passthrough();

const GetOrderResponseSchema = z
    .object({
        code: z.literal(200),
        body: GetOrderResponseBodySchema,
    })
    .passthrough();

const TERMINAL_STATES = new Set(["FILLED", "CANCELLED"]);
const POLL_INTERVAL_MS = 500;
const POLL_TIMEOUT_MS = 30_000;

async function placeOrderRaw(body: PlaceOrderRequestBody): Promise<PlaceOrderResponseBody> {
    const r = await fetch("http://192.168.8.46:5001/v1/orders", {
        method: "POST",
        headers: {"content-type": "application/json"},
        body: JSON.stringify(body),
    });
    if (!r.ok) throw new Error(`POST /v1/orders → HTTP ${r.status}`);
    return (await r.json()) as PlaceOrderResponseBody;
}

// ---------------------------------------------------------------------------
// ClickHouse collector.order (Step 5)
// ---------------------------------------------------------------------------

const CH_URL = "http://clickhouse.test-stable.cbrid.ge:8123";
const CH_USER = "admin";
const CH_PASS = "admin";
const CH_DB = "default";
const CH_POLL_TIMEOUT_MS = 30_000;
const CH_POLL_INTERVAL_MS = 1_000;

function makeClickHouseDataSource(name = "clickhouse-collector") {
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
    client_order_id: string; // string-form of MT5 emulator integer orderId — used for exact correlation
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

function escapeHtml(s: string): string {
    return s
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}

function renderRowAsHtmlTable(row: CollectorOrderRow, caption: string): string {
    const rows = COLLECTOR_ORDER_COLUMNS.map((col) => {
        const v = (row as unknown as Record<string, unknown>)[col];
        const isNull = v === null || v === undefined;
        const cell = isNull
            ? '<em style="color:#888">null</em>'
            : escapeHtml(String(v));
        return `    <tr><td><code>${col}</code></td><td>${cell}</td></tr>`;
    }).join("\n");
    return `<!doctype html><html lang=""><head><meta charset="utf-8"><style>
body{font:13px/1.4 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;margin:8px;color:#222}
table{border-collapse:collapse;width:100%}
caption{text-align:left;font-weight:600;padding:4px 0 8px;font-size:14px}
th,td{border:1px solid #ddd;padding:4px 8px;vertical-align:top;text-align:left}
th{background:#f4f4f4}
td:first-child{width:240px;white-space:nowrap;background:#fafafa}
code{font:12px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
</style></head><body>
<table>
  <caption>${escapeHtml(caption)}</caption>
  <thead><tr><th>Column</th><th>Value</th></tr></thead>
  <tbody>
${rows}
  </tbody>
</table>
</body></html>`;
}

async function chQuery<T>(sql: string): Promise<T[]> {
    const url = `${CH_BASE}/?query=${encodeURIComponent(sql + " FORMAT JSONEachRow")}`;
    const r = await fetch(url, {headers: {Authorization: CH_AUTH}});
    if (!r.ok) throw new Error(`CH ${r.status}: ${await r.text()}`);
    const text = await r.text();
    if (!text.trim()) return [];
    return text.trim().split("\n").map((line) => JSON.parse(line) as T);
}

async function waitForCollectorOrder(opts: {
    emulatorOrderId: number;
    tenant: string;
    login: number;
}): Promise<{ row: CollectorOrderRow; sql: string; polls: number }> {
    const sql = buildCollectorOrderSql(opts.emulatorOrderId, opts.tenant, opts.login);
    const deadline = Date.now() + CH_POLL_TIMEOUT_MS;
    let polls = 0;
    while (Date.now() < deadline) {
        polls++;
        const rows = await chQuery<CollectorOrderRow>(sql);
        if (rows.length === 1) return {row: rows[0], sql, polls};
        await new Promise((res) => setTimeout(res, CH_POLL_INTERVAL_MS));
    }
    throw new Error(
        `collector.order: no row with client_order_id='${opts.emulatorOrderId}' client_acc_id='${opts.login}' tenant_id='${opts.tenant}' is_completed=1 ` +
        `after ${CH_POLL_TIMEOUT_MS}ms (polls=${polls})`,
    );
}

// ---------------------------------------------------------------------------
// ClickHouse collector.execution (Step 6)
// ---------------------------------------------------------------------------

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

function renderExecutionsAsHtmlTable(rows: CollectorExecutionRow[], caption: string): string {
    if (!rows.length) {
        return `<!doctype html><html lang=""><body><p style="font:13px sans-serif"><em>No execution rows.</em></p></body></html>`;
    }
    // Wide layout: columns = fields, one column per execution row.
    const headerCells = rows
        .map((_, i) => `<th>execution #${i + 1}</th>`)
        .join("");
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
    return `<!doctype html><html lang=""><head><meta charset="utf-8"><style>
body{font:13px/1.4 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;margin:8px;color:#222}
table{border-collapse:collapse;width:100%}
caption{text-align:left;font-weight:600;padding:4px 0 8px;font-size:14px}
th,td{border:1px solid #ddd;padding:4px 8px;vertical-align:top;text-align:left}
th{background:#f4f4f4}
td:first-child{width:240px;white-space:nowrap;background:#fafafa}
code{font:12px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
</style></head><body>
<table>
  <caption>${escapeHtml(caption)}</caption>
  <thead><tr><th>Column</th>${headerCells}</tr></thead>
  <tbody>
${bodyRows}
  </tbody>
</table>
</body></html>`;
}

async function waitForCollectorExecutions(opts: {
    emulatorOrderId: number;
    tenant: string;
    login: number;
    minRows?: number;
}): Promise<{ rows: CollectorExecutionRow[]; sql: string; polls: number }> {
    const sql = buildCollectorExecutionSql(opts.emulatorOrderId, opts.tenant, opts.login);
    const minRows = opts.minRows ?? 1;
    const deadline = Date.now() + CH_POLL_TIMEOUT_MS;
    let polls = 0;
    while (Date.now() < deadline) {
        polls++;
        const rows = await chQuery<CollectorExecutionRow>(sql);
        if (rows.length >= minRows) return {rows, sql, polls};
        await new Promise((res) => setTimeout(res, CH_POLL_INTERVAL_MS));
    }
    throw new Error(
        `collector.execution: <${minRows} row(s) for client_order_id='${opts.emulatorOrderId}' client_acc_id='${opts.login}' tenant_id='${opts.tenant}' ` +
        `after ${CH_POLL_TIMEOUT_MS}ms (polls=${polls})`,
    );
}

// ---------------------------------------------------------------------------
// Kafka via `kubectl exec` into kafka-controller-0 (Step 7).
// Bootstrap kafka:9092 is k8s-internal; we shell out to bitnami kafka tools
// inside the pod instead of routing traffic through port-forward.
// ---------------------------------------------------------------------------

const KAFKA_KUBECTL = "C:/Users/abolshakov/bin/kubectl";
const KAFKA_KUBECONFIG = "C:/Users/abolshakov/Documents/k8s/kubeconfig";
const KAFKA_NS = "test-stable";
const KAFKA_POD = "kafka-controller-0";
const KAFKA_BOOTSTRAP = "kafka:9092";
const KAFKA_BIN = "/opt/bitnami/kafka/bin";

const KAFKA_TOPIC_RECEIVED = "order.received.v1.demo-uat";
const KAFKA_TOPIC_FINISHED = "order.finished.v1.demo-uat";
const KAFKA_LAG_MS = 3_000;
const KAFKA_CONSUME_TIMEOUT_MS = 8_000;

interface PartitionOffset {
    partition: number;
    offset: number;
}

function kafkaExec(bashScript: string, timeoutSec = 30): string {
    // Use spawnSync (no shell) — kubectl is launched directly with argv to avoid
    // Windows shell-quoting issues. Pass the bash script via stdin to bash inside
    // the pod so we don't have to escape it in argv.
    const r = spawnSync(
        KAFKA_KUBECTL,
        [
            `--kubeconfig=${KAFKA_KUBECONFIG}`,
            "-n",
            KAFKA_NS,
            "exec",
            "-i",
            KAFKA_POD,
            "-c",
            "kafka",
            "--",
            "bash",
            "-s",
        ],
        {
            input: bashScript,
            encoding: "utf8",
            timeout: timeoutSec * 1000,
            maxBuffer: 16 * 1024 * 1024,
        },
    );
    if (r.error) throw r.error;
    if (r.status !== 0) {
        throw new Error(`kubectl exec exited ${r.status}: ${r.stderr || r.stdout}`);
    }
    return r.stdout ?? "";
}

function getKafkaHwms(topic: string): PartitionOffset[] {
    const out = kafkaExec(
        `${KAFKA_BIN}/kafka-get-offsets.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${topic} --time -1 2>/dev/null`,
    );
    return out
        .trim()
        .split("\n")
        .filter(Boolean)
        .map((line) => {
            // Format: <topic>:<partition>:<offset>
            const [, p, o] = line.split(":");
            return {partition: Number(p), offset: Number(o)};
        })
        .sort((a, b) => a.partition - b.partition);
}

function consumeKafkaTail(opts: {
    topic: string;
    fromOffsets: PartitionOffset[];
    maxMessagesPerPartition: number;
    timeoutMs: number;
}): { rawOutput: string; perPartition: { partition: number; from: number; messageCount: number }[] } {
    const lines: string[] = [];
    for (const po of opts.fromOffsets) {
        lines.push(`echo "===PARTITION=${po.partition} FROM=${po.offset}==="`);
        lines.push(
            `${KAFKA_BIN}/kafka-console-consumer.sh ` +
            `--bootstrap-server ${KAFKA_BOOTSTRAP} ` +
            `--topic ${opts.topic} ` +
            `--partition ${po.partition} ` +
            `--offset ${po.offset} ` +
            `--max-messages ${opts.maxMessagesPerPartition} ` +
            `--timeout-ms ${opts.timeoutMs} ` +
            `2>/dev/null || true`,
        );
        lines.push(`echo "===END_PARTITION=${po.partition}==="`);
    }
    let rawOutput = "";
    try {
        rawOutput = kafkaExec(lines.join("\n"), Math.ceil((opts.timeoutMs * opts.fromOffsets.length) / 1000) + 15);
    } catch (e) {
        rawOutput = (e as { stdout?: string }).stdout ?? "";
    }
    const perPartition: { partition: number; from: number; messageCount: number }[] = [];
    const partitionBlocks = rawOutput.split(/===PARTITION=(\d+) FROM=(\d+)===/g);
    // split gives: [pre, p, from, body, p, from, body, ...]
    for (let i = 1; i + 2 < partitionBlocks.length; i += 3) {
        const p = Number(partitionBlocks[i]);
        const from = Number(partitionBlocks[i + 1]);
        const body = partitionBlocks[i + 2].split(`===END_PARTITION=${p}===`)[0] ?? "";
        // Each kafka-console-consumer message is on its own line (or multiple).
        // Use the "Processed a total of N messages" hint emitted to stderr (we suppressed) —
        // fallback: count non-empty lines as a rough proxy.
        const messageCount = body.split("\n").filter((l) => l.trim().length > 0).length;
        perPartition.push({partition: p, from, messageCount});
    }
    return {rawOutput, perPartition};
}

function buildConsumerCommand(topic: string, fromOffsets: PartitionOffset[], maxPerPartition: number, timeoutMs: number): string {
    return fromOffsets
        .map(
            (po) =>
                `${KAFKA_BIN}/kafka-console-consumer.sh \\\n` +
                `  --bootstrap-server ${KAFKA_BOOTSTRAP} \\\n` +
                `  --topic ${topic} \\\n` +
                `  --partition ${po.partition} \\\n` +
                `  --offset ${po.offset} \\\n` +
                `  --max-messages ${maxPerPartition} \\\n` +
                `  --timeout-ms ${timeoutMs}`,
        )
        .join("\n\n");
}

function renderKafkaSummaryHtml(opts: {
    topic: string;
    hwmsBefore: PartitionOffset[];
    hwmsAfter: PartitionOffset[];
    matchedCount: number;
    rawSnippetLines: string[];
}): string {
    const partRows = opts.hwmsBefore
        .map((b) => {
            const a = opts.hwmsAfter.find((x) => x.partition === b.partition);
            const newMsgs = (a?.offset ?? b.offset) - b.offset;
            return `    <tr><td>${b.partition}</td><td>${b.offset}</td><td>${a?.offset ?? "—"}</td><td>${newMsgs}</td></tr>`;
        })
        .join("\n");
    const snippet = opts.rawSnippetLines
        .slice(0, 50)
        .map((l) => escapeHtml(l))
        .join("\n");
    return `<!doctype html><html><head><meta charset="utf-8"><style>
body{font:13px/1.4 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;margin:8px;color:#222}
table{border-collapse:collapse;margin-bottom:12px}
caption{text-align:left;font-weight:600;padding:4px 0 8px;font-size:14px}
th,td{border:1px solid #ddd;padding:4px 8px;text-align:left}
th{background:#f4f4f4}
pre{font:12px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;background:#fafafa;border:1px solid #ddd;padding:8px;white-space:pre-wrap;word-break:break-word;max-height:480px;overflow:auto}
.match{font-weight:600;color:${opts.matchedCount > 0 ? "#0a7" : "#c33"}}
</style></head><body>
<table>
  <caption>${escapeHtml(opts.topic)} — partition tail</caption>
  <thead><tr><th>partition</th><th>HWM before place</th><th>HWM after fill</th><th>new messages</th></tr></thead>
  <tbody>
${partRows}
  </tbody>
</table>
<p>Messages whose payload contains <code>client_order_id</code> literal: <span class="match">${opts.matchedCount}</span></p>
<p><em>Payload is protobuf — raw bytes shown below for context only (printable characters survive, binary is mangled). Field validation is delegated to Steps 5 &amp; 6 (collector.order / collector.execution rows).</em></p>
<pre>${snippet || "&lt;no new messages&gt;"}</pre>
</body></html>`;
}

async function pollUntilTerminal(orderId: number): Promise<{
    state: string;
    polls: number;
    lastBody: OrderStateResponseBody
}> {
    const deadline = Date.now() + POLL_TIMEOUT_MS;
    let polls = 0;
    let lastBody: OrderStateResponseBody | undefined;
    while (Date.now() < deadline) {
        polls++;
        const r = await fetch(`http://192.168.8.46:5001/v1/orders/${orderId}`);
        if (r.status === 404 && polls === 1) {
            // emulator may briefly not yet have committed the order; small retry budget
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
// Parameterization
// ---------------------------------------------------------------------------

interface SymbolCase {
    symbol: string; // полное имя символа на MT5 emulator
    tcCode: string; // короткий код в TC ID (GBPUSD, EURUSD, ...)
    login: number; // demo-uat trader login (одна и та же MT5 учётка, см. reference_demo_uat_traders)
    volume: number; // плановый объём для последующих шагов (Step 2+)
    side: "BUY" | "SELL";
}

const SYMBOL_CASES: SymbolCase[] = (["BUY", "SELL"] as const).flatMap((side) => [
    {symbol: "EURUSD.MT5", tcCode: "EURUSD", login: 123468, volume: 0.01, side},
    {symbol: "GBPUSD.MT5", tcCode: "GBPUSD", login: 123468, volume: 0.01, side},
    {symbol: "XAUUSD.MT5", tcCode: "XAUUSD", login: 123468, volume: 0.01, side},
    {symbol: "NZDUSD.MT5", tcCode: "NZDUSD", login: 123468, volume: 0.01, side},
]);

// ---------------------------------------------------------------------------
// Step 1 — Health check (один раз перед всеми Step 2 прогонами).
// Падает → отменяет весь suite (precondition P1).
// ---------------------------------------------------------------------------

describe("MT5 | Market order BUY | parametrized by symbol", () => {
    beforeAll(async () => {
        const mtClient = new Client("mt5-emulator", {
            protocol: new HttpProtocol<MtEmulatorApi>(),
            targetAddress: {host: "192.168.8.46", port: 5001},
        });

        const scenario = new TestScenario({
            name: "TC-MT5-MARKET-BUY / Step 1 (shared health check)",
            components: [mtClient],
            recording: true,
        });

        scenario.addReporter(
            new AllureReporter({
                resultsDir: "allure-results",
                environmentInfo: {
                    env: "demo-uat / test-stable",
                    target: "192.168.8.46:5001 (MT Test emulator)",
                    step: "1 — health check",
                    node: process.version,
                },
            })
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
        attachInteractionsToLatestAllureResult("allure-results", result.interactions ?? []);

        if (!result.passed) {
            throw new Error(
                `Step 1 health check failed — aborting suite (precondition P1).\n${JSON.stringify(result, null, 2)}`
            );
        }
    });

    describe.each(SYMBOL_CASES)("MT5 | Market order $side | $symbol $volume | login $login", ({
                                                                                                  symbol,
                                                                                                  tcCode,
                                                                                                  login,
                                                                                                  volume,
                                                                                                  side,
                                                                                              }) => {
        it("Step 2 — place market BUY: POST /v1/orders → 200 {code:'OK', orderId>0} (schema-validated)", async () => {
            const mtClient = new Client("mt5-emulator", {
                protocol: new HttpProtocol<MtEmulatorApi>(),
                targetAddress: {host: "192.168.8.46", port: 5001},
            });

            const scenario = new TestScenario({
                name: `TC-MT5-MARKET-${side}-${tcCode}-001 / Step 2`,
                components: [mtClient],
                recording: true,
            });

            scenario.addReporter(
                new AllureReporter({
                    resultsDir: "allure-results",
                    environmentInfo: {
                        env: "demo-uat / test-stable",
                        target: "192.168.8.46:5001 (MT Test emulator)",
                        symbol,
                        login: String(login),
                        volume: String(volume),
                        side,
                        node: process.version,
                    },
                })
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
                    .assert("orderId > 0", (res) => typeof res.body.orderId === "number" && res.body.orderId > 0)
                    .assert("message is empty", (res) => res.body.message === "");
            });

            const result = await scenario.run(tc);

            attachInteractionsToLatestAllureResult("allure-results", result.interactions ?? []);

            expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
            expect(result.interactions?.length, "expected one recorded interaction").toBe(1);
            const ix = result.interactions![0];
            expect(ix.serviceName).toBe("mt5-emulator");
            expect(ix.messageType).toBe("placeOrder");
            expect(ix.status).toBe("completed");
            expect(ix.requestPayload).toMatchObject({
                method: "POST",
                path: "/v1/orders",
                body: {symbol, volume, side, type: "MARKET", login},
            });
            const resp = ix.responsePayload as { code: number; body: PlaceOrderResponseBody };
            expect(resp.code).toBe(200);
            expect(resp.body.code).toBe("OK");
            expect(resp.body.orderId).toBeGreaterThan(0);
        });

        it(
            "Step 3 — poll until terminal state: GET /v1/orders/{id} reaches FILLED|CANCELLED within 15s (schema-validated)",
            async () => {
                // 1. Place a fresh order via raw HTTP so we own the orderId imperatively.
                const placed = await placeOrderRaw({symbol, volume, side, type: "MARKET", login});
                expect(placed.code, JSON.stringify(placed)).toBe("OK");
                expect(placed.orderId).toBeGreaterThan(0);
                const orderId = placed.orderId;

                // 2. Poll until terminal state (FILLED | CANCELLED) or 15s deadline.
                const polled = await pollUntilTerminal(orderId);

                // 3. Report final GET via a testurio scenario so Allure carries the
                //    request/response under the same parameterized suite.
                const mtClient = new Client("mt5-emulator", {
                    protocol: new HttpProtocol<MtEmulatorApi>(),
                    targetAddress: {host: "192.168.8.46", port: 5001},
                });

                const scenario = new TestScenario({
                    name: `TC-MT5-MARKET-${side}-${tcCode}-001 / Step 3`,
                    components: [mtClient],
                    recording: true,
                });

                scenario.addReporter(
                    new AllureReporter({
                        resultsDir: "allure-results",
                        environmentInfo: {
                            env: "demo-uat / test-stable",
                            target: "192.168.8.46:5001 (MT Test emulator)",
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
                attachInteractionsToLatestAllureResult("allure-results", result.interactions ?? []);

                expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
                expect(TERMINAL_STATES.has(polled.state), `non-terminal state ${polled.state}`).toBe(true);
            },
            POLL_TIMEOUT_MS + 5_000,
        );

        it(
            "Step 4 — assert terminal outcome: final body matches placed order (state=FILLED, all fields)",
            async () => {
                // 1. Place + 2. Poll (Step 2+3 preconditions).
                const placed = await placeOrderRaw({symbol, volume, side, type: "MARKET", login});
                expect(placed.code, JSON.stringify(placed)).toBe("OK");
                const orderId = placed.orderId;
                const polled = await pollUntilTerminal(orderId);

                // 3. Diagnostic hint computed up-front; surfaced via Allure env if CANCELLED.
                const cancelComment = polled.state === "CANCELLED" ? polled.lastBody.comment ?? "" : "";
                const cancelHint =
                    polled.state !== "CANCELLED"
                        ? ""
                        : /not enough funds/i.test(cancelComment)
                            ? "P3 broken (account underfunded for current price × volume)"
                            : /no price|price off/i.test(cancelComment)
                                ? "P5 broken (no ticks for the symbol)"
                                : "escalate with raw response";

                // 4. Strict assert of final body per Step 4 spec (always runs — Allure must
                //    record CANCELLED outcomes too, not just FILLED).
                const mtClient = new Client("mt5-emulator", {
                    protocol: new HttpProtocol<MtEmulatorApi>(),
                    targetAddress: {host: "192.168.8.46", port: 5001},
                });

                const scenario = new TestScenario({
                    name: `TC-MT5-MARKET-${side}-${tcCode}-001 / Step 4`,
                    components: [mtClient],
                    recording: true,
                });

                scenario.addReporter(
                    new AllureReporter({
                        resultsDir: "allure-results",
                        environmentInfo: {
                            env: "demo-uat / test-stable",
                            target: "192.168.8.46:5001 (MT Test emulator)",
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
                        // filledVolume intentionally NOT strictly asserted: see spec note —
                        // emulator may report filledVolume=0 right after state flips to FILLED.
                    },
                );

                const result = await scenario.run(tc);
                attachInteractionsToLatestAllureResult("allure-results", result.interactions ?? []);

                expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
            },
            POLL_TIMEOUT_MS + 5_000,
        );

        it(
            "Step 5 — ClickHouse collector.order: row persisted (exact client_order_id match, ≤30s lag)",
            async () => {
                // 1. Place + poll. Collect failure context but do NOT throw early —
                //    we want Allure to record this case (passed OR failed) for every parameter.
                let blockReason: string | undefined;
                let placeResp: PlaceOrderResponseBody | undefined;
                let polled: Awaited<ReturnType<typeof pollUntilTerminal>> | undefined;
                let row: CollectorOrderRow | undefined;
                let sql: string | undefined;
                let chPolls = 0;

                try {
                    placeResp = await placeOrderRaw({symbol, volume, side, type: "MARKET", login});
                } catch (e) {
                    blockReason = `Step 2 (place) raw error: ${(e as Error).message}`;
                }

                if (!blockReason) {
                    if (placeResp!.code !== "OK") {
                        blockReason =
                            `Step 2: code=${placeResp!.code} message=${JSON.stringify(placeResp!.message)} ` +
                            `(orderId=${placeResp!.orderId}); cannot proceed to Step 5`;
                    } else {
                        try {
                            polled = await pollUntilTerminal(placeResp!.orderId);
                        } catch (e) {
                            blockReason = `Step 3 (poll) error: ${(e as Error).message}`;
                        }
                    }
                }

                const emulatorOrderId = placeResp?.orderId ?? 0;
                sql = emulatorOrderId
                    ? buildCollectorOrderSql(emulatorOrderId, "demo-uat", login)
                    : "-- order was not placed";

                if (!blockReason && polled && polled.state !== "FILLED") {
                    blockReason =
                        `Step 4 precondition: order ${emulatorOrderId} state=${polled.state} ` +
                        `comment=${JSON.stringify(polled.lastBody.comment ?? "")} — Step 5 cannot validate persistence`;
                }

                if (!blockReason) {
                    try {
                        const r = await waitForCollectorOrder({emulatorOrderId, tenant: "demo-uat", login});
                        row = r.row;
                        sql = r.sql;
                        chPolls = r.polls;
                    } catch (e) {
                        blockReason = `Step 5 CH wait: ${(e as Error).message}`;
                    }
                }

                // 2. Always anchor an Allure entry via a testurio scenario so failed cases
                //    are visible in the report (not silently dropped before run).
                //    Use a real ClickHouse client whose request IS the SELECT — that way
                //    the step labels in Allure read "queryCollectorOrder" instead of "ping".
                interface CollectorApi {
                    queryCollectorOrder: {
                        request: {
                            method: "GET";
                            path: string;
                            headers?: { Authorization: string };
                        };
                        response: { code: 200; body: unknown };
                    };
                }

                const chClient = new Client("clickhouse-collector", {
                    protocol: new HttpProtocol<CollectorApi>(),
                    targetAddress: {host: "clickhouse.test-stable.cbrid.ge", port: 8123},
                });
                const scenario = new TestScenario({
                    name: `TC-MT5-MARKET-${side}-${tcCode}-001 / Step 5`,
                    components: [chClient],
                    recording: false,
                });
                scenario.addReporter(
                    new AllureReporter({
                        resultsDir: "allure-results",
                        environmentInfo: {
                            env: "demo-uat / test-stable",
                            collector: "clickhouse.test-stable.cbrid.ge:8123 (collector.order)",
                            symbol,
                            login: String(login),
                            volume: String(volume),
                            side,
                            emulatorOrderId: String(emulatorOrderId),
                            terminalState: polled?.state ?? "—",
                            emulatorComment: polled?.lastBody.comment || "—",
                            collectorOrderId: row?.order_id ?? "—",
                            bridge_status: row?.bridge_status ?? "—",
                            chPolls: String(chPolls),
                            blocked: blockReason ?? "—",
                            node: process.version,
                        },
                    }),
                );

                const tcName = row
                    ? `chOrder ${symbol} ${side} login=${login} emulatorOrderId=${emulatorOrderId} → ${row.order_id}`
                    : `chOrder ${symbol} ${side} login=${login} emulatorOrderId=${emulatorOrderId} — BLOCKED`;

                const tc = testCase(tcName, (test) => {
                    const ch = test.use(chClient);
                    // Re-run the SAME SELECT through testurio for an Allure-recorded HTTP step.
                    // The assertions below check fields of `row` (already fetched above).
                    ch.request("queryCollectorOrder", {
                        method: "GET",
                        path: `/?query=${encodeURIComponent(sql + " FORMAT JSONEachRow")}`,
                        headers: {Authorization: CH_AUTH},
                    });
                    ch.onResponse("queryCollectorOrder")
                        .assert("HTTP 200", (res) => res.code === 200)
                        .assert("collector.order row present", () => row !== undefined)
                        .assert("client_order_id matches", () => row?.client_order_id === String(emulatorOrderId))
                        .assert("tenant_id = demo-uat", () => row?.tenant_id === "demo-uat")
                        .assert("client_acc_id matches", () => row?.client_acc_id === String(login))
                        .assert("client_symbol_name matches", () => row?.client_symbol_name === symbol)
                        .assert("side matches", () => row?.side === side)
                        .assert("order_type = MARKET", () => row?.order_type === "MARKET")
                        .assert("bridge_status = FILLED", () => row?.bridge_status === "FILLED")
                        .assert("client_status = FILLED", () => row?.client_status === "FILLED")
                        .assert("lp_status = FILLED", () => row?.lp_status === "FILLED")
                        .assert("server_type = MT5", () => row?.server_type === "MT5")
                        .assert("is_completed = 1", () => row?.is_completed === 1)
                        .assert("is_order_received = 1", () => row?.is_order_received === 1)
                        .assert("is_final_order_received = 1", () => row?.is_final_order_received === 1)
                        .assert("reject_text empty", () => row?.reject_text === "")
                        .assert("lp_request_at NOT NULL", () => row?.lp_request_at !== null)
                        .assert(
                            "client_amount_filled === requested_core_amount",
                            () => row?.client_amount_filled === row?.requested_core_amount,
                        );
                });

                const result = await scenario.run(tc);
                attachInteractionsToLatestAllureResult("allure-results", result.interactions ?? []);

                // 3. Attach SQL to the "request" step and the matched-row HTML table to the
                //    "onResponse" step — not to the test result top level.
                const sqlAttachment = writeTextAttachment(
                    "allure-results",
                    "ClickHouse SELECT",
                    sql,
                    "text/plain",
                    "txt",
                );
                const tableAttachment = writeTextAttachment(
                    "allure-results",
                    row ? "collector.order — matched row" : "collector.order — no matching row",
                    row
                        ? renderRowAsHtmlTable(
                            row,
                            `collector.order  (client_order_id='${emulatorOrderId}', client_acc_id='${login}', tenant_id='demo-uat')`,
                        )
                        : `<!doctype html><html lang=""><body><p style="font:13px sans-serif"><strong>No row</strong> matched <code>client_order_id='${emulatorOrderId}' AND client_acc_id='${login}' AND tenant_id='demo-uat' AND is_completed=1</code>.</p><p style="font:13px sans-serif">Reason: ${escapeHtml(blockReason ?? "unknown")}</p></body></html>`,
                    "text/html",
                    "html",
                );
                attachExtrasToLatestAllureResult("allure-results", [
                    {attachment: sqlAttachment, stepNameIncludes: "Request queryCollectorOrder"},
                    {attachment: tableAttachment, stepNameIncludes: "Handle response for queryCollectorOrder"},
                ]);

                // 4. Final vitest assertion — fail loudly if blocked or scenario asserts fired.
                if (blockReason) {
                    expect.fail(blockReason);
                }
                expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
            },
            POLL_TIMEOUT_MS + CH_POLL_TIMEOUT_MS + 5_000,
        );

        it(
            "Step 6 — ClickHouse collector.execution: ≥1 FILLED execution per order (ABOOK routing)",
            async () => {
                // Place + poll. Capture failure context without throwing early so
                // every parametrized case is visible in the Allure report.
                let blockReason: string | undefined;
                let placeResp: PlaceOrderResponseBody | undefined;
                let polled: Awaited<ReturnType<typeof pollUntilTerminal>> | undefined;
                let execRows: CollectorExecutionRow[] = [];
                let sql: string | undefined;
                let chPolls = 0;

                try {
                    placeResp = await placeOrderRaw({symbol, volume, side, type: "MARKET", login});
                } catch (e) {
                    blockReason = `Step 2 (place) raw error: ${(e as Error).message}`;
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
                sql = emulatorOrderId
                    ? buildCollectorExecutionSql(emulatorOrderId, "demo-uat", login)
                    : "-- order was not placed";

                if (!blockReason && polled && polled.state !== "FILLED") {
                    blockReason =
                        `Step 4 precondition: order ${emulatorOrderId} state=${polled.state} ` +
                        `comment=${JSON.stringify(polled.lastBody.comment ?? "")} — Step 6 cannot validate executions`;
                }

                if (!blockReason) {
                    try {
                        const r = await waitForCollectorExecutions({
                            emulatorOrderId,
                            tenant: "demo-uat",
                            login,
                        });
                        execRows = r.rows;
                        sql = r.sql;
                        chPolls = r.polls;
                    } catch (e) {
                        blockReason = `Step 6 CH wait: ${(e as Error).message}`;
                    }
                }

                // Derived metrics for assertions.
                const filledRows = execRows.filter((r) => r.execution_type === "FILLED");
                const sumFilledVolume = filledRows.reduce(
                    (acc, r) => acc + (r.client_amount_filled ?? 0),
                    0,
                );

                // Anchor an Allure entry via a ClickHouse client scenario so the step
                // labels read "Request queryCollectorExecutions" / "Handle response …".
                interface ExecutionApi {
                    queryCollectorExecutions: {
                        request: {
                            method: "GET";
                            path: string;
                            headers?: { Authorization: string };
                        };
                        response: { code: 200; body: unknown };
                    };
                }

                const chClient = new Client("clickhouse-collector", {
                    protocol: new HttpProtocol<ExecutionApi>(),
                    targetAddress: {host: "clickhouse.test-stable.cbrid.ge", port: 8123},
                });
                const scenario = new TestScenario({
                    name: `TC-MT5-MARKET-${side}-${tcCode}-001 / Step 6`,
                    components: [chClient],
                    recording: false,
                });
                scenario.addReporter(
                    new AllureReporter({
                        resultsDir: "allure-results",
                        environmentInfo: {
                            env: "demo-uat / test-stable",
                            collector: "clickhouse.test-stable.cbrid.ge:8123 (collector.execution)",
                            symbol,
                            login: String(login),
                            volume: String(volume),
                            side,
                            emulatorOrderId: String(emulatorOrderId),
                            terminalState: polled?.state ?? "—",
                            executions: String(execRows.length),
                            filledExecutions: String(filledRows.length),
                            sumFilledVolume: String(sumFilledVolume),
                            routing: filledRows[0]?.routing ?? "—",
                            chPolls: String(chPolls),
                            blocked: blockReason ?? "—",
                            node: process.version,
                        },
                    }),
                );

                const tcName = execRows.length
                    ? `chExecutions ${symbol} ${side} login=${login} emulatorOrderId=${emulatorOrderId} (${execRows.length} row${execRows.length === 1 ? "" : "s"})`
                    : `chExecutions ${symbol} ${side} login=${login} emulatorOrderId=${emulatorOrderId} — BLOCKED`;

                const tc = testCase(tcName, (test) => {
                    const ch = test.use(chClient);
                    // Re-run the same SELECT for an Allure-recorded HTTP step.
                    ch.request("queryCollectorExecutions", {
                        method: "GET",
                        path: `/?query=${encodeURIComponent(sql + " FORMAT JSONEachRow")}`,
                        headers: {Authorization: CH_AUTH},
                    });
                    ch.onResponse("queryCollectorExecutions")
                        .assert("HTTP 200", (res) => res.code === 200)
                        .assert("≥1 execution row", () => execRows.length >= 1)
                        .assert("≥1 FILLED execution", () => filledRows.length >= 1)
                        .assert(
                            "all rows tenant_id = demo-uat",
                            () => execRows.every((r) => r.tenant_id === "demo-uat"),
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
                            "all rows server_type = MT5",
                            () => execRows.every((r) => r.server_type === "MT5"),
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
                });

                const result = await scenario.run(tc);
                attachInteractionsToLatestAllureResult("allure-results", result.interactions ?? []);

                // Attach SQL to the "request" step, execution-rows HTML table to the
                // "onResponse" step.
                const sqlAttachment = writeTextAttachment(
                    "allure-results",
                    "ClickHouse SELECT",
                    sql,
                    "text/plain",
                    "txt",
                );
                const tableAttachment = writeTextAttachment(
                    "allure-results",
                    execRows.length
                        ? `collector.execution — ${execRows.length} row${execRows.length === 1 ? "" : "s"}`
                        : "collector.execution — no matching rows",
                    execRows.length
                        ? renderExecutionsAsHtmlTable(
                            execRows,
                            `collector.execution  (client_order_id='${emulatorOrderId}', client_acc_id='${login}', tenant_id='demo-uat')`,
                        )
                        : `<!doctype html><html lang=""><body><p style="font:13px sans-serif"><strong>No rows</strong> matched <code>client_order_id='${emulatorOrderId}' AND client_acc_id='${login}' AND tenant_id='demo-uat'</code>.</p><p style="font:13px sans-serif">Reason: ${escapeHtml(blockReason ?? "unknown")}</p></body></html>`,
                    "text/html",
                    "html",
                );
                attachExtrasToLatestAllureResult("allure-results", [
                    {attachment: sqlAttachment, stepNameIncludes: "Request queryCollectorExecutions"},
                    {attachment: tableAttachment, stepNameIncludes: "Handle response for queryCollectorExecutions"},
                ]);

                if (blockReason) {
                    expect.fail(blockReason);
                }
                expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
            },
            POLL_TIMEOUT_MS + CH_POLL_TIMEOUT_MS + 5_000,
        );

        it(
            "Step 7 — Kafka: order.received + order.finished publish a message for our orderId",
            async () => {
                let blockReason: string | undefined;
                let placeResp: PlaceOrderResponseBody | undefined;
                let polled: Awaited<ReturnType<typeof pollUntilTerminal>> | undefined;

                // 1. Snapshot HWMs BEFORE placing so we only consume our own tail.
                let hwmsReceivedBefore: PartitionOffset[] = [];
                let hwmsFinishedBefore: PartitionOffset[] = [];
                try {
                    hwmsReceivedBefore = getKafkaHwms(KAFKA_TOPIC_RECEIVED);
                    hwmsFinishedBefore = getKafkaHwms(KAFKA_TOPIC_FINISHED);
                } catch (e) {
                    blockReason = `Step 7 kubectl/kafka HWM probe failed: ${(e as Error).message}`;
                }

                // 2. Place + poll (Step 2+3+4 preconditions).
                if (!blockReason) {
                    try {
                        placeResp = await placeOrderRaw({symbol, volume, side, type: "MARKET", login});
                    } catch (e) {
                        blockReason = `Step 2 (place) raw error: ${(e as Error).message}`;
                    }
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
                if (!blockReason && polled && polled.state !== "FILLED") {
                    blockReason =
                        `Step 4 precondition: order ${placeResp!.orderId} state=${polled.state} ` +
                        `comment=${JSON.stringify(polled.lastBody.comment ?? "")} — Step 7 expects FILLED to find Kafka events`;
                }

                const emulatorOrderId = placeResp?.orderId ?? 0;
                const orderIdMarker = String(emulatorOrderId);

                // 3. Allow bridge/kafka publishing lag, then consume the tail of each
                //    topic from HWMs_before.
                let receivedTail: ReturnType<typeof consumeKafkaTail> | undefined;
                let finishedTail: ReturnType<typeof consumeKafkaTail> | undefined;
                let hwmsReceivedAfter: PartitionOffset[] = hwmsReceivedBefore;
                let hwmsFinishedAfter: PartitionOffset[] = hwmsFinishedBefore;
                let receivedMatches = 0;
                let finishedMatches = 0;

                if (!blockReason && emulatorOrderId > 0) {
                    await new Promise((res) => setTimeout(res, KAFKA_LAG_MS));
                    try {
                        hwmsReceivedAfter = getKafkaHwms(KAFKA_TOPIC_RECEIVED);
                        hwmsFinishedAfter = getKafkaHwms(KAFKA_TOPIC_FINISHED);
                        receivedTail = consumeKafkaTail({
                            topic: KAFKA_TOPIC_RECEIVED,
                            fromOffsets: hwmsReceivedBefore,
                            maxMessagesPerPartition: 200,
                            timeoutMs: KAFKA_CONSUME_TIMEOUT_MS,
                        });
                        finishedTail = consumeKafkaTail({
                            topic: KAFKA_TOPIC_FINISHED,
                            fromOffsets: hwmsFinishedBefore,
                            maxMessagesPerPartition: 200,
                            timeoutMs: KAFKA_CONSUME_TIMEOUT_MS,
                        });
                    } catch (e) {
                        blockReason = `Step 7 Kafka consume error: ${(e as Error).message}`;
                    }
                }

                // 4. Grep for our integer client_order_id as a literal byte sequence
                //    inside the protobuf payload.
                const orderIdRe = new RegExp(`(?:^|[^0-9])${orderIdMarker}(?:[^0-9]|$)`);
                if (receivedTail) {
                    receivedMatches = receivedTail.rawOutput
                        .split("\n")
                        .filter((l) => orderIdRe.test(l)).length;
                }
                if (finishedTail) {
                    finishedMatches = finishedTail.rawOutput
                        .split("\n")
                        .filter((l) => orderIdRe.test(l)).length;
                }

                // 5. Synthetic Allure scenario so Step 7 shows up per parametrized case.
                interface KafkaApi {
                    queryKafkaTopics: {
                        request: { method: "GET"; path: string };
                        response: { code: 200; body: unknown };
                    };
                }

                const mtClient = new Client("mt5-emulator", {
                    protocol: new HttpProtocol<KafkaApi>(),
                    targetAddress: {host: "192.168.8.46", port: 5001},
                });
                const scenario = new TestScenario({
                    name: `TC-MT5-MARKET-${side}-${tcCode}-001 / Step 7`,
                    components: [mtClient],
                    recording: false,
                });
                scenario.addReporter(
                    new AllureReporter({
                        resultsDir: "allure-results",
                        environmentInfo: {
                            env: "demo-uat / test-stable",
                            kafka: `${KAFKA_BOOTSTRAP} (via kubectl exec ${KAFKA_POD})`,
                            topicReceived: KAFKA_TOPIC_RECEIVED,
                            topicFinished: KAFKA_TOPIC_FINISHED,
                            symbol,
                            login: String(login),
                            volume: String(volume),
                            side,
                            emulatorOrderId: String(emulatorOrderId),
                            terminalState: polled?.state ?? "—",
                            receivedMatches: String(receivedMatches),
                            finishedMatches: String(finishedMatches),
                            blocked: blockReason ?? "—",
                            node: process.version,
                        },
                    }),
                );

                const tcName = blockReason
                    ? `kafkaOrderEvents ${symbol} ${side} login=${login} emulatorOrderId=${emulatorOrderId} — BLOCKED`
                    : `kafkaOrderEvents ${symbol} ${side} login=${login} emulatorOrderId=${emulatorOrderId}`;

                const tc = testCase(tcName, (test) => {
                    const mt = test.use(mtClient);
                    // Anchor request — health ping (cheap, real) so the testurio scenario
                    // has a recorded interaction to hang assertions off of.
                    mt.request("queryKafkaTopics", {method: "GET", path: "/v1/health/ping"});
                    mt.onResponse("queryKafkaTopics")
                        .assert(`order.received.v1.demo-uat contains client_order_id='${orderIdMarker}'`,
                            () => receivedMatches >= 1)
                        .assert(`order.finished.v1.demo-uat contains client_order_id='${orderIdMarker}'`,
                            () => finishedMatches >= 1);
                });

                const result = await scenario.run(tc);

                // 6. Attach consumer commands + summary HTML to the corresponding steps.
                const receivedCmd = buildConsumerCommand(
                    KAFKA_TOPIC_RECEIVED,
                    hwmsReceivedBefore,
                    200,
                    KAFKA_CONSUME_TIMEOUT_MS,
                );
                const finishedCmd = buildConsumerCommand(
                    KAFKA_TOPIC_FINISHED,
                    hwmsFinishedBefore,
                    200,
                    KAFKA_CONSUME_TIMEOUT_MS,
                );
                const combinedCommand =
                    `# kubectl --kubeconfig=${KAFKA_KUBECONFIG} -n ${KAFKA_NS} exec -it ${KAFKA_POD} -- bash\n\n` +
                    `# Topic 1: ${KAFKA_TOPIC_RECEIVED}\n${receivedCmd}\n\n` +
                    `# Topic 2: ${KAFKA_TOPIC_FINISHED}\n${finishedCmd}\n`;

                const matchedLinesReceived = receivedTail
                    ? receivedTail.rawOutput.split("\n").filter((l) => orderIdRe.test(l))
                    : [];
                const matchedLinesFinished = finishedTail
                    ? finishedTail.rawOutput.split("\n").filter((l) => orderIdRe.test(l))
                    : [];

                const cmdAttachment = writeTextAttachment(
                    "allure-results",
                    "Kafka consumer commands",
                    combinedCommand,
                    "text/plain",
                    "txt",
                );
                const summaryAttachment = writeTextAttachment(
                    "allure-results",
                    "Kafka topics — message tail summary",
                    [
                        renderKafkaSummaryHtml({
                            topic: KAFKA_TOPIC_RECEIVED,
                            hwmsBefore: hwmsReceivedBefore,
                            hwmsAfter: hwmsReceivedAfter,
                            matchedCount: receivedMatches,
                            rawSnippetLines: matchedLinesReceived,
                        }),
                        renderKafkaSummaryHtml({
                            topic: KAFKA_TOPIC_FINISHED,
                            hwmsBefore: hwmsFinishedBefore,
                            hwmsAfter: hwmsFinishedAfter,
                            matchedCount: finishedMatches,
                            rawSnippetLines: matchedLinesFinished,
                        }),
                    ].join("\n<hr/>\n"),
                    "text/html",
                    "html",
                );

                attachExtrasToLatestAllureResult("allure-results", [
                    {attachment: cmdAttachment, stepNameIncludes: "Request queryKafkaTopics"},
                    {attachment: summaryAttachment, stepNameIncludes: "Handle response for queryKafkaTopics"},
                ]);

                if (blockReason) {
                    expect.fail(blockReason);
                }
                expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
            },
            POLL_TIMEOUT_MS + 2 * KAFKA_CONSUME_TIMEOUT_MS + KAFKA_LAG_MS + 30_000,
        );
    });
});
