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
 *
 * Known spec discrepancy (defect candidate):
 *   OpenAPI declares the path as `GET /v1/ping`, but the live MT5 emulator
 *   answers only on `GET /v1/health/ping` (`/v1/ping` → HTTP 404).
 */

import {readdirSync, readFileSync, statSync, writeFileSync} from "node:fs";
import {randomUUID} from "node:crypto";
import {join} from "node:path";
import {z} from "zod";
import {Client, HttpProtocol, type Interaction, testCase, TestScenario} from "testurio";
import {AllureReporter} from "@testurio/reporter-allure";
import {beforeAll, describe, expect, it} from "vitest";
import {GetV1PingResponse, PostV1OrdersResponse} from "./mt-api.schema";

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

const CH_BASE = "http://clickhouse.test-stable.cbrid.ge:8123";
const CH_AUTH = "Basic " + Buffer.from("admin:admin").toString("base64");
const CH_POLL_TIMEOUT_MS = 30_000;
const CH_POLL_INTERVAL_MS = 1_000;

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
    });
});
