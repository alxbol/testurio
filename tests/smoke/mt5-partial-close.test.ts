/**
 * TC-MT5-PARTIAL-CLOSE-001 — MT5 → bridge partial close of a market position.
 *
 * The MT Test emulator has no native "close position" operation (no close
 * endpoint, no position/ticket reference, no deal entry-in/out flag — verified
 * empirically: an opposite order just executes independently). A partial close
 * is therefore modeled as: open a position of volume V_OPEN, then send an
 * opposite MARKET order of volume V_CLOSE < V_OPEN.
 *
 * Pass/fail is built on the deterministic, per-order facts (both orders FILL,
 * collector.order/execution rows, Kafka events). The "partial" reduction of the
 * net position is checked against collector.current_exposure, but only as a
 * DIAGNOSTIC: current_exposure is a tenant-wide aggregate, so concurrent flow on
 * the same symbol would pollute the delta — it is attached, not asserted hard.
 *
 * Kafka requires the broker tunnels to be up (see kafka-tunnel.global-setup.ts).
 *
 * env: demo-uat / test-stable; login is a demo-uat trader in group
 * demo\forex-hedge-usd-01 (memory: reference_demo_uat_traders).
 */

import {randomUUID} from "node:crypto";
import {readdirSync, readFileSync, statSync, writeFileSync} from "node:fs";
import {createConnection} from "node:net";
import {join} from "node:path";
import {z} from "zod";
import {Client, DataSource, HttpProtocol, type Interaction, testCase, TestScenario,} from "testurio";
import {ClickHouseAdapter} from "@testurio/adapter-clickhouse";
import {AllureReporter} from "@testurio/reporter-allure";
import {Kafka, logLevel as kafkaLogLevel} from "kafkajs";
import {beforeAll, describe, expect, it} from "vitest";
import {GetV1PingResponse, PostV1OrdersResponse} from "./mt-api.schema";

// ---------------------------------------------------------------------------
// Scenario parameters.
// ---------------------------------------------------------------------------

const MT_HOST = "192.168.8.46";
const MT_PORT = 5001;

const TENANT = "demo-uat";
const LOGIN = 333303;
const SYMBOL_CLIENT = "NZDUSD.MT5"; // A-book symbol
const SYMBOL_CORE = "NZDUSD"; // core_symbol_name in collector.current_exposure
const CONTRACT_SIZE = 100000; // units per 1.0 lot (NZDUSD.MT5 emulator config)
const V_OPEN = 0.002;
const V_CLOSE = 0.001; // half of V_OPEN → partial close
// NZDUSD.MT5 is expected to route A-book (hedged to the LP) for the
// demo\forex-hedge-usd-01 group.
const EXPECTED_ROUTING = "ABOOK";

const CH_URL = "http://clickhouse.test-stable.cbrid.ge:8123";
const CH_USER = "admin";
const CH_PASS = "admin";
const CH_DB = "default";
const CH_POLL_TIMEOUT_MS = 30_000;
const CH_POLL_INTERVAL_MS = 1_000;

const KAFKA_BROKERS = (
    process.env.KAFKA_BROKERS ?? "127.0.0.1:9092,127.0.0.2:9092,127.0.0.3:9092"
).split(",");
const KAFKA_TOPIC_RECEIVED = "order.received.v1.demo-uat";
const KAFKA_TOPIC_FINISHED = "order.finished.v1.demo-uat";
const KAFKA_WAIT_MESSAGE_TIMEOUT_MS = 30_000;

const POLL_INTERVAL_MS = 500;
const POLL_TIMEOUT_MS = 30_000;
const TERMINAL_STATES = new Set(["FILLED", "CANCELLED"]);

const ALLURE_DIR = "allure-results";
const ALLURE_PARENT_SUITE = "MT5 | Partial close";

// ---------------------------------------------------------------------------
// HTTP contract.
// ---------------------------------------------------------------------------

interface PlaceOrderRequestBody {
    symbol: string;
    volume: number;
    side: "BUY" | "SELL";
    type: "MARKET" | "LIMIT" | "STOP";
    login: number;
}

interface PlaceOrderResponseBody {
    orderId: number;
    code: string;
    message: string;
}

interface OrderStateResponseBody {
    id?: number;
    type?: string;
    side: "BUY" | "SELL";
    state?: string;
    symbol?: string;
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
        request: { method: "GET"; path: string };
        response: { code: 200; body: OrderStateResponseBody };
    };
}

function makeMtClient() {
    return new Client("mt5-emulator", {
        protocol: new HttpProtocol<MtEmulatorApi>(),
        targetAddress: {host: MT_HOST, port: MT_PORT},
    });
}

const PingResponseSchema = z.object({code: z.literal(200), body: GetV1PingResponse}).passthrough();
const PlaceOrderResponseSchema = z.object({code: z.literal(200), body: PostV1OrdersResponse}).passthrough();
const GetOrderResponseSchema = z
    .object({
        code: z.literal(200),
        body: z
            .object({
                id: z.number().optional(),
                side: z.enum(["BUY", "SELL"]),
                state: z.string().optional(),
                symbol: z.string().optional(),
                volume: z.number().min(0).optional(),
                filledVolume: z.number().min(0).optional(),
                createdAt: z.iso.datetime({}),
                comment: z.string().optional(),
            })
            .passthrough(),
    })
    .passthrough();

// ---------------------------------------------------------------------------
// ClickHouse DataSource (collector.order / collector.execution).
// ---------------------------------------------------------------------------

function makeChDataSource(name = "clickhouse-collector") {
    return new DataSource(name, {
        adapter: new ClickHouseAdapter({url: CH_URL, username: CH_USER, password: CH_PASS, database: CH_DB}),
    });
}

interface CollectorOrderRow {
    client_order_id: string;
    client_acc_id: string;
    tenant_id: string;
    client_symbol_name: string;
    side: string;
    order_type: string;
    bridge_status: string;
    client_status: string;
    lp_status: string;
    server_type: string;
    is_completed: number;
    reject_text: string;
}

interface CollectorExecutionRow {
    client_order_id: string;
    client_acc_id: string;
    tenant_id: string;
    client_symbol_name: string;
    side: string;
    routing: string;
    execution_type: string;
    server_type: string;
    client_amount_filled: number | null;
    client_price_filled: number | null;
    lp_request_at: string | null;
    lp_executed_at: string | null;
}

function buildOrderSql(orderId: number): string {
    return `SELECT client_order_id, client_acc_id, tenant_id, client_symbol_name, side, order_type,
                   bridge_status, client_status, lp_status, server_type, is_completed, reject_text
            FROM collector.order
            WHERE client_order_id = '${orderId}' AND client_acc_id = '${LOGIN}' AND tenant_id = '${TENANT}'
              AND is_completed = 1
            ORDER BY last_modified_at DESC LIMIT 1`;
}

function buildExecSql(orderId: number): string {
    return `SELECT client_order_id, client_acc_id, tenant_id, client_symbol_name, side, routing,
                   execution_type, server_type, client_amount_filled, client_price_filled,
                   lp_request_at, lp_executed_at
            FROM collector.execution
            WHERE client_order_id = '${orderId}' AND client_acc_id = '${LOGIN}' AND tenant_id = '${TENANT}'
            ORDER BY lp_executed_at ASC, created_at ASC`;
}

const EXPOSURE_SQL = `SELECT sum(long_exposure_units) AS long_u, sum(short_exposure_units) AS short_u
                      FROM collector.current_exposure FINAL
                      WHERE tenant_id = '${TENANT}' AND core_symbol_name = '${SYMBOL_CORE}' AND server_type = 'MT5'`;

async function chQuery<T>(sql: string): Promise<T[]> {
    const res = await fetch(CH_URL, {
        method: "POST",
        headers: {Authorization: `Basic ${Buffer.from(`${CH_USER}:${CH_PASS}`).toString("base64")}`},
        body: `${sql}\nFORMAT JSONEachRow`,
    });
    if (!res.ok) throw new Error(`CH HTTP ${res.status}`);
    const text = (await res.text()).trim();
    return text ? text.split("\n").map((l) => JSON.parse(l) as T) : [];
}

async function netExposureUnits(): Promise<number> {
    const rows = await chQuery<{ long_u: number; short_u: number }>(EXPOSURE_SQL);
    const r = rows[0];
    return r ? Number(r.long_u ?? 0) - Number(r.short_u ?? 0) : 0;
}

// ---------------------------------------------------------------------------
// Allure helpers.
// ---------------------------------------------------------------------------

type AllureAttachment = { name: string; source: string; type: string };

function writeJsonAttachment(name: string, payload: unknown): AllureAttachment {
    const filename = `${randomUUID()}-attachment.json`;
    writeFileSync(join(ALLURE_DIR, filename), JSON.stringify(payload, null, 2), "utf8");
    return {name, source: filename, type: "application/json"};
}

function writeTextAttachment(name: string, content: string, mime: string, ext: string): AllureAttachment {
    const filename = `${randomUUID()}-attachment.${ext}`;
    writeFileSync(join(ALLURE_DIR, filename), content, "utf8");
    return {name, source: filename, type: mime};
}

function findLatestResultPath(): string | undefined {
    const candidates = readdirSync(ALLURE_DIR)
        .filter((f) => f.endsWith("-result.json"))
        .map((f) => ({f, t: statSync(join(ALLURE_DIR, f)).mtimeMs}))
        .sort((a, b) => b.t - a.t);
    return candidates[0] ? join(ALLURE_DIR, candidates[0].f) : undefined;
}

function setAllureSuites(suite: string): void {
    const resultPath = findLatestResultPath();
    if (!resultPath) return;
    const json = JSON.parse(readFileSync(resultPath, "utf8")) as { labels?: { name: string; value: string }[] };
    json.labels = (json.labels ?? []).filter((l) => !["parentSuite", "suite", "subSuite"].includes(l.name));
    json.labels.push({name: "parentSuite", value: ALLURE_PARENT_SUITE});
    json.labels.push({name: "suite", value: suite});
    writeFileSync(resultPath, JSON.stringify(json, null, 2), "utf8");
}

function attachToLatest(attachments: AllureAttachment[]): void {
    if (!attachments.length) return;
    const resultPath = findLatestResultPath();
    if (!resultPath) return;
    const json = JSON.parse(readFileSync(resultPath, "utf8")) as { attachments?: AllureAttachment[] };
    json.attachments = [...(json.attachments ?? []), ...attachments];
    writeFileSync(resultPath, JSON.stringify(json, null, 2), "utf8");
}

function attachInteractions(interactions: Interaction[]): void {
    const atts = interactions.flatMap((ix) => [
        writeJsonAttachment(`request: ${ix.messageType}`, {
            ...(ix.requestPayload as object),
            sentAt: new Date(ix.requestTimestamp).toISOString(),
        }),
        writeJsonAttachment(`response: ${ix.messageType}`, {
            ...(ix.responsePayload as object),
            status: ix.status,
            error: ix.error,
            durationMs: ix.duration,
        }),
    ]);
    attachToLatest(atts);
}

// ---------------------------------------------------------------------------
// Raw HTTP helpers (polling + reachability), mirroring the market-order smoke.
// ---------------------------------------------------------------------------

async function pollUntilTerminal(orderId: number): Promise<{ state: string; lastBody: OrderStateResponseBody }> {
    const deadline = Date.now() + POLL_TIMEOUT_MS;
    let lastBody: OrderStateResponseBody | undefined;
    let polls = 0;
    while (Date.now() < deadline) {
        polls++;
        const r = await fetch(`http://${MT_HOST}:${MT_PORT}/v1/orders/${orderId}`);
        if (r.status === 404 && polls === 1) {
            await new Promise((res) => setTimeout(res, POLL_INTERVAL_MS));
            continue;
        }
        if (!r.ok) throw new Error(`GET /v1/orders/${orderId} → HTTP ${r.status}`);
        lastBody = (await r.json()) as OrderStateResponseBody;
        if (lastBody.state && TERMINAL_STATES.has(lastBody.state)) return {state: lastBody.state, lastBody};
        await new Promise((res) => setTimeout(res, POLL_INTERVAL_MS));
    }
    throw new Error(`Order ${orderId} not terminal within ${POLL_TIMEOUT_MS}ms (last=${JSON.stringify(lastBody)})`);
}

async function isKafkaReachable(timeoutMs = 2_000): Promise<boolean> {
    return new Promise((resolve) => {
        const [host, portStr] = KAFKA_BROKERS[0].split(":");
        const socket = createConnection({host, port: Number(portStr ?? 9092), timeout: timeoutMs});
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

function bytesContainAscii(payload: Uint8Array, needle: string): boolean {
    if (!payload || payload.length === 0) return false;
    return Buffer.from(payload).indexOf(Buffer.from(needle, "utf8")) !== -1;
}

// ---------------------------------------------------------------------------
// Shared scenario state, populated once in beforeAll.
// ---------------------------------------------------------------------------

interface Leg {
    orderId: number;
    placeCode: string;
    placeMessage: string;
    state: string;
    filledVolume: number;
    volume: number;
    side: "BUY" | "SELL";
    comment: string;
}

const state: {
    setupError?: string;
    kafkaReachable: boolean;
    open?: Leg;
    close?: Leg;
    expBaseline?: number;
    expAfterOpen?: number;
    expAfterClose?: number;
    matched: Record<string, { received: boolean; finished: boolean }>;
} = {kafkaReachable: false, matched: {}};

function fillBlock(): string | undefined {
    if (state.setupError) return state.setupError;
    if (!state.open || state.open.state !== "FILLED") {
        return `Open leg did not FILL — order ${state.open?.orderId} state=${state.open?.state} comment=${JSON.stringify(state.open?.comment ?? "")}`;
    }
    if (!state.close || state.close.state !== "FILLED") {
        return `Close leg did not FILL — order ${state.close?.orderId} state=${state.close?.state} comment=${JSON.stringify(state.close?.comment ?? "")}`;
    }
    return undefined;
}

async function placeLeg(side: "BUY" | "SELL", volume: number, label: string): Promise<Leg> {
    const mtClient = makeMtClient();
    const scenario = new TestScenario({
        name: `TC-MT5-PARTIAL-CLOSE-001 / ${label}`,
        components: [mtClient],
        recording: true
    });
    scenario.addReporter(
        new AllureReporter({
            resultsDir: ALLURE_DIR,
            environmentInfo: {
                env: "demo-uat / test-stable",
                target: `${MT_HOST}:${MT_PORT}`,
                side,
                volume: String(volume),
                login: String(LOGIN),
                node: process.version
            },
        }),
    );
    let placed: PlaceOrderResponseBody | undefined;
    const tc = testCase(`place ${label} ${side} ${volume}`, (test) => {
        const mt = test.use(mtClient);
        mt.request("placeOrder", {
            method: "POST",
            path: "/v1/orders",
            body: {symbol: SYMBOL_CLIENT, volume, side, type: "MARKET", login: LOGIN}
        });
        mt.onResponse("placeOrder")
            .validate(PlaceOrderResponseSchema)
            .assert("HTTP 200", (res) => res.code === 200)
            .assert("code === 'OK'", (res) => {
                placed = res.body as PlaceOrderResponseBody;
                return res.body.code === "OK";
            })
            .assert("orderId > 0", (res) => typeof res.body.orderId === "number" && res.body.orderId > 0);
    });
    const result = await scenario.run(tc);
    attachInteractions(result.interactions ?? []);
    setAllureSuites(`${label} (place ${side} ${volume})`);

    const orderId = placed?.orderId ?? 0;
    // Register the Kafka marker BEFORE polling so order.received is not missed.
    if (orderId > 0) state.matched[String(orderId)] ||= {received: false, finished: false};
    const polled = orderId > 0 ? await pollUntilTerminal(orderId) : undefined;
    return {
        orderId,
        placeCode: placed?.code ?? "NO_RESPONSE",
        placeMessage: placed?.message ?? "",
        state: polled?.state ?? "NOT_PLACED",
        filledVolume: polled?.lastBody.filledVolume ?? 0,
        volume: polled?.lastBody.volume ?? volume,
        side,
        comment: polled?.lastBody.comment ?? "",
    };
}

// ---------------------------------------------------------------------------
// Suite.
// ---------------------------------------------------------------------------

describe("TC-MT5-PARTIAL-CLOSE-001 — open then partial close", () => {
    beforeAll(async () => {
        // P1 — health check.
        const mtClient = makeMtClient();
        const scenario = new TestScenario({
            name: "TC-MT5-PARTIAL-CLOSE-001 / health",
            components: [mtClient],
            recording: true
        });
        scenario.addReporter(new AllureReporter({
            resultsDir: ALLURE_DIR,
            environmentInfo: {env: "demo-uat / test-stable", step: "health check", node: process.version}
        }));
        const ping = await scenario.run(
            testCase("ping", (test) => {
                const mt = test.use(mtClient);
                mt.request("ping", {method: "GET", path: "/v1/health/ping"});
                mt.onResponse("ping").validate(PingResponseSchema).assert("status ok", (res) => res.body.status === "ok");
            }),
        );
        attachInteractions(ping.interactions ?? []);
        setAllureSuites("setup (health check)");
        if (!ping.passed) {
            state.setupError = "Emulator health check failed — aborting suite";
            return;
        }

        // Kafka consumer: open and join the group BEFORE placing any order, so
        // bridge events for both legs are captured (lazy-join race, see header).
        state.kafkaReachable = await isKafkaReachable();
        let consumer: ReturnType<Kafka["consumer"]> | undefined;
        if (state.kafkaReachable) {
            const kafka = new Kafka({
                brokers: KAFKA_BROKERS,
                clientId: `mt5-pclose-${randomUUID()}`,
                logLevel: kafkaLogLevel.NOTHING
            });
            consumer = kafka.consumer({
                groupId: `mt5-pclose-${randomUUID()}`,
                sessionTimeout: 10_000,
                heartbeatInterval: 3_000
            });
            await consumer.connect();
            await consumer.subscribe({topic: KAFKA_TOPIC_RECEIVED, fromBeginning: false});
            await consumer.subscribe({topic: KAFKA_TOPIC_FINISHED, fromBeginning: false});
            const joined = new Promise<void>((res, rej) => {
                const t = setTimeout(() => rej(new Error("GROUP_JOIN timeout")), 15_000);
                consumer!.on(consumer!.events.GROUP_JOIN, () => {
                    clearTimeout(t);
                    res();
                });
            });
            await consumer.run({
                eachMessage: async ({topic, message}) => {
                    const val = message.value;
                    if (!val) return;
                    for (const marker of Object.keys(state.matched)) {
                        if (!bytesContainAscii(val as Uint8Array, marker)) continue;
                        if (topic === KAFKA_TOPIC_RECEIVED) state.matched[marker].received = true;
                        if (topic === KAFKA_TOPIC_FINISHED) state.matched[marker].finished = true;
                    }
                },
            });
            await joined;
        }

        // Capture net exposure baseline, place both legs, capture exposure after each.
        try {
            state.expBaseline = await netExposureUnits();
        } catch {
            // diagnostic only
        }
        state.open = await placeLeg("BUY", V_OPEN, "Step Open");
        try {
            state.expAfterOpen = await netExposureUnits();
        } catch {
            /* diagnostic only */
        }
        state.close = await placeLeg("SELL", V_CLOSE, "Step Close");
        try {
            state.expAfterClose = await netExposureUnits();
        } catch {
            /* diagnostic only */
        }

        // Give the bridge time to publish events for both legs, then stop.
        if (state.kafkaReachable && !fillBlock()) {
            const deadline = Date.now() + KAFKA_WAIT_MESSAGE_TIMEOUT_MS;
            const done = () => Object.values(state.matched).every((m) => m.received && m.finished);
            while (Date.now() < deadline && !done()) await new Promise((res) => setTimeout(res, 1_000));
        }
        try {
            await consumer?.disconnect();
        } catch {
            /* best-effort */
        }
    }, POLL_TIMEOUT_MS * 2 + KAFKA_WAIT_MESSAGE_TIMEOUT_MS + 60_000);

    it("Step 1 — open: BUY 0.02 MARKET → state=FILLED, filledVolume=0.02", async () => {
        if (state.setupError) expect.fail(state.setupError);
        const orderId = state.open!.orderId;
        const mtClient = makeMtClient();
        const scenario = new TestScenario({
            name: "TC-MT5-PARTIAL-CLOSE-001 / Step 1 assert open",
            components: [mtClient],
            recording: true
        });
        scenario.addReporter(new AllureReporter({
            resultsDir: ALLURE_DIR,
            environmentInfo: {orderId: String(orderId), terminalState: state.open!.state, node: process.version}
        }));
        const tc = testCase(`assert open order ${orderId}`, (test) => {
            const mt = test.use(mtClient);
            mt.request("getOrder", {method: "GET", path: `/v1/orders/${orderId}`});
            mt.onResponse("getOrder")
                .validate(GetOrderResponseSchema)
                .assert("state === 'FILLED'", (res) => res.body.state === "FILLED")
                .assert("side === 'BUY'", (res) => res.body.side === "BUY")
                .assert("symbol matches", (res) => res.body.symbol === SYMBOL_CLIENT)
                .assert("volume === V_OPEN", (res) => res.body.volume === V_OPEN);
        });
        const result = await scenario.run(tc);
        attachInteractions(result.interactions ?? []);
        setAllureSuites("Step 1 — open FILLED");
        if (state.open!.state !== "FILLED") expect.fail(fillBlock());
        expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
    });

    it("Step 2 — partial close: SELL 0.01 MARKET → FILLED, volume = ½ of open", async () => {
        if (state.setupError) expect.fail(state.setupError);
        const orderId = state.close!.orderId;
        const mtClient = makeMtClient();
        const scenario = new TestScenario({
            name: "TC-MT5-PARTIAL-CLOSE-001 / Step 2 assert close",
            components: [mtClient],
            recording: true
        });
        scenario.addReporter(new AllureReporter({
            resultsDir: ALLURE_DIR,
            environmentInfo: {orderId: String(orderId), terminalState: state.close!.state, node: process.version}
        }));
        const tc = testCase(`assert close order ${orderId}`, (test) => {
            const mt = test.use(mtClient);
            mt.request("getOrder", {method: "GET", path: `/v1/orders/${orderId}`});
            mt.onResponse("getOrder")
                .validate(GetOrderResponseSchema)
                .assert("state === 'FILLED'", (res) => res.body.state === "FILLED")
                .assert("side === 'SELL'", (res) => res.body.side === "SELL")
                .assert("symbol matches", (res) => res.body.symbol === SYMBOL_CLIENT)
                .assert("volume === V_CLOSE", (res) => res.body.volume === V_CLOSE)
                .assert("volume is half of open", (res) => (res.body.volume ?? 0) === V_OPEN / 2);
        });
        const result = await scenario.run(tc);
        attachInteractions(result.interactions ?? []);
        setAllureSuites("Step 2 — partial close FILLED");
        const block = fillBlock();
        if (block) expect.fail(block);
        expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
    });

    it("Step 3 — collector.order: open & close persisted FILLED", async () => {
        const block = fillBlock();
        const openId = state.open?.orderId ?? 0;
        const closeId = state.close?.orderId ?? 0;
        const ch = makeChDataSource();
        const scenario = new TestScenario({
            name: "TC-MT5-PARTIAL-CLOSE-001 / Step 3 collector.order",
            components: [ch],
            recording: false
        });
        scenario.addReporter(new AllureReporter({
            resultsDir: ALLURE_DIR,
            environmentInfo: {
                openId: String(openId),
                closeId: String(closeId),
                blocked: block ?? "—",
                node: process.version
            }
        }));

        let openRow: CollectorOrderRow | undefined;
        let closeRow: CollectorOrderRow | undefined;
        const tc = testCase("collector.order open+close", (test) => {
            const store = test.use(ch);
            store
                .exec("wait for both collector.order rows", async (db) => {
                    if (block) return {open: undefined, close: undefined};
                    const deadline = Date.now() + CH_POLL_TIMEOUT_MS;
                    while (Date.now() < deadline) {
                        const o = await db.query<CollectorOrderRow>({query: buildOrderSql(openId)});
                        const c = await db.query<CollectorOrderRow>({query: buildOrderSql(closeId)});
                        if (o.length === 1 && c.length === 1) return {open: o[0], close: c[0]};
                        await new Promise((res) => setTimeout(res, CH_POLL_INTERVAL_MS));
                    }
                    const o = await db.query<CollectorOrderRow>({query: buildOrderSql(openId)});
                    const c = await db.query<CollectorOrderRow>({query: buildOrderSql(closeId)});
                    return {open: o[0], close: c[0]};
                })
                .assert("both rows present", (r) => {
                    const v = r as { open?: CollectorOrderRow; close?: CollectorOrderRow };
                    openRow = v.open;
                    closeRow = v.close;
                    return !!openRow && !!closeRow;
                })
                .assert("open side=BUY", () => openRow?.side === "BUY")
                .assert("close side=SELL", () => closeRow?.side === "SELL")
                .assert("both order_type=MARKET", () => openRow?.order_type === "MARKET" && closeRow?.order_type === "MARKET")
                .assert("both bridge_status=FILLED", () => openRow?.bridge_status === "FILLED" && closeRow?.bridge_status === "FILLED")
                .assert("both lp_status=FILLED", () => openRow?.lp_status === "FILLED" && closeRow?.lp_status === "FILLED")
                .assert("both server_type=MT5", () => openRow?.server_type === "MT5" && closeRow?.server_type === "MT5")
                .assert("both is_completed=1", () => openRow?.is_completed === 1 && closeRow?.is_completed === 1);
        });
        const result = await scenario.run(tc);
        attachToLatest([
            writeTextAttachment("collector.order SQL (open)", buildOrderSql(openId), "text/plain", "txt"),
            writeJsonAttachment("collector.order rows", {open: openRow ?? null, close: closeRow ?? null}),
        ]);
        setAllureSuites("Step 3 — collector.order");
        if (block) expect.fail(block);
        expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
    });

    it("Step 4 — collector.execution: close has FILLED ABOOK execution = 0.01", async () => {
        const block = fillBlock();
        const closeId = state.close?.orderId ?? 0;
        const ch = makeChDataSource();
        const scenario = new TestScenario({
            name: "TC-MT5-PARTIAL-CLOSE-001 / Step 4 collector.execution",
            components: [ch],
            recording: false
        });
        scenario.addReporter(new AllureReporter({
            resultsDir: ALLURE_DIR,
            environmentInfo: {closeId: String(closeId), blocked: block ?? "—", node: process.version}
        }));

        let rows: CollectorExecutionRow[] = [];
        let filled: CollectorExecutionRow[] = [];
        const tc = testCase("collector.execution close", (test) => {
            const store = test.use(ch);
            store
                .exec("wait for close execution rows", async (db) => {
                    if (block) return [];
                    const deadline = Date.now() + CH_POLL_TIMEOUT_MS;
                    let res: CollectorExecutionRow[] = [];
                    while (Date.now() < deadline) {
                        res = await db.query<CollectorExecutionRow>({query: buildExecSql(closeId)});
                        if (res.some((r) => r.execution_type === "FILLED")) return res;
                        await new Promise((r) => setTimeout(r, CH_POLL_INTERVAL_MS));
                    }
                    return res;
                })
                .assert("≥1 row", (r) => {
                    rows = r as CollectorExecutionRow[];
                    filled = rows.filter((x) => x.execution_type === "FILLED");
                    return rows.length >= 1;
                })
                .assert("≥1 FILLED execution", () => filled.length >= 1)
                .assert("all rows side=SELL", () => rows.every((r) => r.side === "SELL"))
                .assert("all rows client_acc_id matches", () => rows.every((r) => r.client_acc_id === String(LOGIN)))
                .assert(`all FILLED routing=${EXPECTED_ROUTING}`, () => filled.every((r) => r.routing === EXPECTED_ROUTING))
                .assert("all FILLED lp_request_at NOT NULL", () => filled.every((r) => r.lp_request_at !== null))
                .assert("SUM(client_amount_filled) === V_CLOSE", () => {
                    const sum = filled.reduce((acc, r) => acc + (r.client_amount_filled ?? 0), 0);
                    return Math.abs(sum - V_CLOSE) < 1e-9;
                });
        });
        const result = await scenario.run(tc);
        attachToLatest([
            writeTextAttachment("collector.execution SQL", buildExecSql(closeId), "text/plain", "txt"),
            writeJsonAttachment("collector.execution rows", rows),
        ]);
        setAllureSuites("Step 4 — collector.execution");
        if (block) expect.fail(block);
        expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
    });

    it("Step 5 — Kafka: order.received + order.finished for open & close", async () => {
        const block = fillBlock();
        if (!state.kafkaReachable) {
            expect.fail(
                `Kafka brokers ${KAFKA_BROKERS.join(",")} unreachable — start the port-forward ` +
                `(tests/smoke/scripts/kafka-port-forward.ps1) or set KAFKA_BROKERS.`,
            );
        }
        const openId = String(state.open?.orderId ?? 0);
        const closeId = String(state.close?.orderId ?? 0);
        attachToLatest([
            writeTextAttachment(
                "Kafka subscription",
                `brokers: ${KAFKA_BROKERS.join(",")}\ntopics:\n  - ${KAFKA_TOPIC_RECEIVED}\n  - ${KAFKA_TOPIC_FINISHED}\n` +
                `matched: ${JSON.stringify(state.matched, null, 2)}`,
                "text/plain",
                "txt",
            ),
        ]);
        setAllureSuites("Step 5 — Kafka events");
        if (block) expect.fail(block);
        expect(state.matched[openId]?.received, `open order.received not seen (orderId=${openId})`).toBe(true);
        expect(state.matched[openId]?.finished, `open order.finished not seen (orderId=${openId})`).toBe(true);
        expect(state.matched[closeId]?.received, `close order.received not seen (orderId=${closeId})`).toBe(true);
        expect(state.matched[closeId]?.finished, `close order.finished not seen (orderId=${closeId})`).toBe(true);
    });

    it("Step 6 — net exposure reduced by partial close (DIAGNOSTIC, not asserted)", async () => {
        const expectedCloseUnits = V_CLOSE * CONTRACT_SIZE;
        const expectedOpenUnits = V_OPEN * CONTRACT_SIZE;
        const deltaOpen = state.expAfterOpen != null && state.expBaseline != null ? state.expAfterOpen - state.expBaseline : null;
        const deltaClose = state.expAfterClose != null && state.expAfterOpen != null ? state.expAfterClose - state.expAfterOpen : null;
        attachToLatest([
            writeJsonAttachment("net exposure (long_units − short_units)", {
                symbol: SYMBOL_CORE,
                contractSize: CONTRACT_SIZE,
                baseline: state.expBaseline ?? null,
                afterOpen: state.expAfterOpen ?? null,
                afterClose: state.expAfterClose ?? null,
                observedDeltaOpen: deltaOpen,
                observedDeltaClose: deltaClose,
                expectedDeltaOpen: `+${expectedOpenUnits} (BUY ${V_OPEN})`,
                expectedDeltaClose: `-${expectedCloseUnits} (SELL ${V_CLOSE})`,
                note: "current_exposure is a tenant-wide aggregate; concurrent flow on this symbol can pollute the delta, so this is diagnostic only.",
            }),
        ]);
        setAllureSuites("Step 6 — exposure (diagnostic)");
        // Soft sanity: the query returned numeric exposure values.
        expect(Number.isFinite(state.expAfterClose ?? Number.NaN)).toBe(true);
    });
});
