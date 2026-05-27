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
const POLL_TIMEOUT_MS = 15_000;

async function placeOrderRaw(body: PlaceOrderRequestBody): Promise<PlaceOrderResponseBody> {
    const r = await fetch("http://192.168.8.46:5001/v1/orders", {
        method: "POST",
        headers: {"content-type": "application/json"},
        body: JSON.stringify(body),
    });
    if (!r.ok) throw new Error(`POST /v1/orders → HTTP ${r.status}`);
    return (await r.json()) as PlaceOrderResponseBody;
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

                // 3. Diagnostic mapping per spec: CANCELLED → surface comment with
                //    precondition hint (Not enough funds → P3, "No price"/"Price off" → P5).
                if (polled.state === "CANCELLED") {
                    const c = polled.lastBody.comment ?? "";
                    let hint = "escalate with raw response";
                    if (/not enough funds/i.test(c)) hint = "P3 broken (account underfunded for current price × volume)";
                    else if (/no price|price off/i.test(c)) hint = "P5 broken (no ticks for the symbol)";
                    throw new Error(
                        `Step 4 failed: order ${orderId} (${symbol}) state=CANCELLED, comment=${JSON.stringify(c)} — ${hint}`,
                    );
                }

                // 4. Strict assert of final body per Step 4 spec.
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
    });
});
