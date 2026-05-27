/**
 * TC-MT5-MARKET-BUY-GBPUSD-001 — smoke for MT5 → bridge → fill happy path.
 *
 * Currently implements Step 1 (Health check) only.
 * Response body is verified twice:
 *   1. against the Zod schema generated from MT.openapi.yaml (shape contract),
 *   2. by a semantic assertion (status === "ok").
 *
 * Known spec discrepancy (defect candidate):
 *   OpenAPI declares the path as `GET /v1/ping`, but the live MT5 emulator
 *   answers only on `GET /v1/health/ping` (`/v1/ping` → HTTP 404).
 *   We use the working live path while reusing the schema body shape from OpenAPI.
 */

import {readdirSync, readFileSync, statSync, writeFileSync} from "node:fs";
import {randomUUID} from "node:crypto";
import {join} from "node:path";
import {z} from "zod";
import {Client, HttpProtocol, type Interaction, testCase, TestScenario} from "testurio";
import {AllureReporter} from "@testurio/reporter-allure";
import {describe, expect, it} from "vitest";
import {GetV1PingResponse} from "./mt-api.schema";

/**
 * Post-run helper: appends recorded request/response payloads as Allure
 * attachments on the matching `request` / `onResponse` step in the latest
 * result JSON. Falls back to a test-case-level attachment for orphan
 * interactions whose step isn't found.
 *
 * `AllureReporter.includePayloads` is still inert in 0.6.2 (reads
 * `step.metadata` which the executor doesn't populate), so we surface
 * the recorder's interactions ourselves.
 */
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
        const requestStep = json.steps.find(
            (s) => s.name.includes("request") && s.name.includes(ix.messageType),
        );
        const responseStep = json.steps.find(
            (s) => s.name.includes("onResponse") && s.name.includes(ix.messageType),
        );

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

interface MtEmulatorApi {
    ping: {
        request: { method: "GET"; path: "/v1/health/ping" };
        response: { code: 200; body: { status?: string } };
    };
}

// Full-response Zod schema (testurio `.validate()` validates HttpResponse,
// not just body — see schema-validation.integration.test.ts §6.5).
const PingResponseSchema = z
    .object({
        code: z.literal(200),
        body: GetV1PingResponse,
    })
    .passthrough();

// ---------------------------------------------------------------------------
// Step 1 — Health check
// ---------------------------------------------------------------------------

describe("MT5 | Market order BUY | GBPUSD 0.01 | login 123461", () => {
    it("Step 1 — health check: GET /v1/health/ping → 200 {status:'ok'} (schema-validated)", async () => {
        const mtClient = new Client("mt5-emulator", {
            protocol: new HttpProtocol<MtEmulatorApi>(),
            targetAddress: {host: "192.168.8.46", port: 5001},
        });

        const scenario = new TestScenario({
            name: "TC-MT5-MARKET-BUY-GBPUSD-001",
            components: [mtClient],
            recording: true,
        });

        scenario.addReporter(
            new AllureReporter({
                resultsDir: "allure-results",
                environmentInfo: {
                    env: "demo-uat / test-stable",
                    target: "192.168.8.46:5001 (MT Test emulator)",
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

        attachInteractionsToLatestAllureResult("allure-results", result.interactions ?? []);

        expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
        expect(result.interactions?.length, "expected one recorded interaction").toBe(1);
        const ix = result.interactions![0];
        expect(ix.serviceName).toBe("mt5-emulator");
        expect(ix.messageType).toBe("ping");
        expect(ix.status).toBe("completed");
        expect(ix.requestPayload).toMatchObject({method: "GET", path: "/v1/health/ping"});
        expect((ix.responsePayload as { code: number }).code).toBe(200);
    });
});
