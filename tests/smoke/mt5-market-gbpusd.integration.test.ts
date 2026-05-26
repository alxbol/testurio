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

import {z} from "zod";
import {Client, HttpProtocol, testCase, TestScenario} from "testurio";
import {describe, expect, it} from "vitest";
import {GetV1PingResponse} from "./mt-api.schema";

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
        });

        const tc = testCase("ping", (test) => {
            const mt = test.use(mtClient);

            mt.request("ping", {method: "GET", path: "/v1/health/ping"});

            mt.onResponse("ping")
                .validate(PingResponseSchema)
                .assert("HTTP 200", (res) => res.code === 200)
                .assert("status === 'ok'", (res) => res.body.status === "ok");
        });

        const result = await scenario.run(tc);

        expect(result.passed, JSON.stringify(result, null, 2)).toBe(true);
    });
});
