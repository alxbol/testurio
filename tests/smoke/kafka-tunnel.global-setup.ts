/**
 * Vitest globalSetup for the Kafka broker tunnels (smoke Step 7).
 *
 * Runs ONCE per vitest invocation — before any test file is imported and
 * regardless of `fileParallelism`. This makes it the single owner of the
 * port-forwards, avoiding the spawn/teardown races that per-file `beforeAll`
 * hooks would cause when the MT4 and MT5 smoke suites run in parallel.
 *
 * `ensureKafkaTunnels()` is fully guarded: it no-ops when the brokers are
 * already up, when kubectl/kubeconfig are absent, or when the one-time hosts
 * entries are missing — so this is safe to keep registered for every run.
 * Set KAFKA_PF_DISABLE=1 to skip it entirely (e.g. unit-only local runs).
 */

import {ensureKafkaTunnels} from "./kafka-tunnel";

export default async function setup(): Promise<() => Promise<void>> {
    if (process.env.KAFKA_PF_DISABLE) {
        return async () => {
        };
    }
    return ensureKafkaTunnels();
}
