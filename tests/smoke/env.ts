/**
 * Smoke-test environment config. Connection targets and credentials live in a
 * git-ignored `.env` at the repo root (loaded via vitest.config.ts `test.env`);
 * `.env.example` documents the required keys. Nothing sensitive is committed.
 */

function required(name: string): string {
    const value = process.env[name];
    if (!value) {
        throw new Error(`Missing required env var ${name} — copy .env.example to .env and fill it in`);
    }
    return value;
}

// MT Test emulator (MT4/MT5).
export const MT_HOST = required("MT_HOST");

// ClickHouse (collector.order / collector.execution).
export const CH_URL = required("CH_URL");
export const CH_USER = required("CH_USER");
export const CH_PASS = required("CH_PASS");
