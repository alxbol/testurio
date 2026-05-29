/**
 * Embedded Kafka port-forward for the smoke suites (Step 7).
 *
 * test-stable Kafka has 3 brokers (kafka-controller-0/1/2). After the metadata
 * exchange kafkajs follows the advertised hostname of EVERY broker, so a single
 * forward is not enough — we spawn one `kubectl port-forward` per broker, each
 * bound to its own 127.0.0.X:9092, and rely on a one-time hosts mapping so the
 * advertised FQDNs resolve to those loopback addresses.
 *
 * `ensureKafkaTunnels()` is idempotent and safe to call from a test `beforeAll`:
 *   - if the brokers are already reachable (standalone script running) → no-op;
 *   - if kubectl/kubeconfig are missing → warns and no-ops (Step 7 blocks
 *     gracefully via its own reachability check);
 *   - if the hosts entries are missing → prints the exact lines to add once and
 *     no-ops (spawning would be pointless — kafkajs could not resolve brokers).
 *
 * The one-time hosts step (requires admin) is intentionally NOT automated to
 * avoid an unexpected UAC prompt on every IDE run. Run once, in admin PowerShell:
 *
 *   Add-Content -Path $env:windir\System32\drivers\etc\hosts -Value @"
 *   127.0.0.1 kafka-controller-0.kafka-controller-headless.test-stable.svc.cluster.local
 *   127.0.0.2 kafka-controller-1.kafka-controller-headless.test-stable.svc.cluster.local
 *   127.0.0.3 kafka-controller-2.kafka-controller-headless.test-stable.svc.cluster.local
 *   "@
 */

import {type ChildProcess, execFileSync, spawn} from "node:child_process";
import {existsSync, readFileSync} from "node:fs";
import {createConnection} from "node:net";
import {join} from "node:path";

export interface KafkaTunnelConfig {
    namespace: string;
    kubeconfig: string;
    kubectl: string;
    port: number;
    brokers: { pod: string; address: string }[];
}

const DEFAULTS: KafkaTunnelConfig = {
    namespace: process.env.KAFKA_PF_NAMESPACE ?? "test-stable",
    kubeconfig: process.env.KAFKA_PF_KUBECONFIG ?? "C:/Users/abolshakov/Documents/k8s/kubeconfig",
    kubectl:
        process.env.KAFKA_PF_KUBECTL ??
        (process.platform === "win32" ? "C:/Users/abolshakov/bin/kubectl.exe" : "kubectl"),
    port: 9092,
    brokers: [
        {pod: "kafka-controller-0", address: "127.0.0.1"},
        {pod: "kafka-controller-1", address: "127.0.0.2"},
        {pod: "kafka-controller-2", address: "127.0.0.3"},
    ],
};

const READY_TIMEOUT_MS = 25_000;
const READY_INTERVAL_MS = 500;

function advertisedHost(pod: string, namespace: string): string {
    return `${pod}.kafka-controller-headless.${namespace}.svc.cluster.local`;
}

function isReachable(host: string, port: number, timeoutMs = 1_500): Promise<boolean> {
    return new Promise((resolve) => {
        const socket = createConnection({host, port, timeout: timeoutMs});
        const done = (ok: boolean) => {
            socket.destroy();
            resolve(ok);
        };
        socket.once("connect", () => done(true));
        socket.once("timeout", () => done(false));
        socket.once("error", () => resolve(false));
    });
}

async function allReachable(cfg: KafkaTunnelConfig): Promise<boolean> {
    const results = await Promise.all(cfg.brokers.map((b) => isReachable(b.address, cfg.port)));
    return results.every(Boolean);
}

function hostsFilePath(): string {
    if (process.platform === "win32") {
        return join(process.env.WINDIR ?? "C:\\Windows", "System32", "drivers", "etc", "hosts");
    }
    return "/etc/hosts";
}

function missingHostsEntries(cfg: KafkaTunnelConfig): string[] {
    let content = "";
    try {
        content = readFileSync(hostsFilePath(), "utf8");
    } catch {
        // Unreadable hosts file → treat all entries as missing.
    }
    return cfg.brokers
        .map((b) => ({
            line: `${b.address} ${advertisedHost(b.pod, cfg.namespace)}`,
            host: advertisedHost(b.pod, cfg.namespace)
        }))
        .filter((e) => !content.includes(e.host))
        .map((e) => e.line);
}

// kubectl may be a bare command on PATH (non-Windows) or an absolute path.
function kubectlPresent(kubectl: string): boolean {
    const looksLikePath = kubectl.includes("/") || kubectl.includes("\\") || kubectl.endsWith(".exe");
    return looksLikePath ? existsSync(kubectl) : true;
}

function killChild(child: ChildProcess): void {
    if (child.pid == null) return;
    if (process.platform === "win32") {
        try {
            execFileSync("taskkill", ["/pid", String(child.pid), "/T", "/F"], {stdio: "ignore"});
            return;
        } catch {
            // fall through to signal-based kill
        }
    }
    try {
        child.kill("SIGTERM");
    } catch {
        // best-effort
    }
}

/**
 * Ensure the 3 Kafka broker tunnels are up. Returns a teardown that kills the
 * processes this call spawned (no-op if nothing was spawned).
 */
export async function ensureKafkaTunnels(
    override: Partial<KafkaTunnelConfig> = {},
): Promise<() => Promise<void>> {
    const cfg: KafkaTunnelConfig = {...DEFAULTS, ...override};
    const noop = async () => {
    };

    if (await allReachable(cfg)) {
        console.log("[kafka-tunnel] brokers already reachable — reusing existing forwards");
        return noop;
    }

    if (!kubectlPresent(cfg.kubectl)) {
        console.warn(`[kafka-tunnel] kubectl not found at "${cfg.kubectl}" — Step 7 will be skipped. Set KAFKA_PF_KUBECTL to override.`);
        return noop;
    }
    if (!existsSync(cfg.kubeconfig)) {
        console.warn(`[kafka-tunnel] kubeconfig not found at "${cfg.kubeconfig}" — Step 7 will be skipped. Set KAFKA_PF_KUBECONFIG to override.`);
        return noop;
    }

    const missing = missingHostsEntries(cfg);
    if (missing.length > 0) {
        console.warn(
            "[kafka-tunnel] Missing hosts entries — Step 7 will be skipped.\n" +
            "Add ONCE in an admin PowerShell, then re-run:\n" +
            `  Add-Content -Path $env:windir\\System32\\drivers\\etc\\hosts -Value @"\n` +
            missing.map((l) => `  ${l}`).join("\n") +
            `\n  "@`,
        );
        return noop;
    }

    console.log(`[kafka-tunnel] starting ${cfg.brokers.length} port-forward(s) to ${cfg.namespace}…`);
    const children: ChildProcess[] = [];
    for (const b of cfg.brokers) {
        const args = [
            `--kubeconfig=${cfg.kubeconfig}`,
            "-n",
            cfg.namespace,
            "port-forward",
            `pod/${b.pod}`,
            `${cfg.port}:9092`,
            "--address",
            b.address,
        ];
        // stderr is ignored (not piped) so the unread pipe can't fill and stall
        // kubectl on a long run; child.on("error") still surfaces spawn failures.
        const child = spawn(cfg.kubectl, args, {stdio: "ignore", windowsHide: true});
        // Only hard spawn errors are surfaced; bind conflicts are tolerated
        // because readiness (below) is the source of truth.
        child.on("error", (err) => console.warn(`[kafka-tunnel] ${b.pod} spawn error: ${err.message}`));
        children.push(child);
    }

    const teardown = async () => {
        for (const c of children) killChild(c);
    };

    const deadline = Date.now() + READY_TIMEOUT_MS;
    while (Date.now() < deadline) {
        if (await allReachable(cfg)) {
            console.log("[kafka-tunnel] all brokers reachable");
            return teardown;
        }
        await new Promise((res) => setTimeout(res, READY_INTERVAL_MS));
    }

    console.warn(`[kafka-tunnel] brokers not reachable within ${READY_TIMEOUT_MS}ms — Step 7 may be skipped`);
    return teardown;
}
