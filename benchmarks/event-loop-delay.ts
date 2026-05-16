import { Readable } from "node:stream";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { ingestStream } from "../src/core/ingest-stream";
import path from "node:path";
import os from "node:os";

const MS_PER_CHUNK = 100;
const CHUNK_COUNT = 10;

function createSink() {
    return {
        write: async () => {},
        finalize: async () => {},
        abort: async () => {}
    };
}

async function runMainThreadTransform(): Promise<{ durationMs: number; maxLoopDelayMs: number }> {
    const data = Array.from({ length: CHUNK_COUNT }, (_, i) => Buffer.from(`chunk-${i}`));
    const source = Readable.from(data);
    const sink = createSink();

    const h = monitorEventLoopDelay();
    h.enable();

    const start = Date.now();
    await ingestStream(source, sink, {
        transform: async (chunk) => {
            const s = Date.now();
            while (Date.now() - s < MS_PER_CHUNK) {}
            return chunk;
        }
    });
    const durationMs = Date.now() - start;

    h.disable();
    const maxLoopDelayMs = Math.round(h.max / 1e6);

    return { durationMs, maxLoopDelayMs };
}

async function runWorkerThreadTransform(): Promise<{ durationMs: number; maxLoopDelayMs: number }> {
    const data = Array.from({ length: CHUNK_COUNT }, (_, i) => Buffer.from(`chunk-${i}`));
    const source = Readable.from(data);
    const sink = createSink();
    const poolSize = Math.min(4, os.cpus().length);
    const transformPath = path.resolve('dist-tests/tests/fixtures/heavy-transform.js');

    const h = monitorEventLoopDelay();
    h.enable();

    const start = Date.now();
    await ingestStream(source, sink, {
        parallel: { workerPath: transformPath, poolSize }
    });
    const durationMs = Date.now() - start;

    h.disable();
    const maxLoopDelayMs = Math.round(h.max / 1e6);

    return { durationMs, maxLoopDelayMs };
}

export async function run() {
    console.log("\n━━━ Benchmark 4: Event Loop Responsiveness ━━━\n");

    const main = await runMainThreadTransform();
    const worker = await runWorkerThreadTransform();

    console.table([
        {
            Mode: "Main-thread transform",
            "Duration (ms)": main.durationMs,
            "Max Event Loop Delay (ms)": main.maxLoopDelayMs,
            "Blocks Event Loop": "YES"
        },
        {
            Mode: "Worker-thread transform",
            "Duration (ms)": worker.durationMs,
            "Max Event Loop Delay (ms)": worker.maxLoopDelayMs,
            "Blocks Event Loop": "NO"
        }
    ]);
    console.log(`Chunks: ${CHUNK_COUNT} | CPU work: ${MS_PER_CHUNK}ms per chunk | CPUs: ${os.cpus().length}\n`);
}
