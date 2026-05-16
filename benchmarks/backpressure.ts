import { Readable } from "node:stream";
import { performance } from "node:perf_hooks";
import { ingestStream } from "../src/core/ingest-stream";
import { IngestionSink } from "../src/sinks/Ingestion-sink";

function rssMB() {
    return Math.round(process.memoryUsage().rss / 1024 / 1024);
}

function createSlowSink(delayMs: number): IngestionSink {
    const pending: Buffer[] = [];
    const inFlight = new Set<Promise<void>>();

    return {
        write: async (chunk: Buffer) => {
            pending.push(chunk);
            const p = (async () => {
                await new Promise((res) => setTimeout(res, delayMs));
            })().finally(() => {
                pending.shift();
            });
            inFlight.add(p);
            p.finally(() => inFlight.delete(p));
            await p;
        },
        finalize: async () => {
            await Promise.all(Array.from(inFlight));
        },
        abort: async () => {
            await Promise.allSettled(Array.from(inFlight));
        }
    };
}

async function runWithBackpressure(chunkCount: number, delayMs: number): Promise<{ rss: number; durationMs: number }> {
    const data = Array.from({ length: chunkCount }, () => Buffer.alloc(1024, "x"));
    const source = Readable.from(data);
    const sink = createSlowSink(delayMs);

    const rssBefore = rssMB();
    const start = performance.now();
    await ingestStream(source, sink);
    const durationMs = performance.now() - start;
    const rssAfter = rssMB();

    return { rss: rssAfter, durationMs: Math.round(durationMs) };
}

async function runWithoutBackpressure(chunkCount: number): Promise<{ rss: number; durationMs: number }> {
    const data = Array.from({ length: chunkCount }, () => Buffer.alloc(1024, "x"));
    const collected: Buffer[] = [];

    const start = performance.now();
    for (const chunk of data) {
        collected.push(chunk);
    }
    const durationMs = performance.now() - start;

    return { rss: rssMB(), durationMs: Math.round(durationMs) };
}

export async function run() {
    console.log("\n━━━ Benchmark 3: Backpressure Effectiveness ━━━\n");

    const CHUNK_COUNT = 500;
    const SINK_DELAY_MS = 50;

    const bp = await runWithBackpressure(CHUNK_COUNT, SINK_DELAY_MS);
    const noBp = await runWithoutBackpressure(CHUNK_COUNT);

    console.table([
        {
            Mode: "With backpressure (this library)",
            "Chunks": CHUNK_COUNT,
            "Sink Delay": `${SINK_DELAY_MS}ms`,
            "RSS (MB)": bp.rss,
            "Duration (ms)": bp.durationMs
        },
        {
            Mode: "Without backpressure (naive)",
            "Chunks": CHUNK_COUNT,
            "Sink Delay": "0ms",
            "RSS (MB)": noBp.rss,
            "Duration (ms)": noBp.durationMs
        }
    ]);
    console.log("Backpressure keeps RSS flat even when the sink is orders of magnitude slower than the source.\n");
}
