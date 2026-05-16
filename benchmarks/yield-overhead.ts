import { Readable } from "node:stream";
import { performance } from "node:perf_hooks";
import { ingestStream } from "../src/core/ingest-stream";

const CHUNK_COUNT = 10_000;
const CHUNK_SIZE = 1024;

function createSink() {
    return {
        write: async () => {},
        finalize: async () => {},
        abort: async () => {}
    };
}

async function runWithSignal(): Promise<number> {
    const data = Array.from({ length: CHUNK_COUNT }, () => Buffer.alloc(CHUNK_SIZE, "x"));
    const source = Readable.from(data);
    const sink = createSink();
    const controller = new AbortController();

    const start = performance.now();
    await ingestStream(source, sink, { signal: controller.signal });
    return performance.now() - start;
}

async function runWithoutSignal(): Promise<number> {
    const data = Array.from({ length: CHUNK_COUNT }, () => Buffer.alloc(CHUNK_SIZE, "x"));
    const source = Readable.from(data);
    const sink = createSink();

    const start = performance.now();
    await ingestStream(source, sink);
    return performance.now() - start;
}

export async function run() {
    console.log("\n━━━ Benchmark 5: setImmediate Yield Overhead ━━━\n");

    const withSignalMs = await runWithSignal();
    const withoutSignalMs = await runWithoutSignal();
    const savedMs = withSignalMs - withoutSignalMs;
    const speedup = withSignalMs / withoutSignalMs;

    console.table([
        {
            Mode: "With signal (yield every chunk)",
            "Time (ms)": Math.round(withSignalMs),
            "Chunks": CHUNK_COUNT,
            "Yields": CHUNK_COUNT
        },
        {
            Mode: "Without signal (yield every 64 chunks)",
            "Time (ms)": Math.round(withoutSignalMs),
            "Chunks": CHUNK_COUNT,
            "Yields": Math.ceil(CHUNK_COUNT / 64)
        }
    ]);

    console.log(`Speedup: ${speedup.toFixed(2)}x | Time saved: ${Math.round(savedMs)}ms (${Math.round(CHUNK_COUNT / 64)} vs ${CHUNK_COUNT} yields)\n`);
}
