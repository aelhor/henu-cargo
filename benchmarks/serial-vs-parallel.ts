import { Readable } from "node:stream";
import { performance } from "node:perf_hooks";
import { ingestStream } from "../src/core/ingest-stream";
import path from "node:path";
import os from "node:os";

const MS_PER_CHUNK = 200;
const CHUNK_COUNT = 10;

const heavyTransform = async (chunk: Buffer) => {
    const start = Date.now();
    while (Date.now() - start < MS_PER_CHUNK) {}
    return chunk;
};

const createSink = () => ({
    write: async () => {},
    finalize: async () => {},
    abort: async () => {}
});

async function runSerial(): Promise<number> {
    const data = Array.from({ length: CHUNK_COUNT }, (_, i) => Buffer.from(`chunk-${i}`));
    const source = Readable.from(data);
    const sink = createSink();
    const start = performance.now();
    await ingestStream(source, sink, { transform: heavyTransform });
    return performance.now() - start;
}

async function runParallel(poolSize: number): Promise<number> {
    const data = Array.from({ length: CHUNK_COUNT }, (_, i) => Buffer.from(`chunk-${i}`));
    const source = Readable.from(data);
    const sink = createSink();
    const transformPath = path.resolve('dist-tests/tests/fixtures/heavy-transform.js');
    const start = performance.now();
    await ingestStream(source, sink, {
        parallel: { workerPath: transformPath, poolSize }
    });
    return performance.now() - start;
}

export async function run() {
    console.log("\n━━━ Benchmark 1: Serial vs Parallel Throughput ━━━\n");

    const serialMs = await runSerial();
    const pool4 = Math.min(4, os.cpus().length);
    const poolMax = Math.min(os.cpus().length, CHUNK_COUNT);
    const parallel4Ms = await runParallel(pool4);
    const parallelMaxMs = await runParallel(poolMax);

    const results = [
        {
            Mode: "Serial",
            "Pool Size": "-",
            "Time (ms)": Math.round(serialMs),
            "Speedup": "1.0x"
        },
        {
            Mode: "Parallel",
            "Pool Size": pool4,
            "Time (ms)": Math.round(parallel4Ms),
            "Speedup": `${(serialMs / parallel4Ms).toFixed(1)}x`
        },
        {
            Mode: "Parallel",
            "Pool Size": poolMax,
            "Time (ms)": Math.round(parallelMaxMs),
            "Speedup": `${(serialMs / parallelMaxMs).toFixed(1)}x`
        }
    ];

    console.table(results);
    console.log(`Chunks: ${CHUNK_COUNT} | Transform: ${MS_PER_CHUNK}ms CPU work per chunk | CPUs: ${os.cpus().length}\n`);
}
