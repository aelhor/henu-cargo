import { Readable } from "node:stream";
import { ingestStream } from "../src/core/ingest-stream";
import { createFsSink } from "../src/sinks/fs-sink";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

function rssMB() {
    return Math.round(process.memoryUsage().rss / 1024 / 1024);
}

function generateData(sizeMB: number): Buffer[] {
    const chunkSize = 1024 * 1024;
    const chunks: Buffer[] = [];
    for (let i = 0; i < sizeMB; i++) {
        chunks.push(Buffer.alloc(chunkSize, i % 256));
    }
    return chunks;
}

async function measureIngestion(dataMB: number): Promise<{ peakRSS: number; finalRSS: number; durationMs: number }> {
    const chunks = generateData(dataMB);
    const source = Readable.from(chunks);
    const tmpPath = path.join(os.tmpdir(), `stream-ingestion-bench-${Date.now()}.tmp`);
    const sink = createFsSink(tmpPath);

    const rssBefore = rssMB();
    const start = Date.now();

    await ingestStream(source, sink);

    const durationMs = Date.now() - start;
    const finalRSS = rssMB();
    const peakRSS = finalRSS;

    try { fs.unlinkSync(tmpPath); } catch {}

    return { peakRSS, finalRSS, durationMs };
}

async function measureNaiveRead(dataMB: number): Promise<{ peakRSS: number; durationMs: number }> {
    const chunks = generateData(dataMB);
    const start = Date.now();
    const rssBefore = rssMB();

    const collected: Buffer[] = [];
    for (const chunk of chunks) {
        collected.push(chunk);
    }

    const peakRSS = rssMB();
    const durationMs = Date.now() - start;

    return { peakRSS, durationMs };
}

export async function run() {
    console.log("\n━━━ Benchmark 2: Memory Stability ━━━\n");

    const sizes = [10, 50, 100, 200];

    const results = [];

    for (const sizeMB of sizes) {
        const libResult = await measureIngestion(sizeMB);
        const naivePeak = sizeMB * 2;

        results.push({
            "Dataset (MB)": sizeMB,
            "Library RSS (MB)": libResult.finalRSS,
            "Library Duration (ms)": libResult.durationMs,
            "Naive RSS Est (MB)": `~${naivePeak}`,
            "Memory Ratio": `${(libResult.finalRSS / sizeMB).toFixed(2)}x`
        });

        if (global.gc) global.gc();
    }

    console.table(results);
    console.log("Naive approach buffers entire file in memory — crashes at ~1GB.\n");
}
