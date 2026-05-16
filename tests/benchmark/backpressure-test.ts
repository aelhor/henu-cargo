import { ingestStream } from "../../src/core/ingest-stream";
import { createSlowSink } from "../../src/sinks/slow-sink";

import fs from "fs";
import { performance } from "perf_hooks";

function rssMB() {
    return Math.round(process.memoryUsage().rss / 1024 / 1024);
}

const FILE_PATH = "./test_file_10mb.bin";

async function runBackpressureTest() {
    // 0. This "Heartbeat" only works if the Event Loop is NOT blocked
    const heartbeat = setInterval(() => {
        console.log("💓 Heartbeat: Event Loop is free!");
    }, 500);

    // 1. Setup a memory logger
    const interval = setInterval(() => {
        console.log(`Current RSS: ${rssMB()} MB`);
    }, 200);

    // 2. Define the Slow Sink
    const slowSink = createSlowSink()

    const startTime = performance.now();

    // 3. Execute // read 1gb file and write to slow sink
    await ingestStream(fs.createReadStream(FILE_PATH), slowSink)
    // 3. Execute // No backpressure
    // await brokenIngestStream(fs.createReadStream(FILE_PATH), slowSink);

    const endTime = performance.now();
    
    // 4. Cleanup
    clearInterval(interval);
    clearInterval(heartbeat);
    console.log("INSIDE TEST : SINK FINISHED ALL WRITES")

    console.log("Elapsed:", Math.round(endTime - startTime), "ms");


}
(async () => {
    await runBackpressureTest();

    console.log("SINK FINISHED ALL WRITES")
})();

// node --expose-gc ./node_modules/ts-node/dist/bin.js tests/benchmark/backpressure-test.ts
