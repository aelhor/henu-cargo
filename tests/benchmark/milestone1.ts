// 0 ceate a large file 

import { randomBytes } from "crypto";
import fs from "fs";
import { performance } from "perf_hooks";
import { IngestionSink } from "../../src/sinks/Ingestion-sink";
import { ingestStream } from "../../src/core/ingest-stream";

const FILE_PATH = "./large_file_2.bin";
const ONE_GB = 1024 * 1024 * 1024;
const CHUNK_SIZE = 10 * 1024 * 1024; // 10 MB

function rssMB() {
    return Math.round(process.memoryUsage().rss / 1024 / 1024);
}

function createLargeFile() {
    fs.writeFileSync(FILE_PATH, ""); // reset file

    let written = 0;
    //   const chunk = Buffer.alloc(CHUNK_SIZE, 0);

    while (written < ONE_GB) {
        // use random bytes to simulate a real file 
        const chunk = randomBytes(CHUNK_SIZE);
        fs.appendFileSync(FILE_PATH, chunk);
        written += CHUNK_SIZE;
    }
}

// createLargeFile();



// 1. Setup global metrics collector (an array to store results)
let metrics: {
    [key: string]: {
        rssBeforeMB: number | null;
        rssAfterMB: number | null;
        rssDeltaMB: number | null;
        checksum: number | null;
        ElapsedMS: number | null
    }
} = {
    'readFileSync': {
        rssBeforeMB: null,
        rssAfterMB: null,
        rssDeltaMB: null,
        checksum: null,
        ElapsedMS: null
    },
    'readFilePromise': {
        rssBeforeMB: null,
        rssAfterMB: null,
        rssDeltaMB: null,
        checksum: null,
        ElapsedMS: null
    },
    'ingestStream': {
        rssBeforeMB: null,
        rssAfterMB: null,
        rssDeltaMB: null,
        checksum: null,
        ElapsedMS: null
    }
}

// 2. Test A: fs.readFileSync
//    - Measure RSS, run the read, measure RSS delta.

function runReadFileSyncTest() {
    global.gc?.(); // best-effort GC
    const startMemory = rssMB();
    const startTime = performance.now();
    metrics.readFileSync.rssBeforeMB = startMemory;

    // Read entire file into memory
    const data = fs.readFileSync(FILE_PATH);

    // Simulate a sink (force memory touch)
    let checksum = 0;
    for (let i = 0; i < data.length; i += 4096) {
        checksum ^= data[i];
    }

    const endTime = performance.now();
    const endMemory = rssMB();
    metrics.readFileSync.rssAfterMB = endMemory;
    metrics.readFileSync.rssDeltaMB = endMemory - startMemory;
    metrics.readFileSync.ElapsedMS = Math.round(endTime - startTime);
    metrics.readFileSync.checksum = checksum;

}
// 3. Test B: fs.promises.readFile (The Async version)
//    - global.gc() to clear RAM from Test A
//    - Measure RSS, await readFile, measure RSS delta.
async function runReadFilePromiseTest() {
    global.gc?.(); // best-effort GC
    const startMemory = rssMB();
    const startTime = performance.now();
    metrics.readFilePromise.rssBeforeMB = startMemory;

    // Read entire file into memory
    let checksum = 0;

    const data = await fs.promises.readFile(FILE_PATH);

    // Simulate a sink (force memory touch)
    for (let i = 0; i < data.length; i += 4096) {
        checksum ^= data[i];
    }

    const endTime = performance.now();
    const endMemory = rssMB();
    metrics.readFilePromise.rssAfterMB = endMemory;
    metrics.readFilePromise.rssDeltaMB = endMemory - startMemory;
    metrics.readFilePromise.ElapsedMS = Math.round(endTime - startTime);
    metrics.readFilePromise.checksum = checksum;
}
// 4. Test C: your ingestStream
//    - global.gc()
//    - Measure RSS, await ingestStream(fs.createReadStream), measure RSS delta.
async function runIngestStreamTest() {
    global.gc?.();
    const startMemory = rssMB();
    const startTime = performance.now();

    let received = 0;
    let finalized = false;
    let aborted = false;
    let abortError: Error | undefined;
    let checksum = 0;

    const sink: IngestionSink = {
        async write(chunk: Buffer) {
            received += chunk.length;
            // Simulate a sink (force memory touch) by xor'ing each byte in the chunk into the checksum
            for (let i = 0; i < chunk.length; i += 4096) {
                checksum ^= chunk[i];
            }
        },
        async finalize() {
            finalized = true;
        },
        async abort(error: Error) {
            aborted = true;
            abortError = error;
        }
    };

    metrics.ingestStream.rssBeforeMB = startMemory;

    const readStream = fs.createReadStream(FILE_PATH);
    const result = await ingestStream(readStream, sink);

    if (result.totalBytes !== received) {
        throw new Error(
            `Byte count mismatch: ingestStream=${result.totalBytes} sinkReceived=${received}`
        );
    }
    if (!finalized) {
        throw new Error("Sink did not finalize");
    }
    if (aborted) {
        throw new Error("Sink aborted during ingestion");
    }

    const endTime = performance.now();
    const endMemory = rssMB();

    metrics.ingestStream.rssAfterMB = endMemory;
    metrics.ingestStream.rssDeltaMB = endMemory - startMemory;
    metrics.ingestStream.ElapsedMS = Math.round(endTime - startTime);
    metrics.ingestStream.checksum = checksum;
}


const args = process.argv.slice(2);
const testNum = args[0];

(async () => {
    if (testNum === '1') {
        runReadFileSyncTest();
    } else if (testNum === '2') {
        await runReadFilePromiseTest();
    } else if (testNum === '3') {
        await runIngestStreamTest();
    } else {
        console.log('Invalid test number. Please provide a number between 1 and 3');
        process.exit(1);
    }

    console.log('metrics:', metrics);
})().catch((err) => {
    console.error('❌ benchmark failed');
    console.error(err);
    process.exit(1);
});