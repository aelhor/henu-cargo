import { Readable } from "node:stream";
import { IngestionSink } from "../sinks/Ingestion-sink";
import { IngestionOptions } from "../types/options";
import { WorkerPool } from "../workers/worker-pool";
import { Sequencer } from "../workers/sequencer";

export interface ParallelIngestResult {
    totalBytes: number;
    totalChunks: number;
}

export async function parallelIngest(
    source: Readable,
    sink: IngestionSink,
    options: IngestionOptions,
    pool: WorkerPool,
    onChunk?: (totalBytes: number, chunksProcessed: number) => void
): Promise<ParallelIngestResult> {
    const sequencer = new Sequencer();

    const concurrency = options.parallel!.concurrency || options.parallel!.poolSize * 2;

    let inFlight = 0;
    let sequenceId = 0;
    let totalChunks = 0;
    let pipelineError: Error | null = null;
    let drainedBytes = 0;
    let drainedChunks = 0;

    const slotReleaseQueue: (() => void)[] = [];

    const PROGRESS_INTERVAL_CHUNKS = 64;

    pool.onResult = ({ chunk, sequenceId: id }) => {
        sequencer.insert(id, chunk);
        releaseSlot();
    };

    pool.onError = (error: Error) => {
        pipelineError = error;
        releaseSlot();
    };

    async function acquireSlot(): Promise<void> {
        if (pipelineError) return;
        while (inFlight >= concurrency) {
            await new Promise<void>(resolve => slotReleaseQueue.push(resolve));
            if (pipelineError) return;
        }
        inFlight++;
    }

    function releaseSlot() {
        inFlight--;
        if (slotReleaseQueue.length > 0) {
            slotReleaseQueue.shift()!();
        }
    }

    for await (const chunk of source) {
        if (pipelineError) throw pipelineError;
        if (options?.signal?.aborted) throw new Error(options.signal.reason || "Aborted");

        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);

        await acquireSlot();
        if (pipelineError) throw pipelineError;

        totalChunks++;
        pool.dispatch(buf, sequenceId++);
    }

    if (pipelineError) throw pipelineError;

    let totalBytes = 0;
    for (let i = 0; i < totalChunks; i++) {
        const result = await sequencer.waitFor(i);

        if (result !== null) {
            await sink.write(result);
            totalBytes += result.length;
        }

        drainedChunks++;
        if (onChunk && drainedChunks % PROGRESS_INTERVAL_CHUNKS === 0) {
            onChunk(totalBytes, drainedChunks);
        }
    }

    return { totalBytes, totalChunks };
}
