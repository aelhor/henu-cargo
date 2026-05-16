import { IngestionOptions } from "../types/options";
import { IngestionResult } from "../types/result";
import { Readable } from "node:stream";
import { IngestionSink } from "../sinks/Ingestion-sink";
import { WorkerPool } from "../workers/worker-pool";
import { parallelIngest } from "./parallel-ingest";

function validateIngestionParams(source: Readable, sink: IngestionSink) {
    if (!source) {
        throw new Error("source is required");
    }
    if (!sink) {
        throw new Error("sink is required");
    }
    if (!sink.abort || !sink.finalize || !sink.write) {
        throw new Error("sink must implement all required methods");
    }
}

export async function ingestStream(
    source: Readable,
    sink: IngestionSink,
    options?: IngestionOptions
): Promise<IngestionResult> {
    const onAbort = () => {
        source.destroy(new Error(options?.signal?.reason || 'Aborted'));
    };

    const startTime = Date.now();
    let totalBytes = 0;
    let chunksProcessed = 0;
    let pool: WorkerPool | null = null;
    const PROGRESS_INTERVAL_CHUNKS = 64;

    function reportProgress() {
        if (!options?.onProgress) return;
        options.onProgress({
            totalBytes,
            chunksProcessed,
            elapsedMs: Date.now() - startTime,
        });
    }

    function buildResult(status: 'success' | 'aborted' | 'failed', error?: Error, sinkAbortError?: Error): IngestionResult {
        const duration = Date.now() - startTime;
        return {
            totalBytes,
            duration,
            status,
            error,
            sinkAbortError,
            chunksProcessed,
            throughputBytesPerSec: duration > 0 ? Math.round(totalBytes / (duration / 1000)) : 0,
        };
    }

    try {
        validateIngestionParams(source, sink);

        if (options?.transform && options?.parallel) {
            throw new Error("options.transform and options.parallel are mutually exclusive — use one or the other");
        }

        if (options?.parallel) {
            pool = await WorkerPool.create(
                options.parallel.poolSize,
                options.parallel.workerPath
            );
        }

        if (options?.signal?.aborted) {
            throw new Error(options?.signal?.reason || 'Aborted');
        }

        options?.signal?.addEventListener('abort', onAbort);

        if (pool) {
            const result = await parallelIngest(source, sink, options!, pool, (bytes, count) => {
                totalBytes = bytes;
                chunksProcessed = count;
                reportProgress();
            });
            totalBytes = result.totalBytes;
            chunksProcessed = result.totalChunks;
        }
        else {
            for await (const chunk of source) {
                if (options?.signal?.aborted) {
                    throw new Error("Aborted");
                }

                let buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);

                if (options?.transform) {
                    const transformed = await options.transform(buf);
                    if (transformed === null) {
                        continue;
                    }
                    buf = transformed;
                }

                chunksProcessed++;

                if (options?.signal) {
                    await new Promise(resolve => setImmediate(resolve));
                    if (options.signal.aborted) {
                        throw new Error(options.signal.reason || "Aborted");
                    }
                } else if (chunksProcessed % PROGRESS_INTERVAL_CHUNKS === 0) {
                    await new Promise(resolve => setImmediate(resolve));
                }

                totalBytes += buf.length;
                await sink.write(buf);

                if (chunksProcessed % PROGRESS_INTERVAL_CHUNKS === 0) {
                    reportProgress();
                }
            }
        }

        await sink.finalize();
        return buildResult('success');
    }
    catch (error) {
        let sinkAbortError: Error | undefined;
        if (sink && typeof sink.abort === 'function') {
            try {
                await sink.abort(error as Error);
            } catch (abortError) {
                sinkAbortError = abortError as Error;
            }
        }
        return buildResult(
            options?.signal?.aborted ? 'aborted' : 'failed',
            error as Error,
            sinkAbortError
        );
    }
    finally {
        if (!source.destroyed) {
            source.destroy();
        }
        options?.signal?.removeEventListener('abort', onAbort);
        pool?.terminate();
    }
}
