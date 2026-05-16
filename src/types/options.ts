export interface IngestionOptions {
    signal?: AbortSignal
    transform?: (chunk: Buffer) => Buffer | Promise<Buffer | null> | null
    parallel?: {
        workerPath: string;
        poolSize: number;
        concurrency?: number;
    };
    onProgress?: (info: {
        totalBytes: number;
        chunksProcessed: number;
        elapsedMs: number;
    }) => void;
}
