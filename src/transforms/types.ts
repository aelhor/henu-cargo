
export interface WorkerTransformFunction {
    (chunk: Buffer): Promise<Buffer | null>;
}
