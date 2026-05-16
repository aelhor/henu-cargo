export interface IngestionSink {
    write(chunk: Buffer): Promise<void>
    finalize(): Promise<void>
    abort(error: Error): Promise<void>
}