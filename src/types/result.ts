export interface IngestionResult {
  totalBytes: number
  duration: number
  status?: 'success' | 'aborted' | 'failed'
  error?: Error
  sinkAbortError?: Error
  chunksProcessed: number
  throughputBytesPerSec: number
}
