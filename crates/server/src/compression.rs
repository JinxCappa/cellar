//! NAR compression utilities.
//!
//! Provides functions for compressing and decompressing NAR data using
//! various algorithms (zstd, xz). Includes streaming compression to avoid
//! buffering entire NAR files in memory.

use async_compression::tokio::write::{XzEncoder, ZstdEncoder};
use bytes::Bytes;
use cellar_core::config::CompressionConfig;
use cellar_core::hash::{ContentHash, ContentHasher, NarHash};
use cellar_storage::traits::StreamingUpload;
use tokio::io::AsyncWriteExt;

/// Result of compressing NAR data.
pub struct CompressedNar {
    /// The compressed data.
    pub data: Bytes,
    /// Hash of the compressed data.
    pub file_hash: NarHash,
    /// Size of the compressed data.
    pub file_size: u64,
}

/// Streaming NAR compressor that processes chunks incrementally.
///
/// For compressed formats (Zstd, Xz), this streams data through the encoder
/// without buffering the entire input. However, the output is still collected
/// in memory because we need to compute the hash of the compressed data.
///
/// Note: For `CompressionConfig::None`, data must still be buffered since
/// we need the complete data to compute the content hash. This is inherent
/// to the design - if you need truly streaming uncompressed output without
/// buffering, compute the hash separately during upload.
pub struct StreamingCompressor {
    inner: StreamingCompressorInner,
}

enum StreamingCompressorInner {
    None(Vec<u8>),
    Zstd(ZstdEncoder<Vec<u8>>),
    Xz(XzEncoder<Vec<u8>>),
}

impl StreamingCompressor {
    /// Create a new streaming compressor for the specified algorithm.
    pub fn new(compression: CompressionConfig) -> Self {
        let inner = match compression {
            CompressionConfig::None => StreamingCompressorInner::None(Vec::new()),
            CompressionConfig::Zstd => {
                let output = Vec::new();
                StreamingCompressorInner::Zstd(ZstdEncoder::with_quality(
                    output,
                    async_compression::Level::Default,
                ))
            }
            CompressionConfig::Xz => {
                let output = Vec::new();
                StreamingCompressorInner::Xz(XzEncoder::with_quality(
                    output,
                    async_compression::Level::Default,
                ))
            }
        };
        Self { inner }
    }

    /// Write a chunk of data to the compressor.
    pub async fn write_chunk(&mut self, data: &[u8]) -> std::io::Result<()> {
        match &mut self.inner {
            StreamingCompressorInner::None(buf) => {
                buf.extend_from_slice(data);
            }
            StreamingCompressorInner::Zstd(encoder) => {
                encoder.write_all(data).await?;
            }
            StreamingCompressorInner::Xz(encoder) => {
                encoder.write_all(data).await?;
            }
        }
        Ok(())
    }

    /// Finalize compression and return the compressed data with hash.
    pub async fn finish(self) -> std::io::Result<CompressedNar> {
        let compressed = match self.inner {
            StreamingCompressorInner::None(buf) => buf,
            StreamingCompressorInner::Zstd(mut encoder) => {
                encoder.shutdown().await?;
                encoder.into_inner()
            }
            StreamingCompressorInner::Xz(mut encoder) => {
                encoder.shutdown().await?;
                encoder.into_inner()
            }
        };

        let hash = ContentHash::compute(&compressed);

        Ok(CompressedNar {
            file_size: compressed.len() as u64,
            file_hash: NarHash::from_content_hash(hash),
            data: Bytes::from(compressed),
        })
    }
}

/// Compress NAR data using the specified algorithm.
pub async fn compress_nar(
    data: &[u8],
    compression: CompressionConfig,
) -> std::io::Result<CompressedNar> {
    let compressed = match compression {
        CompressionConfig::None => {
            // No compression, just return the original data
            let hash = ContentHash::compute(data);
            return Ok(CompressedNar {
                data: Bytes::copy_from_slice(data),
                file_hash: NarHash::from_content_hash(hash),
                file_size: data.len() as u64,
            });
        }
        CompressionConfig::Zstd => compress_zstd(data).await?,
        CompressionConfig::Xz => compress_xz(data).await?,
    };

    // Compute hash of compressed data
    let hash = ContentHash::compute(&compressed);

    Ok(CompressedNar {
        file_size: compressed.len() as u64,
        file_hash: NarHash::from_content_hash(hash),
        data: Bytes::from(compressed),
    })
}

/// Compress data using zstd.
async fn compress_zstd(data: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut encoder = ZstdEncoder::with_quality(&mut output, async_compression::Level::Default);
    encoder.write_all(data).await?;
    encoder.shutdown().await?;
    Ok(output)
}

/// Compress data using xz.
async fn compress_xz(data: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut encoder = XzEncoder::with_quality(&mut output, async_compression::Level::Default);
    encoder.write_all(data).await?;
    encoder.shutdown().await?;
    Ok(output)
}

/// Result of true streaming compression directly to storage.
pub struct StreamingCompressionResult {
    /// Hash of the compressed data (computed incrementally).
    pub file_hash: NarHash,
    /// Total size of compressed data.
    pub file_size: u64,
    /// Temporary storage key where data was written.
    pub temp_key: String,
}

/// Minimum buffer size before flushing to storage.
/// S3 multipart uploads require minimum 5MB parts (except last part).
/// We use 8MB to be safe and reduce the number of parts.
const MIN_FLUSH_SIZE: usize = 8 * 1024 * 1024;

/// True streaming compressor that writes directly to storage with incremental hashing.
///
/// Unlike `StreamingCompressor` which buffers all compressed output in memory,
/// this compressor streams compressed data directly to object storage while
/// computing the hash incrementally. This enables processing of arbitrarily
/// large NAR files without memory pressure.
///
/// The compressor writes to a temporary key, and the caller is responsible for
/// copying to the final content-addressed key after getting the hash from `finish()`.
pub struct TrueStreamingCompressor {
    upload: Box<dyn StreamingUpload>,
    hasher: ContentHasher,
    buffer: Vec<u8>,
    temp_key: String,
    bytes_flushed: u64,
    compression: CompressionConfig,
    // We store uncompressed data and compress in batches for simplicity.
    // A more sophisticated implementation would use async_compression's stream adapters.
    pending_data: Vec<u8>,
}

impl TrueStreamingCompressor {
    /// Create a new streaming compressor that writes to the given storage upload.
    pub fn new(
        upload: Box<dyn StreamingUpload>,
        compression: CompressionConfig,
        temp_key: String,
    ) -> Self {
        Self {
            upload,
            hasher: ContentHash::hasher(),
            buffer: Vec::with_capacity(MIN_FLUSH_SIZE + 1024 * 1024), // Extra space for compression overhead
            temp_key,
            bytes_flushed: 0,
            compression,
            pending_data: Vec::new(),
        }
    }

    /// Write a chunk of uncompressed data.
    ///
    /// The data will be compressed and written to storage when the internal
    /// buffer exceeds the flush threshold.
    pub async fn write_chunk(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.pending_data.extend_from_slice(data);

        // Compress and flush when we have enough pending data
        // We compress in chunks to balance memory usage and compression efficiency
        while self.pending_data.len() >= MIN_FLUSH_SIZE {
            self.compress_and_buffer_chunk(MIN_FLUSH_SIZE).await?;
        }

        // Flush to storage when buffer is full
        if self.buffer.len() >= MIN_FLUSH_SIZE {
            self.flush_buffer().await?;
        }

        Ok(())
    }

    /// Compress a chunk of pending data and add to buffer.
    async fn compress_and_buffer_chunk(&mut self, size: usize) -> std::io::Result<()> {
        let chunk: Vec<u8> = self.pending_data.drain(..size).collect();

        let compressed = match self.compression {
            CompressionConfig::None => chunk,
            CompressionConfig::Zstd => {
                // Compress the chunk using zstd
                let mut output = Vec::new();
                let mut encoder =
                    ZstdEncoder::with_quality(&mut output, async_compression::Level::Default);
                encoder.write_all(&chunk).await?;
                encoder.shutdown().await?;
                output
            }
            CompressionConfig::Xz => {
                // Compress the chunk using xz
                let mut output = Vec::new();
                let mut encoder =
                    XzEncoder::with_quality(&mut output, async_compression::Level::Default);
                encoder.write_all(&chunk).await?;
                encoder.shutdown().await?;
                output
            }
        };

        self.buffer.extend_from_slice(&compressed);
        Ok(())
    }

    /// Flush the internal buffer to storage.
    async fn flush_buffer(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Update hash with the data being flushed
        self.hasher.update(&self.buffer);

        // Write to storage
        let data = Bytes::from(std::mem::take(&mut self.buffer));
        self.bytes_flushed += data.len() as u64;

        self.upload
            .write(data)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(())
    }

    /// Finalize compression and return the result.
    ///
    /// This compresses any remaining data, flushes to storage, and returns
    /// the hash and total size of the compressed data.
    pub async fn finish(mut self) -> std::io::Result<StreamingCompressionResult> {
        // Compress any remaining pending data
        if !self.pending_data.is_empty() {
            let remaining = std::mem::take(&mut self.pending_data);
            let compressed = match self.compression {
                CompressionConfig::None => remaining,
                CompressionConfig::Zstd => {
                    let mut output = Vec::new();
                    let mut encoder =
                        ZstdEncoder::with_quality(&mut output, async_compression::Level::Default);
                    encoder.write_all(&remaining).await?;
                    encoder.shutdown().await?;
                    output
                }
                CompressionConfig::Xz => {
                    let mut output = Vec::new();
                    let mut encoder =
                        XzEncoder::with_quality(&mut output, async_compression::Level::Default);
                    encoder.write_all(&remaining).await?;
                    encoder.shutdown().await?;
                    output
                }
            };
            self.buffer.extend_from_slice(&compressed);
        }

        // Flush any remaining buffer
        self.flush_buffer().await?;

        // Finish the upload
        let _total_bytes = self
            .upload
            .finish()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        let hash = self.hasher.finalize();

        Ok(StreamingCompressionResult {
            file_hash: NarHash::from_content_hash(hash),
            file_size: self.bytes_flushed,
            temp_key: self.temp_key,
        })
    }

    /// Abort the upload on error.
    ///
    /// This should be called if an error occurs during compression to clean up
    /// any partially uploaded data.
    pub async fn abort(self) -> std::io::Result<()> {
        self.upload
            .abort()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_compress_none() {
        let data = b"test NAR data";
        let result = compress_nar(data, CompressionConfig::None).await.unwrap();
        assert_eq!(result.data.as_ref(), data);
        assert_eq!(result.file_size, data.len() as u64);
    }

    #[tokio::test]
    async fn test_compress_zstd() {
        let data = b"test NAR data that should compress well when repeated many times \
                     test NAR data that should compress well when repeated many times \
                     test NAR data that should compress well when repeated many times";
        let result = compress_nar(data, CompressionConfig::Zstd).await.unwrap();
        // Compressed data should be smaller for repetitive content
        assert!(result.file_size < data.len() as u64);
    }

    #[tokio::test]
    async fn test_compress_xz() {
        let data = b"test NAR data that should compress well when repeated many times \
                     test NAR data that should compress well when repeated many times \
                     test NAR data that should compress well when repeated many times";
        let result = compress_nar(data, CompressionConfig::Xz).await.unwrap();
        // Compressed data should be smaller for repetitive content
        assert!(result.file_size < data.len() as u64);
    }
}
