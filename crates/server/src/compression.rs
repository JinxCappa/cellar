//! NAR compression utilities.
//!
//! Provides functions for compressing and decompressing NAR data using
//! various algorithms (zstd, xz). Includes streaming compression to avoid
//! buffering entire NAR files in memory.

use async_compression::tokio::write::{ZstdEncoder, XzEncoder};
use bytes::Bytes;
use cellar_core::config::CompressionConfig;
use cellar_core::hash::{ContentHash, NarHash};
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
