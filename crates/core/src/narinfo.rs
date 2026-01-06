//! Narinfo types and formatting for Nix compatibility.

use crate::hash::NarHash;
use crate::store_path::StorePath;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A narinfo file content for Nix substituter compatibility.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NarInfo {
    /// The store path this narinfo describes.
    pub store_path: StorePath,
    /// URL to download the NAR (relative path).
    pub url: String,
    /// Compression type (none, xz, zstd, etc.).
    pub compression: Compression,
    /// Hash of the (possibly compressed) file.
    pub file_hash: NarHash,
    /// Size of the (possibly compressed) file.
    pub file_size: u64,
    /// Hash of the uncompressed NAR.
    pub nar_hash: NarHash,
    /// Size of the uncompressed NAR.
    pub nar_size: u64,
    /// References to other store paths.
    pub references: Vec<StorePath>,
    /// Optional deriver store path.
    pub deriver: Option<StorePath>,
    /// Signatures.
    pub signatures: Vec<Signature>,
    /// Content-addressable path info (if applicable).
    pub ca: Option<String>,
}

impl NarInfo {
    /// Create a new narinfo for an uncompressed NAR.
    pub fn new_uncompressed(store_path: StorePath, nar_hash: NarHash, nar_size: u64) -> Self {
        let url = format!("nar/{}.nar", store_path.hash());
        Self {
            store_path,
            url,
            compression: Compression::None,
            file_hash: nar_hash.clone(),
            file_size: nar_size,
            nar_hash,
            nar_size,
            references: Vec::new(),
            signatures: Vec::new(),
            deriver: None,
            ca: None,
        }
    }

    /// Create a new narinfo for a compressed NAR.
    pub fn new_compressed(
        store_path: StorePath,
        nar_hash: NarHash,
        nar_size: u64,
        compression: Compression,
        file_hash: NarHash,
        file_size: u64,
    ) -> Self {
        let extension = match compression {
            Compression::None => "nar",
            Compression::Xz => "nar.xz",
            Compression::Zstd => "nar.zst",
            Compression::Gzip => "nar.gz",
            Compression::Bzip2 => "nar.bz2",
        };
        let url = format!("nar/{}.{}", store_path.hash(), extension);
        Self {
            store_path,
            url,
            compression,
            file_hash,
            file_size,
            nar_hash,
            nar_size,
            references: Vec::new(),
            signatures: Vec::new(),
            deriver: None,
            ca: None,
        }
    }

    /// Add a signature.
    pub fn add_signature(&mut self, signature: Signature) {
        self.signatures.push(signature);
    }

    /// Format as standard narinfo text.
    pub fn to_narinfo_text(&self) -> String {
        let mut lines = Vec::new();

        lines.push(format!("StorePath: {}", self.store_path));
        lines.push(format!("URL: {}", self.url));
        lines.push(format!("Compression: {}", self.compression));
        lines.push(format!("FileHash: {}", self.file_hash.to_sri()));
        lines.push(format!("FileSize: {}", self.file_size));
        lines.push(format!("NarHash: {}", self.nar_hash.to_sri()));
        lines.push(format!("NarSize: {}", self.nar_size));

        if !self.references.is_empty() {
            let refs: Vec<_> = self.references.iter().map(|r| r.basename()).collect();
            lines.push(format!("References: {}", refs.join(" ")));
        }

        if let Some(ref deriver) = self.deriver {
            lines.push(format!("Deriver: {}", deriver.basename()));
        }

        if let Some(ref ca) = self.ca {
            lines.push(format!("CA: {}", ca));
        }

        for sig in &self.signatures {
            lines.push(format!("Sig: {}", sig));
        }

        lines.join("\n") + "\n"
    }

    /// Parse from narinfo text format.
    pub fn parse(text: &str) -> crate::Result<Self> {
        let mut store_path: Option<StorePath> = None;
        let mut url: Option<String> = None;
        let mut compression = Compression::None;
        let mut file_hash: Option<NarHash> = None;
        let mut file_size: Option<u64> = None;
        let mut nar_hash: Option<NarHash> = None;
        let mut nar_size: Option<u64> = None;
        let mut references = Vec::new();
        let mut deriver: Option<StorePath> = None;
        let mut signatures = Vec::new();
        let mut ca: Option<String> = None;

        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let (key, value) = line
                .split_once(": ")
                .ok_or_else(|| crate::Error::NarInfoParse(format!("invalid line: {line}")))?;

            match key {
                "StorePath" => store_path = Some(StorePath::parse(value)?),
                "URL" => url = Some(value.to_string()),
                "Compression" => compression = Compression::parse(value)?,
                "FileHash" => file_hash = Some(NarHash::from_sri(value)?),
                "FileSize" => {
                    file_size = Some(value.parse().map_err(|e| {
                        crate::Error::NarInfoParse(format!("invalid FileSize: {e}"))
                    })?)
                }
                "NarHash" => nar_hash = Some(NarHash::from_sri(value)?),
                "NarSize" => {
                    nar_size =
                        Some(value.parse().map_err(|e| {
                            crate::Error::NarInfoParse(format!("invalid NarSize: {e}"))
                        })?)
                }
                "References" => {
                    for r in value.split_whitespace() {
                        references.push(StorePath::from_basename(r).map_err(|e| {
                            crate::Error::NarInfoParse(format!("invalid reference '{r}': {e}"))
                        })?);
                    }
                }
                "Deriver" => {
                    deriver = Some(StorePath::from_basename(value).map_err(|e| {
                        crate::Error::NarInfoParse(format!("invalid deriver '{value}': {e}"))
                    })?);
                }
                "Sig" => signatures.push(Signature::parse(value)?),
                "CA" => ca = Some(value.to_string()),
                _ => {} // Ignore unknown fields
            }
        }

        Ok(Self {
            store_path: store_path
                .ok_or_else(|| crate::Error::NarInfoParse("missing StorePath".to_string()))?,
            url: url.ok_or_else(|| crate::Error::NarInfoParse("missing URL".to_string()))?,
            compression,
            file_hash: file_hash
                .ok_or_else(|| crate::Error::NarInfoParse("missing FileHash".to_string()))?,
            file_size: file_size
                .ok_or_else(|| crate::Error::NarInfoParse("missing FileSize".to_string()))?,
            nar_hash: nar_hash
                .ok_or_else(|| crate::Error::NarInfoParse("missing NarHash".to_string()))?,
            nar_size: nar_size
                .ok_or_else(|| crate::Error::NarInfoParse("missing NarSize".to_string()))?,
            references,
            signatures,
            deriver,
            ca,
        })
    }

    /// Get the fingerprint string used for signing.
    ///
    /// Nix fingerprint format: `1;/nix/store/<path>;sha256:<nix_base32>;narsize;refs...`
    /// The nar hash MUST use `sha256:<nix_base32>` format (HashFormat::Nix32 in Nix source),
    /// NOT SRI format (`sha256-<base64>`), otherwise Nix cannot verify signatures.
    pub fn fingerprint(&self) -> String {
        let mut refs: Vec<_> = self.references.iter().map(|r| r.to_string()).collect();
        refs.sort();
        format!(
            "1;{};sha256:{};{};{}",
            self.store_path,
            self.nar_hash.to_nix_base32(),
            self.nar_size,
            refs.join(",")
        )
    }
}

/// Compression type for NAR files.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    #[default]
    None,
    Xz,
    Zstd,
    Gzip,
    Bzip2,
}

impl Compression {
    /// Parse from string.
    pub fn parse(s: &str) -> crate::Result<Self> {
        match s.to_lowercase().as_str() {
            "none" | "" => Ok(Self::None),
            "xz" => Ok(Self::Xz),
            "zstd" => Ok(Self::Zstd),
            "gzip" | "gz" => Ok(Self::Gzip),
            "bzip2" | "bz2" => Ok(Self::Bzip2),
            _ => Err(crate::Error::NarInfoParse(format!(
                "unknown compression: {s}"
            ))),
        }
    }

    /// Get the file extension for this compression type.
    pub fn extension(&self) -> &'static str {
        match self {
            Self::None => "",
            Self::Xz => ".xz",
            Self::Zstd => ".zst",
            Self::Gzip => ".gz",
            Self::Bzip2 => ".bz2",
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Xz => write!(f, "xz"),
            Self::Zstd => write!(f, "zstd"),
            Self::Gzip => write!(f, "gzip"),
            Self::Bzip2 => write!(f, "bzip2"),
        }
    }
}

/// A narinfo signature.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signature {
    /// Key name (e.g., "cache.example.com-1").
    pub key_name: String,
    /// Base64-encoded signature bytes.
    pub signature: String,
}

impl Signature {
    /// Create a new signature.
    pub fn new(key_name: impl Into<String>, signature: impl Into<String>) -> Self {
        Self {
            key_name: key_name.into(),
            signature: signature.into(),
        }
    }

    /// Parse from "keyname:signature" format.
    pub fn parse(s: &str) -> crate::Result<Self> {
        let (key_name, signature) = s
            .split_once(':')
            .ok_or_else(|| crate::Error::NarInfoParse(format!("invalid signature format: {s}")))?;
        Ok(Self {
            key_name: key_name.to_string(),
            signature: signature.to_string(),
        })
    }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.key_name, self.signature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_narinfo_roundtrip() {
        let store_path =
            StorePath::parse("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test").unwrap();
        let nar_hash =
            NarHash::from_sri("sha256-LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564=").unwrap();

        let narinfo = NarInfo::new_uncompressed(store_path, nar_hash, 12345);
        let text = narinfo.to_narinfo_text();
        let parsed = NarInfo::parse(&text).unwrap();

        assert_eq!(narinfo.store_path, parsed.store_path);
        assert_eq!(narinfo.nar_size, parsed.nar_size);
    }

    #[test]
    fn test_parse_non_ascii_reference_does_not_panic() {
        let text = "\
StorePath: /nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test
URL: nar/abc.nar
Compression: none
NarHash: sha256-LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564=
NarSize: 100
References: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\u{00e9}\u{00e9}-foo";
        let result = NarInfo::parse(text);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_non_ascii_deriver_does_not_panic() {
        let text = "\
StorePath: /nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test
URL: nar/abc.nar
Compression: none
NarHash: sha256-LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564=
NarSize: 100
Deriver: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\u{00e9}\u{00e9}-foo.drv";
        let result = NarInfo::parse(text);
        assert!(result.is_err());
    }

    #[test]
    fn test_narinfo_roundtrip_with_references() {
        let store_path =
            StorePath::parse("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test").unwrap();
        let nar_hash =
            NarHash::from_sri("sha256-LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564=").unwrap();

        let mut narinfo = NarInfo::new_uncompressed(store_path, nar_hash, 12345);
        narinfo.references = vec![
            StorePath::parse("/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-dep1").unwrap(),
            StorePath::parse("/nix/store/cccccccccccccccccccccccccccccccc-dep2").unwrap(),
        ];
        narinfo.deriver =
            Some(StorePath::parse("/nix/store/dddddddddddddddddddddddddddddddd-test.drv").unwrap());

        let text = narinfo.to_narinfo_text();
        assert!(text.contains("References: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-dep1 cccccccccccccccccccccccccccccccc-dep2"));
        assert!(text.contains("Deriver: dddddddddddddddddddddddddddddddd-test.drv"));

        let parsed = NarInfo::parse(&text).unwrap();
        assert_eq!(parsed.references.len(), 2);
        assert_eq!(
            parsed.references[0].to_path_string(),
            "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-dep1"
        );
        assert_eq!(
            parsed.references[1].to_path_string(),
            "/nix/store/cccccccccccccccccccccccccccccccc-dep2"
        );
        assert_eq!(
            parsed.deriver.unwrap().to_path_string(),
            "/nix/store/dddddddddddddddddddddddddddddddd-test.drv"
        );
    }

    #[test]
    fn test_fingerprint_uses_nix32_hash_and_full_store_paths() {
        let store_path =
            StorePath::parse("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test").unwrap();
        let nar_hash =
            NarHash::from_sri("sha256-LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564=").unwrap();

        let mut narinfo = NarInfo::new_uncompressed(store_path, nar_hash.clone(), 12345);
        narinfo.references =
            vec![StorePath::parse("/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-dep1").unwrap()];

        let fp = narinfo.fingerprint();
        // References use full store paths
        assert!(fp.contains("/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-dep1"));
        // NarHash uses sha256:<nix_base32> format (NOT sha256-<base64> SRI)
        assert!(fp.contains("sha256:"));
        assert!(!fp.contains("sha256-"));
        assert!(fp.contains(&format!("sha256:{}", nar_hash.to_nix_base32())));
    }

    #[test]
    fn test_fingerprint_sorts_references() {
        let store_path =
            StorePath::parse("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test").unwrap();
        let nar_hash =
            NarHash::from_sri("sha256-LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564=").unwrap();

        let mut narinfo = NarInfo::new_uncompressed(store_path, nar_hash, 12345);
        // Insert references in reverse order
        narinfo.references = vec![
            StorePath::parse("/nix/store/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-dep3").unwrap(),
            StorePath::parse("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-dep1").unwrap(),
            StorePath::parse("/nix/store/mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm-dep2").unwrap(),
        ];

        let fp = narinfo.fingerprint();
        let refs_part = fp.rsplit_once(';').unwrap().1;
        let refs: Vec<_> = refs_part.split(',').collect();
        assert_eq!(refs[0], "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-dep1");
        assert_eq!(refs[1], "/nix/store/mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm-dep2");
        assert_eq!(refs[2], "/nix/store/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-dep3");
    }

    #[test]
    fn test_signature_parse() {
        let sig = Signature::parse("cache.example.com-1:ABCD1234").unwrap();
        assert_eq!(sig.key_name, "cache.example.com-1");
        assert_eq!(sig.signature, "ABCD1234");
        assert_eq!(sig.to_string(), "cache.example.com-1:ABCD1234");
    }
}
