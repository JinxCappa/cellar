/// Get current process RSS (Resident Set Size) in bytes
pub fn get_process_rss() -> usize {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        if let Ok(status) = fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:")
                    && let Some(kb_str) = line.split_whitespace().nth(1)
                    && let Ok(kb) = kb_str.parse::<usize>()
                {
                    return kb * 1024;
                }
            }
        }
        0
    }

    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        let pid = std::process::id().to_string();
        if let Ok(output) = Command::new("ps")
            .args(["-o", "rss=", "-p", pid.as_str()])
            .output()
            && let Ok(rss_str) = String::from_utf8(output.stdout)
            && let Ok(rss_kb) = rss_str.trim().parse::<usize>()
        {
            return rss_kb * 1024;
        }
        0
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        eprintln!("WARNING: RSS measurement not supported on this platform");
        0
    }
}

/// Format memory size in human-readable format
pub fn format_memory_size(bytes: usize) -> String {
    if bytes > 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes > 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes > 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} bytes", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use std::fs;
    #[cfg(target_os = "macos")]
    use std::process::Command;

    #[test]
    fn test_get_process_rss() {
        let rss = get_process_rss();
        if rss > 0 {
            assert!(rss > 1024 * 1024, "RSS should be > 1MB, got {}", rss);
            return;
        }

        // If RSS is zero, verify the underlying probe is genuinely unavailable.
        // This allows sandboxed environments while still catching parser regressions.
        #[cfg(target_os = "linux")]
        {
            match fs::read_to_string("/proc/self/status") {
                Ok(status) => {
                    assert!(
                        !status.lines().any(|line| line.starts_with("VmRSS:")),
                        "RSS probe returned 0 even though /proc/self/status includes VmRSS"
                    );
                }
                Err(_) => {
                    // Sandboxed environments may block reading `/proc`.
                    // Treat this as probe unavailable, not a parser regression.
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            let pid = std::process::id().to_string();
            match Command::new("ps")
                .args(["-o", "rss=", "-p", pid.as_str()])
                .output()
            {
                Ok(output) => {
                    assert!(
                        !output.status.success(),
                        "RSS probe returned 0 even though ps succeeded"
                    );
                }
                Err(_) => {
                    // Sandboxed environments may block executing `ps`.
                    // Treat this as probe unavailable, not a parser regression.
                }
            }
        }
    }

    #[test]
    fn test_format_memory_size() {
        assert_eq!(format_memory_size(500), "500 bytes");
        assert_eq!(format_memory_size(2048), "2.00 KB");
        assert_eq!(format_memory_size(2 * 1024 * 1024), "2.00 MB");
        assert_eq!(format_memory_size(3 * 1024 * 1024 * 1024), "3.00 GB");
    }
}
