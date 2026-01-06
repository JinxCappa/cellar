//! Service file generation for various init systems.

mod templates;

use anyhow::Result;
use clap::{Args, Subcommand, ValueEnum};
use std::path::Path;

/// Escape a string for safe use as a shell argument.
///
/// Uses single-quoting with embedded single quotes escaped via the
/// `'\''` idiom (end quote, literal quote, start quote).
fn escape_shell_arg(s: &str) -> String {
    format!("'{}'", s.replace("'", "'\\''"))
}

/// Escape a string for safe use in XML content.
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Available init systems for service generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum InitSystem {
    /// systemd (Linux)
    Systemd,
    /// OpenRC (Gentoo, Alpine)
    Openrc,
    /// runit (Void Linux, others)
    Runit,
    /// launchd (macOS)
    Launchd,
}

impl std::fmt::Display for InitSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InitSystem::Systemd => write!(f, "systemd"),
            InitSystem::Openrc => write!(f, "openrc"),
            InitSystem::Runit => write!(f, "runit"),
            InitSystem::Launchd => write!(f, "launchd"),
        }
    }
}

/// Service management commands.
#[derive(Subcommand)]
pub enum ServiceCommands {
    /// Generate a service file for the target init system
    Generate(GenerateArgs),
}

/// Arguments for the service generate command.
#[derive(Args)]
pub struct GenerateArgs {
    /// Target init system
    ///
    /// Default is detected from the local system (systemd on most Linux,
    /// launchd on macOS). If generating for a different target machine,
    /// specify this explicitly.
    #[arg(long, value_enum)]
    init_system: Option<InitSystem>,

    /// Generate user service variant instead of system service
    #[arg(long, default_value_t = false)]
    user: bool,

    /// Path to cellard binary on target system
    #[arg(long, default_value = "/usr/local/bin/cellard")]
    binary: String,

    /// Config file path on target system
    ///
    /// Defaults to /etc/cellar/server.toml for system services,
    /// or ~/.config/cellar/server.toml for user services.
    /// Mutually exclusive with --env-file.
    #[arg(long)]
    config: Option<String>,

    /// Environment file path on target system
    ///
    /// Use an environment file instead of a config file.
    /// Mutually exclusive with --config.
    #[arg(long)]
    env_file: Option<String>,
}

/// Detect the init system on the local machine.
///
/// Detection order:
/// 1. macOS -> launchd
/// 2. /run/systemd/system exists -> systemd
/// 3. /sbin/rc-service exists -> openrc
/// 4. /etc/runit or /run/runit exists -> runit
/// 5. Default to systemd on Linux
pub fn detect_init_system() -> InitSystem {
    if cfg!(target_os = "macos") {
        return InitSystem::Launchd;
    }

    if Path::new("/run/systemd/system").exists() {
        return InitSystem::Systemd;
    }

    if Path::new("/sbin/rc-service").exists() {
        return InitSystem::Openrc;
    }

    if Path::new("/etc/runit").exists() || Path::new("/run/runit").exists() {
        return InitSystem::Runit;
    }

    // Default to systemd on Linux
    InitSystem::Systemd
}

/// Handle the service command.
pub fn handle_service_command(command: ServiceCommands) -> Result<()> {
    match command {
        ServiceCommands::Generate(args) => generate(args),
    }
}

/// Generate a service file and print it to stdout.
fn generate(args: GenerateArgs) -> Result<()> {
    // Validate mutual exclusivity
    if args.config.is_some() && args.env_file.is_some() {
        anyhow::bail!("--config and --env-file are mutually exclusive");
    }

    let init_system = args.init_system.unwrap_or_else(detect_init_system);

    // Determine default config path based on init system and user flag
    let config_path = if args.env_file.is_some() {
        None
    } else if let Some(config) = args.config {
        Some(config)
    } else if args.user {
        // launchd doesn't expand ~, so use the actual path for launchd user services
        if init_system == InitSystem::Launchd {
            dirs::config_dir().map(|p| p.join("cellar/server.toml").display().to_string())
        } else {
            Some("~/.config/cellar/server.toml".to_string())
        }
    } else {
        Some("/etc/cellar/server.toml".to_string())
    };

    let output = match init_system {
        InitSystem::Systemd => generate_systemd(
            &args.binary,
            config_path.as_deref(),
            args.env_file.as_deref(),
            args.user,
        ),
        InitSystem::Openrc => generate_openrc(
            &args.binary,
            config_path.as_deref(),
            args.env_file.as_deref(),
        ),
        InitSystem::Runit => generate_runit(
            &args.binary,
            config_path.as_deref(),
            args.env_file.as_deref(),
            args.user,
        ),
        InitSystem::Launchd => generate_launchd(
            &args.binary,
            config_path.as_deref(),
            args.env_file.as_deref(),
            args.user,
        ),
    }?;

    print!("{output}");
    Ok(())
}

/// Generate a systemd unit file.
fn generate_systemd(
    binary_path: &str,
    config_path: Option<&str>,
    env_file_path: Option<&str>,
    user: bool,
) -> Result<String> {
    let template = if user {
        templates::SYSTEMD_USER
    } else {
        templates::SYSTEMD_SYSTEM
    };

    let wanted_by = if user {
        "default.target"
    } else {
        "multi-user.target"
    };

    let config_line = config_path
        .map(|p| format!("Environment=\"CELLAR_CONFIG={p}\"\n"))
        .unwrap_or_default();

    let env_file_line = env_file_path
        .map(|p| format!("EnvironmentFile={p}\n"))
        .unwrap_or_default();

    let output = template
        .replace("{binary_path}", binary_path)
        .replace("{config_line}", &config_line)
        .replace("{env_file_line}", &env_file_line)
        .replace("{wanted_by}", wanted_by);

    Ok(output)
}

/// Generate an OpenRC init script.
fn generate_openrc(
    binary_path: &str,
    config_path: Option<&str>,
    env_file_path: Option<&str>,
) -> Result<String> {
    let config_line = config_path
        .map(|p| format!("export CELLAR_CONFIG={}\n", escape_shell_arg(p)))
        .unwrap_or_default();

    let env_file_line = env_file_path
        .map(|p| {
            let escaped = escape_shell_arg(p);
            format!("\n# Source environment file\n[ -f {escaped} ] && . {escaped}\n")
        })
        .unwrap_or_default();

    let output = templates::OPENRC
        .replace("{binary_path}", &escape_shell_arg(binary_path))
        .replace("{config_line}", &config_line)
        .replace("{env_file_line}", &env_file_line);

    Ok(output)
}

/// Generate runit run scripts.
///
/// For runit, we output the run script. The log script is printed as a comment
/// since it needs to be in a separate file.
fn generate_runit(
    binary_path: &str,
    config_path: Option<&str>,
    env_file_path: Option<&str>,
    user: bool,
) -> Result<String> {
    let env_source = env_file_path
        .map(|p| {
            let escaped = escape_shell_arg(p);
            format!("[ -f {escaped} ] && . {escaped}\n")
        })
        .unwrap_or_default();

    let config_export = config_path
        .map(|p| format!("export CELLAR_CONFIG={}\n", escape_shell_arg(p)))
        .unwrap_or_default();

    // For system services, run as a dedicated user
    let chpst = if user {
        String::new()
    } else {
        "chpst -u cellar ".to_string()
    };

    let run_script = templates::RUNIT_RUN
        .replace("{binary_path}", &escape_shell_arg(binary_path))
        .replace("{env_source}", &env_source)
        .replace("{config_export}", &config_export)
        .replace("{chpst}", &chpst);

    // Include the log script as a comment for reference
    let mut output = run_script;
    output.push_str("\n# --- log/run (save to a separate file) ---\n");
    for line in templates::RUNIT_LOG.lines() {
        output.push_str("# ");
        output.push_str(line);
        output.push('\n');
    }

    Ok(output)
}

/// Generate a launchd plist file.
fn generate_launchd(
    binary_path: &str,
    config_path: Option<&str>,
    env_file_path: Option<&str>,
    user: bool,
) -> Result<String> {
    let template = if user {
        templates::LAUNCHD_USER
    } else {
        templates::LAUNCHD_SYSTEM
    };

    // Build environment dictionary
    let mut env_entries = Vec::new();

    if let Some(config) = config_path {
        env_entries.push(format!(
            "        <key>CELLAR_CONFIG</key>\n        <string>{}</string>",
            escape_xml(config)
        ));
    }

    // Note: launchd doesn't directly support sourcing env files like systemd does.
    // We add a comment about this limitation.
    if let Some(env_file) = env_file_path {
        env_entries.push(format!(
            "        <!-- Note: launchd does not support EnvironmentFile directly.\n             \
             Source the file in a wrapper script or set variables here.\n             \
             Env file: {} -->",
            escape_xml(env_file)
        ));
    }

    let env_dict = if env_entries.is_empty() {
        String::new()
    } else {
        format!(
            "    <key>EnvironmentVariables</key>\n    <dict>\n{}\n    </dict>\n",
            env_entries.join("\n")
        )
    };

    // For user services, use the user's log directory
    let log_path = if user {
        dirs::data_local_dir()
            .map(|p| p.join("cellar").display().to_string())
            .unwrap_or_else(|| "~/Library/Logs".to_string())
    } else {
        "/var/log".to_string()
    };

    let output = template
        .replace("{binary_path}", &escape_xml(binary_path))
        .replace("{env_dict}", &env_dict)
        .replace("{log_path}", &escape_xml(&log_path));

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_system_display() {
        assert_eq!(InitSystem::Systemd.to_string(), "systemd");
        assert_eq!(InitSystem::Openrc.to_string(), "openrc");
        assert_eq!(InitSystem::Runit.to_string(), "runit");
        assert_eq!(InitSystem::Launchd.to_string(), "launchd");
    }

    #[test]
    fn systemd_system_service() {
        let output = generate_systemd(
            "/usr/local/bin/cellard",
            Some("/etc/cellar/server.toml"),
            None,
            false,
        )
        .unwrap();

        assert!(output.contains("ExecStart=/usr/local/bin/cellard"));
        assert!(output.contains("CELLAR_CONFIG=/etc/cellar/server.toml"));
        assert!(output.contains("WantedBy=multi-user.target"));
        assert!(output.contains("NoNewPrivileges=yes"));
    }

    #[test]
    fn systemd_user_service() {
        let output = generate_systemd(
            "/usr/local/bin/cellard",
            Some("~/.config/cellar/server.toml"),
            None,
            true,
        )
        .unwrap();

        assert!(output.contains("ExecStart=/usr/local/bin/cellard"));
        assert!(output.contains("CELLAR_CONFIG=~/.config/cellar/server.toml"));
        assert!(output.contains("WantedBy=default.target"));
        // User services should not have security hardening
        assert!(!output.contains("NoNewPrivileges=yes"));
    }

    #[test]
    fn systemd_with_env_file() {
        let output = generate_systemd(
            "/usr/local/bin/cellard",
            None,
            Some("/etc/cellar/cellar.env"),
            false,
        )
        .unwrap();

        assert!(output.contains("EnvironmentFile=/etc/cellar/cellar.env"));
        assert!(!output.contains("CELLAR_CONFIG="));
    }

    #[test]
    fn openrc_service() {
        let output = generate_openrc(
            "/usr/local/bin/cellard",
            Some("/etc/cellar/server.toml"),
            None,
        )
        .unwrap();

        assert!(output.contains("command='/usr/local/bin/cellard'"));
        assert!(output.contains("export CELLAR_CONFIG='/etc/cellar/server.toml'"));
        assert!(output.contains("#!/sbin/openrc-run"));
    }

    #[test]
    fn runit_system_service() {
        let output = generate_runit(
            "/usr/local/bin/cellard",
            Some("/etc/cellar/server.toml"),
            None,
            false,
        )
        .unwrap();

        assert!(output.contains("exec chpst -u cellar '/usr/local/bin/cellard'"));
        assert!(output.contains("export CELLAR_CONFIG='/etc/cellar/server.toml'"));
        assert!(output.contains("#!/bin/sh"));
    }

    #[test]
    fn runit_user_service() {
        let output = generate_runit(
            "/usr/local/bin/cellard",
            Some("~/.config/cellar/server.toml"),
            None,
            true,
        )
        .unwrap();

        // User services don't use chpst
        assert!(output.contains("exec '/usr/local/bin/cellard'"));
        assert!(!output.contains("chpst -u"));
    }

    #[test]
    fn launchd_system_service() {
        let output = generate_launchd(
            "/usr/local/bin/cellard",
            Some("/etc/cellar/server.toml"),
            None,
            false,
        )
        .unwrap();

        assert!(output.contains("<string>/usr/local/bin/cellard</string>"));
        assert!(output.contains("<key>CELLAR_CONFIG</key>"));
        assert!(output.contains("<string>/etc/cellar/server.toml</string>"));
        assert!(output.contains("<string>/var/log/cellard.log</string>"));
    }

    #[test]
    fn launchd_user_service() {
        let output = generate_launchd(
            "/usr/local/bin/cellard",
            Some("~/.config/cellar/server.toml"),
            None,
            true,
        )
        .unwrap();

        assert!(output.contains("<string>/usr/local/bin/cellard</string>"));
        // User services use local log directory
        assert!(output.contains("cellard.log</string>"));
    }

    #[test]
    fn detect_init_system_returns_valid() {
        // Just verify it doesn't panic and returns a valid value
        let system = detect_init_system();
        assert!(matches!(
            system,
            InitSystem::Systemd | InitSystem::Openrc | InitSystem::Runit | InitSystem::Launchd
        ));
    }

    #[test]
    fn escape_shell_arg_basic() {
        assert_eq!(escape_shell_arg("hello"), "'hello'");
        assert_eq!(escape_shell_arg("/usr/bin/foo"), "'/usr/bin/foo'");
    }

    #[test]
    fn escape_shell_arg_with_single_quotes() {
        // Single quotes are escaped by ending the quote, adding escaped quote, starting new quote
        assert_eq!(escape_shell_arg("it's"), "'it'\\''s'");
        assert_eq!(escape_shell_arg("'"), "''\\'''");
    }

    #[test]
    fn escape_shell_arg_command_injection() {
        // Command substitution should be safely quoted
        assert_eq!(escape_shell_arg("$(whoami)"), "'$(whoami)'");
        assert_eq!(escape_shell_arg("`id`"), "'`id`'");
        assert_eq!(escape_shell_arg("foo; rm -rf /"), "'foo; rm -rf /'");
    }

    #[test]
    fn escape_xml_basic() {
        assert_eq!(escape_xml("hello"), "hello");
        assert_eq!(escape_xml("/usr/bin/foo"), "/usr/bin/foo");
    }

    #[test]
    fn escape_xml_special_chars() {
        assert_eq!(escape_xml("<script>"), "&lt;script&gt;");
        assert_eq!(escape_xml("foo & bar"), "foo &amp; bar");
        assert_eq!(escape_xml("\"quoted\""), "&quot;quoted&quot;");
        assert_eq!(escape_xml("it's"), "it&apos;s");
    }

    #[test]
    fn escape_xml_combined() {
        assert_eq!(
            escape_xml("<path>&special</path>"),
            "&lt;path&gt;&amp;special&lt;/path&gt;"
        );
    }

    #[test]
    fn openrc_shell_injection_escaped() {
        let output = generate_openrc("$(whoami)", None, None).unwrap();

        // The malicious command should be safely quoted
        assert!(output.contains("command='$(whoami)'"));
        // Should NOT contain unquoted command substitution
        assert!(!output.contains("command=$(whoami)"));
        assert!(!output.contains("command=\"$(whoami)\""));
    }

    #[test]
    fn runit_shell_injection_escaped() {
        let output = generate_runit("$(rm -rf /)", None, None, true).unwrap();

        // The malicious command should be safely quoted
        assert!(output.contains("exec '$(rm -rf /)'"));
        // Should NOT contain unquoted command substitution
        assert!(!output.contains("exec $(rm -rf /)"));
    }

    #[test]
    fn launchd_xml_injection_escaped() {
        let output =
            generate_launchd("/path/with<special>&chars", Some("<config>"), None, false).unwrap();

        // Binary path should be XML-escaped
        assert!(output.contains("<string>/path/with&lt;special&gt;&amp;chars</string>"));
        // Config should be XML-escaped
        assert!(output.contains("<string>&lt;config&gt;</string>"));
        // Should NOT contain unescaped special chars that would break XML
        assert!(!output.contains("<string>/path/with<special>"));
    }
}
