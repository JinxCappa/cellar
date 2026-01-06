//! Service file templates for various init systems.

/// Systemd unit file template for system services.
///
/// Placeholders:
/// - `{binary_path}` - Path to cellard binary
/// - `{config_path}` - Config file path (optional, for CELLAR_CONFIG)
/// - `{env_file_path}` - Environment file path (optional, for EnvironmentFile)
/// - `{wanted_by}` - Target to install into (multi-user.target or default.target)
pub const SYSTEMD_SYSTEM: &str = r#"[Unit]
Description=Cellar Binary Cache Server
Documentation=https://github.com/cellar-cache/cellar
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={binary_path}
Restart=on-failure
RestartSec=5
{config_line}{env_file_line}
# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
MemoryDenyWriteExecute=yes
LockPersonality=yes

[Install]
WantedBy={wanted_by}
"#;

/// Systemd unit file template for user services.
///
/// User services have fewer security restrictions since they run as the user.
pub const SYSTEMD_USER: &str = r#"[Unit]
Description=Cellar Binary Cache Server
Documentation=https://github.com/cellar-cache/cellar
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={binary_path}
Restart=on-failure
RestartSec=5
{config_line}{env_file_line}
[Install]
WantedBy={wanted_by}
"#;

/// OpenRC init script template.
///
/// Placeholders:
/// - `{binary_path}` - Path to cellard binary
/// - `{config_path}` - Config file path (optional)
/// - `{env_file_path}` - Environment file path (optional)
pub const OPENRC: &str = r#"#!/sbin/openrc-run

description="Cellar Binary Cache Server"

command={binary_path}
command_background=true
pidfile="/run/${RC_SVCNAME}.pid"
{config_line}{env_file_line}
depend() {
    need net
    after firewall
}

start_pre() {
    checkpath --directory --mode 0755 /run
}
"#;

/// Runit run script template.
///
/// Placeholders:
/// - `{binary_path}` - Path to cellard binary
/// - `{config_path}` - Config file path (optional)
/// - `{env_file_path}` - Environment file path (optional)
/// - `{user}` - User to run as (for system services)
pub const RUNIT_RUN: &str = r#"#!/bin/sh
exec 2>&1
{env_source}{config_export}exec {chpst}{binary_path}
"#;

/// Runit log run script template.
pub const RUNIT_LOG: &str = r#"#!/bin/sh
exec svlogd -tt /var/log/cellard
"#;

/// Launchd plist template for system services.
///
/// Placeholders:
/// - `{binary_path}` - Path to cellard binary
/// - `{config_path}` - Config file path (optional)
/// - `{env_file_path}` - Environment file path (optional)
pub const LAUNCHD_SYSTEM: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.cellar.cellard</string>
    <key>ProgramArguments</key>
    <array>
        <string>{binary_path}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/var/log/cellard.log</string>
    <key>StandardErrorPath</key>
    <string>/var/log/cellard.log</string>
{env_dict}</dict>
</plist>
"#;

/// Launchd plist template for user services.
pub const LAUNCHD_USER: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.cellar.cellard</string>
    <key>ProgramArguments</key>
    <array>
        <string>{binary_path}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{log_path}/cellard.log</string>
    <key>StandardErrorPath</key>
    <string>{log_path}/cellard.log</string>
{env_dict}</dict>
</plist>
"#;
