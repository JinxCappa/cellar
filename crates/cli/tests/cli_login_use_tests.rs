#![allow(deprecated)] // cargo_bin is deprecated but still functional

use assert_cmd::Command;
use httpmock::Method::GET;
use httpmock::MockServer;
use predicates::str::contains;
use std::fs;
use std::net::TcpListener;
use tempfile::TempDir;

fn can_bind_localhost() -> bool {
    TcpListener::bind("127.0.0.1:0").is_ok()
}

fn mock_whoami(server: &MockServer) {
    server.mock(|when, then| {
        when.method(GET).path("/v1/auth/whoami");
        then.status(200).json_body(serde_json::json!({
            "token_id": "00000000-0000-0000-0000-000000000001",
            "cache_id": "00000000-0000-0000-0000-000000000010",
            "cache_name": "default",
            "scopes": ["cache:admin"],
            "public_base_url": "https://cache.example.com",
            "signing_public_key": "cache-1:PUBLICKEY"
        }));
    });
}

#[test]
fn login_writes_client_config_and_default() {
    if !can_bind_localhost() {
        eprintln!("Skipping httpmock tests: cannot bind to localhost");
        return;
    }

    let server = MockServer::start();
    mock_whoami(&server);

    let temp = TempDir::new().unwrap();
    let config_path = temp.path().join("client.toml");

    let expected_url = server.base_url().trim_end_matches('/').to_string();

    Command::cargo_bin("cellarctl")
        .unwrap()
        .arg("login")
        .arg("ci")
        .arg(server.base_url())
        .arg("--token-stdin")
        .arg("--set-default")
        .arg("--client-config")
        .arg(&config_path)
        .write_stdin("secret-token\n")
        .assert()
        .success();

    let contents = fs::read_to_string(&config_path).unwrap();
    let value: toml::Value = toml::from_str(&contents).unwrap();
    let default_cache = value.get("default_cache").and_then(|v| v.as_str()).unwrap();
    assert_eq!(default_cache, "ci");

    let caches = value.get("caches").and_then(|v| v.as_table()).unwrap();
    let profile = caches.get("ci").and_then(|v| v.as_table()).unwrap();
    assert_eq!(
        profile.get("url").and_then(|v| v.as_str()).unwrap(),
        expected_url
    );
    assert_eq!(
        profile.get("token").and_then(|v| v.as_str()).unwrap(),
        "secret-token"
    );
}

#[test]
fn use_sets_nix_conf_with_whoami_fallback() {
    if !can_bind_localhost() {
        eprintln!("Skipping httpmock tests: cannot bind to localhost");
        return;
    }

    let server = MockServer::start();
    mock_whoami(&server);

    let temp = TempDir::new().unwrap();
    let config_path = temp.path().join("client.toml");
    let xdg_path = temp.path().join("xdg");
    fs::create_dir_all(&xdg_path).unwrap();

    let config = format!(
        r#"
default_cache = "ci"

[caches.ci]
url = "{url}"
token = "secret-token"
"#,
        url = server.base_url().trim_end_matches('/')
    );
    fs::write(&config_path, config).unwrap();

    Command::cargo_bin("cellarctl")
        .unwrap()
        .arg("use")
        .arg("ci")
        .arg("--set-nix-conf")
        .arg("--client-config")
        .arg(&config_path)
        .env("XDG_CONFIG_HOME", &xdg_path)
        .assert()
        .success();

    let nix_conf = xdg_path.join("nix").join("nix.conf");
    let contents = fs::read_to_string(&nix_conf).unwrap();
    assert!(contents.contains("substituters = https://cache.example.com"));
    assert!(contents.contains("trusted-public-keys = cache-1:PUBLICKEY"));
}

#[test]
fn cache_list_requires_paired_flags() {
    Command::cargo_bin("cellarctl")
        .unwrap()
        .arg("cache")
        .arg("--server")
        .arg("http://localhost:8080")
        .arg("list")
        .assert()
        .failure()
        .stderr(contains("missing paired flag"));
}

#[test]
fn login_rejects_missing_scheme() {
    let temp = TempDir::new().unwrap();
    let config_path = temp.path().join("client.toml");

    Command::cargo_bin("cellarctl")
        .unwrap()
        .arg("login")
        .arg("ci")
        .arg("cache.example.com")
        .arg("--token")
        .arg("token")
        .arg("--client-config")
        .arg(&config_path)
        .assert()
        .failure()
        .stderr(contains("cache URL must start with http:// or https://"));
}
