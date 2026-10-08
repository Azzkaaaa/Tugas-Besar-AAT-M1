use std::{env, fmt, fs, io, time::Duration};

use serde::Deserialize;

#[derive(Debug)]
pub struct Config {
    pub signing_key: String,
    pub client_ids: Vec<String>,
    pub project_id: String,
    pub organization_id: String,
    pub port: u16,
}

/// The subset of provisioning output this service needs, written by auth-bootstrap
/// to its own volume so client secrets are never mounted here.
#[derive(Deserialize, Default)]
#[serde(default)]
struct ActionsFile {
    actions_signing_key: String,
    client_ids: Vec<String>,
    project_id: String,
    organization_id: String,
}

#[derive(Debug)]
pub enum Error {
    /// auth-bootstrap has not written (all of) the file yet.
    Pending(String),
    Invalid(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Pending(reason) | Error::Invalid(reason) => f.write_str(reason),
        }
    }
}

pub fn port() -> Result<u16, Error> {
    env::var("PORT").map_or(Ok(3002), |value| value.parse().map_err(|_| Error::Invalid("Invalid PORT".into())))
}

pub fn load() -> Result<Config, Error> {
    let path = env::var("ACTIONS_CONFIG_FILE").unwrap_or_else(|_| "/actions/actions.json".into());
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(Error::Pending("Actions configuration not provisioned yet".into()));
        }
        Err(_) => return Err(Error::Invalid("Cannot read actions configuration".into())),
    };
    let file: ActionsFile = serde_json::from_str(&raw).map_err(|_| Error::Invalid("Invalid actions configuration".into()))?;
    for (name, value) in
        [("actions_signing_key", &file.actions_signing_key), ("project_id", &file.project_id), ("organization_id", &file.organization_id)]
    {
        if value.is_empty() {
            return Err(Error::Pending(format!("Missing {name}")));
        }
    }
    if file.client_ids.is_empty() || file.client_ids.iter().any(String::is_empty) {
        return Err(Error::Pending("Missing client_ids".into()));
    }
    Ok(Config {
        signing_key: file.actions_signing_key,
        client_ids: file.client_ids,
        project_id: file.project_id,
        organization_id: file.organization_id,
        port: port()?,
    })
}

/// auth-bootstrap writes the file only after Zitadel can resolve this container (it
/// registers the Actions target URL), so start first and wait for it.
pub async fn provisioned() -> Result<Config, Error> {
    for attempt in 0.. {
        match load() {
            Err(Error::Pending(_)) if attempt < 300 => {
                if attempt == 0 {
                    println!(r#"{{"event":"waiting_for_provisioning"}}"#);
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            result => return result,
        }
    }
    unreachable!()
}
