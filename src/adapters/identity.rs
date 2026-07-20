use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{Duration, Utc};
use hmac::{Hmac, KeyInit, Mac};
use openidconnect::core::{CoreAuthenticationFlow, CoreClient, CoreProviderMetadata};
use openidconnect::reqwest;
use openidconnect::{
    AccessTokenHash, AuthorizationCode, ClientId, ClientSecret, CsrfToken, IssuerUrl, Nonce,
    OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope, TokenResponse,
};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::{MambaError, Result};

const LOGIN_TTL_MINUTES: i64 = 10;

#[derive(Clone)]
pub struct OidcProvider {
    metadata: CoreProviderMetadata,
    client_id: String,
    client_secret: String,
    redirect_url: String,
    secure_cookie: bool,
    issuer: String,
    http_client: reqwest::Client,
    state_key: [u8; 32],
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PendingLogin {
    state: String,
    tenant_id: String,
    nonce: String,
    pkce_verifier: String,
    return_to: String,
    expires_at: chrono::DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OidcLoginStart {
    pub authorization_url: String,
    pub tenant_id: String,
    pub state_cookie: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OidcIdentity {
    pub tenant_id: String,
    pub subject: String,
    pub email: Option<String>,
    pub return_to: String,
}

impl OidcProvider {
    pub async fn from_env() -> Result<Option<Self>> {
        let Some(issuer) = env_value("MAMBA_OIDC_ISSUER")? else {
            return Ok(None);
        };
        let client_id = required_env("MAMBA_OIDC_CLIENT_ID")?;
        let client_secret = required_env("MAMBA_OIDC_CLIENT_SECRET")?;
        let redirect_url = required_env("MAMBA_OIDC_REDIRECT_URL")?;
        Self::discover(issuer, client_id, client_secret, redirect_url)
            .await
            .map(Some)
    }

    async fn discover(
        issuer: String,
        client_id: String,
        client_secret: String,
        redirect_url: String,
    ) -> Result<Self> {
        validate_issuer_url(&issuer)?;
        validate_redirect_url(&redirect_url)?;
        let secure_cookie = redirect_url.starts_with("https://");
        let http_client = reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| oidc_error("could not build HTTP client", error))?;
        let issuer_url = IssuerUrl::new(issuer.clone())
            .map_err(|error| oidc_error("invalid issuer URL", error))?;
        let metadata = CoreProviderMetadata::discover_async(issuer_url, &http_client)
            .await
            .map_err(|error| oidc_error("discovery failed", error))?;
        let mut key_material = client_secret.as_bytes().to_vec();
        key_material.extend_from_slice(b"\0mambaflow-oidc-state\0");
        key_material.extend_from_slice(issuer.as_bytes());
        let state_key = Sha256::digest(&key_material).into();
        Ok(Self {
            metadata,
            client_id,
            client_secret,
            redirect_url,
            secure_cookie,
            issuer,
            http_client,
            state_key,
        })
    }

    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    pub fn secure_cookie(&self) -> bool {
        self.secure_cookie
    }

    pub async fn begin_login(&self, tenant_id: &str, return_to: &str) -> Result<OidcLoginStart> {
        let return_to = validate_return_to(return_to)?;
        let client = CoreClient::from_provider_metadata(
            self.metadata.clone(),
            ClientId::new(self.client_id.clone()),
            Some(ClientSecret::new(self.client_secret.clone())),
        )
        .set_redirect_uri(
            RedirectUrl::new(self.redirect_url.clone())
                .map_err(|error| oidc_error("invalid redirect URL", error))?,
        );
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let (authorization_url, state, nonce) = client
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                CsrfToken::new_random,
                Nonce::new_random,
            )
            .add_scope(Scope::new("email".into()))
            .add_scope(Scope::new("profile".into()))
            .set_pkce_challenge(challenge)
            .url();
        let now = Utc::now();
        let pending = PendingLogin {
            state: state.secret().clone(),
            tenant_id: tenant_id.to_string(),
            nonce: nonce.secret().clone(),
            pkce_verifier: verifier.secret().clone(),
            return_to,
            expires_at: now + Duration::minutes(LOGIN_TTL_MINUTES),
        };
        let state_cookie = self.encode_state(&pending)?;
        Ok(OidcLoginStart {
            authorization_url: authorization_url.to_string(),
            tenant_id: tenant_id.to_string(),
            state_cookie,
        })
    }

    pub async fn complete_login(
        &self,
        code: &str,
        state: &str,
        state_cookie: &str,
    ) -> Result<OidcIdentity> {
        let pending = self.decode_state(state_cookie)?;
        if !bool::from(pending.state.as_bytes().ct_eq(state.as_bytes())) {
            return Err(MambaError::PermissionDenied("invalid OIDC state".into()));
        }
        if pending.expires_at <= Utc::now() {
            return Err(MambaError::PermissionDenied(
                "OIDC login state expired".into(),
            ));
        }
        let client = CoreClient::from_provider_metadata(
            self.metadata.clone(),
            ClientId::new(self.client_id.clone()),
            Some(ClientSecret::new(self.client_secret.clone())),
        )
        .set_redirect_uri(
            RedirectUrl::new(self.redirect_url.clone())
                .map_err(|error| oidc_error("invalid redirect URL", error))?,
        );
        let token_response = client
            .exchange_code(AuthorizationCode::new(code.to_string()))
            .map_err(|error| oidc_error("authorization code is not supported", error))?
            .set_pkce_verifier(PkceCodeVerifier::new(pending.pkce_verifier))
            .request_async(&self.http_client)
            .await
            .map_err(|error| oidc_error("token exchange failed", error))?;
        let id_token = token_response.id_token().ok_or_else(|| {
            MambaError::PermissionDenied("OIDC provider did not return an ID token".into())
        })?;
        let verifier = client.id_token_verifier();
        let nonce = Nonce::new(pending.nonce);
        let claims = id_token
            .claims(&verifier, &nonce)
            .map_err(|error| oidc_error("ID token verification failed", error))?;
        if let Some(expected_hash) = claims.access_token_hash() {
            let actual_hash = AccessTokenHash::from_token(
                token_response.access_token(),
                id_token
                    .signing_alg()
                    .map_err(|error| oidc_error("unsupported ID token algorithm", error))?,
                id_token
                    .signing_key(&verifier)
                    .map_err(|error| oidc_error("ID token signing key is unavailable", error))?,
            )
            .map_err(|error| oidc_error("access token hash could not be verified", error))?;
            if &actual_hash != expected_hash {
                return Err(MambaError::PermissionDenied(
                    "OIDC access token hash mismatch".into(),
                ));
            }
        }
        Ok(OidcIdentity {
            tenant_id: pending.tenant_id,
            subject: claims.subject().as_str().to_string(),
            email: claims.email().map(|email| email.as_str().to_string()),
            return_to: pending.return_to,
        })
    }

    fn encode_state(&self, state: &PendingLogin) -> Result<String> {
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(state)?);
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.state_key)
            .map_err(|_| MambaError::Validation("invalid OIDC state key".into()))?;
        mac.update(payload.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        Ok(format!("v1.{payload}.{signature}"))
    }

    fn decode_state(&self, value: &str) -> Result<PendingLogin> {
        let mut parts = value.split('.');
        if parts.next() != Some("v1") {
            return Err(MambaError::PermissionDenied(
                "invalid OIDC state cookie".into(),
            ));
        }
        let payload = parts
            .next()
            .ok_or_else(|| MambaError::PermissionDenied("invalid OIDC state cookie".into()))?;
        let signature = parts
            .next()
            .ok_or_else(|| MambaError::PermissionDenied("invalid OIDC state cookie".into()))?;
        if parts.next().is_some() {
            return Err(MambaError::PermissionDenied(
                "invalid OIDC state cookie".into(),
            ));
        }
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| MambaError::PermissionDenied("invalid OIDC state signature".into()))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.state_key)
            .map_err(|_| MambaError::Validation("invalid OIDC state key".into()))?;
        mac.update(payload.as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| MambaError::PermissionDenied("invalid OIDC state signature".into()))?;
        let payload = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| MambaError::PermissionDenied("invalid OIDC state payload".into()))?;
        serde_json::from_slice(&payload)
            .map_err(|_| MambaError::PermissionDenied("invalid OIDC state payload".into()))
    }
}

#[derive(Clone, Default)]
pub struct ScimAuthenticator {
    fallback_hash: Option<[u8; 32]>,
    tenant_hashes: BTreeMap<String, [u8; 32]>,
}

impl ScimAuthenticator {
    pub fn from_env() -> Result<Self> {
        let fallback_hash = if let Some(token) = env_value("MAMBA_SCIM_BEARER_TOKEN")? {
            validate_scim_token(&token)?;
            Some(Sha256::digest(token.as_bytes()).into())
        } else {
            None
        };
        let mut tenant_hashes = BTreeMap::new();
        if let Some(value) = env_value("MAMBA_SCIM_TOKENS_JSON")? {
            let tokens =
                serde_json::from_str::<BTreeMap<String, String>>(&value).map_err(|_| {
                    MambaError::Validation(
                        "MAMBA_SCIM_TOKENS_JSON must be a JSON object of Tenant IDs to tokens"
                            .into(),
                    )
                })?;
            for (tenant_id, token) in tokens {
                if !tenant_id.starts_with("TEN-") {
                    return Err(MambaError::Validation(format!(
                        "invalid Tenant ID in MAMBA_SCIM_TOKENS_JSON: {tenant_id}"
                    )));
                }
                validate_scim_token(&token)?;
                tenant_hashes.insert(tenant_id, Sha256::digest(token.as_bytes()).into());
            }
        }
        Ok(Self {
            fallback_hash,
            tenant_hashes,
        })
    }

    pub fn new(token: &str) -> Result<Self> {
        validate_scim_token(token)?;
        Ok(Self {
            fallback_hash: Some(Sha256::digest(token.as_bytes()).into()),
            tenant_hashes: BTreeMap::new(),
        })
    }

    pub fn enabled(&self) -> bool {
        self.fallback_hash.is_some() || !self.tenant_hashes.is_empty()
    }

    pub fn verify(&self, tenant_id: &str, token: &str) -> bool {
        let expected = if self.tenant_hashes.is_empty() {
            self.fallback_hash
        } else {
            self.tenant_hashes.get(tenant_id).copied()
        };
        let Some(expected) = expected else {
            return false;
        };
        let actual: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        bool::from(expected.ct_eq(&actual))
    }
}

fn validate_scim_token(token: &str) -> Result<()> {
    if token.chars().count() < 32 {
        return Err(MambaError::Validation(
            "SCIM bearer token must contain at least 32 characters".into(),
        ));
    }
    Ok(())
}

fn required_env(name: &str) -> Result<String> {
    env_value(name)?.ok_or_else(|| {
        MambaError::Validation(format!(
            "{name} is required when MAMBA_OIDC_ISSUER is configured"
        ))
    })
}

fn env_value(name: &str) -> Result<Option<String>> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| MambaError::Validation(format!("{name} must be valid UTF-8")))?;
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(MambaError::Validation(format!("{name} cannot be empty")));
    }
    Ok(Some(value))
}

fn validate_redirect_url(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| MambaError::Validation("invalid MAMBA_OIDC_REDIRECT_URL".into()))?;
    let secure = url.scheme() == "https";
    let loopback = url.scheme() == "http"
        && url
            .host_str()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"));
    if (!secure && !loopback) || url.query().is_some() || url.fragment().is_some() {
        return Err(MambaError::Validation(
            "OIDC redirect URL must use HTTPS or loopback HTTP and contain no query or fragment"
                .into(),
        ));
    }
    Ok(())
}

fn validate_issuer_url(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| MambaError::Validation("invalid MAMBA_OIDC_ISSUER".into()))?;
    let secure = url.scheme() == "https";
    let loopback = url.scheme() == "http"
        && url
            .host_str()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"));
    if (!secure && !loopback) || url.query().is_some() || url.fragment().is_some() {
        return Err(MambaError::Validation(
            "OIDC issuer must use HTTPS or loopback HTTP and contain no query or fragment".into(),
        ));
    }
    Ok(())
}

fn validate_return_to(value: &str) -> Result<String> {
    let value = value.trim();
    if value != "/console" {
        return Err(MambaError::Validation(
            "OIDC return path must be /console".into(),
        ));
    }
    Ok(value.to_string())
}

fn oidc_error(context: &str, error: impl std::fmt::Display) -> MambaError {
    MambaError::ExternalConnector(format!("OIDC {context}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex as StdMutex};

    use axum::body::Bytes;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use hmac::{Hmac, KeyInit, Mac};
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn scim_tokens_use_constant_time_hash_comparison() {
        let token = "a-strong-scim-provisioning-token-12345";
        let auth = ScimAuthenticator {
            fallback_hash: Some(Sha256::digest(token.as_bytes()).into()),
            tenant_hashes: BTreeMap::new(),
        };
        assert!(auth.verify("TEN-test", token));
        assert!(!auth.verify("TEN-test", "a-strong-scim-provisioning-token-xxxxx"));
    }

    #[test]
    fn tenant_scim_tokens_do_not_fall_back_across_tenants() {
        let tenant_token = "tenant-a-scim-provisioning-token-123456";
        let fallback_token = "fallback-scim-provisioning-token-12345";
        let auth = ScimAuthenticator {
            fallback_hash: Some(Sha256::digest(fallback_token.as_bytes()).into()),
            tenant_hashes: BTreeMap::from([(
                "TEN-a".into(),
                Sha256::digest(tenant_token.as_bytes()).into(),
            )]),
        };

        assert!(auth.verify("TEN-a", tenant_token));
        assert!(!auth.verify("TEN-a", fallback_token));
        assert!(!auth.verify("TEN-b", fallback_token));
        assert!(!auth.verify("TEN-b", tenant_token));
    }

    #[test]
    fn oidc_redirects_and_return_paths_are_closed() {
        assert!(validate_redirect_url("https://flow.example/auth/oidc/callback").is_ok());
        assert!(validate_redirect_url("http://127.0.0.1:7777/auth/oidc/callback").is_ok());
        assert!(validate_redirect_url("http://evil.example/callback").is_err());
        assert!(validate_return_to("https://evil.example").is_err());
    }

    #[tokio::test]
    async fn oidc_discovery_pkce_nonce_signature_and_state_integrity_are_enforced() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let issuer = format!("http://{address}");
        let client_id = "mamba-test-client".to_string();
        let client_secret = "mamba-test-client-secret-at-least-32-bytes".to_string();
        let nonce = Arc::new(StdMutex::new(None::<String>));
        let metadata_issuer = issuer.clone();
        let metadata_client = client_id.clone();
        let token_issuer = issuer.clone();
        let token_secret = client_secret.clone();
        let token_nonce = nonce.clone();
        let service = Router::new()
            .route(
                "/.well-known/openid-configuration",
                get(move || {
                    let issuer = metadata_issuer.clone();
                    async move {
                        Json(json!({
                            "issuer": issuer,
                            "authorization_endpoint": format!("{issuer}/authorize"),
                            "token_endpoint": format!("{issuer}/token"),
                            "jwks_uri": format!("{issuer}/jwks"),
                            "response_types_supported": ["code"],
                            "subject_types_supported": ["public"],
                            "id_token_signing_alg_values_supported": ["HS256"],
                            "token_endpoint_auth_methods_supported": ["client_secret_basic"]
                        }))
                    }
                }),
            )
            .route("/jwks", get(|| async { Json(json!({"keys": []})) }))
            .route(
                "/token",
                post(move |body: Bytes| {
                    let issuer = token_issuer.clone();
                    let client_id = metadata_client.clone();
                    let secret = token_secret.clone();
                    let nonce = token_nonce.clone();
                    async move {
                        let form = serde_urlencoded::from_bytes::<BTreeMap<String, String>>(&body)
                            .unwrap();
                        assert_eq!(form.get("code").map(String::as_str), Some("valid-code"));
                        assert!(
                            form.get("code_verifier")
                                .is_some_and(|value| value.len() >= 43)
                        );
                        let nonce = nonce.lock().unwrap().clone().unwrap();
                        let now = Utc::now().timestamp();
                        let claims = json!({
                            "iss": issuer,
                            "sub": "oidc-subject-42",
                            "aud": client_id,
                            "exp": now + 300,
                            "iat": now,
                            "nonce": nonce,
                            "email": "pilot@example.com",
                            "email_verified": true
                        });
                        let id_token = hmac_jwt(&claims, secret.as_bytes());
                        Json(json!({
                            "access_token": "access-token",
                            "token_type": "Bearer",
                            "expires_in": 300,
                            "id_token": id_token
                        }))
                    }
                }),
            );
        let server = tokio::spawn(async move { axum::serve(listener, service).await.unwrap() });

        let provider = OidcProvider::discover(
            issuer,
            client_id,
            client_secret,
            "http://127.0.0.1:7777/auth/oidc/callback".into(),
        )
        .await
        .unwrap();
        let login = provider.begin_login("TEN-test", "/console").await.unwrap();
        let authorization = reqwest::Url::parse(&login.authorization_url).unwrap();
        let values = authorization
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<BTreeMap<_, _>>();
        *nonce.lock().unwrap() = values.get("nonce").cloned();
        let state = values.get("state").unwrap();
        assert_eq!(
            values.get("code_challenge_method").map(String::as_str),
            Some("S256")
        );

        let identity = provider
            .complete_login("valid-code", state, &login.state_cookie)
            .await
            .unwrap();
        assert_eq!(identity.tenant_id, "TEN-test");
        assert_eq!(identity.subject, "oidc-subject-42");
        assert_eq!(identity.email.as_deref(), Some("pilot@example.com"));
        let mut tampered = login.state_cookie.clone();
        tampered.push('x');
        assert!(
            provider
                .complete_login("valid-code", state, &tampered)
                .await
                .is_err()
        );
        server.abort();
    }

    fn hmac_jwt(claims: &serde_json::Value, secret: &[u8]) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
        let signing_input = format!("{header}.{claims}");
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret).unwrap();
        mac.update(signing_input.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        format!("{signing_input}.{signature}")
    }
}
