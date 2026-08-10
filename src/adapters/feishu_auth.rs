//! 飞书扫码/免登登录。
//!
//! 为什么不复用 `identity.rs` 的 OIDC 实现：飞书**不提供** OIDC Discovery
//! (`.well-known/openid-configuration`)，也不签发 ID Token，因此
//! `CoreProviderMetadata::discover_async` 那条路走不通。这里按飞书自己的三段式
//! OAuth 实现，端点与集群内 GitLab 现用的 `oauth2_generic` 配置保持一致，
//! 这样同一个人在 GitLab 和 Relay 拿到的 `open_id` 完全相同：
//!
//! ```text
//! authorize  GET  https://open.feishu.cn/open-apis/authen/v1/authorize
//! token      POST https://open.feishu.cn/open-apis/authen/v2/oauth/token
//! user_info  GET  https://open.feishu.cn/open-apis/authen/v1/user_info
//! ```
//!
//! **没有用 PKCE**：飞书文档未声明 `authen/v2/oauth/token` 接受 `code_verifier`，
//! 传未定义参数有被拒风险。这里是机密客户端——换码必须带 `client_secret`，
//! 回调地址也在飞书后台登记过，授权码被截获也换不出 Token。CSRF 由下面的
//! HMAC 签名 state cookie 挡住。等飞书正式支持后可以再补。

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{Duration, Utc};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::{RelayError, Result};

const LOGIN_TTL_MINUTES: i64 = 10;
const AUTHORIZE_ENDPOINT: &str = "https://open.feishu.cn/open-apis/authen/v1/authorize";
const TOKEN_ENDPOINT: &str = "https://open.feishu.cn/open-apis/authen/v2/oauth/token";
const USER_INFO_ENDPOINT: &str = "https://open.feishu.cn/open-apis/authen/v1/user_info";

/// 登录后自动开户的边界。
///
/// 飞书的 `user_info` **不返回部门**，要按部门过滤得额外申请
/// `contact:user.base:readonly` 并调用通讯录接口。当前按企业维度放行：
/// `tenant_key` 命中即认为是本企业员工。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeishuProvisioning {
    allowed_tenant_keys: Vec<String>,
}

impl FeishuProvisioning {
    pub fn allows(&self, tenant_key: &str) -> bool {
        self.allowed_tenant_keys
            .iter()
            .any(|allowed| allowed == tenant_key)
    }
}

#[derive(Clone)]
pub struct FeishuProvider {
    app_id: String,
    app_secret: String,
    redirect_url: String,
    provisioning: FeishuProvisioning,
    secure_cookie: bool,
    http_client: reqwest::Client,
    state_key: [u8; 32],
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PendingLogin {
    state: String,
    tenant_id: String,
    return_to: String,
    expires_at: chrono::DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeishuLoginStart {
    pub authorization_url: String,
    pub tenant_id: String,
    pub state_cookie: String,
}

/// 一次成功登录解析出的飞书身份。
///
/// `open_id` 是应用内稳定的用户标识，也是 GitLab `oauth2_generic` 的
/// `uid_field`，两边天然对齐。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeishuIdentity {
    pub tenant_id: String,
    pub open_id: String,
    pub name: String,
    pub email: Option<String>,
    pub tenant_key: String,
    pub return_to: String,
}

impl FeishuProvider {
    pub fn from_env() -> Result<Option<Self>> {
        let Some(app_id) = env_value("RELAY_FEISHU_APP_ID")? else {
            return Ok(None);
        };
        let app_secret = required_env("RELAY_FEISHU_APP_SECRET")?;
        let redirect_url = required_env("RELAY_FEISHU_REDIRECT_URL")?;
        // 白名单必须显式配置。缺省不放行任何人，避免把「自动开户」误配成
        // 「任何飞书用户都能进」——外部企业的用户同样能走完 OAuth 流程。
        let allowed = required_env("RELAY_FEISHU_ALLOWED_TENANT_KEYS")?;
        Self::new(app_id, app_secret, redirect_url, &allowed)
    }

    pub fn new(
        app_id: String,
        app_secret: String,
        redirect_url: String,
        allowed_tenant_keys: &str,
    ) -> Result<Option<Self>> {
        validate_redirect_url(&redirect_url)?;
        let allowed_tenant_keys = parse_tenant_keys(allowed_tenant_keys)?;
        let secure_cookie = redirect_url.starts_with("https://");
        // crate 用的是 rustls-no-provider，构建 reqwest client 之前必须先装 provider，
        // 否则直接 panic。已装过会返回 Err，忽略即可。
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http_client = reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| feishu_error("could not build HTTP client", error))?;
        let mut key_material = app_secret.as_bytes().to_vec();
        key_material.extend_from_slice(b"\0relay-feishu-state\0");
        key_material.extend_from_slice(app_id.as_bytes());
        let state_key = Sha256::digest(&key_material).into();
        Ok(Some(Self {
            app_id,
            app_secret,
            redirect_url,
            provisioning: FeishuProvisioning {
                allowed_tenant_keys,
            },
            secure_cookie,
            http_client,
            state_key,
        }))
    }

    pub fn secure_cookie(&self) -> bool {
        self.secure_cookie
    }

    pub fn provisioning(&self) -> &FeishuProvisioning {
        &self.provisioning
    }

    pub fn begin_login(&self, tenant_id: &str, return_to: &str) -> Result<FeishuLoginStart> {
        let return_to = validate_return_to(return_to)?;
        let state = random_state();
        let now = Utc::now();
        let pending = PendingLogin {
            state: state.clone(),
            tenant_id: tenant_id.to_string(),
            return_to,
            expires_at: now + Duration::minutes(LOGIN_TTL_MINUTES),
        };
        let state_cookie = self.encode_state(&pending)?;
        let query = serde_urlencoded::to_string([
            ("client_id", self.app_id.as_str()),
            ("redirect_uri", self.redirect_url.as_str()),
            ("response_type", "code"),
            ("state", state.as_str()),
        ])
        .map_err(|error| feishu_error("could not build authorization URL", error))?;
        Ok(FeishuLoginStart {
            authorization_url: format!("{AUTHORIZE_ENDPOINT}?{query}"),
            tenant_id: tenant_id.to_string(),
            state_cookie,
        })
    }

    pub async fn complete_login(
        &self,
        code: &str,
        state: &str,
        state_cookie: &str,
    ) -> Result<FeishuIdentity> {
        let pending = self.decode_state(state_cookie)?;
        if !bool::from(pending.state.as_bytes().ct_eq(state.as_bytes())) {
            return Err(RelayError::PermissionDenied("invalid Feishu state".into()));
        }
        if pending.expires_at <= Utc::now() {
            return Err(RelayError::PermissionDenied(
                "Feishu login state expired".into(),
            ));
        }
        let access_token = self.exchange_code(code).await?;
        let profile = self.fetch_user_info(&access_token).await?;

        // 外部企业的用户也能走完整个 OAuth 流程，所以这一步不能省。
        if !self.provisioning.allows(&profile.tenant_key) {
            // 把被拒的 tenant_key 记进服务端日志：首次部署时管理员往往不知道本企业的
            // tenant_key（查询它需要 tenant:tenant:readonly 权限），让第一次登录失败
            // 直接把值告诉运维，比让人去翻后台快得多。
            // 只进日志不进 HTTP 响应——没必要告诉浏览器端本部署放行了哪些企业。
            eprintln!(
                "Feishu sign-in rejected: tenant_key={} is not in RELAY_FEISHU_ALLOWED_TENANT_KEYS",
                profile.tenant_key
            );
            return Err(RelayError::PermissionDenied(
                "Feishu tenant is not allowed to sign in".into(),
            ));
        }
        if profile.open_id.trim().is_empty() {
            return Err(RelayError::PermissionDenied(
                "Feishu did not return an open_id".into(),
            ));
        }
        let name = profile.name.trim();
        let name = if name.is_empty() {
            profile.open_id.clone()
        } else {
            name.to_string()
        };
        Ok(FeishuIdentity {
            tenant_id: pending.tenant_id,
            open_id: profile.open_id,
            name,
            email: profile
                .enterprise_email
                .or(profile.email)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            tenant_key: profile.tenant_key,
            return_to: pending.return_to,
        })
    }

    async fn exchange_code(&self, code: &str) -> Result<String> {
        let response = self
            .http_client
            .post(TOKEN_ENDPOINT)
            .json(&serde_json::json!({
                "grant_type": "authorization_code",
                "client_id": self.app_id,
                "client_secret": self.app_secret,
                "code": code,
                "redirect_uri": self.redirect_url,
            }))
            .send()
            .await
            .map_err(|error| feishu_error("token request failed", error))?;
        let body = response
            .text()
            .await
            .map_err(|error| feishu_error("token response could not be read", error))?;
        let token = serde_json::from_str::<TokenResponse>(&body)
            .map_err(|error| feishu_error("token response could not be parsed", error))?;
        // v2 把业务错误放在 HTTP 200 的 body 里，不能只看状态码。
        if token.code != 0 {
            return Err(RelayError::PermissionDenied(format!(
                "Feishu rejected the authorization code (code {})",
                token.code
            )));
        }
        let access_token = token.access_token.unwrap_or_default();
        if access_token.trim().is_empty() {
            return Err(RelayError::PermissionDenied(
                "Feishu did not return a user access token".into(),
            ));
        }
        Ok(access_token)
    }

    async fn fetch_user_info(&self, access_token: &str) -> Result<UserInfo> {
        let response = self
            .http_client
            .get(USER_INFO_ENDPOINT)
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|error| feishu_error("user_info request failed", error))?;
        let body = response
            .text()
            .await
            .map_err(|error| feishu_error("user_info response could not be read", error))?;
        let payload = serde_json::from_str::<UserInfoResponse>(&body)
            .map_err(|error| feishu_error("user_info response could not be parsed", error))?;
        if payload.code != 0 {
            return Err(RelayError::PermissionDenied(format!(
                "Feishu rejected the user_info request (code {})",
                payload.code
            )));
        }
        payload
            .data
            .ok_or_else(|| RelayError::PermissionDenied("Feishu returned no user profile".into()))
    }

    fn encode_state(&self, state: &PendingLogin) -> Result<String> {
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(state)?);
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.state_key)
            .map_err(|_| RelayError::Validation("invalid Feishu state key".into()))?;
        mac.update(payload.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        Ok(format!("v1.{payload}.{signature}"))
    }

    fn decode_state(&self, value: &str) -> Result<PendingLogin> {
        let mut parts = value.split('.');
        if parts.next() != Some("v1") {
            return Err(RelayError::PermissionDenied(
                "invalid Feishu state cookie".into(),
            ));
        }
        let payload = parts
            .next()
            .ok_or_else(|| RelayError::PermissionDenied("invalid Feishu state cookie".into()))?;
        let signature = parts
            .next()
            .ok_or_else(|| RelayError::PermissionDenied("invalid Feishu state cookie".into()))?;
        if parts.next().is_some() {
            return Err(RelayError::PermissionDenied(
                "invalid Feishu state cookie".into(),
            ));
        }
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| RelayError::PermissionDenied("invalid Feishu state signature".into()))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.state_key)
            .map_err(|_| RelayError::Validation("invalid Feishu state key".into()))?;
        mac.update(payload.as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| RelayError::PermissionDenied("invalid Feishu state signature".into()))?;
        let payload = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| RelayError::PermissionDenied("invalid Feishu state payload".into()))?;
        serde_json::from_slice(&payload)
            .map_err(|_| RelayError::PermissionDenied("invalid Feishu state payload".into()))
    }
}

#[derive(serde::Deserialize)]
struct TokenResponse {
    #[serde(default)]
    code: i64,
    access_token: Option<String>,
}

#[derive(serde::Deserialize)]
struct UserInfoResponse {
    #[serde(default)]
    code: i64,
    data: Option<UserInfo>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct UserInfo {
    #[serde(default)]
    open_id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    tenant_key: String,
    email: Option<String>,
    enterprise_email: Option<String>,
}

fn parse_tenant_keys(value: &str) -> Result<Vec<String>> {
    let keys = value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| entry.to_string())
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Err(RelayError::Validation(
            "RELAY_FEISHU_ALLOWED_TENANT_KEYS must list at least one tenant key".into(),
        ));
    }
    if let Some(invalid) = keys
        .iter()
        .find(|key| key.chars().count() > 64 || key.chars().any(|c| c.is_control()))
    {
        return Err(RelayError::Validation(format!(
            "invalid Feishu tenant key: {invalid}"
        )));
    }
    Ok(keys)
}

fn random_state() -> String {
    let mut material = uuid::Uuid::new_v4().as_bytes().to_vec();
    material.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    URL_SAFE_NO_PAD.encode(material)
}

fn required_env(name: &str) -> Result<String> {
    env_value(name)?.ok_or_else(|| {
        RelayError::Validation(format!(
            "{name} is required when RELAY_FEISHU_APP_ID is configured"
        ))
    })
}

fn env_value(name: &str) -> Result<Option<String>> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| RelayError::Validation(format!("{name} must be valid UTF-8")))?;
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(RelayError::Validation(format!("{name} cannot be empty")));
    }
    Ok(Some(value))
}

fn validate_redirect_url(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| RelayError::Validation("invalid RELAY_FEISHU_REDIRECT_URL".into()))?;
    let secure = url.scheme() == "https";
    let loopback = url.scheme() == "http"
        && url
            .host_str()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"));
    if (!secure && !loopback) || url.query().is_some() || url.fragment().is_some() {
        return Err(RelayError::Validation(
            "Feishu redirect URL must use HTTPS or loopback HTTP and contain no query or fragment"
                .into(),
        ));
    }
    Ok(())
}

fn validate_return_to(value: &str) -> Result<String> {
    let value = value.trim();
    if value != "/console" {
        return Err(RelayError::Validation(
            "Feishu return path must be /console".into(),
        ));
    }
    Ok(value.to_string())
}

fn feishu_error(context: &str, error: impl std::fmt::Display) -> RelayError {
    RelayError::ExternalConnector(format!("Feishu {context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> FeishuProvider {
        FeishuProvider::new(
            "cli_test".into(),
            "secret-test".into(),
            "https://relay.example.com/auth/feishu/callback".into(),
            "tk_allowed",
        )
        .unwrap()
        .unwrap()
    }

    #[test]
    fn authorization_url_targets_feishu_and_carries_signed_state() {
        let provider = provider();
        let login = provider.begin_login("TEN-test", "/console").unwrap();
        assert!(login.authorization_url.starts_with(AUTHORIZE_ENDPOINT));
        assert!(login.authorization_url.contains("client_id=cli_test"));
        assert!(login.authorization_url.contains("response_type=code"));
        // state 必须同时出现在跳转地址和签名 cookie 里，否则回调无从校验。
        let pending = provider.decode_state(&login.state_cookie).unwrap();
        assert!(
            login
                .authorization_url
                .contains(&format!("state={}", urlencode(&pending.state)))
        );
        assert_eq!(pending.tenant_id, "TEN-test");
        assert_eq!(pending.return_to, "/console");
    }

    #[test]
    fn state_cookie_signature_is_rejected_after_tampering() {
        let provider = provider();
        let login = provider.begin_login("TEN-test", "/console").unwrap();
        let mut parts = login.state_cookie.split('.');
        let payload = parts.nth(1).unwrap().to_string();
        let forged = format!("v1.{payload}.{}", URL_SAFE_NO_PAD.encode([0u8; 32]));
        assert!(provider.decode_state(&forged).is_err());
    }

    #[test]
    fn state_cookie_from_another_app_secret_is_rejected() {
        let mine = provider();
        let theirs = FeishuProvider::new(
            "cli_test".into(),
            "a-different-secret".into(),
            "https://relay.example.com/auth/feishu/callback".into(),
            "tk_allowed",
        )
        .unwrap()
        .unwrap();
        let login = theirs.begin_login("TEN-test", "/console").unwrap();
        assert!(mine.decode_state(&login.state_cookie).is_err());
    }

    #[test]
    fn only_allowed_tenant_keys_may_provision() {
        let provider = provider();
        assert!(provider.provisioning().allows("tk_allowed"));
        // 外部企业的用户也能走完 OAuth，必须在这里挡住。
        assert!(!provider.provisioning().allows("tk_someone_else"));
    }

    #[test]
    fn allowed_tenant_keys_must_not_be_empty() {
        let error = FeishuProvider::new(
            "cli_test".into(),
            "secret-test".into(),
            "https://relay.example.com/auth/feishu/callback".into(),
            "  ,  ",
        );
        assert!(error.is_err());
    }

    #[test]
    fn redirect_url_must_be_https_or_loopback() {
        assert!(validate_redirect_url("https://relay.example.com/auth/feishu/callback").is_ok());
        assert!(validate_redirect_url("http://127.0.0.1:7777/auth/feishu/callback").is_ok());
        assert!(validate_redirect_url("http://relay.example.com/auth/feishu/callback").is_err());
        assert!(validate_redirect_url("https://relay.example.com/cb?next=/admin").is_err());
    }

    #[test]
    fn return_path_is_restricted_to_the_console() {
        assert!(validate_return_to("/console").is_ok());
        assert!(validate_return_to("https://evil.example.com").is_err());
        assert!(validate_return_to("//evil.example.com").is_err());
    }

    fn urlencode(value: &str) -> String {
        serde_urlencoded::to_string([("v", value)])
            .unwrap()
            .trim_start_matches("v=")
            .to_string()
    }
}
