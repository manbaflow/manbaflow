//! 用户模型凭据的对称加密。
//!
//! 这是整个系统里唯一**需要还原明文**的秘密：Worker 得拿真实的 API Key 去调模型。
//! 接入令牌不同——那些只存哈希，验证时比对哈希即可，永远不需要还原。
//!
//! 主密钥来自环境变量，本身不进数据库也不进 Ledger。没配主密钥时整个功能直接
//! 关闭并说明原因，而不是降级成明文存储——那种"能用但不安全"的中间状态最危险。
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};

use crate::error::{RelayError, Result};

pub const KEY_ENV: &str = "RELAY_CREDENTIAL_KEY";
const NONCE_LEN: usize = 12;

pub struct SecretBox {
    key: LessSafeKey,
    rng: SystemRandom,
}

impl SecretBox {
    /// 从环境变量读主密钥。未配置返回 `None`——调用方据此判断功能是否可用。
    pub fn from_env() -> Result<Option<Self>> {
        let Some(raw) = std::env::var(KEY_ENV).ok().filter(|v| !v.trim().is_empty()) else {
            return Ok(None);
        };
        Self::new(raw.trim()).map(Some)
    }

    /// 主密钥是 64 个十六进制字符（32 字节）。用 `openssl rand -hex 32` 生成。
    pub fn new(hex_key: &str) -> Result<Self> {
        let bytes = decode_hex(hex_key)?;
        if bytes.len() != 32 {
            return Err(RelayError::Validation(format!(
                "{KEY_ENV} 需要 32 字节（64 个十六进制字符），当前 {} 字节",
                bytes.len()
            )));
        }
        let key = UnboundKey::new(&AES_256_GCM, &bytes)
            .map_err(|_| RelayError::Validation("主密钥不可用".into()))?;
        Ok(Self {
            key: LessSafeKey::new(key),
            rng: SystemRandom::new(),
        })
    }

    /// 返回 (nonce, 密文)。每次加密都用新的随机 nonce——同一把密钥下 nonce 复用
    /// 会让 GCM 的安全性直接崩掉。
    pub fn seal(&self, plaintext: &str) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        self.rng
            .fill(&mut nonce_bytes)
            .map_err(|_| RelayError::Validation("随机数生成失败".into()))?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let mut buffer = plaintext.as_bytes().to_vec();
        self.key
            .seal_in_place_append_tag(nonce, Aad::empty(), &mut buffer)
            .map_err(|_| RelayError::Validation("加密失败".into()))?;
        Ok((nonce_bytes.to_vec(), buffer))
    }

    pub fn open(&self, nonce: &[u8], ciphertext: &[u8]) -> Result<String> {
        if nonce.len() != NONCE_LEN {
            return Err(RelayError::Validation("nonce 长度不对".into()));
        }
        let mut nonce_bytes = [0u8; NONCE_LEN];
        nonce_bytes.copy_from_slice(nonce);
        let mut buffer = ciphertext.to_vec();
        let plaintext = self
            .key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::empty(),
                &mut buffer,
            )
            // 换过主密钥就会走到这里。说清楚是密钥不匹配，别让人去查数据损坏。
            .map_err(|_| RelayError::Validation(format!("凭据解密失败：{KEY_ENV} 可能已更换")))?;
        String::from_utf8(plaintext.to_vec())
            .map_err(|_| RelayError::Validation("凭据不是合法的 UTF-8".into()))
    }
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(RelayError::Validation(format!(
            "{KEY_ENV} 必须是十六进制字符串"
        )));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|_| RelayError::Validation(format!("{KEY_ENV} 不是合法的十六进制")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "5f4dcc3b5aa765d61d8327deb882cf995f4dcc3b5aa765d61d8327deb882cf99";

    #[test]
    fn sealed_credentials_round_trip() {
        let box_ = SecretBox::new(KEY).unwrap();
        let (nonce, ciphertext) = box_.seal("sk-ant-secret").unwrap();
        assert_ne!(ciphertext, b"sk-ant-secret");
        assert_eq!(box_.open(&nonce, &ciphertext).unwrap(), "sk-ant-secret");
    }

    #[test]
    fn each_encryption_uses_a_fresh_nonce() {
        let box_ = SecretBox::new(KEY).unwrap();
        let (first, _) = box_.seal("same").unwrap();
        let (second, _) = box_.seal("same").unwrap();
        assert_ne!(first, second, "nonce 复用会让 GCM 失去安全性");
    }

    #[test]
    fn a_different_master_key_cannot_open_it() {
        let (nonce, ciphertext) = SecretBox::new(KEY).unwrap().seal("sk-ant-secret").unwrap();
        let other =
            SecretBox::new("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff")
                .unwrap();
        assert!(other.open(&nonce, &ciphertext).is_err());
    }

    #[test]
    fn short_keys_are_rejected() {
        assert!(SecretBox::new("abcd").is_err());
    }
}
