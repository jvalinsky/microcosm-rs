use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, errors::Error as JWTError};
use serde::Serialize;
use std::fs;
use std::io::Error as IOError;
use std::path::Path;
use std::string::FromUtf8Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TokensSetupError {
    #[error("failed to read private key")]
    ReadPrivateKey(IOError),
    #[error("failed to retrieve private key: {0}")]
    PrivateKey(JWTError),
    #[error("failed to read private key")]
    ReadJwks(IOError),
    #[error("failed to retrieve jwks: {0}")]
    DecodeJwks(FromUtf8Error),
}

#[derive(Debug, Error)]
pub enum TokenMintingError {
    #[error("failed to mint: {0}")]
    EncodingError(#[from] JWTError),
}

pub struct Tokens {
    encoding_key: EncodingKey,
    jwks: String,
}

impl Tokens {
    pub fn from_files(
        priv_f: impl AsRef<Path>,
        jwks_f: impl AsRef<Path>,
    ) -> Result<Self, TokensSetupError> {
        let private_key_data: Vec<u8> =
            fs::read(priv_f).map_err(TokensSetupError::ReadPrivateKey)?;
        let encoding_key =
            EncodingKey::from_ec_pem(&private_key_data).map_err(TokensSetupError::PrivateKey)?;

        let jwks_data: Vec<u8> = fs::read(jwks_f).map_err(TokensSetupError::ReadJwks)?;
        let jwks = String::from_utf8(jwks_data).map_err(TokensSetupError::DecodeJwks)?;

        Ok(Self { encoding_key, jwks })
    }

    pub fn mint(&self, t: impl ToString) -> Result<String, TokenMintingError> {
        let sub = t.to_string();

        let dt_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("unix epoch is in the past");
        let dt_exp = dt_now + Duration::from_secs(30 * 86_400);
        let exp = dt_exp.as_secs();

        Ok(encode(
            &Header::new(Algorithm::ES256),
            &Claims { sub, exp },
            &self.encoding_key,
        )?)
    }

    pub fn jwks(&self) -> String {
        self.jwks.clone()
    }
}

#[derive(Debug, Serialize)]
struct Claims {
    sub: String,
    exp: u64,
}
