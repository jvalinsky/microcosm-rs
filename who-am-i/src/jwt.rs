use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, errors::Error as JWTError};
use serde::Serialize;
use std::fs;
use std::io::Error as IOError;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TokensSetupError {
    #[error(transparent)]
    Io(#[from] IOError),
    #[error("failed to retrieve ec key: {0}")]
    FromEc(JWTError),
}

#[derive(Debug, Error)]
pub enum TokenMintingError {
    #[error("failed to mint: {0}")]
    FromEc(#[from] JWTError),
}

pub struct Tokens {
    encoding_key: EncodingKey,
}

impl Tokens {
    pub fn from_file(f: impl AsRef<Path>) -> Result<Self, TokensSetupError> {
        let data: Vec<u8> = fs::read(f)?;
        let encoding_key = EncodingKey::from_ec_pem(&data).map_err(TokensSetupError::FromEc)?;
        Ok(Self { encoding_key })
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
}

#[derive(Debug, Serialize)]
struct Claims {
    sub: String,
    exp: u64,
}
