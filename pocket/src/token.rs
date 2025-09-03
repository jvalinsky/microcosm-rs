use jwt_compact::{Claims, UntrustedToken};
use atrium_crypto::did::parse_multikey;
use atrium_crypto::verify::Verifier;
use std::collections::HashMap;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct MiniDoc {
    signing_key: String,
}

pub async fn verify(
    expected_aud: &str,
    expected_lxm: &str,
    token: &str,
) -> Result<String, &'static str> {
    let untrusted = UntrustedToken::new(token).unwrap();

    let claims: Claims<HashMap<String, String>> = untrusted.deserialize_claims_unchecked().unwrap();

    let Some(did) = claims.custom.get("iss") else {
        return Err("jwt must include the user's did in `iss`");
    };

    if !did.starts_with("did:") {
        return Err("iss should be a did");
    }
    if did.contains("#") {
        return Err("iss should be a user did without a service identifier");
    }

    println!("Claims: {claims:#?}");
    println!("did: {did:#?}");

    let endpoint = "https://slingshot.microcosm.blue/xrpc/com.bad-example.identity.resolveMiniDoc";
    let doc: MiniDoc = reqwest::get(format!("{endpoint}?identifier={did}"))
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    log::info!("got minidoc response: {doc:?}");

    let (alg, public_key) = parse_multikey(&doc.signing_key).unwrap();
    log::info!("parsed key: {public_key:?}");

    Verifier::default().verify(
        alg,
        &public_key,
        &untrusted.signed_data,
        untrusted.signature_bytes(),
    ).unwrap();
    // if this passes, then our claims were trustworthy after all(??)

    let Some(aud) = claims.custom.get("aud") else {
        return Err("missing aud");
    };
    if aud != expected_aud {
        return Err("wrong aud");
    }
    let Some(lxm) = claims.custom.get("lxm") else {
        return Err("missing lxm");
    };
    if lxm != expected_lxm {
        return Err("wrong lxm");
    }

    Ok(did.to_string())
}
