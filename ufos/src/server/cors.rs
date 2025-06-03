use dropshot::{HttpError, HttpResponseHeaders, HttpResponseOk};
use schemars::JsonSchema;
use serde::Serialize;

pub type OkCorsResponse<T> = Result<HttpResponseHeaders<HttpResponseOk<T>>, HttpError>;

/// Helper for constructing Ok responses: return OkCors(T).into()
/// (not happy with this yet)
pub struct OkCors<T: Serialize + JsonSchema + Send + Sync>(pub T);

impl<T> From<OkCors<T>> for OkCorsResponse<T>
where
    T: Serialize + JsonSchema + Send + Sync,
{
    fn from(ok: OkCors<T>) -> OkCorsResponse<T> {
        let mut res = HttpResponseHeaders::new_unnamed(HttpResponseOk(ok.0));
        res.headers_mut()
            .insert("access-control-allow-origin", "*".parse().unwrap());
        Ok(res)
    }
}

// TODO: cors for HttpError
