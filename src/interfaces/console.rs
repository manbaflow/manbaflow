use axum::http::{HeaderValue, header};
use axum::response::{Html, IntoResponse, Response};

const HTML: &str = include_str!("web/console.html");
const CSS: &str = include_str!("web/console.css");
const JS: &str = include_str!("web/console.js");

pub async fn index() -> Response {
    secured(Html(HTML).into_response(), "text/html; charset=utf-8")
}

pub async fn stylesheet() -> Response {
    secured(CSS.into_response(), "text/css; charset=utf-8")
}

pub async fn script() -> Response {
    secured(JS.into_response(), "text/javascript; charset=utf-8")
}

fn secured(mut response: Response, content_type: &'static str) -> Response {
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    response
}
