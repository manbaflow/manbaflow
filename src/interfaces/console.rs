use axum::http::{HeaderValue, header};
use axum::response::{Html, IntoResponse, Response};

// 前端产物由 frontend/ 下的 Vite 构建产出，文件名固定（见 vite.config.js），
// 所以这里仍然是两个 include_str!，不需要目录级嵌入。
// 产物提交进仓库：这样没装 Node 的人也能直接 cargo build。
const HTML: &str = include_str!("web/index.html");
const JS: &str = include_str!("web/assets/app.js");

pub async fn index() -> Response {
    secured(Html(HTML).into_response(), "text/html; charset=utf-8")
}

pub async fn script() -> Response {
    secured(JS.into_response(), "text/javascript; charset=utf-8")
}

fn secured(mut response: Response, content_type: &'static str) -> Response {
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        // style-src 放开 'unsafe-inline'：antd v5 是 CSS-in-JS，运行时往
        // <head> 注入 <style>，不放开整个界面就是无样式的。脚本仍然只允许
        // 同源文件，inline script 依旧禁止——那才是 XSS 的主要入口。
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
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
