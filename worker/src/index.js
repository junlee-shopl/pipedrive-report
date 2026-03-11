// Shopl 영업 보고서 대시보드 — IP 제한 프록시
// 허용 IP 목록에 없으면 403 반환
// 환경변수 ALLOWED_IPS: 쉼표 구분 IP 목록 (Cloudflare Worker Secret)

const ORIGIN = "https://junlee-shopl.github.io";
const PATH_PREFIX = "/pipedrive-report";

export default {
  async fetch(request, env) {
    const allowedIPs = (env.ALLOWED_IPS || "").split(",").map(ip => ip.trim()).filter(Boolean);
    const clientIP = request.headers.get("CF-Connecting-IP");

    if (!allowedIPs.includes(clientIP)) {
      return new Response(
        `<!DOCTYPE html><html><head><meta charset="utf-8"><title>접근 제한</title></head>
        <body style="font-family:sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;background:#f5f5f5">
        <div style="text-align:center;padding:40px;background:#fff;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,0.1)">
        <h1 style="color:#c62828;font-size:1.5em">접근이 제한되었습니다</h1>
        <p style="color:#666;margin-top:12px">사내 네트워크에서만 접근 가능합니다.</p>
        <p style="color:#999;font-size:12px;margin-top:20px">IP: ${clientIP}</p>
        </div></body></html>`,
        {
          status: 403,
          headers: { "Content-Type": "text/html; charset=utf-8" },
        }
      );
    }

    // GitHub Pages로 프록시
    const url = new URL(request.url);
    const targetPath = url.pathname === "/" ? PATH_PREFIX + "/" : PATH_PREFIX + url.pathname;
    const targetURL = ORIGIN + targetPath + url.search;

    const response = await fetch(targetURL, {
      headers: {
        "User-Agent": request.headers.get("User-Agent") || "",
        "Accept": request.headers.get("Accept") || "",
      },
    });

    // 응답 헤더 복사 후 반환
    const newHeaders = new Headers(response.headers);
    newHeaders.set("X-Proxy-By", "shopl-report-worker");
    newHeaders.delete("x-frame-options");

    return new Response(response.body, {
      status: response.status,
      headers: newHeaders,
    });
  },
};
