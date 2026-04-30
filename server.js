const http = require("http");
const fs = require("fs");
const path = require("path");

const root = __dirname;
const port = process.env.PORT || 5173;
const apiTarget = "http://localhost:8000";
const types = {
  ".html": "text/html; charset=utf-8",
  ".jsx": "text/javascript; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".wav": "audio/wav",
  ".mp3": "audio/mpeg",
  ".ogg": "audio/ogg"
};

http
  .createServer((req, res) => {
    if (req.url.startsWith("/api/")) {
      const upstream = new URL(req.url, apiTarget);
      const proxyReq = http.request(upstream, { method: req.method, headers: req.headers }, (proxyRes) => {
        res.writeHead(proxyRes.statusCode || 502, proxyRes.headers);
        proxyRes.pipe(res);
      });
      proxyReq.on("error", () => {
        res.writeHead(502, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Backend offline" }));
      });
      req.pipe(proxyReq);
      return;
    }
    const urlPath = decodeURIComponent(req.url.split("?")[0]);
    const filePath = path.join(root, urlPath === "/" ? "index.html" : urlPath);
    if (!filePath.startsWith(root)) {
      res.writeHead(403);
      res.end("Forbidden");
      return;
    }
    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(404);
        res.end("Not found");
        return;
      }
      res.writeHead(200, {
        "Content-Type": types[path.extname(filePath)] || "text/plain",
        "Cache-Control": filePath.includes(`${path.sep}assets${path.sep}`) ? "public, max-age=3600" : "no-cache"
      });
      res.end(data);
    });
  })
  .listen(port, () => {
    console.log(`TAPESTRY running at http://localhost:${port}`);
  });
