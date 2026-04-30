const http = require("http");
const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..", "dist");
const port = process.env.PORT || 4173;
const types = {
  ".html": "text/html; charset=utf-8",
  ".jsx": "text/javascript; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".wav": "audio/wav",
  ".mp3": "audio/mpeg",
  ".ogg": "audio/ogg",
};

http.createServer((req, res) => {
  const urlPath = decodeURIComponent(req.url.split("?")[0]);
  const requested = path.join(root, urlPath === "/" ? "index.html" : urlPath);
  const filePath = requested.startsWith(root) && fs.existsSync(requested)
    ? requested
    : path.join(root, "index.html");
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end("Not found");
      return;
    }
    res.writeHead(200, {
      "Content-Type": types[path.extname(filePath)] || "text/plain",
      "Cache-Control": "no-cache",
    });
    res.end(data);
  });
}).listen(port, () => {
  console.log(`TAPESTRY preview running at http://localhost:${port}`);
});
