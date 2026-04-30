const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");
const dist = path.join(root, "dist");
const apiUrl = process.env.VITE_API_URL || "https://tapestry-2iyf.onrender.com";
const buildId = process.env.VERCEL_GIT_COMMIT_SHA || process.env.TAPESTRY_BUILD_ID || Date.now().toString();
const appFile = `App.${buildId.slice(0, 12)}.js`;

function copyRecursive(src, dest) {
  if (!fs.existsSync(src)) return;
  const stat = fs.statSync(src);
  if (stat.isDirectory()) {
    fs.mkdirSync(dest, { recursive: true });
    for (const entry of fs.readdirSync(src)) {
      copyRecursive(path.join(src, entry), path.join(dest, entry));
    }
    return;
  }
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.copyFileSync(src, dest);
}

fs.rmSync(dist, { recursive: true, force: true });
fs.mkdirSync(dist, { recursive: true });

copyRecursive(path.join(root, "index.html"), path.join(dist, "index.html"));
copyRecursive(path.join(root, "App.jsx"), path.join(dist, appFile));
copyRecursive(path.join(root, "tapestry.css"), path.join(dist, "tapestry.css"));
for (const dir of ["src", "assets", "components"]) {
  copyRecursive(path.join(root, dir), path.join(dist, dir));
}

const apiPath = path.join(dist, "src", "api.js");
let apiSource = fs.readFileSync(apiPath, "utf8");
apiSource = apiSource.replace("__TAPESTRY_API_URL__", apiUrl.replace(/\\/g, "\\\\").replace(/"/g, '\\"'));
fs.writeFileSync(apiPath, apiSource);

const indexPath = path.join(dist, "index.html");
let indexSource = fs.readFileSync(indexPath, "utf8");
indexSource = indexSource.replace(/src="\/App[^"]*"/, `src="/${appFile}"`);
fs.writeFileSync(indexPath, indexSource);

console.log(`TAPESTRY static build written to ${dist}`);
console.log(`VITE_API_URL=${apiUrl}`);
console.log(`APP_BUNDLE=/${appFile}`);
