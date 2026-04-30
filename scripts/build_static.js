const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");
const dist = path.join(root, "dist");
const apiUrl = process.env.VITE_API_URL || "https://tapestry-api.onrender.com";

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

for (const file of ["index.html", "App.jsx", "tapestry.css"]) {
  copyRecursive(path.join(root, file), path.join(dist, file));
}
for (const dir of ["src", "assets", "components"]) {
  copyRecursive(path.join(root, dir), path.join(dist, dir));
}

const apiPath = path.join(dist, "src", "api.js");
let apiSource = fs.readFileSync(apiPath, "utf8");
apiSource = apiSource.replace("__TAPESTRY_API_URL__", apiUrl.replace(/\\/g, "\\\\").replace(/"/g, '\\"'));
fs.writeFileSync(apiPath, apiSource);

console.log(`TAPESTRY static build written to ${dist}`);
console.log(`VITE_API_URL=${apiUrl}`);
