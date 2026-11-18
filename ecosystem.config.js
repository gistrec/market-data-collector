// pm2 process config. Secrets come from .env next to this file.
const fs = require("fs");
const path = require("path");

const env = {};
const envFile = path.join(__dirname, ".env");
if (fs.existsSync(envFile)) {
  for (const line of fs.readFileSync(envFile, "utf8").split("\n")) {
    const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/);
    if (m) env[m[1]] = m[2];
  }
}

module.exports = {
  apps: [
    {
      name: "market-data-collector",
      cwd: __dirname,
      // `interpreter: "none"` stops pm2 from running the venv binary with node.
      script: ".venv/bin/market-data-collector",
      interpreter: "none",
      env,
      autorestart: true,
      restart_delay: 5000,
      kill_timeout: 20000, // let the producer flush on SIGTERM
    },
  ],
};
