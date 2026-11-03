// Runs before `npm run build` (the root "prebuild" script). On Windows it seeds
// Tauri's bundler cache from the files committed beside it, so `tauri build`
// finds NSIS/WiX already present and never calls GitHub. Elsewhere it does
// nothing: only the Windows installers need these tools.
import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

if (process.platform !== "win32") process.exit(0);

const script = join(dirname(fileURLToPath(import.meta.url)), "install-bundler-tools.ps1");
const result = spawnSync(
  "powershell.exe",
  ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script],
  { stdio: "inherit" },
);
process.exit(result.status ?? 1);
