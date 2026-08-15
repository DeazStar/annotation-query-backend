import path from "node:path";
import { withDocs } from "@farming-labs/next/config";

const repoRoot = path.resolve(process.cwd(), "..");

export default withDocs({
  distDir: ".next",
  allowedDevOrigins: ["127.0.0.1"],
  turbopack: {
    root: repoRoot,
  },
});
