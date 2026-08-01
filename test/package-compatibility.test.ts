import { readFileSync } from "node:fs";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import { getRuntimePackageMetadata } from "@ramideltoro/nutsnews-worker-runtime";
import { describe, expect, it } from "vitest";

import {
  SUPPORTED_CONTRACTS_PACKAGE_VERSION,
  SUPPORTED_RUNTIME_PACKAGE_VERSION
} from "../src/index.js";

describe("package compatibility", () => {
  it("accepts the installed worker runtime release", () => {
    const contracts = getContractPackageMetadata();
    const runtime = getRuntimePackageMetadata();

    expect(contracts.packageVersion).toBe(SUPPORTED_CONTRACTS_PACKAGE_VERSION);
    expect(runtime.packageVersion).toBe(SUPPORTED_RUNTIME_PACKAGE_VERSION);
    expect(runtime.contractsPackageVersion).toBe(SUPPORTED_CONTRACTS_PACKAGE_VERSION);
    expect(SUPPORTED_CONTRACTS_PACKAGE_VERSION).toBe("1.0.0");
    expect(SUPPORTED_RUNTIME_PACKAGE_VERSION).toBe("1.0.0");
  });

  it("locks both packages to immutable GitHub Packages artifacts without overrides", () => {
    const manifest = JSON.parse(readFileSync("package.json", "utf8")) as {
      readonly dependencies: Readonly<Record<string, string>>;
      readonly overrides?: unknown;
    };
    const lock = JSON.parse(readFileSync("package-lock.json", "utf8")) as {
      readonly packages: Readonly<Record<string, {
        readonly dependencies?: Readonly<Record<string, string>>;
        readonly integrity?: string;
        readonly overrides?: unknown;
        readonly resolved?: string;
        readonly version?: string;
      }>>;
    };
    const root = lock.packages[""];
    const contracts = lock.packages["node_modules/@ramideltoro/nutsnews-worker-contracts"];
    const runtime = lock.packages["node_modules/@ramideltoro/nutsnews-worker-runtime"];

    expect(manifest.dependencies).toMatchObject({
      "@ramideltoro/nutsnews-worker-contracts": "1.0.0",
      "@ramideltoro/nutsnews-worker-runtime": "1.0.0"
    });
    expect(manifest.overrides).toBeUndefined();
    expect(root?.dependencies).toMatchObject({
      "@ramideltoro/nutsnews-worker-contracts": "1.0.0",
      "@ramideltoro/nutsnews-worker-runtime": "1.0.0"
    });
    expect(root?.overrides).toBeUndefined();
    expect(contracts?.version).toBe("1.0.0");
    expect(contracts?.resolved).toMatch(/^https:\/\/npm\.pkg\.github\.com\/download\/@ramideltoro\/nutsnews-worker-contracts\/1\.0\.0\//u);
    expect(contracts?.integrity).toMatch(/^sha512-/u);
    expect(runtime?.version).toBe("1.0.0");
    expect(runtime?.resolved).toMatch(/^https:\/\/npm\.pkg\.github\.com\/download\/@ramideltoro\/nutsnews-worker-runtime\/1\.0\.0\//u);
    expect(runtime?.integrity).toMatch(/^sha512-/u);
    expect(runtime?.dependencies).toEqual({
      "@ramideltoro/nutsnews-worker-contracts": "1.0.0",
      "amqplib": "2.0.1"
    });
  });
});
