import {
  describe,
  expect,
  it
} from "vitest";

import { PERSISTENCE_CONFIG_SCHEMA } from "../src/config.js";
import { createPersistencePrometheusTelemetrySink } from "../src/telemetry.js";

const BUILD_REVISION = "0123456789abcdef0123456789abcdef01234567";

describe("persistence immutable telemetry identity", () => {
  it("exports one bounded non-unknown build and deployment series", () => {
    const output = createPersistencePrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-persistence",
        version: "0.1.0",
        environment: "production",
        host: "backend-vps",
        revision: BUILD_REVISION,
        deployment: "shadow",
        adapter: "production"
      },
      expectedActive: false
    }).collect();
    const identitySamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_build_info{")
      || line.startsWith("nutsnews_worker_deployment_info{"));
    const expectedActiveSamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_expected_active{"));

    expect(identitySamples).toHaveLength(2);
    expect(expectedActiveSamples).toEqual([
      'nutsnews_worker_expected_active{environment="production",service="nutsnews-worker-article-persistence"} 0'
    ]);
    expect(identitySamples.join("\n")).toContain(`revision="${BUILD_REVISION}"`);
    expect(identitySamples.join("\n")).toContain('deployment="shadow"');
    expect(identitySamples.join("\n")).toContain('adapter="production"');
    expect(identitySamples.join("\n")).not.toContain("unknown");
  });

  it("lets Runtime expose truthful cutover ownership instead of a hardcoded shadow value", () => {
    const output = createPersistencePrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-persistence",
        version: "0.1.0",
        environment: "production",
        host: "backend-vps",
        revision: BUILD_REVISION,
        deployment: "production",
        adapter: "production"
      },
      expectedActive: true
    }).collect();

    expect(output.split("\n").filter((line) => line.startsWith("nutsnews_worker_expected_active{"))).toEqual([
      'nutsnews_worker_expected_active{environment="production",service="nutsnews-worker-article-persistence"} 1'
    ]);
    expect(output).not.toContain("nutsnews_worker_consumer_active");
  });

  it("declares the immutable revision as required and non-sensitive in production", () => {
    expect(PERSISTENCE_CONFIG_SCHEMA.find((variable) => variable.name === "NUTSNEWS_PERSISTENCE_BUILD_REVISION")).toMatchObject({
      requiredInProduction: true,
      sensitive: false
    });
  });
});
