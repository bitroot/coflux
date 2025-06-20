import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebars: SidebarsConfig = {
  docs: [
    "intro",
    {
      type: "category",
      label: "Getting started",
      items: [
        "getting_started/install",
        "getting_started/server",
        "getting_started/workflows",
        "getting_started/workers",
        "getting_started/runs",
      ],
    },
    "concepts",
    "executions",
    "concurrency",
    "retries",
    "caching",
    "groups",
    "logging",
    {
      type: "category",
      label: "Advanced",
      items: [
        "suspense",
        "deferring",
        "memoising",
        "assets",
        "sensors",
        "stubs",
        "blobs",
      ],
    },
    "examples",
  ],
};

export default sidebars;
