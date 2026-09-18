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
        "getting_started/studio",
      ],
    },
    "concepts",
    "examples",
    {
      type: "category",
      label: "Workflows",
      items: [
        "workflows",
        "executions",
        "parallelism",
        "groups",
        "inputs",
      ],
    },
    {
      type: "category",
      label: "Execution",
      items: [
        "retries",
        "timeouts",
        "recurring",
        "caching",
        "memoizing",
        "deferring",
        "concurrency",
        "suspense",
        "checkpoints",
        "streams",
        "select",
      ],
    },
    {
      type: "category",
      label: "Data & storage",
      items: [
        "serialization",
        "blobs",
        "assets",
        "catalog",
        "logging",
        "metrics",
      ],
    },
    {
      type: "category",
      label: "Configuration",
      items: [
        "server_config",
        "cli_config",
        "authentication",
        "pools",
      ],
    },
    {
      type: "category",
      label: "Reference",
      items: [
        "python_reference",
        "cli_reference",
      ],
    },
  ],
};

export default sidebars;
