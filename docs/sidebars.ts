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
    {
      type: "category",
      label: "Workflows",
      items: [
        "workflows",
        "executions",
        "concurrency",
        "groups",
      ],
    },
    {
      type: "category",
      label: "Execution",
      items: [
        "retries",
        "recurring",
        "caching",
        "memoizing",
        "deferring",
        "suspense",
      ],
    },
    {
      type: "category",
      label: "Data & storage",
      items: [
        "serialization",
        "blobs",
        "assets",
        "logging",
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
  ],
};

export default sidebars;
