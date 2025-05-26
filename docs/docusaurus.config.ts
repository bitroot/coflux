import { themes as prismThemes } from "prism-react-renderer";
import type { Config } from "@docusaurus/types";
import type * as Preset from "@docusaurus/preset-classic";

const config: Config = {
  title: "Coflux",
  favicon: "img/icon.svg",

  url: "https://docs.coflux.com",
  baseUrl: "/",

  organizationName: "bitroot",
  projectName: "coflux",

  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      {
        docs: {
          routeBasePath: "/",
          sidebarPath: "./sidebars.ts",
        },
        theme: {
          customCss: "./src/css/custom.css",
        },
      } satisfies Preset.Options,
    ],
  ],

  headTags: [
    {
      tagName: "link",
      attributes: { rel: "preconnect", href: "https://fonts.googleapis.com" },
    },
    {
      tagName: "link",
      attributes: {
        rel: "preconnect",
        href: "https://fonts.gstatic.com",
        crossorigin: "anonymous",
      },
    },
    {
      tagName: "link",
      attributes: {
        rel: "stylesheet",
        href: "https://fonts.googleapis.com/css2?family=Raleway:ital,wght@0,100..900;1,100..900&display=swap",
      },
    },
  ],

  themeConfig: {
    navbar: {
      logo: {
        alt: "Coflux",
        src: "img/logo.svg",
        href: "https://coflux.com",
      },
      items: [
        {
          href: "https://github.com/bitroot/coflux",
          label: "GitHub",
          position: "right",
        },
      ],
    },
    footer: {
      copyright: `© ${new Date().getFullYear()} Bitroot Ltd. All Rights Reserved.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
    mermaid: {
      theme: { light: "base", dark: "base" },
      options: {
        theme: "base",
        themeVariables: {
          primaryColor: "#f1f5f9",
          primaryTextColor: "#164e63",
          primaryBorderColor: "#64748b",
          lineColor: "#155e75",
        },
      },
    },
  } satisfies Preset.ThemeConfig,

  markdown: {
    mermaid: true,
  },

  themes: ["@docusaurus/theme-mermaid"],
};

export default config;
