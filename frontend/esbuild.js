const postCssPlugin = require("esbuild-style-plugin");

require("esbuild")
  .context({
    entryPoints: ["src/main.ts"],
    outfile: "../server/priv/static/app.js",
    bundle: true,
    minify: process.argv.includes("--minify"),
    plugins: [
      postCssPlugin({
        postcss: {
          plugins: [require("tailwindcss"), require("autoprefixer")],
        },
      }),
    ],
  })
  .then((context) => {
    if (process.argv.includes("--watch")) {
      context.watch().then(() => {
        console.log("Watching...");
      });
    } else {
      context
        .rebuild()
        .then(() => context.dispose())
        .then(() => console.log("Done"));
    }
  })
  .catch(() => {
    process.exit(1);
  });
