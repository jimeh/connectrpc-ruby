const {defineConfig} = require('@playwright/test');
const fs = require('node:fs');
const config = JSON.parse(fs.readFileSync(process.env.BROWSER_FIXTURE, 'utf8'));
module.exports = defineConfig({
  testDir: __dirname,
  testMatch: 'rpc.spec.cjs',
  workers: 1,
  retries: 0,
  timeout: 20000,
  outputDir: '../vendor/browser/results',
  reporter: [['list'], ['json', {outputFile: '../vendor/browser/results.json'}]],
  use: {
    browserName: 'chromium',
    // Explicit fixture-only exceptions. These do not alter host trust/security.
    ignoreHTTPSErrors: true,
    launchOptions: {executablePath: config.chromium, chromiumSandbox: false},
  },
});
