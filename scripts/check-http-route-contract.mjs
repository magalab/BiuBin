import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));

const [routerSource, openapiSource, viteSource] = await Promise.all([
  readFile(`${root}/crates/app/src/http/mod.rs`, 'utf8'),
  readFile(`${root}/crates/app/src/http/openapi.rs`, 'utf8'),
  readFile(`${root}/web/vite.config.ts`, 'utf8'),
]);

const routeSectionStart = routerSource.indexOf(
  '// HTTPBin-compatible fixture routes use the root namespace.',
);
const routeSectionEnd = routerSource.indexOf('.route("/ws/echo"', routeSectionStart);

if (routeSectionStart === -1 || routeSectionEnd === -1) {
  throw new Error('unable to locate the HTTP fixture route section in http/mod.rs');
}

const routeSection = routerSource.slice(routeSectionStart, routeSectionEnd);
const registeredRoutes = [
  ...routeSection.matchAll(/\.route\(\s*"([^"]+)"/g),
].map((match) => match[1]);

const metadataPaths = [
  ...openapiSource.matchAll(/capability_path:\s*"([^"]+)"/g),
].map((match) => match[1]);

const proxySectionStart = viteSource.indexOf('const httpFixtureProxy');
const proxySectionEnd = viteSource.indexOf('].map', proxySectionStart);

if (proxySectionStart === -1 || proxySectionEnd === -1) {
  throw new Error('unable to locate httpFixtureProxy in web/vite.config.ts');
}

const proxySection = viteSource.slice(proxySectionStart, proxySectionEnd);
const proxyPrefixes = [
  ...proxySection.matchAll(/'([^']+)'/g),
].map((match) => match[1]);

function routePrefix(path) {
  // All HTTP fixtures live directly below one root-level prefix. This also
  // folds concrete variants such as /image/png into the /image proxy.
  return path.split('/').slice(0, 2).join('/') || '/';
}

const registeredPrefixes = new Set(registeredRoutes.map(routePrefix));
const metadataPrefixes = new Set(metadataPaths.map(routePrefix));
const proxiedPrefixes = new Set(proxyPrefixes);
const problems = [];

for (const prefix of metadataPrefixes) {
  if (!registeredPrefixes.has(prefix)) {
    problems.push(`OpenAPI/capabilities path prefix is not registered by Axum: ${prefix}`);
  }
  if (!proxiedPrefixes.has(prefix)) {
    problems.push(`OpenAPI/capabilities path prefix is not proxied by Vite: ${prefix}`);
  }
}

for (const prefix of registeredPrefixes) {
  if (!metadataPrefixes.has(prefix)) {
    problems.push(`registered HTTP fixture route has no OpenAPI/capabilities metadata: ${prefix}`);
  }
}

for (const prefix of proxiedPrefixes) {
  // /openapi serves the standalone Scalar page and is intentionally not a fixture.
  if (prefix !== '/openapi' && !metadataPrefixes.has(prefix)) {
    problems.push(`Vite proxies a path with no OpenAPI/capabilities metadata: ${prefix}`);
  }
}

if (problems.length > 0) {
  console.error('HTTP route contract check failed:');
  for (const problem of problems) {
    console.error(`- ${problem}`);
  }
  process.exitCode = 1;
} else {
  console.log(
    `HTTP route contract OK (${registeredPrefixes.size} Axum prefixes, ${metadataPrefixes.size} metadata prefixes, ${proxiedPrefixes.size} Vite prefixes)`,
  );
}
