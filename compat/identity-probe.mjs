// Build once per disposable SDK suite, then inspect only that suite's local database.
import { execFileSync } from 'node:child_process';
import { join } from 'node:path';
export function identityProbe(root, directory) {
  const binary = join(directory, process.platform === 'win32' ? 'identity-probe.exe' : 'identity-probe');
  execFileSync('go', ['build', '-o', binary, './internal/cmd/sqlite-identities'], {
    cwd: root, env: { ...process.env, CGO_ENABLED: '0' }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  return (project, controls = []) => JSON.parse(execFileSync(binary,
    [join(directory, 'events.db'), project, JSON.stringify(controls)], { encoding: 'utf8' }));
}
