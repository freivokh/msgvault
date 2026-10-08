import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

import { shellQuote } from './shell-quote';

const roundTripValues = [
  'you@example.com',
  "o'brien@example.com",
  'Work App',
  'a&b|c<d>e^f(g)',
  'a"b@example.com',
  'say "hi" & leave',
  'Work %QUOTE_PROBE% App',
  '100%',
  'Work!',
  'Hey!QUOTE_PROBE!',
  '!undefined!',
  'a^b%QUOTE_PROBE%',
  '%%!!',
  'x\\"y\\%z!',
  'trailing\\',
  'C:\\Users\\me\\"quoted"\\',
  '',
];

describe('shellQuote', () => {
  it.each([
    ['you@example.com', '"you@example.com"'],
    ["o'brien@example.com", '"o\'brien@example.com"'],
    ['Work & Home', '"Work & Home"'],
    ['trailing\\', '"trailing\\\\"'],
    ['a"b@example.com', '"a"\\^""b@example.com"'],
    ['Work %QUOTE_PROBE% App', '"Work "%"QUOTE_PROBE"%" App"'],
  ])('quotes %j for Windows cmd as %s', (value, quoted) => {
    expect(shellQuote(value, 'windows')).toBe(quoted);
  });

  it('keeps POSIX single quoting elsewhere', () => {
    expect(shellQuote("o'brien@example.com", 'darwin')).toBe("'o'\\''brien@example.com'");
    expect(shellQuote('you@example.com', undefined)).toBe("'you@example.com'");
  });

  it.runIf(process.platform === 'win32')('round-trips every value through real cmd.exe', () => {
    const dir = mkdtempSync(join(tmpdir(), 'msgvault-shell-quote-'));
    try {
      const script = join(dir, 'argv.js');
      writeFileSync(script, 'console.log(JSON.stringify(process.argv.slice(2)));');
      for (const delayedExpansion of ['/V:OFF', '/V:ON']) {
        for (const value of roundTripValues) {
          // Each value sits before an argument cmd must not split or execute, so a leaked quote state shows up.
          const line = `"${process.execPath}" "${script}" ${shellQuote(value, 'windows')} --oauth-app ${shellQuote('Work & echo LEAKED', 'windows')}`;
          const { stdout } = spawnSync('cmd.exe', ['/d', delayedExpansion, '/s', '/c', `"${line}"`], {
            env: { ...process.env, QUOTE_PROBE: 'expanded' },
            windowsVerbatimArguments: true,
            encoding: 'utf8',
          });
          expect(JSON.parse(stdout.trim()), `${delayedExpansion} ${value}`).toEqual([value, '--oauth-app', 'Work & echo LEAKED']);
        }
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
