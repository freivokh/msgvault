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
    expect(shellQuote(value, 'cmd')).toBe(quoted);
  });

  it('quotes POSIX arguments', () => {
    expect(shellQuote("o'brien@example.com", 'posix')).toBe("'o'\\''brien@example.com'");
    expect(shellQuote('you@example.com', 'posix')).toBe("'you@example.com'");
  });

  it.runIf(process.platform === 'win32')('round-trips arguments through Command Prompt with delayed expansion off', () => {
    const dir = mkdtempSync(join(tmpdir(), 'msgvault-shell-quote-'));
    try {
      const script = join(dir, 'argv.js');
      writeFileSync(script, 'console.log(JSON.stringify(process.argv.slice(2)));');
      const pairs = [
        ...roundTripValues.map((value) => [value, 'Work & echo LEAKED']),
        ['a^b@example.com', 'Work!'],
        ['a^b@example.com', '!QUOTE_PROBE!'],
        ['a!b@example.com', 'Work ^Budget'],
        ['a!b@example.com', 'Work ^!Budget'],
      ];
      for (const [email, app] of pairs) {
        const line = `"${process.execPath}" "${script}" ${shellQuote(email, 'cmd')} --oauth-app ${shellQuote(app, 'cmd')}`;
        const result = spawnSync('cmd.exe', ['/d', '/v:off', '/s', '/c', `"${line}"`], {
          env: { ...process.env, QUOTE_PROBE: 'expanded' },
          windowsVerbatimArguments: true,
          encoding: 'utf8',
          timeout: 5_000,
        });
        expect(result.error).toBeUndefined();
        expect(result.status, result.stderr).toBe(0);
        expect(JSON.parse(result.stdout.trim()), `${email} ${app}`).toEqual([email, '--oauth-app', app]);
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 60_000);

  it.runIf(process.platform !== 'win32')('round-trips arguments through a POSIX shell', () => {
    const dir = mkdtempSync(join(tmpdir(), 'msgvault-shell-quote-'));
    try {
      const script = join(dir, 'argv.js');
      writeFileSync(script, 'console.log(JSON.stringify(process.argv.slice(2)));');
      for (const value of [...roundTripValues, 'Work $Budget', '$(echo LEAKED)']) {
        const line = `${shellQuote(process.execPath, 'posix')} ${shellQuote(script, 'posix')} ${shellQuote(value, 'posix')} --oauth-app ${shellQuote('Work $Budget', 'posix')}`;
        const result = spawnSync('/bin/sh', ['-c', line], {
          env: { ...process.env, Budget: 'expanded' },
          encoding: 'utf8',
          timeout: 5_000,
        });
        expect(result.error).toBeUndefined();
        expect(result.status, result.stderr).toBe(0);
        expect(JSON.parse(result.stdout.trim())).toEqual([value, '--oauth-app', 'Work $Budget']);
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 60_000);
});
