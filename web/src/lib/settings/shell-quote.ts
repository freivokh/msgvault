// Quotes one CLI argument for the shell on the daemon's platform (Go's GOOS).
export function shellQuote(value: string, platform: string | undefined): string {
  if (platform === 'windows') return cmdQuote(value);
  return "'" + value.replaceAll("'", "'\\''") + "'";
}

// cmd.exe and Windows argv parsing both honor double quotes, but cmd still expands %VAR% and !VAR! inside them and an embedded quote flips its state.
// Each of those characters goes between two quoted runs: a variable name would then include a quote, so it never matches, and \^" is a literal quote to both parsers.
// One gap remains: with delayed expansion on (off by default), cmd drops a ^ when the same line also has a !.
function cmdQuote(value: string): string {
  let quoted = '"';
  let slashes = 0;
  for (const char of value) {
    if (char === '\\') {
      slashes++;
      continue;
    }
    if (char === '"' || char === '%' || char === '!') {
      quoted += '\\'.repeat(slashes * 2) + '"' + (char === '"' ? '\\^"' : char) + '"';
    } else {
      quoted += '\\'.repeat(slashes) + char;
    }
    slashes = 0;
  }
  return quoted + '\\'.repeat(slashes * 2) + '"';
}
