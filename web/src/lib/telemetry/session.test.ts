import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { createSessionAwareAPIClient } from '../api/client';
import { startSessionReporting } from './session';

let now = 0;
let hidden = false;
let requests: Request[];
let stop: () => void;

beforeEach(() => {
  vi.useFakeTimers();
  now = 0;
  hidden = false;
  requests = [];
  vi.spyOn(performance, 'now').mockImplementation(() => now);
  vi.spyOn(document, 'hidden', 'get').mockImplementation(() => hidden);
});
afterEach(() => {
  stop?.();
  vi.restoreAllMocks();
  vi.useRealTimers();
});
function start(fetchFn?: typeof fetch) {
  stop = startSessionReporting(createSessionAwareAPIClient(fetchFn ?? (async (input) => {
    requests.push(input as Request);
    return new Response(null, { status: 202 });
  }), () => 'csrf-token'));
}
function visibility(value: boolean) {
  hidden = value;
  document.dispatchEvent(new Event('visibilitychange'));
}
function close(persisted = false) {
  window.dispatchEvent(new PageTransitionEvent('pagehide', { persisted }));
}
async function bucket(index = 0) {
  return (await requests[index].clone().json()).properties.duration_bucket;
}

it.each([
  [0, 'under_1m'], [59_999, 'under_1m'], [60_000, '1_to_5m'],
  [120_000, '1_to_5m'], [300_000, '5_to_30m'],
  [1_800_000, '5_to_30m'], [1_800_001, 'over_30m'],
])('reports %i visible milliseconds once as %s', async (elapsed, expected) => {
  start();
  now = elapsed;
  close();
  close();
  expect(requests).toHaveLength(1);
  expect(await bucket()).toBe(expected);
  expect(requests[0].keepalive).toBe(true);
  expect(requests[0].credentials).toBe('same-origin');
  expect(requests[0].headers.get('X-CSRF-Token')).toBe('csrf-token');
});

it('counts twenty visible stretches without adding hidden time', async () => {
  start();
  for (let i = 0; i < 20; i++) {
    now += 60_000;
    visibility(true);
    now += 600_000;
    visibility(false);
  }
  expect(requests).toHaveLength(0);
  close();
  expect(await bucket()).toBe('5_to_30m');
});

it('ignores a session that was always hidden', () => {
  hidden = true;
  start();
  vi.advanceTimersByTime(1_800_000);
  close();
  expect(requests).toHaveLength(0);
});

it.each([false, true])('expires hidden time with delayed timers=%s', async (delayed) => {
  start();
  now = 120_000;
  visibility(true);
  if (delayed) vi.setSystemTime(Date.now() + 1_800_000);
  else vi.advanceTimersByTime(1_800_000);
  visibility(false);
  expect(requests).toHaveLength(1);
  expect(await bucket()).toBe('1_to_5m');
  now += 10_000;
  close();
  expect(requests).toHaveLength(2);
  expect(await bucket(1)).toBe('under_1m');
});

it.each([60_000, 1_800_000])('restores a cached page after %i hidden milliseconds', async (gap) => {
  start();
  now = 120_000;
  close(true);
  close(true);
  now += gap;
  vi.setSystemTime(Date.now() + gap);
  window.dispatchEvent(new PageTransitionEvent('pageshow', { persisted: true }));
  now += 120_000;
  close();
  expect(requests).toHaveLength(gap === 60_000 ? 1 : 2);
  for (let i = 0; i < requests.length; i++) expect(await bucket(i)).toBe('1_to_5m');
});

it('removes listeners and timers on authentication cleanup', () => {
  start();
  now = 120_000;
  visibility(true);
  stop();
  vi.advanceTimersByTime(1_800_000);
  visibility(false);
  close();
  expect(requests).toHaveLength(0);
  expect(vi.getTimerCount()).toBe(0);
});

it('bounds stalled delivery and ignores rejection', async () => {
  start((input) => {
    requests.push(input as Request);
    return new Promise((_, reject) => {
      (input as Request).signal.addEventListener('abort', () => reject(new Error('aborted')));
    });
  });
  now = 120_000;
  close();
  await vi.advanceTimersByTimeAsync(3000);
  expect(requests[0].signal.aborted).toBe(true);
  expect(vi.getTimerCount()).toBe(0);
});
