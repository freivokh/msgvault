import { test, expect, loginToMeetingArchive } from "./fixtures/meeting-daemon";

test("logging in reports app_opened through the daemon", async ({
  page,
  daemon,
}) => {
  const telemetry = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      new URL(response.url()).pathname === "/api/v1/telemetry/events" &&
      response.request().postDataJSON()?.event === "app_opened",
  );
  await loginToMeetingArchive(page, daemon);
  const response = await telemetry;
  expect(response.status()).toBe(202);
  expect(response.request().postDataJSON()).toEqual({
    event: "app_opened",
    properties: { surface: "web" },
  });
});

test("closing a two-minute visit reports its duration through the daemon", async ({ page, daemon }) => {
  await page.clock.install();
  await loginToMeetingArchive(page, daemon);
  await page.clock.runFor(120_000);
  const telemetry = page.waitForResponse((response) =>
    new URL(response.url()).pathname === "/api/v1/telemetry/events" &&
    response.request().postDataJSON()?.event === "session_ended",
  );
  await page.evaluate(() => window.dispatchEvent(new PageTransitionEvent('pagehide')));
  const response = await telemetry;
  expect(response.status()).toBe(202);
  expect(response.request().postDataJSON()).toEqual({
    event: "session_ended", properties: { surface: "web", duration_bucket: "1_to_5m" },
  });
  expect(response.request().headers()['x-csrf-token']).toBeTruthy();
});
