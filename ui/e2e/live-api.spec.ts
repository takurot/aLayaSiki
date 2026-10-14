import { test, expect } from "@playwright/test";
import { createHmac } from "node:crypto";
function token() {
  const header = Buffer.from(
    JSON.stringify({ alg: "HS256", typ: "JWT" }),
  ).toString("base64url");
  const claims = Buffer.from(
    JSON.stringify({
      sub: "ui-user",
      tenant: "ui-tenant",
      roles: ["admin"],
      exp: Math.floor(Date.now() / 1000) + 3600,
      iss: "ui-e2e",
      aud: "ui-e2e",
    }),
  ).toString("base64url");
  const payload = `${header}.${claims}`;
  return `${payload}.${createHmac("sha256", "ui-e2e-only-secret-at-least-32-bytes").update(payload).digest("base64url")}`;
}
test("real API graph and evidence-backed chat", async ({ page, request }) => {
  const jwt = token();
  const ingest = await request.post("http://127.0.0.1:18080/v1/ingest", {
    headers: { Authorization: `Bearer ${jwt}` },
    data: {
      Text: {
        content:
          "Rust startup acquired a company. The company builds graph database technology.",
        metadata: { source: "ui-e2e.md" },
        idempotency_key: "ui-e2e-document",
      },
    },
  });
  expect(ingest.ok()).toBeTruthy();
  await page.goto("/");
  await page.getByLabel("Access token").fill(jwt);
  await page.getByText("Connect", { exact: true }).click();
  const graphNode = page.getByRole("button", { name: /Node Rust startup/ });
  await expect(graphNode).toBeVisible();
  await graphNode.focus();
  await page.keyboard.press("Enter");
  await expect(page.locator("aside").getByText(/ID:/)).toBeVisible();
  await page
    .getByLabel("Query", { exact: true })
    .fill("Rust company graph technology");
  await page.getByText("Send", { exact: true }).click();
  await expect(page.getByText("Evidence", { exact: true })).toBeVisible();
  await expect(page.getByText("Citations", { exact: true })).toBeVisible();
  await expect(page.getByText(/Model:/)).toBeVisible();
  await page.screenshot({ path: "test-results/live-api.png", fullPage: true });
  await page.getByText("Disconnect", { exact: true }).click();
  await expect(graphNode).toHaveCount(0);
  await expect(page.getByLabel("Query", { exact: true })).toBeDisabled();
});
