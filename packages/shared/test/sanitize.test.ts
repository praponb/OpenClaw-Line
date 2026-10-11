import { describe, expect, it } from "vitest";
import { sanitize, sanitizeDeep } from "../src/sanitize.js";

describe("sanitize", () => {
  it.each([
    ["conn string password", "Connecting to postgres://etl_user:hunter2@warehouse:5432/analytics", "postgres://etl_user:[REDACTED:conn_password]@warehouse", "hunter2"],
    ["email", "Processing for jane.doe@example.com now", "[REDACTED:email]", "jane.doe@example.com"],
    ["jwt", "token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTYifQ.abcDEF123456_-x", "[REDACTED:jwt]", "eyJhbGciOiJIUzI1NiJ9"],
    ["bearer", "Authorization: Bearer abcdef0123456789xyz", "Bearer [REDACTED:bearer]", "abcdef0123456789xyz"],
    ["password kv", 'password="Sup3rS3cret!" user=bob', 'password="[REDACTED:secret]"', "Sup3rS3cret"],
    ["api key kv", "api_key: 9f8e7d6c5b4a", "api_key: [REDACTED:secret]", "9f8e7d6c5b4a"],
    ["json secret", '{"client_secret": "abc123xyz"}', '"client_secret": "[REDACTED:secret]"', "abc123xyz"],
    ["aws key", "key AKIAIOSFODNN7EXAMPLE used", "[REDACTED:aws_access_key]", "AKIAIOSFODNN7EXAMPLE"],
    ["stripe key", "stripe sk_live_51HxyzABCDEF0123456789", "[REDACTED:stripe_key]", "sk_live_51Hxyz"],
    ["moonshot key", "MOONSHOT key sk-AbCdEfGhIjKlMnOpQrStUvWxYz0123456789abcd leaked", "[REDACTED:api_key]", "sk-AbCdEf"],
    ["github token", "ghp_abcdefghijklmnopqrstuvwxyz0123456789", "[REDACTED:github_token]", "ghp_abcdef"],
    ["databricks pat", "using dapi0123456789abcdef0123456789abcdef-2 for auth", "[REDACTED:databricks_token]", "dapi0123456789"],
    ["databricks oauth secret", "secret dose0123456789abcdef0123456789abcdef", "[REDACTED:databricks_token]", "dose0123456789"],
    ["card number", "card 4242 4242 4242 4242 declined", "[REDACTED:card_number]", "4242 4242"],
    ["mastercard 2-series", "pan 2223003122003222 used", "[REDACTED:card_number]", "2223003122003222"],
    ["amex", "amex 378282246310005 ok", "[REDACTED:card_number]", "378282246310005"],
    ["thai national id", "customer citizen_id=1103700123458 failed KYC", "[REDACTED:thai_national_id]", "1103700123458"],
    ["thai national id, formatted", "id 3-1015-00987-65-8 rejected", "[REDACTED:thai_national_id]", "3-1015-00987-65-8"],
    ["thai mobile", "call 081-234-5678 now", "[REDACTED:thai_phone]", "081-234-5678"],
    ["thai mobile, plain", "phone:0912345678,", "[REDACTED:thai_phone]", "0912345678"],
    ["thai international", "contact +66 81 234 5678", "[REDACTED:thai_phone]", "+66 81 234 5678"],
    ["bangkok landline", "office 02-123-4567", "[REDACTED:thai_phone]", "02-123-4567"],
  ])("redacts %s", (_name, input, expected, secret) => {
    const out = sanitize(input);
    expect(out).toContain(expected);
    expect(out).not.toContain(secret);
  });

  it("keeps evidence that is not secret", () => {
    const line = `KeyError: 'payment_intent_id' in /opt/airflow/dags/stripe_ingest/extract_stripe.py, line 36 run_id=scheduled__2026-10-10T02:00:00+00:00`;
    expect(sanitize(line)).toBe(line);
  });

  it("keeps ids, run ids and amounts that are not Thai IDs or phone numbers", () => {
    // 907227724233005, 794488822565591 and 205995526214147 pass Luhn: real Databricks run ids that were masked before.
    const text = "run 907227724233005 run 794488822565591 run 205995526214147 job 954973835355130 amount 33800 id 1103700123459 evt_20261011_c00012 ch_20261011_00081 v2.1.296";
    expect(sanitize(text)).toBe(text); // 1103700123459 has a wrong check digit
  });

  it("does not redact non-Luhn digit runs such as ids", () => {
    expect(sanitize("order 1234567890123 failed")).toBe("order 1234567890123 failed");
  });

  it("sanitizes nested values", () => {
    expect(sanitizeDeep({ a: ["x@y.io"], b: { c: "ok" }, n: 1 })).toEqual({ a: ["[REDACTED:email]"], b: { c: "ok" }, n: 1 });
  });
});

describe("sanitize false positives", () => {
  it("does not treat URL path segments as secret keys", () => {
    const msg = "Airflow request failed for /auth/token: fetch failed";
    expect(sanitize(msg)).toBe(msg);
  });
});

describe("sanitize prefixed keys", () => {
  it("still redacts env-style keys like DB_PASSWORD", () => {
    const out = sanitize("DB_PASSWORD=Pg-Pass-123 AIRFLOW__CORE__FERNET_KEY=x");
    expect(out).toContain("DB_PASSWORD=[REDACTED:secret]");
    expect(out).not.toContain("Pg-Pass-123");
  });
});
