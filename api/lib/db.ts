import { createClient, type Client } from "@libsql/client";

function getClient(): Client {
  const rawUrl = process.env.TURSO_DATABASE_URL;
  const rawToken = process.env.TURSO_AUTH_TOKEN;

  const url = (rawUrl && rawUrl.trim() !== "" && rawUrl !== "undefined")
    ? rawUrl.trim()
    : "file:kpis.db";

  const authToken = (rawToken && rawToken.trim() !== "" && rawToken !== "undefined")
    ? rawToken.trim()
    : undefined;

  if (url.startsWith("file:")) {
    return createClient({ url });
  }

  return createClient({ url, authToken });
}

let clientInstance: Client | null = null;

export function getDb(): Client {
  if (!clientInstance) {
    clientInstance = getClient();
  }
  return clientInstance;
}

export function resetDb(): void {
  if (clientInstance) {
    try {
      clientInstance.close();
    } catch (e) {
      // ignore close errors
    }
    clientInstance = null;
  }
}

// Proxy to allow existing `db.execute(...)` and `db.batch(...)` calls to work seamlessly
export const db: Client = new Proxy({} as Client, {
  get(_target, prop) {
    const client = getDb();
    const value = (client as any)[prop];
    if (typeof value === "function") {
      return value.bind(client);
    }
    return value;
  }
});
