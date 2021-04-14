import { Client, ClientConfig } from "pg";
import { bgWhite } from "ansi-colors";

export type DBClient = {
  client?: Client;
  config: ClientConfig;
};

export const makeClient = (config: ClientConfig): DBClient => {
  return {
    config,
  };
};

export const getPGClient = async (client: DBClient) => {
  if (client.client) {
    return client.client;
  }

  const pgClient = new Client(client.config);
  client.client = pgClient;
  await pgClient.connect();

  return pgClient;
};

export const query = async (
  client: DBClient,
  query: string,
  values?: any[]
) => {
  const pg = await getPGClient(client);

  try {
    return await pg.query(query, values);
  } catch (e) {
    console.log(e);
    let errorQuery = query;

    if (e.severity === "ERROR") {
      const before = errorQuery.slice(0, e.position - 1);
      const after = errorQuery.slice(e.position - 1);

      errorQuery = before + "|error>|" + after;
    }

    throw new Error(
      `SQL Error: ${
        e.message
      }, query: \n${errorQuery}\n values: ${JSON.stringify(values)}`
    );
  }
};

export const closeClient = async (client: DBClient) => {
  if (client.client) {
    await client.client.end();
    client.client = undefined;
  }
};
