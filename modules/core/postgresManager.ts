import { Client, ClientConfig } from "pg";
import { v4 as genUUID } from "uuid";
import { DBClient } from "./dbClient";

interface DBTestManager {
  getTestDB(
    debugName: string
  ): Promise<{
    release: () => Promise<void>;
    client: DBClient;
  }>;
}

export const makeDBTestManager = async (
  config: ClientConfig
): Promise<DBTestManager> => {
  const client = new Client(config);

  return {
    async getTestDB(name: string) {
      await client.connect();

      const dbName =
        name.toLowerCase() + genUUID().toLowerCase().replace(/-/g, "");

      await client.query(`CREATE DATABASE ${dbName}`);

      const dbConfig = {
        ...config,
        database: dbName,
      };

      const dbClient = new Client(dbConfig);

      await dbClient.connect();

      return {
        release: async () => {
          await dbClient.end();
          await client.query(`DROP DATABASE ${dbName} WITH (FORCE);`);
        },

        client: { client: dbClient, config: dbConfig },
      };
    },
  };
};
