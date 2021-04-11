import { Client, ClientConfig } from "pg";
import { v4 as genUUID } from "uuid";
import { closeClient, DBClient, makeClient } from "./dbClient";

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

      const cortextClient = makeClient({ ...config, database: dbName })

      return {
        release: async () => {
          closeClient(cortextClient)
          await client.query(`DROP DATABASE ${dbName} WITH (FORCE);`);
        },

        client: cortextClient,
      };
    },
  };
};
