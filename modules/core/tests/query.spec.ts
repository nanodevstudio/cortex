import { DBClient, query } from "@/core/dbClient";
import { makeDBTestManager } from "@/core/postgresManager";
import { select } from "@/core/query";
import { buildSchemaAndSeed } from "@/core/reset";
import * as t from "@/core/types";
import { ClientConfig } from "pg";

let client: DBClient;
let release: () => Promise<void>;

beforeAll(async () => {
  const config: ClientConfig = {
    host: "localhost",
    port: 5432,
    user: "amp",
    password: "",
    database: "postgres",
  };

  const testManager = await makeDBTestManager(config);

  ({ client, release } = await testManager.getTestDB("reset"));
});

afterAll(async () => {
  if (release) {
    release();
  }
});

describe("db/framework/select()", () => {
  test("can select inserted data", async () => {
    class Model {
      id = t.generatedId;
      name = t.text;
    }

    await buildSchemaAndSeed({
      client,
      models: [Model],
      seeders: [],
    });

    await query(client, `INSERT INTO model ("name") VALUES ('test')`);

    const result = await select(Model, "name").get(client);

    expect(result).toEqual([{ name: "test" }]);
  });
});
