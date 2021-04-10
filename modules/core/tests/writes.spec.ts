import * as t from "@/core/types";
import { ClientConfig } from "pg";
import { DBClient } from "../dbClient";
import { makeDBTestManager } from "../postgresManager";
import { select } from "../query";
import { buildSchemaAndSeed } from "../reset";
import { insert } from "../writes";

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

describe("db/framework/insert()", () => {
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

    const { id } = await insert(Model, { name: "test" }).transact(client);
    const result = await select(Model, "name").get(client);

    expect(result).toEqual([{ name: "test" }]);
  });
});
