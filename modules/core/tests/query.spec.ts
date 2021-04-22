import { DBClient, query } from "@/core/dbClient";
import { makeDBTestManager } from "@/core/postgresManager";
import { select } from "@/core/query";
import { buildSchemaAndSeed, resetAndSeed } from "@/core/reset";
import * as uuid from "uuid";
import * as t from "@/core/types";
import { ClientConfig } from "pg";
import { insert, insertAll } from "../writes";
import { expectType } from "./test-utils";

let client: DBClient;
let release: (() => Promise<void>) | undefined;

beforeAll(async () => {
  if (release) {
    await release();
    release = undefined;
  }

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
    await release();
    release = undefined;
  }
});

describe("db/framework/select()", () => {
  test("can select inserted data", async () => {
    class Model {
      id = t.generatedId;
      name = t.text;
    }

    await buildSchemaAndSeed({
      db: client,
      models: [Model],
      seeds: [],
    });

    await query(client, `INSERT INTO model ("name") VALUES ('test')`);

    const result = await select(Model, "name").get(client);

    expect(result).toEqual([{ name: "test" }]);
  });

  test("can select by id", async () => {
    class Model {
      id = t.generatedId;
      name = t.text;
    }

    const id = uuid.v4();

    await resetAndSeed({
      db: client,
      models: [Model],
      seeds: [
        ({ db }) =>
          insertAll(Model, [
            { id, name: "test" },
            { name: "another" },
            { name: "third" },
          ]).transact(db),
      ],
    });

    const result = await select(Model, "id", "name")
      .where({ id: id })
      .one(client);

    expectType<string>()(result!.id);

    expect(result).toEqual({ id: id, name: "test" });
  });
});
