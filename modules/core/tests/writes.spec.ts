import * as t from "@/core/types";
import { ClientConfig } from "pg";
import { DBClient } from "../dbClient";
import { makeDBTestManager } from "../postgresManager";
import { select } from "../query";
import { resetAndSeed } from "../reset";
import { insert, insertAll, update } from "../writes";
import { expectType } from "./test-utils";

let db: DBClient;
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

  ({ client: db, release } = await testManager.getTestDB("reset"));
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

    await resetAndSeed({
      client: db,
      models: [Model],
      seeders: [],
    });

    const { id } = await insert(Model, { name: "test" }).transact(db);

    expectType<string>()(id);

    const result = await select(Model, "name").get(db);

    expect(result).toEqual([{ name: "test" }]);
  });
});

describe("db/framework/update()", () => {
  test("can update and return data", async () => {
    class Model {
      id = t.generatedId;
      name = t.text;
      updateValue = t.integer;
    }

    await resetAndSeed({
      client: db,
      models: [Model],
      seeders: [],
    });

    await insertAll(Model, [
      { name: "test", updateValue: 5 },
      { name: "next", updateValue: 5 },
    ]).transact(db);

    const result = await update(Model, { updateValue: 6 })
      .where({ name: "test" })
      .return("name", "updateValue")
      .transact(db);

    expect(result).toEqual([{ name: "test", updateValue: 6 }]);
  });

  test("can update and return multiple rows", async () => {
    class Model {
      id = t.generatedId;
      name = t.text;
      updateValue = t.integer;
    }

    await resetAndSeed({
      client: db,
      models: [Model],
      seeders: [],
    });

    await insertAll(Model, [
      { name: "test", updateValue: 5 },
      { name: "test2", updateValue: 5 },
      { name: "test3", updateValue: 4 },
      { name: "test4", updateValue: 3 },
    ]).transact(db);

    const result = await update(Model, { updateValue: 99 })
      .where({ updateValue: 5 })
      .return("name", "updateValue")
      .transact(db);

    expect(result).toEqual(
      expect.arrayContaining([
        { name: "test", updateValue: 99 },
        { name: "test2", updateValue: 99 },
      ])
    );
  });
});
