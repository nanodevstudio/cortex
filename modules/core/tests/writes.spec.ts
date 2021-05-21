import * as t from "@/core/types";
import { ClientConfig } from "pg";
import { DBClient } from "../dbClient";
import { makeDBTestManager } from "../postgresManager";
import { select } from "../query";
import { resetAndSeed } from "../reset";
import { insert, insertAll, remove, update } from "../writes";
import { expectType } from "./test-utils";
import * as uuid from "uuid";

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
      db: db,
      models: [Model],
      seeds: [],
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
      db: db,
      models: [Model],
      seeds: [],
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
      db: db,
      models: [Model],
      seeds: [],
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

  test("optionla types are not required", async () => {
    class Model {
      id = t.generatedId;
      name = t.optional(t.text);
      age = t.integer;
    }

    await resetAndSeed({
      db: db,
      models: [Model],
      seeds: [],
    });

    await insert(Model, { name: "test", age: 5 }).transact(db);
    await insert(Model, { age: 6 }).transact(db);

    expect(
      await select(Model, "name", "age")
        .where({
          age: 6,
        })
        .one(db)
    ).toEqual({
      name: null,
      age: 6,
    });
  });

  test("writes json arrays as arrays instead of empty objects", async () => {
    class Model {
      id = t.generatedId;
      json = t.jsonb<any[]>();
    }

    await resetAndSeed({
      db: db,
      models: [Model],
      seeds: [],
    });

    await insert(Model, { json: [] }).transact(db);
    const result = await select(Model, "id", "json").one(db);

    expect(result?.json).toEqual([]);
  });

  test("can write lists to the database", async () => {
    class Model {
      id = t.generatedId;
      array = t.array(t.uuid);
    }

    await resetAndSeed({
      db: db,
      models: [Model],
      seeds: [],
    });

    const uidList = [uuid.v4(), uuid.v4(), uuid.v4()];

    await insert(Model, { array: uidList }).transact(db);
    const result = await select(Model, "id", "array").one(db);

    expect(result?.array).toEqual(uidList);
  });

  test("can delete records", async () => {
    class Model {
      id = t.generatedId;
      name = t.text;
    }

    await resetAndSeed({
      db: db,
      models: [Model],
      seeds: [
        async ({ db }) => {
          await insertAll(Model, [
            { name: "one" },
            { name: "two" },
            { name: "three" },
          ]).transact(db);
        },
      ],
    });

    const checkQuery = async (results: any[]) => {
      const result = await select(Model, "name").get(db);

      expect(result).toEqual(expect.arrayContaining(results));
    };

    await checkQuery([{ name: "one" }, { name: "two" }, { name: "three" }]);

    const deleted = await remove(Model)
      .where({
        name: "one",
      })
      .return("name")
      .transact(db);

    expect(deleted).toEqual(expect.arrayContaining([{ name: "one" }]));

    await checkQuery([{ name: "two" }, { name: "three" }]);

    await remove(Model)
      .where({
        name: "three",
      })
      .transact(db);

    await checkQuery([{ name: "two" }]);
  });
});
