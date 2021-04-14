import { ClientConfig } from "pg";
import { Model, resetAndSeed } from "..";
import { count } from "../aggregate";
import { DBClient } from "../dbClient";
import { makeDBTestManager } from "../postgresManager";
import { select, SelectRow } from "../query";
import { ModelSymbol, ReturnTypeUnsafe, SelectionEntry } from "../symbolic";
import * as t from "../types";
import { insert, insertAll } from "../writes";
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

class User {
  id = t.generatedId;
  name = t.text;
}

class Project {
  id = t.generatedId;
  name = t.text;
  user = t.ref(User, "id");
}

const reset = async () => {
  await resetAndSeed({
    db: db,
    models: [User, Project],
    seeds: [
      async ({ db }) => {
        const [{ id: testId }, { id: anotherId }] = await insertAll(User, [
          { name: "test" },
          { name: "another" },
        ]).transact(db);

        await insertAll(Project, [
          { user: testId, name: "test" },
          { user: anotherId, name: "test" },
          { user: anotherId, name: "another project" },
        ]).transact(db);
      },
    ],
  });
};

afterAll(async () => {
  if (release) {
    release();
  }
});

describe("DBQuery::with", () => {
  test("can select a column with the with clause", async () => {
    await reset();

    const result = await select(User)
      .with((base) => ({
        myName: base.name,
      }))
      .where({ name: "test" })
      .one(db);

    expectType<string | undefined>()(result?.myName);

    expect(result).toEqual({ myName: "test" });
  });

  test("can select related entities", async () => {
    await reset();

    const result = await select(User, "name")
      .with((user) => ({
        projects: select(Project, "name").where({
          user: user.id,
          name: "test",
        }),
      }))
      .where({ name: "test" })
      .one(db);

    expectType<string | undefined>()(result?.projects[0]?.name);

    expect(result).toEqual({
      name: "test",
      projects: [{ name: "test" }],
    });
  });

  test("can get single entities by using select", async () => {
    await reset();

    const result = await select(Project, "name")
      .with((project) => {
        return {
          user: select(project.user, "name"),
        };
      })
      .where({ name: "test" })
      .one(db);

    expect(result).toEqual({
      name: "test",
      user: { name: "test" },
    });
  });

  test("can get count of a related entity", async () => {
    await reset();

    const result = await select(User, "name")
      .with((user) => ({
        projects: count(select(Project).where({ user: user.id })),
      }))
      .where({ name: "another" })
      .one(db);

    expectType<number | undefined>()(result?.projects);

    expect(result).toEqual({
      name: "another",
      projects: 2,
    });
  });
});