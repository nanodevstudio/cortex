import { ClientConfig } from "pg";
import { resetAndSeed } from "..";
import { count } from "../aggregate";
import { DBClient } from "../dbClient";
import { anyOf, equal, notEqual } from "../operators";
import { makeDBTestManager } from "../postgresManager";
import { select } from "../query";
import { SeedFn } from "../reset";
import * as t from "../types";
import { insertAll, sql, update } from "../writes";
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

class Sort {
  id = t.generatedId;
  value = t.integer;
}

class User {
  id = t.generatedId;
  name = t.text;
}

class Project {
  id = t.generatedId;
  name = t.text;
  compareNumber1 = t.integer;
  compareNumber2 = t.integer;
  user = t.ref(User, "id");
}

const sortBy = (array: any[], getOrderValue: (value: any) => number) => {
  return array.slice().sort((a, b) => {
    const aValue = getOrderValue(a);
    const bValue = getOrderValue(b);

    if (aValue > bValue) {
      return 1;
    }

    if (aValue < bValue) {
      return -1;
    }

    return 0;
  });
};

const reset = async (seed?: SeedFn<any>) => {
  await resetAndSeed({
    db: db,
    models: [User, Project],
    seeds: [
      seed ||
        (async ({ db }) => {
          const [{ id: testId }, { id: anotherId }] = await insertAll(User, [
            { name: "test" },
            { name: "another" },
          ]).transact(db);

          await insertAll(Project, [
            {
              user: testId,
              name: "test",
              compareNumber1: 1,
              compareNumber2: 3,
            },
            {
              user: testId,
              name: "test2",
              compareNumber1: 5,
              compareNumber2: 8,
            },
            {
              user: anotherId,
              name: "test",
              compareNumber1: 1,
              compareNumber2: 1,
            },
            {
              user: anotherId,
              name: "another project",
              compareNumber1: 1,
              compareNumber2: 5,
            },
          ]).transact(db);
        }),
    ],
  });
};

const resetForSort = async () => {
  await resetAndSeed({
    db: db,
    models: [Sort],
    seeds: [
      async ({ db }) => {
        await insertAll(Sort, [
          { value: 1 },
          { value: 45 },
          { value: 9 },
          { value: 95 },
          { value: 78 },
          { value: 62 },
          { value: 5 },
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
      .with((user) => ({
        myName: user.name,
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

describe("DBQuery::where", () => {
  test("can filter on a related entity", async () => {
    await reset();

    const result = await select(Project, "name")
      .where({
        user: {
          name: "another",
        },
      })
      .orderBy("name")
      .get(db);

    expect(result.length).toBe(2);
    expect(result).toEqual([{ name: "another project" }, { name: "test" }]);
  });

  test("can filter on a column via sql", async () => {
    await reset();

    const result = await select(Project, "name")
      .where((project) => sql`${project.name} LIKE 'test%'`)
      .orderBy("name")
      .get(db);

    expect(result.length).toBe(3);
    expect(result).toEqual([
      { name: "test" },
      { name: "test" },
      { name: "test2" },
    ]);
  });
});

describe("op(operator, value)", () => {
  test("can use != operator", async () => {
    await reset();

    const result = await select(Project, "name")
      .where({
        name: notEqual("test"),
      })
      .get(db);

    expect(result.length).toBeGreaterThan(0);
    expect(result.some((value) => value.name === "test")).toBe(false);
  });

  test("can use == operator with to compare fields", async () => {
    await reset();

    const result = await select(Project, "name")
      .where((project) => ({
        name: "test",
        compareNumber1: equal(project.compareNumber2),
      }))
      .get(db);

    expect(result.length).toBe(1);
  });

  test("can use anyOf operator to select multiple", async () => {
    await reset();

    const result = await select(Project, "name")
      .where({
        compareNumber2: anyOf([3, 8]),
      })
      .orderBy("compareNumber2")
      .get(db);

    expect(result.length).toBe(2);
    expect(result).toEqual([{ name: "test" }, { name: "test2" }]);
  });
});

describe("orderBy", () => {
  test("can sort asc / desc properly", async () => {
    await resetForSort();

    const result1 = await select(Sort, "id", "value").orderBy("value").get(db);
    const result2 = await select(Sort, "id", "value")
      .orderBy("value", "DESC")
      .get(db);

    expect(result1).toEqual(sortBy(result1, (value) => value.value));
    expect(result2).toEqual(sortBy(result2, (value) => value.value).reverse());
  });
});

describe("limit", () => {
  test("can limit sql properly", async () => {
    await resetForSort();

    const result1 = await select(Sort, "id", "value")
      .orderBy("value")
      .limit(2)
      .get(db);

    expect(result1).toEqual(
      sortBy(result1, (value) => value.value).slice(0, 2)
    );
  });
});

describe("update by sql", () => {
  test("update via sql columns", async () => {
    await reset();

    await update(Project, {
      compareNumber1: (project) => project.compareNumber2,
    }).transact(db);

    const results = await select(
      Project,
      "compareNumber1",
      "compareNumber2"
    ).get(db);

    for (const result of results) {
      expect(result.compareNumber1).toEqual(result.compareNumber2);
    }
  });
});
