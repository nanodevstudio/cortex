import { ClientConfig } from "pg";
import { resetAndSeed } from "..";
import { count } from "../aggregate";
import { DBClient } from "../dbClient";
import { getQualifiedSQLTable } from "../generateSchema";
import { makeIndex } from "../indexs";
import { anyOf, equal, notEqual } from "../operators";
import { page } from "../page";
import { makeDBTestManager } from "../postgresManager";
import { select, subselect } from "../query";
import { SeedFn } from "../reset";
import {
  joinTerms,
  maintainWeightedTSV,
  matchTSVhWithAllTerms,
  searchExtensions,
  tsvSearchRank,
} from "../search";
import * as t from "../types";
import { insertAll, joinSQL, raw, sql, update } from "../writes";
import { expectType } from "./test-utils";

let db: DBClient;
let release: () => Promise<void>;

beforeAll(async () => {
  const config: ClientConfig = {
    host: "localhost",
    port: 5432,
    user: "samueldesota",
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

  test("can order related entities", async () => {
    await reset();

    const result = await select(User, "name")
      .with((user) => ({
        projects: select(Project, "name")
          .where({
            user: user.id,
            name: "test",
          })
          .orderBy("compareNumber2"),
      }))
      .where({ name: "test" })
      .one(db);

    expectType<string | undefined>()(result?.projects[0]?.name);

    expect(result).toEqual({
      name: "test",
      projects: [{ name: "test" }],
    });
  });

  test("subselects return empty array instead of null when no results", async () => {
    await reset();

    const result = await select(User, "name")
      .with((user) => ({
        projects: select(Project, "name").where({
          user: user.id,
          name: "this_name_does_not_exist",
        }),
      }))
      .where({ name: "test" })
      .one(db);

    expect(result).toEqual({
      name: "test",
      projects: [],
    });
  });

  test("can get single entities by using select", async () => {
    await reset();

    const result = await select(Project, "name")
      .with((project) => {
        project.user;
        return {
          user: subselect(project.user, "name"),
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

    const resultNone = await select(Project, "name")
      .where({
        compareNumber2: anyOf([]),
      })
      .orderBy("compareNumber2")
      .get(db);

    expect(resultNone.length).toBe(0);
    expect(resultNone).toEqual([]);
  });
});

test("can use anyOf operator to select by list type", async () => {
  class WithList {
    id = t.generatedId;
    list = t.array(t.uuid);
  }

  class Refed {
    id = t.generatedId;
    name = t.text;
  }

  await resetAndSeed({
    db: db,
    models: [WithList, Refed],
    seeds: [
      async ({ db }) => {
        const [{ id: oneId }, { id: twoId }] = await insertAll(Refed, [
          { name: "one" },
          { name: "two" },
          { name: "three" },
        ]).transact(db);

        await insertAll(WithList, [
          {
            list: [oneId, twoId],
          },
        ]).transact(db);
      },
    ],
  });

  const result = await select(WithList)
    .with((withList) => ({
      refed: select(Refed, "name").where({ id: anyOf(withList.list) }),
    }))
    .one(db);

  expect(result).toEqual({ refed: [{ name: "one" }, { name: "two" }] });
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

describe("page", () => {
  test("returns page count", async () => {
    await resetForSort();

    const results = await page(
      db,
      { limit: 2, offset: 1 },
      select(Sort, "value")
    );

    expect(results).toEqual({
      hasMore: true,
      page: [{ value: 45 }, { value: 9 }],
      total: 7,
    });

    const lastPage = await page(
      db,
      { limit: 1, offset: 6 },
      select(Sort, "value")
    );

    expect(lastPage).toEqual({
      hasMore: false,
      page: [{ value: 5 }],
      total: 7,
    });
  });
});

describe("custom indexes", () => {
  test("full text search index", async () => {
    class Search {
      id = t.generatedId;
      name = t.text;
      description = t.text;
      weighted_tsv = t.optional(t.makeType<string[]>("tsvector"));
      tags = t.optional(t.text);
    }

    const searchIndexes = {
      name: makeIndex(
        Search,
        (search) => sql`gin(${search.name} gin_trgm_ops)`
      ),
      description: makeIndex(
        Search,
        (search) => sql`gin(${search.description} gin_trgm_ops)`
      ),
      weighted_tsv: makeIndex(
        Search,
        (search) => sql`gist(${search.weighted_tsv})`
      ),
    };

    await resetAndSeed({
      db: db,
      before: [...searchExtensions],

      after: [
        maintainWeightedTSV(Search, (search) => ({
          column: "weighted_tsv",
          weights: {
            A: search.name,
            B: search.description,
            C: search.tags,
          },
        })),
      ],

      models: [Search],
      seeds: [
        async ({ db }) => {
          await insertAll(Search, [
            {
              tags: "ballet classical",
              name: "Allégro",
              description:
                "In ballet, allégro is a term applied to bright, fast or brisk steps and movement.  All steps where the dancer jumps are considered allégro, such as sautés, jetés, cabrioles, assemblés, and so on. Allégro in Ballet Class In ballet class, allégro combinations are usually done toward the last part of class, as the dancer is... ",
            },
            {
              name: "Balançoire",
              description:
                "Balançoire is a ballet term applied to exercises such as grande battements or degagés.  When a dancer is doing a combination with balançoire, they will repeatedly swing their leg from front to back and may tilt their upper body slightly forward or backwards, opposite to the direction their leg is moving. ",
            },
            {
              name: "Battement jeté, grand",
              description:
                "Grand Battement Jeté is a classical ballet term meaning a “large battement thrown.” Grand battement jeté is often used in the russian school to better describe how a grand battement is “thrown.”  Its the idea that the working leg quickly gets to the top of the position as opposed to slowly.  A quicker grand battement... ",
            },
            {
              name: "Balloné",
              description:
                "The term balloné in classical ballet technique is step where the leg is extended to the second or fourth position (front, side or back) at 45 degrees; then the knee is bent and the foot brough to a sur le cou-de-pied position.  At 45 degrees, it is called petit balloné and when done at 90 degrees,... ",
            },
            {
              name: "Battement Battu",
              description:
                "Battement Battu is a classical ballet term which means “beaten battement.” Battement battu is done by placing your working foot in a sur cou-de-pied position and taping the opposite leg’s ankle devant or derriere (back or front). Battement Battu in Ballet Class Battement Battu is typically done at barre, during a frappé exercise. Battement Battu... ",
            },
            {
              name: "Cambré",
              description:
                "Cambré is a classical ballet term meaning “arched.” When a dancer is doing cambré, their body is bent from the waist and stretching backward or sideways with the head following the movement of the upper body and arms. ",
            },
            {
              name: "Chassé en tournant",
              description:
                "Chassé en tournant is a classical ballet term meaning “chase, turning.”  This is when a dancer performs a chassé but does a single turn in the air as the feet and legs come together, then lands on the back leg with the front leg extended front.  Like chassés, chassé en tournants can be done in... ",
            },
            {
              name: "Coda",
              description:
                "A Coda is a classical ballet term that refers to the finale of a group of dancers and more often, the finale of a pas de deux. In the typical structure of a pas de deux in classical ballet, the coda is the fourth section, having just followed the female’s variation.  In a pas de... ",
            },
            {
              name: "Coupé",
              description:
                "Coupé is a classical ballet term meaning “cut” or “cutting.”  A coupé describes a step where one foot cuts the other foot away, taking its place.  Its usually done as an in-between step for a larger step, such as a coupé jeté or a coupé-chassé en tournant (the typical preparation for many big jumps for... ",
            },
            {
              name: "Dedans, en",
              description:
                "En Dedans is a classical ballet term meaning “inward.” En dedans is always attached to another ballet term to describe the direction it should move. For example, a pirouette en dedans would mean that a dancer is pushing their back leg to the front and turning “inward” to their supporting leg. Another definition to think... ",
            },
            {
              name: "Détourné, demi",
              description:
                "Demi Détourné is a classical ballet term meaning “half turn aside.”  A demi détourné is when a dancer will do a half turn on both feet on demi-pointe or pointe, while switching the position of the feet as they finish.  It gets its meaning from the ballet terms demi and detourné. A demi detourné is... ",
            },
            {
              name: "Effacé",
              description:
                "Effacé is classical ballet term meaning “shaded.”  The term describes another step or pose in which the legs looks open, or not crossed, when seen from the front. You can say that effacé is the opposite of croisé. A dancer can perform a step effacé devant or derriére, and either à terre (on the floor) or... ",
            },
            {
              name: "Fouetté turns",
              description:
                "Fouetté turns is a classical ballet term meaning “whipped turns.”  A fouetté turn is when a dancer, usually female, does a full turn in passe (pirouette), followed by a plie on the standing leg while the retiré leg extends to  croise front and rond de jambes to the side (a la seconde).  As the leg... ",
            },
            {
              name: "Failli",
              description:
                "Failli is a classical ballet term describing a step where the dancer seems to degage each leg to the front immediately after the other with a small jump.  A failli is usually done as a preperation step for jumps and is considered an in-between step. Because of the nature of a failli, it helps a... ",
            },
            {
              name: "Jeté, grand",
              description:
                "Grand jeté is a classical ballet term meaning “big throw.”  It describes a big jump where the dancer throws one leg into the air, pushes off the floor with the other, jumping into the air and landing again on the first leg. A grand jeté is considered a basic grand allegro step that is often... ",
            },
            {
              name: "Plié",
              description:
                "A plié is when a dancer is basically bending at the knees.  They are typically done in 1st, 2nd, 4th and 5th positions in classical ballet, both at the barre and center in classes.  Correct use and development of a plié is a basic but essential movement to a dancer’s technique. Pliés are often seen... ",
            },
            {
              name: "Pas de deux",
              description:
                "Pas de deux is a classical ballet term meaning “Dance for two” or “steps for two.”  Pas de deux can be used to describe many “dances for two” and is usually used in context or with another word to describe what pas de deux.  Often dancers will shorten pas de deux to simply “pas” since... ",
            },
            {
              name: "Piqué",
              description:
                "Piqué is a classical ballet term meaning “pricking” and is a descriptive word to be used with other ballet terms.  For example, a piqué turn would describe a “pricking turn.”  It is meant to describe how a dancer transfers weight onto a leg on full pointe or high demi-pointe which is also known as piqué... ",
            },
            {
              name: "Pirouette en dehors",
              description:
                "A pirouette en dehors is a classical ballet term meaning “a spin, turning outward” and describes when a dancer turns toward the direction of the leg they lift into the turning position.  For example, a dancer with their left foot in front, will lift the right foot into the pirouette and also turn to the... ",
            },
            {
              name: "Raccourci",
              description:
                "Raccourci is a classical ballet term meaning “shortened.”  It is the same as the ballet term retiré and most commonly used in the French School of ballet. In raccourci, a dancer has his or her working leg in the air at second position, ideally at 90 degrees, with the knee bent so the foot’s toes... ",
            },
            {
              name: "Reverence, grande",
              description:
                "A grande reverence is the elaborate curtsy performed by a female dancer after a performance to acknowledge the applause of the audience. Students, both male and female dancers, can also perform a grande reverence at the end of class to show respect to their teacher (and, if present, piano accompanist). The grande reverence exercise is... ",
            },
            {
              name: "The Russian School",
              description:
                "The French dancer Jean-Baptiste Lande founded the Russian School in 1738 in St. Petersburg where his French influence continued under other great teachers. Then in 1885, Virginia Zucchi, a famous Italian ballerina, performed in St. Petersburg and showed a different style of ballet. Where the Russian ballet dancers were taught to have a soft, elegant... ",
            },
            {
              name: "Sissonne",
              description:
                "Sissonne is a classical ballet term that describes a dancer jumping from two feet and splitting their legs “like scissors” in the air before landing.  It is a very common and popular ballet step, seen in performances and throughout classes of most skill levels. A sissonne in its most simplest form is commonly taught to... ",
            },
          ]).transact(db);
        },
      ],
    });

    const query = "classical ballet";

    const results = await select(Search, "name", "description")
      .where((search) => {
        return matchTSVhWithAllTerms(search.weighted_tsv, query);
      })
      .with((search) => {
        return {
          rank: tsvSearchRank(search.weighted_tsv, query),
        };
      })
      .orderBy("rank", "DESC")
      .get(db);

    expect(results.length).toBe(20);
  });
});
