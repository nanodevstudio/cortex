import { runSeeds, SeedFn } from "@/core/reset";
import { makeClient } from "../dbClient";

const stubDb = makeClient({
  port: 4,
  host: "fake",
});

describe("runSeeds()", () => {
  test("can depend on modules and order is reliable", async () => {
    const runOrder: string[] = [];

    const first: SeedFn<number> = async () => {
      runOrder.push(first.name);
      return 5;
    };

    const second: SeedFn<void> = async ({ wait }) => {
      await wait(first);
      runOrder.push(second.name);
    };

    const third: SeedFn<void> = async ({ wait }) => {
      await wait(second);
      runOrder.push(third.name);
    };

    const fourth: SeedFn<void> = async ({ wait }) => {
      await wait(third);
      await wait(first);
      runOrder.push(fourth.name);
    };

    await runSeeds(stubDb, [third, fourth, first, second]);

    expect(runOrder).toEqual(["first", "second", "third", "fourth"]);
  });
});
