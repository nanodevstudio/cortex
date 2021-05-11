import { Client } from "pg";
import { closeClient, DBClient, query } from "./dbClient";
import { generateForiegnKeys, generateSQLInsert } from "./generateSchema";
import { Model } from "./model";
import glob from "tiny-glob";
import assert from "assert";
import * as c from "ansi-colors";
import { join } from "path";

export interface SeedContext {
  wait: <R>(dep: SeedFn<R>) => Promise<R>;
  db: DBClient;
}

export interface SeedFn<R = void> {
  (ctx: SeedContext): Promise<R> | R;
}

export type GlobPath = string;

export interface ResetBasis {
  db: DBClient;
  models: (Model<any> | GlobPath)[];
  seeds: (SeedFn<any> | GlobPath)[];
}

const globAbsolute = async (path: string) => {
  return await (await glob(path)).map((fsPath) => join(process.cwd(), fsPath));
};

const resolveModules = async <T>(vals: (T | string)[]): Promise<T[]> => {
  return Promise.all(
    vals.map(async (val) => {
      if (typeof val === "string") {
        const paths = await globAbsolute(val);

        return paths.map((path) => require(path).default);
      }

      return val;
    })
  ).then((res) => res.flat() as T[]);
};

export const runSeeds = async (db: DBClient, seeds: SeedFn<any>[]) => {
  const modulesWaiting = new Map<SeedFn<any>, ((r: any) => void)[]>();
  const results = new Map<SeedFn<any>, any>();

  await Promise.all(
    seeds.map(async (seed) => {
      const result = await seed({
        wait: async <R>(seed: SeedFn<R>) => {
          if (results.has(seed)) {
            return results.get(seed);
          }

          return new Promise((resolve) => {
            if (modulesWaiting.get(seed) == null) {
              modulesWaiting.set(seed, []);
            }

            modulesWaiting.get(seed)!.push(resolve);
          });
        },
        db,
      });

      results.set(seed, result);
      console.log(c.green(`✅ ran seed: ${seed.name}`));

      const waiting = modulesWaiting.get(seed) ?? [];
      modulesWaiting.delete(seed);

      waiting.forEach((resolve) => resolve(result));
    })
  );
};

export const buildSchemaAndSeed = async (basis: ResetBasis) => {
  await query(basis.db, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);

  const models = await resolveModules(basis.models);
  const seeds = await resolveModules(basis.seeds);

  assert(
    models.every((model) => model instanceof Function),
    "Expected cortext models to be classes"
  );

  assert(
    seeds.every((seed) => seed instanceof Function),
    "Expected cortex seeds to be functions, or globs to modules with seed fns"
  );

  const insertQueries = models.map((model) => generateSQLInsert(model));
  for (const insertQuery of insertQueries) {
    const res = await query(basis.db, insertQuery);
  }

  const fks = models
    .map((model) => generateForiegnKeys(model).join(";\n"))
    .join(";\n");

  if (fks.length > 0) {
    await query(basis.db, fks);
  }

  await runSeeds(basis.db, seeds);
};

export const resetAndSeed = async (basis: ResetBasis) => {
  await closeClient(basis.db);

  const adminClient = new Client({
    ...basis.db.config,
    database: "postgres",
  });

  await adminClient.connect();
  await adminClient.query(
    `DROP DATABASE IF EXISTS ${basis.db.config.database};`
  );

  await adminClient.query(`CREATE DATABASE ${basis.db.config.database};`);
  await adminClient.end();

  return buildSchemaAndSeed(basis);
};
