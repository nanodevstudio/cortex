import { Client } from "pg";
import { closeClient, DBClient, query } from "./dbClient";
import { generateForiegnKeys, generateSQLInsert } from "./generateSchema";
import { Model } from "./model";

interface SeedFn {
  (client: DBClient): Promise<void>;
}

interface ResetBasis {
  client: DBClient;
  models: Model<any>[];
  seeders: SeedFn[];
}

export const buildSchemaAndSeed = async (basis: ResetBasis) => {
  await query(basis.client, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);

  const insertQueries = basis.models.map((model) => generateSQLInsert(model));
  for (const insertQuery of insertQueries) {
    const res = await query(basis.client, insertQuery);
  }

  const fks = basis.models
    .map((model) => generateForiegnKeys(model).join(";\n"))
    .join(";\n");

  if (fks.length > 0) {
    await query(basis.client, fks);
  }

  for (const seeder of basis.seeders) {
    await seeder(basis.client);
  }
};

export const resetAndSeed = async (basis: ResetBasis) => {
  await closeClient(basis.client);

  const adminClient = new Client({
    ...basis.client.config,
    database: "postgres",
  });

  await adminClient.connect();
  await adminClient.query(
    `DROP DATABASE IF EXISTS ${basis.client.config.database} WITH (FORCE);`
  );

  await adminClient.query(`CREATE DATABASE ${basis.client.config.database};`);
  await adminClient.end();

  return buildSchemaAndSeed(basis);
};
