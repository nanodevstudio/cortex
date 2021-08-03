import { basename } from "path";
import { DBClient, querySQL } from "./dbClient";
import { resolveModulesWithNames } from "./reset";
import { raw, sql } from "./writes";
import * as c from "ansi-colors";

export type MigrationFn = (db: DBClient) => void | Promise<void>;

export interface MigrationOptions {
  db: DBClient;
  migrations: string[];
}

const migrationTableName = "__cortex__migrations";

export const migrate = async ({ db, migrations }: MigrationOptions) => {
  await querySQL(db, sql`BEGIN TRANSACTION;`);

  await querySQL(
    db,
    sql`CREATE TABLE IF NOT EXISTS ${raw(migrationTableName)} (
		id text PRIMARY KEY NOT NULL,
		"transactedAt" timestamp without time zone NOT NULL
	 );`
  );

  const ranMigrations = new Set(
    (
      await querySQL(db, sql`SELECT id FROM ${raw(migrationTableName)};`)
    ).rows.map((value) => value.id)
  );

  const migrationFns = await resolveModulesWithNames<MigrationFn>(migrations);
  const newMigrationFns = migrationFns
    .map((value) => ({ ...value, id: basename(value.path) }))
    .filter((migration) => !ranMigrations.has(migration.id))
    .sort((a, b) => {
      return a.id.localeCompare(b.id);
    });

  for (const migration of newMigrationFns) {
    try {
      await migration.module(db);
      await querySQL(
        db,
        sql`INSERT INTO ${raw(migrationTableName)} VALUES (${migration.id});`
      );
      console.log(c.green(`✅ ran migration: ${migration.id}`));
    } catch (e) {
      console.error(e);
      console.log(c.green(`❌ migration failed, rolledback: ${migration.id}`));
      await querySQL(db, sql`ROLLBACK;`);
      return;
    }
  }

  await querySQL(db, sql`COMMIT;`);
};
