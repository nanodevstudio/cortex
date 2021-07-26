export const migrationTemplate = `
import { DBClient, querySQL, sql } from '@nanodev/cortex'

export default async (db: DBClient) => {
  await querySQL(db, sql\`\`)
}
`;
