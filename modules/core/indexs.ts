import { createHash } from "crypto";
import { getQualifiedSQLTable } from "./generateSchema";
import { Model } from "./model";
import { emptyQuery } from "./query";
import { ModelSymbol, symbolFromQuery } from "./symbolic";
import { getQueryFromSegments, raw, sql, SQLSegment } from "./writes";

export const makeIndex = <M>(
  model: Model<M>,
  opts: (model: ModelSymbol<M>) => SQLSegment
) => {
  const query = emptyQuery(model, { qualify: false });
  const symbol = symbolFromQuery(query);
  const usingQuery = opts(symbol);
  const onQuery = sql`ON ${raw(
    getQualifiedSQLTable(model)
  )} USING ${usingQuery}`;

  const md5 = createHash("md5");
  md5.update(JSON.stringify(getQueryFromSegments(onQuery)));
  const hash = md5.digest("hex");

  const indexQuery = sql`CREATE INDEX IF NOT EXISTS ${raw(
    JSON.stringify(hash)
  )} ${onQuery}`;

  if (model.indexes == null) {
    model.indexes = [];
  }

  model.indexes?.push(indexQuery);

  return {
    id: hash,
  };
};
