import { querySQL } from "./dbClient";
import { getQualifiedSQLColumn, getQualifiedSQLTable } from "./generateSchema";
import { Model } from "./model";
import { DBQuery, emptyQuery } from "./query";
import {
  DecodeSelector,
  FieldSelection,
  IDecodeSelector,
  ModelSymbol,
  symbolFromQuery,
} from "./symbolic";
import { joinSQL, raw, sql, SQLSegmentList } from "./writes";
import immer from "immer";
import * as uuid from "uuid";

export const searchExtensions = [
  sql`CREATE EXTENSION IF NOT EXISTS pg_trgm`,
  sql`CREATE EXTENSION IF NOT EXISTS btree_gin`,
];

export const maintainWeightedTSV = <M>(
  model: Model<M>,
  getOptions: (symbol: ModelSymbol<M>) => {
    column: keyof M;
    weights: Partial<{
      A: SQLSegmentList | FieldSelection<any, any>;
      B: SQLSegmentList | FieldSelection<any, any>;
      C: SQLSegmentList | FieldSelection<any, any>;
      D: SQLSegmentList | FieldSelection<any, any>;
    }>;
  }
) => {
  const query = immer(emptyQuery(model), (query) => {
    query.id = "new";
  });
  const options = getOptions(symbolFromQuery(query));
  const weightSegments = Object.entries(options.weights).map(
    ([priority, value]) => {
      return sql`setweight(to_tsvector('english', COALESCE(${value}, '')), '${raw(
        priority
      )}')`;
    }
  );

  const fnName = raw(
    JSON.stringify(`maintainWeightedTSV.${options.column}.update_tsv_fn`)
  );
  const triggerName = raw(
    JSON.stringify(`maintainWeightedTSV.${options.column}.update_trigger`)
  );

  return sql`
CREATE FUNCTION ${fnName}() RETURNS trigger AS $$  
begin  
  ${raw(getQualifiedSQLColumn(query, options.column as any))} :=
     ${joinSQL(weightSegments, sql` || `)};
  return new;
end  
$$ LANGUAGE plpgsql;

CREATE TRIGGER ${triggerName} BEFORE INSERT OR UPDATE  
ON ${raw(getQualifiedSQLTable(model))}
FOR EACH ROW EXECUTE PROCEDURE ${fnName}(); 
	`;
};

export const tsvSearchRank = (
  column: any,
  query: string
): IDecodeSelector<number> => {
  return {
    id: uuid.v4(),
    select: sql`ts_rank_cd(${column},  plainto_tsquery('english', ${query}))`,
  };
};

export const matchTSVhWithAllTerms = (column: any, query: string) => {
  return sql`${column} @@ plainto_tsquery('english', ${query})`;
};

export const joinTerms = (decode: DecodeSelector<any>) => {
  return sql`select string_agg(DISTINCT text, ', ') from (${decode}) as str_query`;
};
