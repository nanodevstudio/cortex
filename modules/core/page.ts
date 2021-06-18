import { DBClient, query, querySQL } from "./dbClient";
import { DBQuery, QueryData, QueryResult } from "./query";
import { decodeSelector } from "./symbols";
import { sql } from "./writes";

interface PageOptions {
  offset: number;
  limit: number;
}

export const page = async <M, SelectData extends any[]>(
  db: DBClient,
  pageOptions: PageOptions,
  dataQuery: DBQuery<M, SelectData>
): Promise<{
  total: number;
  hasMore: boolean;
  page: QueryResult<QueryData<M, SelectData>>[];
}> => {
  const pageQuery = await querySQL(
    db,
    sql`WITH data_query as (${dataQuery.toSegment()}) SELECT (SELECT count(dqc.*) FROM data_query as dqc) as total_count, (SELECT json_agg(dq.*) FROM (SELECT dql.* FROM data_query as dql LIMIT ${
      pageOptions.limit
    } OFFSET ${pageOptions.offset}) as dq) as data`
  );

  const dataRow = pageQuery.rows[0];
  const results =
    dataQuery[decodeSelector].decodeResult?.(pageQuery.rows[0], "data") ??
    pageQuery.rows[0].data;

  const lastIndex = results.length + pageOptions.offset;
  const total = parseInt(dataRow.total_count, 10);

  return {
    total: total,
    page: results,
    hasMore: lastIndex !== total,
  };
};
