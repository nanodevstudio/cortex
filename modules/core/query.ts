import { DBClient, query } from "./dbClient";
import {
  getQualifiedSQLColumn,
  getQualifiedSQLTable,
  getSQLName,
} from "./generateSchema";
import { Model } from "./model";
import { FieldTypeF } from "./types";
import * as uuid from "uuid";
import {
  getQueryFromSegments,
  joinSQL,
  raw,
  sql,
  SQLSegment,
  SQLSegmentList,
} from "./writes";
import immer from "immer";

export interface QueryData<M, SelectData extends any[]> {
  model: Model<M>;
  selectKeys: SelectData;
  select: SQLSegment[];
  where: SQLSegment[];
}

export type TypeOfField<Field> = Field extends FieldTypeF<infer T> ? T : never;

export type SelectFieldValue<M, key> = key extends keyof M
  ? TypeOfField<M[key]>
  : never;

export type SelectRow<M, SelectData extends any[]> = {
  [key in SelectData[number]]: SelectFieldValue<M, key>;
};

export type ModelT<M> = M extends Model<infer t> ? t : never;

export type QueryResult<data> = data extends QueryData<infer M, infer S>
  ? SelectRow<M, S>
  : never;

export type WhereClause<M> = Partial<
  {
    [key in keyof M]: SelectFieldValue<M, key>;
  }
>;

export const whereToSQL = (where: QueryData<any, any>["where"]) => {
  return where.length > 0 ? sql`WHERE ${joinSQL(where, sql` AND `)}` : null;
};

const convertToSelect = (query: QueryData<any, any>) => {
  return joinSQL([
    sql`SELECT ${joinSQL(query.select, sql`, `)}`,
    sql`FROM ${raw(getQualifiedSQLTable(query.model))}`,
    whereToSQL(query.where),
  ]);
};

export const addWhereClause = <Q extends QueryData<any, any>>(
  query: Q,
  clause: WhereClause<Q["model"]>
) => {
  return immer(query, (query) => {
    query.where.push(
      ...Object.entries(clause).map(([key, value]) => {
        return sql`${raw(getQualifiedSQLColumn(query.model, key))} = ${value}`;
      })
    );
  });
};

class DBQuery<M, SelectData extends any[]> {
  constructor(public query: QueryData<M, SelectData>) {}

  async get(db: DBClient): Promise<QueryResult<QueryData<M, SelectData>>[]> {
    const [sql, values] = getQueryFromSegments(convertToSelect(this.query));
    const queryResult = await query(db, sql, values);

    return queryResult.rows;
  }

  async one(
    db: DBClient
  ): Promise<QueryResult<QueryData<M, SelectData>> | undefined> {
    return (await this.get(db))[0];
  }

  where(clause: WhereClause<M>) {
    return new DBQuery(addWhereClause(this.query, clause));
  }
}

export const emptyQuery = <M>(model: Model<M>): QueryData<M, any> => ({
  model,
  select: [],
  selectKeys: [],
  where: [],
});

export const select = <T, SelectData extends (keyof T)[]>(
  model: Model<T>,
  ...keys: SelectData
) => {
  return new DBQuery<T, SelectData>({
    ...emptyQuery(model),
    selectKeys: keys,
    select: keys.map((key) => raw(getQualifiedSQLColumn(model, key as any))),
  });
};
