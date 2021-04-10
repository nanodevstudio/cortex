import { DBClient, query } from "./dbClient";
import {
  getQualifiedSQLColumn,
  getQualifiedSQLTable,
  getSQLName,
} from "./generateSchema";
import { Model } from "./model";
import { FieldTypeF } from "./types";
import * as uuid from "uuid";

export interface QueryData<M, SelectData extends any[]> {
  model: Model<M>;
  selectKeys: SelectData;
  select: string[];
  where: string[];
  vars: {
    [id: string]: any;
  };
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

export const convertToSelect = (query: QueryData<any, any>) => {
  const sql = `SELECT ${query.select.join(", ")} FROM ${getQualifiedSQLTable(
    query.model
  )} ${query.where.length > 0 ? `WHERE ${query.where.join(" AND ")}` : ""};`;

  return [
    Object.keys(query.vars).reduce((sql, varId, i) => {
      return sql.replace(new RegExp(varId), () => `$${i + 1}`);
    }, sql),
    Object.values(query.vars),
  ] as const;
};

class DBQuery<M, SelectData extends any[]> {
  constructor(public query: QueryData<M, SelectData>) {}

  async get(db: DBClient): Promise<QueryResult<QueryData<M, SelectData>>[]> {
    const [sql, values] = convertToSelect(this.query);
    const queryResult = await query(db, sql, values);

    return queryResult.rows;
  }

  async one(
    db: DBClient
  ): Promise<QueryResult<QueryData<M, SelectData>> | undefined> {
    return (await this.get(db))[0];
  }

  where(clause: WhereClause<M>) {
    const newVars: QueryData<any, any>["vars"] = {};
    const clauses = Object.entries(clause).map(([key, value]) => {
      const varId = uuid.v4();
      newVars[varId] = value;
      return `${getQualifiedSQLColumn(this.query.model, key)} = ${varId}`;
    });

    return new DBQuery({
      ...this.query,
      vars: {
        ...this.query.vars,
        ...newVars,
      },
      where: [...this.query.where, ...clauses],
    });
  }
}

const emptyQuery = <M>(model: Model<M>): QueryData<M, any> => ({
  model,
  select: [],
  selectKeys: [],
  where: [],
  vars: {},
});

export const select = <T, SelectData extends (keyof T)[]>(
  model: Model<T>,
  ...keys: SelectData
) => {
  return new DBQuery<T, SelectData>({
    ...emptyQuery(model),
    selectKeys: keys,
    select: keys.map((key) => getQualifiedSQLColumn(model, key as any)),
  });
};
