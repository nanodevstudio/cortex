import immer from "immer";
import * as uuid from "uuid";
import { DBClient, getPGClient, query, querySQL } from "./dbClient";
import {
  getModelField,
  getModelInstance,
  getQualifiedSQLColumn,
  getQualifiedSQLTable,
} from "./generateSchema";
import { Model } from "./model";
import { ProtectPromise } from "./protectPromise";
import {
  addWhereClause,
  emptyQuery,
  makeSelectClause,
  QueryData,
  SelectRow,
  WhereClause,
  whereToSQL,
} from "./query";
import escape from "pg-escape";
import { ModelSymbol, QueryExpression, symbolFromQuery } from "./symbolic";
import { queryExpression } from "./symbols";
import { FieldType, FieldTypeF } from "./types";

export type UpdateTypeOfField<Field> = Field extends FieldTypeF<any, infer U>
  ? U
  : never;

export type SelectFieldUpdateType<M, key> = key extends keyof M
  ? UpdateTypeOfField<M[key]>
  : never;

export type SelectField<M, key> = key extends keyof M ? M[key] : never;

export type ExistsOrNever<T> = T extends undefined ? never : T;
export type UndefinedOrNever<T> = T extends undefined ? T : never;

export type InsertValue<M, T> =
  | T
  | ((modelSymbol: ModelSymbol<M>) => SQLSegment | QueryExpression<M, T>);

export type InsertInput<M> = {
  [key in keyof M as M[key] extends FieldTypeF<any, infer U, any>
    ? undefined extends U
      ? never
      : key
    : never]-?: M[key] extends FieldTypeF<any, infer T, any>
    ? InsertValue<M, T>
    : never;
} &
  {
    [key in keyof M as M[key] extends FieldTypeF<any, infer U, any>
      ? undefined extends U
        ? key
        : never
      : never]?: M[key] extends FieldTypeF<any, infer T, any>
      ? InsertValue<M, T>
      : never;
  };

export type PrimaryResult<M> = {
  [key in keyof M as M[key] extends FieldTypeF<any, any, true>
    ? key
    : never]: M[key] extends FieldTypeF<infer T, any, true> ? T : never;
};

export type SQLSegment = SQLSegmentList | SQLValue | SQLString;

export class SQLSegmentList {
  constructor(public items: (SQLValue | SQLString | SQLSegmentList)[]) {}
}

export class SQLValue {
  constructor(public value: any) {}
}

export class SQLString {
  constructor(public string: string) {}
}

class ArrayEncode {
  constructor(public value: any[]) {}

  toPostgres(prep: (value: any) => any) {
    return prep(JSON.stringify(this.value));
  }
}

export const escapeValue = (value: any): string => {
  if (typeof value === "string") {
    return escape.literal(value);
  }

  if (typeof value === "number") {
    return `${value}`;
  }

  if (typeof value === "boolean") {
    return value ? "TRUE" : "FALSE";
  }

  if (value === null) {
    return "NULL";
  }

  if (Array.isArray(value)) {
    return `(${value.map((value) => escapeValue(value)).join(", ")}}`;
  }

  if (value && value.toPostgres) {
    return value.toPostgres(escapeValue);
  }

  throw new Error(`illegal sql value ${value}`);
};

export const getQueryFromSegments = (segment: SQLSegmentList) => {
  const values: any[] = [];

  const combineSegments = (segment: SQLSegmentList): string => {
    return segment.items
      .map((item) => {
        if (item instanceof SQLValue) {
          const value = item.value;

          return escapeValue(value);
        }

        if (item instanceof SQLString) {
          return item.string;
        }

        return combineSegments(item);
      })
      .join("");
  };

  return [combineSegments(segment), values] as const;
};

export const sql = (strings: TemplateStringsArray, ...values: any[]) => {
  return new SQLSegmentList(
    strings.flatMap((string, i) => {
      if (values.length === i) {
        return [new SQLString(string)];
      }

      const value = values[i];
      let segment;

      if (
        value instanceof SQLValue ||
        value instanceof SQLString ||
        value instanceof SQLSegmentList
      ) {
        segment = value;
      } else if (value && value[queryExpression]) {
        segment = value[queryExpression]().sql;
      } else {
        segment = new SQLValue(value);
      }

      return [new SQLString(string), segment];
    })
  );
};

export const raw = (text: string) => new SQLString(text);

export const joinSQL = (
  segments: (SQLSegment | null)[],
  join: SQLSegment = sql` `
) => {
  return new SQLSegmentList(
    segments
      .filter((item): item is SQLSegmentList => item != null)
      .flatMap((segment, i) => [...(i === 0 ? [] : [join]), segment])
  );
};

class InsertQuery<M> extends ProtectPromise {
  constructor(public model: Model<M>, public data: InsertInput<M>) {
    super(".transact(db)");
  }

  toSQL() {
    const keys = Object.keys(this.data);
    const values = Object.values(this.data);
    const primaryKeys = Object.keys(getModelInstance(this.model)).filter(
      (key) => getModelField(this.model, key)?.primary
    );

    const keysSegment = raw(
      `${keys.map((key) => JSON.stringify(key)).join(", ")}`
    );
    const valuesSegment = joinSQL(
      values.map((value, i) => {
        const key = keys[i];
        const field = getModelField(this.model, key)!;

        if (field == null) {
          throw new Error(`no such field ${this.model.name}::${key}`);
        }

        return new SQLValue(encodeValue(field, value));
      }),
      sql`, `
    );

    const modelName = raw(getQualifiedSQLTable(this.model));
    const primaryKeysSegment = raw(primaryKeys.join(", "));

    return sql`INSERT INTO ${modelName} (${keysSegment}) VALUES (${valuesSegment})${
      primaryKeys.length > 0 ? sql` RETURNING ${primaryKeysSegment}` : sql``
    };`;
  }

  async transact(db: DBClient): Promise<PrimaryResult<M>> {
    const [sql, values] = getQueryFromSegments(this.toSQL());
    const result = await query(db, sql, values);

    return result.rows[0];
  }
}

class InsertAllQuery<M> extends ProtectPromise {
  constructor(public model: Model<M>, public data: InsertInput<M>[]) {
    super(".transact(db)");
  }

  async transact(db: DBClient): Promise<PrimaryResult<M>[]> {
    const client = await getPGClient(db);
    let results: any[] = [];
    // HACK just using tx for now
    await client.query("BEGIN");

    for (const insert of this.data.map(
      (data) => new InsertQuery(this.model, data)
    )) {
      results.push(await insert.transact(db));
    }

    await client.query("COMMIT");

    return results;
  }
}

export const insert = <M>(model: Model<M>, record: InsertInput<M>) => {
  return new InsertQuery<M>(model, record);
};

export const insertAll = <M>(model: Model<M>, record: InsertInput<M>[]) => {
  return new InsertAllQuery<M>(model, record);
};

export const encodeValue = (
  field: FieldType<any, any, any, any>,
  value: any
) => {
  if (field.encode) {
    const res = field.encode(value);
    res;
    return res;
  }

  return value;
};

class UpdateQuery<M, SelectData extends any[]> extends ProtectPromise {
  constructor(
    public model: Model<M>,
    public record: Partial<InsertInput<M>>,
    public query: QueryData<M, SelectData>
  ) {
    super(".transact(db)");
  }

  where(clause: WhereClause<M>) {
    return new UpdateQuery(
      this.model,
      this.record,
      addWhereClause(this.query, clause)
    );
  }

  return<NextSelectData extends (keyof M)[]>(...select: NextSelectData) {
    return new UpdateQuery<M, [...SelectData, ...NextSelectData]>(
      this.model,
      this.record,
      immer(this.query, (query) => {
        query.selectKeys.push(
          ...select.map((key) => ({
            key: key,
            selector: {
              id: uuid.v4(),
              select: raw(getQualifiedSQLColumn(this.query, key as any)),
            },
          }))
        );
      }) as any
    );
  }

  toSQL() {
    const { record, query, model } = this;

    const assignments = Object.entries(record).map(([key, value]) => {
      const field = getModelField(model, key)!;

      if (field == null) {
        throw new Error(`no such field ${model.name}::${key}`);
      }

      const column = raw(JSON.stringify(key));
      let result: any = value;

      if (result && result instanceof Function) {
        result = result(symbolFromQuery(query));
      }

      if (result && result[queryExpression]) {
        result = result[queryExpression]().sql;
      }

      if (isSQLSegment(result)) {
        return sql`${column} = ${result}`;
      }

      return sql`${column} = ${encodeValue(field, result)}`;
    });

    return joinSQL([
      sql`UPDATE ${raw(getQualifiedSQLTable(model))} as ${raw(
        JSON.stringify(query.id)
      )}`,
      sql`SET ${joinSQL(assignments, sql`, `)}`,
      whereToSQL(query.where),
      query.selectKeys.length > 0
        ? sql`RETURNING ${makeSelectClause(query)}`
        : null,
    ]);
  }

  async transact(db: DBClient): Promise<SelectRow<M, SelectData>[]> {
    const [sql, values] = getQueryFromSegments(this.toSQL());
    const result = await query(db, sql, values);

    return result.rows;
  }
}

class RemoveQuery<M, SelectData extends any[]> extends ProtectPromise {
  constructor(
    public model: Model<M>,
    public query: QueryData<M, SelectData>,
    public protectRemoveAll: boolean = true
  ) {
    super(".transact(db)");
  }

  where(clause: WhereClause<M>) {
    return new RemoveQuery(
      this.model,
      addWhereClause(this.query, clause),
      false
    );
  }

  return<NextSelectData extends (keyof M)[]>(...select: NextSelectData) {
    return new RemoveQuery<M, [...SelectData, ...NextSelectData]>(
      this.model,
      immer(this.query, (query) => {
        query.selectKeys.push(
          ...select.map((key) => ({
            key: key,
            selector: {
              id: uuid.v4(),
              select: raw(getQualifiedSQLColumn(this.query, key as any)),
            },
          }))
        );
      }) as any,
      this.protectRemoveAll
    );
  }

  allowDeleteAll() {
    return new RemoveQuery<M, SelectData>(this.model, this.query, false);
  }

  toSQL() {
    const { query, model } = this;

    if (this.protectRemoveAll) {
      throw new Error(
        "This query would remove all documents from this table. We prevented it, if you'd really like to do this then use the .allowDeleteAll() method"
      );
    }

    return joinSQL([
      sql`DELETE FROM ${raw(getQualifiedSQLTable(model))} as ${raw(
        JSON.stringify(query.id)
      )}`,
      whereToSQL(query.where),
      query.selectKeys.length > 0
        ? sql`RETURNING ${makeSelectClause(query)}`
        : null,
    ]);
  }

  async transact(db: DBClient): Promise<SelectRow<M, SelectData>[]> {
    const [sql, values] = getQueryFromSegments(this.toSQL());
    const result = await query(db, sql, values);

    return result.rows;
  }
}

export const transact = async <M>(
  db: DBClient,
  txs: { toSQL: () => SQLSegment }[]
): Promise<any[]> => {
  const query = joinSQL(
    txs.map((tx) => tx.toSQL()),
    sql`;`
  );

  const result = await querySQL(db, query);

  return result.rows as any;
};

export const update = <M>(model: Model<M>, record: Partial<InsertInput<M>>) => {
  return new UpdateQuery<M, []>(model, record, emptyQuery(model));
};

export const remove = <M>(model: Model<M>) => {
  return new RemoveQuery<M, []>(model, emptyQuery(model));
};

export const isSQLSegment = (sql: any): sql is SQLSegment => {
  return (
    (sql != null && sql instanceof SQLSegmentList) ||
    sql instanceof SQLString ||
    sql instanceof SQLValue
  );
};
