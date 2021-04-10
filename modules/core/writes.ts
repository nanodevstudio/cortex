import { DBClient, getPGClient, query } from "./dbClient";
import {
  getModelField,
  getModelInstance,
  getQualifiedSQLColumn,
  getQualifiedSQLTable,
  getSQLName,
} from "./generateSchema";
import { Model } from "./model";
import { FieldType, FieldTypeF } from "./types";

type UndefinedProperties<T> = {
  [P in keyof T]-?: undefined extends T[P] ? P : never;
}[keyof T];

type ToOptional<T> = Partial<Pick<T, UndefinedProperties<T>>> &
  Pick<T, Exclude<keyof T, UndefinedProperties<T>>>;

export type UpdateTypeOfField<Field> = Field extends FieldTypeF<any, infer U>
  ? U
  : never;

export type SelectFieldUpdateType<M, key> = key extends keyof M
  ? UpdateTypeOfField<M[key]>
  : never;

export type SelectField<M, key> = key extends keyof M ? M[key] : never;

export type ExistsOrNever<T> = T extends undefined ? never : T;
export type UndefinedOrNever<T> = T extends undefined ? T : never;

export type InsertInput<M> = {
  [key in keyof M as M[key] extends FieldTypeF<any, infer U, any>
    ? undefined extends U
      ? never
      : key
    : never]-?: M[key] extends FieldTypeF<any, infer T, any> ? T : never;
} &
  {
    [key in keyof M as M[key] extends FieldTypeF<any, infer U, any>
      ? undefined extends U
        ? key
        : never
      : never]?: M[key] extends FieldType<any, infer T, any> ? T : never;
  };

export type PrimaryResult<M> = {
  [key in keyof M as M[key] extends FieldTypeF<any, any, true>
    ? key
    : never]: M[key] extends FieldTypeF<infer T, any, true> ? T : never;
};

class SQLSegmentList {
  constructor(public items: (SQLValue | SQLString | SQLSegmentList)[]) {}
}

class SQLValue {
  constructor(public value: any) {}
}

class SQLString {
  constructor(public string: string) {}
}

export const getQueryFromSegments = (segment: SQLSegmentList) => {
  const values: any[] = [];

  const combineSegments = (segment: SQLSegmentList): string => {
    return segment.items
      .map((item) => {
        if (item instanceof SQLValue) {
          values.push(item.value);
          return `$${values.length}`;
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
      const segment =
        value instanceof SQLValue ||
        value instanceof SQLString ||
        value instanceof SQLSegmentList
          ? value
          : new SQLValue(value);

      return [new SQLString(string), segment];
    })
  );
};

export const raw = (text: string) => new SQLString(text);

class InsertQuery<M> {
  constructor(public model: Model<M>, public data: InsertInput<M>) {}

  toSQL() {
    const keys = Object.keys(this.data);
    const values = Object.values(this.data);
    const primaryKeys = Object.keys(getModelInstance(this.model)).filter(
      (key) => getModelField(this.model, key)?.primary
    );

    const keysSegment = raw(
      `${keys.map((key) => JSON.stringify(key)).join(", ")}`
    );
    const valuesSegment = new SQLSegmentList(
      values.map((value, i) => {
        const segment = sql`${raw(i === 0 ? "" : ",")}${value}`;
        return segment;
      })
    );

    const modelName = raw(getQualifiedSQLTable(this.model));
    const primaryKeysSegment = raw(primaryKeys.join(", "));

    return sql`INSERT INTO ${modelName} (${keysSegment}) VALUES (${valuesSegment}) RETURNING ${primaryKeysSegment};`;
  }

  async transact(db: DBClient): Promise<PrimaryResult<M>> {
    const [sql, values] = getQueryFromSegments(this.toSQL());
    const result = await query(db, sql, values);

    return result.rows[0];
  }
}

class InsertAllQuery<M> {
  constructor(public model: Model<M>, public data: InsertInput<M>[]) {}

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
