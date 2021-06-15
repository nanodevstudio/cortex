import immer from "immer";
import * as uuid from "uuid";
import { DBClient, query } from "./dbClient";
import assert from "assert";
import {
  getModelField,
  getModelInstance,
  getQualifiedSQLColumn,
  getQualifiedSQLTable,
} from "./generateSchema";
import { Model } from "./model";
import { isWhereOperator, WhereOperator } from "./operators";
import { ProtectPromise } from "./protectPromise";
import {
  DecodeSelector,
  FieldSelection,
  FieldSelectionDecoder,
  IDecodeSelector,
  ModelSymbol,
  ObjectToSelectionEntries,
  QueryExpression,
  SelectionEntry,
  symbolFromQuery,
} from "./symbolic";
import { decodeSelector, queryExpression, symbolQuery } from "./symbols";
import { FieldTypeF } from "./types";
import {
  getQueryFromSegments,
  isSQLSegment,
  joinSQL,
  raw,
  sql,
  SQLSegment,
} from "./writes";

export interface SQLJoin<M> {
  id: string;
  type: "inner" | "outer" | "left";
  model: Model<M>;
  clauses: SQLSegment[];
}

export interface QueryData<M, SelectData extends any[]> {
  id: string;
  orderBy: SQLSegment[];
  model: Model<M>;
  selectKeys: SelectData;
  limit?: number;
  where: SQLSegment[];
  join: {
    type: "inner" | "outer" | "left";
    query: QueryData<any, any[]>;
  }[];
}

export type TypeOfField<Field> = Field extends FieldTypeF<
  infer T,
  any,
  any,
  any
>
  ? T
  : never;

export type ReferenceModel<Field> = Field extends FieldTypeF<
  any,
  any,
  any,
  { model: Model<infer M>; column: any }
>
  ? M
  : never;

export type SelectFieldValue<M, Selector> = Selector extends keyof M
  ? TypeOfField<M[Selector]>
  : Selector extends SelectionEntry<any, infer Result>
  ? Result
  : never;

export type Strings<K> = K extends string ? K : never;

type DecodeSelectors<K> = K extends SelectionEntry<any, any> ? K : never;

export type SelectRow<M, SelectData extends any[]> = {
  [key in Strings<SelectData[number]>]: SelectFieldValue<M, key>;
} &
  {
    [key in DecodeSelectors<SelectData[number]>["key"]]: Extract<
      SelectData[number],
      { key: key }
    > extends SelectionEntry<any, infer Res>
      ? Res
      : never;
  };

export type ModelT<M> = M extends Model<infer t> ? t : never;

export type QueryResult<data> = data extends QueryData<infer M, infer S>
  ? SelectRow<M, S>
  : never;

export type RelatedFilter<Ft> = WhereClause<ReferenceModel<Ft>>;

export type WhereClauseData<M> = Partial<
  {
    [key in keyof M]:
      | SelectFieldValue<M, key>
      | RelatedFilter<M[key]>
      | QueryExpression<any, SelectFieldValue<M, key>>
      | WhereOperator<M, SelectFieldValue<M, key>>
      | SQLSegment;
  }
>;

export type WhereClause<M> =
  | WhereClauseData<M>
  | ((model: ModelSymbol<M>) => WhereClauseData<M> | SQLSegment)
  | SQLSegment
  | null;

export const whereToSQL = (where: QueryData<any, any>["where"]) => {
  return where.length > 0 ? sql`WHERE ${joinSQL(where, sql` AND `)}` : null;
};

export const flattenJoins = (
  join: QueryData<any, any>["join"]
): QueryData<any, any>["join"] => {
  return join.flatMap((join) => [join, ...flattenJoins(join.query.join)]);
};

export const joinsToSQL = (join: QueryData<any, any>["join"]) => {
  if (join.length === 0) {
    return null;
  }

  const flattened = flattenJoins(join);

  return joinSQL(
    flattened.map((join) => {
      return sql`${raw(join.type.toUpperCase())} JOIN ${raw(
        getQualifiedSQLTable(join.query.model)
      )} as ${raw(JSON.stringify(join.query.id))} ON ${joinSQL(
        join.query.where,
        sql` AND `
      )}`;
    })
  );
};

export const makeJSONSelectClause = (
  single: boolean,
  query: QueryData<any, any>
) => {
  if (query.selectKeys.length === 0) {
    return joinSQL(
      Object.keys(getModelInstance(query.model))
        .filter((key) => {
          return getModelField(query.model, key)?.primary;
        })
        .map((key) => {
          return raw(getQualifiedSQLColumn(query, key));
        }),
      sql`, `
    );
  }

  const clauses = joinSQL(
    query.selectKeys.map(
      ({ selector, key }: any) =>
        sql`${key}::text, ${getSelector(selector).select}`
    ),
    sql`, `
  );

  if (single) {
    return sql`to_json(json_build_object(${clauses}))`;
  }

  return sql`to_json(array_agg(json_build_object(${clauses})))`;
};

const limitToSQL = (limit: number | undefined) => {
  return limit != null ? sql`LIMIT ${limit}` : null;
};

const convertToJSONSingleSelect = (query: QueryData<any, any>) => {
  return joinSQL([
    sql`SELECT ${makeJSONSelectClause(true, query)}`,
    sql`FROM ${raw(getQualifiedSQLTable(query.model))} as ${raw(
      JSON.stringify(query.id)
    )}`,
    joinsToSQL(query.join),
    whereToSQL(query.where),
    orderToSQL(query.orderBy),
    limitToSQL(query.limit),
  ]);
};

const convertToJSONSelect = (query: QueryData<any, any>) => {
  return joinSQL([
    sql`SELECT ${makeJSONSelectClause(false, query)}`,
    sql`FROM ${raw(getQualifiedSQLTable(query.model))} as ${raw(
      JSON.stringify(query.id)
    )}`,
    joinsToSQL(query.join),
    whereToSQL(query.where),
    orderToSQL(query.orderBy),
    limitToSQL(query.limit),
  ]);
};

export const makeSelectClause = (query: QueryData<any, any>) => {
  if (query.selectKeys.length === 0) {
    return joinSQL(
      Object.entries(query.model)
        .filter(([key, field]) => {
          return field.primary;
        })
        .map(([key, field]) => {
          return sql`${getQualifiedSQLColumn(query, field)}`;
        }),
      sql`, `
    );
  }

  return joinSQL(
    query.selectKeys.map(
      ({ selector, key }: any) =>
        sql`${getSelector(selector).select} as ${raw(JSON.stringify(key))}`
    ),
    sql`, `
  );
};

const orderToSQL = (orderItems: SQLSegment[]) => {
  if (orderItems.length > 0) {
    return sql`ORDER BY ${joinSQL(orderItems, sql`, `)}`;
  }

  return null;
};

const convertToSelect = (query: QueryData<any, any>) => {
  return joinSQL([
    sql`SELECT ${makeSelectClause(query)}`,
    sql`FROM ${raw(getQualifiedSQLTable(query.model))} as ${raw(
      JSON.stringify(query.id)
    )}`,
    joinsToSQL(query.join),
    whereToSQL(query.where),
    orderToSQL(query.orderBy),
    limitToSQL(query.limit),
  ]);
};

export const addWhereClause = <Q extends QueryData<any, any>>(
  query: Q,
  clause: WhereClause<Q["model"]>
) => {
  return immer(query, (query) => {
    clause =
      clause instanceof Function ? clause(symbolFromQuery(query)) : clause;

    if (clause == null) {
      return;
    }

    if (isSQLSegment(clause)) {
      query.where.push(sql`(${clause})`);
    } else {
      Object.entries(clause).forEach(([key, value]) => {
        if (value != null && (value as any)[queryExpression]) {
          value = (value as any)[queryExpression](query).sql;
        }

        const field = getModelField(query.model, key);

        if (isWhereOperator(value)) {
          query.where.push(
            value.getClause(query, raw(getQualifiedSQLColumn(query, key)))
          );
        } else if (
          field &&
          field.references &&
          value &&
          typeof value !== "string" &&
          !isSQLSegment(value)
        ) {
          const joinQuery = immer(
            emptyQuery(field.references.model),
            (joinQuery) => {
              joinQuery.where.push(
                sql`${raw(getQualifiedSQLColumn(query, key))} = ${raw(
                  getQualifiedSQLColumn(joinQuery, field.references.column)
                )}`
              );
            }
          );

          const queryEntries =
            value instanceof Function
              ? value(symbolFromQuery(joinQuery))
              : value;

          query.join.push({
            type: "inner",
            query: addWhereClause(joinQuery, queryEntries),
          });
        } else {
          query.where.push(
            sql`${raw(getQualifiedSQLColumn(query, key))} = ${value}`
          );
        }
      });
    }
  });
};

type UnknownToNever<T> = unknown extends T ? never : T;

export const getSelector = <T>(
  value: DecodeSelector<T>
): IDecodeSelector<T> => {
  if ((value as any)[decodeSelector]) {
    return (value as any)[decodeSelector];
  }

  return value as IDecodeSelector<T>;
};

export class DBQuery<M, SelectData extends any[]> extends ProtectPromise {
  constructor(public query: QueryData<M, SelectData>) {
    super(".get(db)");
  }

  get [decodeSelector](): IDecodeSelector<
    QueryResult<QueryData<M, SelectData>>[]
  > {
    const id = this.query.id + "-subquery";
    const select = convertToJSONSelect(this.query);

    return {
      id: id,
      select: sql`(${select})`,
      decodeResult: (value: any) => {
        return value[id];
      },
    };
  }

  toSQL() {
    return getQueryFromSegments(convertToSelect(this.query));
  }

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

  with<R>(
    create: (base: ModelSymbol<M>) => R
  ): DBQuery<M, [...SelectData, ...ObjectToSelectionEntries<R>]> {
    const result = create(symbolFromQuery(this.query));

    return new DBQuery(
      immer(this.query, (query) => {
        const entries = Object.entries(result).map(([key, selector]) => {
          return { key, selector: selector as IDecodeSelector<any> };
        });

        query.selectKeys.push(...entries);
      }) as any
    );
  }

  limit(count: number): DBQuery<M, SelectData> {
    return new DBQuery(
      immer(this.query, (query) => {
        query.limit = count;
      }) as any
    );
  }

  orderBy(
    key:
      | UnknownToNever<
          Extract<SelectData[number], { key: any }> extends { key: infer K }
            ? K
            : never
        >
      | keyof M,
    direction: "ASC" | "DESC" = "ASC"
  ) {
    return new DBQuery(
      immer(this.query, (query) => {
        assert(typeof key === "string");

        query.orderBy = [
          this.query.selectKeys.some((item) => item.key === key)
            ? raw(JSON.stringify(key) + " " + direction)
            : raw(getQualifiedSQLColumn(this.query, key) + " " + direction),
        ];
      })
    );
  }

  where(clause: WhereClause<M>): DBQuery<M, SelectData> {
    return new DBQuery(addWhereClause(this.query, clause));
  }
}

export const emptyQuery = <M>(model: Model<M>): QueryData<M, any> => ({
  id: uuid.v4(),
  model,
  selectKeys: [],
  join: [],
  where: [],
  orderBy: [],
});

interface RefSelect {
  <M, SelectData extends (keyof M)[]>(
    model: ModelSymbol<M> & FieldSelection<M, any>,
    ...keys: SelectData
  ): ReferenceSelector<M, SelectData>;
}

interface Select {
  <T, SelectData extends (keyof T)[]>(
    model: Model<T>,
    ...keys: SelectData
  ): DBQuery<T, SelectData>;
}

class ReferenceSelector<M, SelectData extends any[]> {
  id = uuid.v4();

  constructor(public queryData: QueryData<M, SelectData>) {}

  get [decodeSelector](): IDecodeSelector<
    QueryResult<QueryData<M, SelectData>>
  > {
    return {
      id: this.id,
      select: sql`(${convertToJSONSingleSelect(this.queryData)})`,
    };
  }

  with<R>(
    create: (base: ModelSymbol<M>) => R
  ): ReferenceSelector<M, [...SelectData, ...ObjectToSelectionEntries<R>]> {
    const result = create(symbolFromQuery(this.queryData));

    return new ReferenceSelector(
      immer(this.queryData, (query) => {
        const entries = Object.entries(result).map(([key, selector]) => {
          return { key, selector: selector as IDecodeSelector<any> };
        });

        query.selectKeys.push(...entries);
      }) as any
    );
  }

  where(clause: WhereClause<M>): ReferenceSelector<M, SelectData> {
    return new ReferenceSelector(addWhereClause(this.queryData, clause));
  }
}

export const subselect: RefSelect = ((model: any, ...keys: any[]) => {
  const query = model[symbolQuery];
  const queryModel = query.model;

  return new ReferenceSelector({
    ...query,
    selectKeys: keys.map((key: any) => ({
      key: key,
      selector: new FieldSelectionDecoder(
        getModelField(queryModel, key) as any,
        query,
        key
      ),
    })) as any,
  });
}) as any;

export const select: Select = ((model: any, ...keys: any[]) => {
  const query = emptyQuery(model);

  return new DBQuery<any, any>({
    ...query,
    selectKeys: keys.map((key: any) => ({
      key: key,
      selector: new FieldSelectionDecoder(
        getModelField(model, key) as any,
        query,
        key
      ),
    })) as any,
  });
}) as any;
