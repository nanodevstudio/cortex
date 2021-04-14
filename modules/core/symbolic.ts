import {
  getModelField,
  getQualifiedSQLColumn,
  getSQLName,
} from "./generateSchema";
import { Model } from "./model";
import {
  addWhereClause,
  emptyQuery,
  QueryData,
  select,
  Strings,
} from "./query";
import { FieldType, FieldTypeF } from "./types";
import * as uuid from "uuid";
import { raw, sql, SQLSegment, SQLSegmentList } from "./writes";

export type ReadType<F> = F extends FieldTypeF<infer read, any, any>
  ? read
  : never;
export type WriteType<F> = F extends FieldTypeF<any, infer write, any>
  ? write
  : never;

export type ReturnTypeUnsafe<T> = T extends (...args: any) => any
  ? ReturnType<T>
  : never;

export const symbolField = Symbol("symbolField");

export type ModelSymbol<M> = {
  [key in Strings<keyof M>]: ReturnTypeUnsafe<M[key]> extends {
    references: { model: Model<infer RefM>; column: any };
  }
    ? ModelSymbol<RefM>
    : M[key] extends FieldTypeF<infer a, infer b, infer c>
    ? FieldSelectionDecoder<FieldType<a, b, c>>
    : never;
};

export type FieldSymbol<F> = ReadType<F>;

export const expression: unique symbol = Symbol("expression");
export const decodeSelector: unique symbol = Symbol("decodeSelector");

export interface Expression<T> {
  [expression]: IExpression<T>;
}

export interface IExpression<T> {
  sql: SQLSegment;
}

export type DecodeSelector<DecodeResult> =
  | {
      [decodeSelector]: IDecodeSelector<DecodeResult>;
    }
  | IDecodeSelector<DecodeResult>;

export interface IDecodeSelector<DecodeResult> {
  id: string;
  select: SQLSegment;
  decodeResult?: (value: any) => DecodeResult;
}

export interface SelectionEntry<
  Key extends string | number | symbol,
  DecodeResult
> {
  key: Key;
  selector: DecodeSelector<DecodeResult>;
}

type UnionToTuple<T> = (
  (T extends any ? (t: T) => T : never) extends infer U
    ? (U extends any ? (u: U) => any : never) extends (v: infer V) => any
      ? V
      : never
    : never
) extends (_: any) => infer W
  ? [...UnionToTuple<Exclude<T, W>>, W]
  : [];

export type ObjectToSelectionEntries<O> = UnionToTuple<
  {
    [key in keyof O]: O[key] extends DecodeSelector<infer R>
      ? SelectionEntry<key, R>
      : never;
  }[keyof O]
>;

export class FieldSelectionDecoder<F extends FieldType<any, any, any>>
  implements IDecodeSelector<ReadType<F>> {
  public select: SQLSegment;
  public id: string = uuid.v4();

  constructor(
    private field: F,
    private query: QueryData<any, any>,
    private key: string
  ) {
    this.select = sql`${raw(getQualifiedSQLColumn(query, key))}`;
  }

  get [expression](): IExpression<ReadType<F>> {
    return {
      sql: raw(getQualifiedSQLColumn(this.query, this.key)),
    };
  }

  decodeResult(value: any): ReadType<F> {
    return value[this.id];
  }
}

export const symbolQuery: unique symbol = Symbol("symbolModel");

export const symbolFromQuery = <M>(
  query: QueryData<M, any>
): ModelSymbol<M> => {
  return new Proxy({} as any, {
    get(self, key: string | symbol) {
      if (key === symbolQuery) {
        return query;
      }

      if (typeof key === "symbol") {
        return undefined;
      }

      const field = getModelField(query.model, key);

      if (field && (field as any).references) {
        return symbolFromQuery(
          addWhereClause(emptyQuery((field as any).references.model), {
            [(field as any).references.column]: raw(
              getQualifiedSQLColumn(query, key)
            ),
          })
        );
      }

      if (field != null) {
        return new FieldSelectionDecoder(field, query, key);
      } else {
        throw new Error(`model error: no key ${key} on ${query.model.name}`);
      }
    },
  });
};