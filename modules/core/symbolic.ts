import * as uuid from "uuid";
import { getModelField, getQualifiedSQLColumn } from "./generateSchema";
import { Model } from "./model";
import {
  addWhereClause,
  emptyQuery,
  QueryData,
  SQLJoin,
  Strings,
} from "./query";
import { decodeSelector, queryExpression, symbolQuery } from "./symbols";
import { FieldType, FieldTypeF } from "./types";
import { raw, sql, SQLSegment } from "./writes";

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

export type FieldSelection<M, F> = DecodeSelector<ReadType<F>> &
  QueryExpression<M, ReadType<F>>;

export type ModelSymbol<M> = {
  [key in Strings<keyof M>]: M[key] extends FieldTypeF<
    any,
    any,
    any,
    { model: Model<infer RefM>; column: string }
  >
    ? ModelSymbol<RefM> &
        (M[key] extends FieldTypeF<infer a, infer b, infer c, infer r>
          ? FieldSelection<RefM, FieldTypeF<a, b, c, r>>
          : never)
    : M[key] extends FieldTypeF<infer a, infer b, infer c>
    ? FieldSelection<M, FieldTypeF<a, b, c>>
    : never;
};

export type FieldSymbol<F> = ReadType<F>;

export interface QueryExpression<M, T> {
  [queryExpression]: (query: QueryData<M, any>) => IExpression<T>;
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
  decodeResult?: (value: any, key: string) => DecodeResult;
}

export type SelectionEntry<Key, DecodeResult> = {
  key: Key;
  selector: DecodeSelector<DecodeResult>;
};

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
  implements IDecodeSelector<ReadType<F>>
{
  public select: SQLSegment;
  public id: string = uuid.v4();

  constructor(
    private field: F,
    private query: QueryData<any, any>,
    private key: string
  ) {
    this.select = sql`${raw(getQualifiedSQLColumn(query, key))}`;
  }

  [queryExpression](): IExpression<ReadType<F>> {
    return {
      sql: raw(getQualifiedSQLColumn(this.query, this.key)),
    };
  }

  decodeResult(value: any, key: string): ReadType<F> {
    return value[key];
  }
}

export const symbolFromQuery = <M>(
  query: QueryData<M, any>,
  decoder?: FieldSelectionDecoder<any>
): ModelSymbol<M> => {
  return new Proxy({} as any, {
    get(self, key: string | symbol) {
      if (key === symbolQuery) {
        return query;
      }

      if (typeof key === "symbol") {
        if (decoder) {
          if (key === queryExpression) {
            return () => decoder[queryExpression]();
          }

          if (key === decodeSelector) {
            return decoder;
          }
        }

        return undefined;
      }

      const field = getModelField(query.model, key);

      if (field && (field as any).references) {
        const fieldSelection = new FieldSelectionDecoder(field, query, key);

        return symbolFromQuery(
          addWhereClause(emptyQuery((field as any).references.model), {
            [(field as any).references.column]: raw(
              getQualifiedSQLColumn(query, key)
            ),
          }),
          fieldSelection
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
