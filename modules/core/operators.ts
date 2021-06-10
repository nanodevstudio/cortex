import { QueryData } from "./query";
import { QueryExpression } from "./symbolic";
import { queryExpression, whereOperator } from "./symbols";
import { joinSQL, raw, sql, SQLSegment, SQLValue } from "./writes";

export const isWhereOperator = <T>(
  value: any
): value is WhereOperator<any, any> => {
  return value != null && value[whereOperator] === true;
};

export interface WhereOperator<M, T> {
  [whereOperator]: true;
  type(): T;
  getClause(query: QueryData<M, any>, fieldExpression: SQLSegment): SQLSegment;
}

class Operator<M, T> implements WhereOperator<M, T> {
  constructor(public operator: string, public value: QueryExpression<M, T>) {}

  [whereOperator] = true as const;

  type(): T {
    throw new Error("virtual method, do not call");
  }

  getClause(query: QueryData<M, any>, source: SQLSegment) {
    return sql`${source} ${raw(this.operator)} ${
      this.value[queryExpression](query).sql
    }`;
  }
}

class ValueQueryExpression implements QueryExpression<any, any> {
  constructor(public value: any) {}

  [queryExpression](model: any) {
    return { sql: new SQLValue(this.value) };
  }
}

class AnyValueQueryExpression implements QueryExpression<any, any> {
  constructor(public value: any[]) {}

  [queryExpression](model: any) {
    return {
      sql: sql`(${joinSQL(
        this.value.map((value) => sql`${value}`),
        sql`, `
      )})`,
    };
  }
}

export const asExpression = (value: any): QueryExpression<any, any> => {
  if (value[queryExpression]) {
    return value;
  }

  return new ValueQueryExpression(value);
};

export const notEqual = <T, M>(
  value: T | QueryExpression<M, T> | null
): WhereOperator<M, T> => {
  return new Operator<M, T>("!=", asExpression(value));
};

export const anyOf = <T, M>(
  value: T[] | QueryExpression<M, T>
): WhereOperator<M, T> => {
  return new Operator<M, T>(
    "IN",
    Array.isArray(value)
      ? new AnyValueQueryExpression(value)
      : asExpression(queryExpression)
  );
};

export const equal = <T, M>(
  value: T | QueryExpression<M, T> | null
): WhereOperator<M, T> => {
  return new Operator<M, T>("=", asExpression(value));
};

export const gt = <T extends number, M>(
  value: T | QueryExpression<M, T> | null
): WhereOperator<M, T> => {
  return new Operator<M, T>(">", asExpression(value));
};

export const lt = <T extends number, M>(
  value: T | QueryExpression<M, T> | null
): WhereOperator<M, T> => {
  return new Operator<M, T>("<", asExpression(value));
};

export const lte = <T extends number, M>(
  value: T | QueryExpression<M, T> | null
): WhereOperator<M, T> => {
  return new Operator<M, T>("<=", asExpression(value));
};

export const gte = <T extends number, M>(
  value: T | QueryExpression<M, T> | null
): WhereOperator<M, T> => {
  return new Operator<M, T>(">=", asExpression(value));
};

export const op = <T, M>(operator: string, value: any): WhereOperator<M, T> => {
  return new Operator<M, T>(operator, asExpression(value));
};
