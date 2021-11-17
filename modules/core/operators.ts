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

class NilMatch implements WhereOperator<any, any> {
  constructor() {}

  [whereOperator] = true as const;

  type(): any {
    throw new Error("virtual method, do not call");
  }

  getClause(query: QueryData<any, any>, source: SQLSegment) {
    return sql`FALSE`;
  }
}

class ValueQueryExpression implements QueryExpression<any, any> {
  constructor(public value: any) {}

  [queryExpression](model: any) {
    return { sql: sql`${this.value}` };
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

class AnyWrapper implements QueryExpression<any, any> {
  constructor(public value: QueryExpression<any, any>) {}

  [queryExpression](model: any) {
    return {
      sql: sql`ANY(${this.value[queryExpression](model).sql})`,
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
  value: T[] | QueryExpression<any, T[]>
): WhereOperator<M, T> => {
  if (Array.isArray(value) && value.length === 0) {
    return new NilMatch();
  }

  if (!Array.isArray(value)) {
    return new Operator<M, T>("=", new AnyWrapper(asExpression(value)));
  }

  return new Operator<M, T>(
    "IN",
    Array.isArray(value)
      ? new AnyValueQueryExpression(value)
      : asExpression(value)
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

export const isNot = <T extends number, M>(
  value: T | SQLSegment | QueryExpression<M, T> | null
): WhereOperator<M, T> => {
  return new Operator<M, T>("IS NOT", asExpression(value));
};

export const is = <T extends number, M>(
  value: T | SQLSegment | QueryExpression<M, T> | null
): WhereOperator<M, T> => {
  return new Operator<M, T>("IS", asExpression(value));
};
