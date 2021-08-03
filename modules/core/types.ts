import immer from "immer";
import { getModelField } from "./generateSchema";
import { Model } from "./model";

export interface SQLType {
  baseType: string;
  modifiers: string[];
}

export type FieldReferencesType =
  | { model: Model<any>; column: any }
  | undefined;

export interface FieldType<
  T,
  U = T,
  P extends true | false = false,
  R extends FieldReferencesType = any
> {
  T: T;
  U: U;
  primary: P;
  encode?: (value: any) => any;
  sqlType: SQLType;
  references: R;
  columnName?: string;
}

export interface FieldTypeF<
  T,
  U = T,
  P extends true | false = false,
  R extends { model: Model<any>; column: string } | undefined = any
> {
  (): FieldType<T, U, P, R>;
}

export const castType = <T>() => ({} as any as T);

export const makeType = <T, U = T>(
  baseType: string,
  encode?: (value: any) => any
): FieldTypeF<T, U, false, undefined> => {
  return () => ({
    T: castType<T>(),
    U: castType<U>(),
    primary: false,
    encode,
    references: undefined,
    sqlType: {
      baseType,
      modifiers: ["NOT NULL"],
    },
  });
};

export class JSONValue {
  constructor(public value: any) {}

  toPostgres(prep: any) {
    return prep(JSON.stringify(this.value));
  }
}

export const text = makeType<string>("text");
export const utcTimestamp = makeType<Date>("timestamp without time zone");
export const real = makeType<number>("real");
export const integer = makeType<number>("integer");
export const uuid = makeType<string>("uuid");
export const boolean = makeType<boolean>("boolean");
export const serial = makeType<number, number | undefined>("serial");
export const textEnum = <T extends string>() => makeType<T>("text");
export const jsonb = <T>() =>
  makeType<T>("jsonb", (value) => new JSONValue(value));

export const array = <T, U, P extends boolean>(
  typeF: FieldTypeF<T, U, P, undefined>
): FieldTypeF<T[], U[], P, undefined> => {
  return () => {
    const type = typeF();

    return {
      ...type,
      encode: (value: any[]) => {
        const values = value
          .map((value) => type.encode?.(value) ?? value)
          .map((value) => JSON.stringify(value))
          .join(",");

        return `{${values}}`;
      },
      sqlType: {
        ...type.sqlType,
        baseType: `${type.sqlType.baseType}[]`,
      },
    } as any;
  };
};

export const applyModifiers = (type: SQLType, modifiers: string[]): SQLType => {
  return {
    ...type,
    modifiers: [...type.modifiers, ...modifiers],
  };
};

export const primaryGenerated = <T, U, P>(
  typeF: FieldTypeF<T, U, any>
): FieldTypeF<T, U | undefined, true, undefined> => {
  return () => {
    const type = typeF();

    return {
      ...type,
      T: type.T,
      primary: true,
      U: castType<typeof type.U | undefined>(),
      sqlType: applyModifiers(type.sqlType, [
        "DEFAULT public.uuid_generate_v4()",
        "PRIMARY KEY",
      ]),
    } as any;
  };
};

export const generatedId = primaryGenerated(uuid);

export const primary = <T, U, R extends FieldReferencesType>(
  fieldType: FieldTypeF<T, U, any, R>
): FieldTypeF<T, U, true, R> => {
  return () => ({
    ...fieldType(),
    primary: true,
    sqlType: applyModifiers(fieldType().sqlType, ["PRIMARY KEY"]),
  });
};

export const ref = <T, K extends keyof T>(
  ref: Model<T>,
  columnName: K
): (() => FieldType<
  T[K] extends FieldTypeF<infer T, any, any> ? NonNullable<T> : never,
  T[K] extends FieldTypeF<any, infer U, any> ? NonNullable<U> : never,
  false,
  { model: Model<T>; column: K }
>) => {
  return () => {
    return {
      T: castType<
        T[K] extends FieldTypeF<infer T, any, any> ? NonNullable<T> : never
      >(),
      U: castType<
        T[K] extends FieldTypeF<any, infer U, any> ? NonNullable<U> : never
      >(),
      primary: false,
      references: {
        model: ref,
        column: columnName,
      },
      sqlType: {
        baseType: getModelField(ref, columnName as any)!.sqlType.baseType,
        modifiers: ["NOT NULL"],
      },
    };
  };
};

export const optional = <T, U, R extends FieldReferencesType>(
  type: FieldTypeF<T, U, false, R>
) => {
  return (() => {
    return immer(type(), (type) => {
      type.sqlType.modifiers = type.sqlType.modifiers.filter(
        (modifier) => modifier != "NOT NULL"
      );
    });
  }) as FieldTypeF<T | undefined, U | undefined, false, R>;
};
