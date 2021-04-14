import { getModelField } from "./generateSchema";
import { Model } from "./model";
import immer from "immer";

export interface SQLType {
  baseType: string;
  modifiers: string[];
}

export interface FieldType<T, U = T, P extends true | false = false> {
  T: T;
  U: U;
  primary: P;
  sqlType: SQLType;
  columnName?: string;
}

export interface FieldTypeF<T, U = T, P extends true | false = false> {
  (): FieldType<T, U, P>;
}

export const castType = <T>() => (({} as any) as T);

export const makeType = <T, U = T>(
  baseType: string
): FieldTypeF<T, U, false> => {
  return () => ({
    T: castType<T>(),
    U: castType<U>(),
    primary: false,
    sqlType: {
      baseType,
      modifiers: ["NOT NULL"],
    },
  });
};

export const text = makeType<string>("text");
export const utcTimestamp = makeType<Date>("timestamp without time zone");
export const integer = makeType<number>("integer");
export const uuid = makeType<string>("uuid");
export const boolean = makeType<boolean>("boolean");
export const serial = makeType<number, number | undefined>("serial");
export const jsonb = makeType<string>("jsonb");
export const textEnum = <T extends string>() => makeType<T>("text");

export const array = <T, U, P extends boolean>(
  typeF: FieldTypeF<T, U, P>
): FieldTypeF<T[], U[], P> => {
  return () => {
    const type = typeF();

    return {
      ...type,
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
): FieldTypeF<T, U | undefined, true> => {
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

export const primary = <T, U>(
  fieldType: FieldTypeF<T, U, any>
): FieldTypeF<T, U, true> => {
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
  false
> & { references: { model: Model<T>; column: K } }) => {
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

export const optional = <T, U>(type: FieldTypeF<T, U, false>) => {
  return (() => {
    return immer(type(), (type) => {
      type.sqlType.modifiers = type.sqlType.modifiers.filter(
        (modifier) => modifier != "NOT NULL"
      );
    });
  }) as FieldTypeF<T | undefined, U | undefined, false>;
};
