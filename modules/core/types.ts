import { getModelField } from "./generateSchema";
import { Model } from "./model";

export interface SQLType {
  baseType: string;
  modifiers: string[];
}

export interface FieldType<T, U = T, P extends true | false = false> {
  T: T;
  U: U;
  primary: P;
  references?: {
    model: any;
    column: string;
  };
  sqlType: SQLType;
  columnName?: string;
}

export interface FieldTypeF<T, U = T, P extends true | false = false> {
  (): FieldType<T, U, P>;
}

export const castType = <T>() => (({} as any) as T);

export const makeType = <T>(baseType: string): FieldTypeF<T, T, false> => {
  return () => ({
    T: castType<T>(),
    U: castType<T>(),
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
export const serial = makeType<number>("serial");
export const jsonb = makeType<string>("jsonb");

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
): FieldTypeF<
  T[K] extends FieldType<infer T, any, any> ? NonNullable<T> : never,
  T[K] extends FieldType<any, infer U, any> ? NonNullable<U> : never,
  false
> => {
  return () => {
    return {
      T: castType<
        T[K] extends FieldType<infer T, any, any> ? NonNullable<T> : never
      >(),
      U: castType<
        T[K] extends FieldType<any, infer U, any> ? NonNullable<U> : never
      >(),
      primary: false,
      references: {
        model: ref,
        column: columnName as string,
      },
      sqlType: {
        baseType: getModelField(ref, columnName as any)!.sqlType.baseType,
        modifiers: ["NOT NULL"],
      },
    };
  };
};
