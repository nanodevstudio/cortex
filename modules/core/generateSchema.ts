import { Model } from "./model";
import { FieldType } from "./types";

const camelToSnakeCase = (str: string) => {
  return (
    str[0].toLowerCase() +
    str.slice(1).replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`)
  );
};

export const getModelInstance = <T>(model: Model<T>) => {
  return new model();
};

export const getModelField = <T>(
  model: Model<T>,
  field: string
): FieldType<any, any, boolean> | undefined => {
  return (getModelInstance(model) as any)[field]();
};

export const getSQLName = <T>(model: Model<T>) => {
  return camelToSnakeCase(model.name);
};

export const getQualifiedSQLTable = <T>(model: Model<T>) => {
  return "public." + JSON.stringify(camelToSnakeCase(model.name));
};

export const getQualifiedSQLColumn = <T>(model: Model<T>, key: string) => {
  return getQualifiedSQLTable(model) + "." + JSON.stringify(key);
};

export const joinLines = (lines: (string | string[])[]) =>
  lines.flat().join("\n");

export const generateForiegnKeys = <T>(model: Model<T>) => {
  const entries = Object.entries(getModelInstance(model));
  return entries
    .filter(([_, field]) => field.references)
    .map(([key, field]) => {
      const { model: refModel, column: refColumn } = field.references;
      const constraintName = JSON.stringify(
        `fk__${getSQLName(model)}_${key}__${getSQLName(refModel)}_${refColumn}`
      );

      return `ALTER TABLE ${getQualifiedSQLTable(
        model
      )} ADD CONSTRAINT ${constraintName} FOREIGN KEY(${JSON.stringify(
        key
      )}) REFERENCES ${getQualifiedSQLTable(refModel)}(${JSON.stringify(
        refColumn
      )})`;
    });
};

export const generateSQLInsert = <T>(model: Model<T>) => {
  const modelName = getQualifiedSQLTable(model);
  const keys = Object.keys(getModelInstance(model));
  const columns = keys.map((key) => {
    return {
      name: key,
      type: getModelField(model, key as any)!.sqlType,
    };
  });

  return joinLines([
    `CREATE TABLE ${modelName} (`,
    columns
      .map(
        ({ name, type }) =>
          `${JSON.stringify(name)} ${type.baseType} ${type.modifiers.join(" ")}`
      )
      .join(",\n"),
    ");",
  ]);
};
