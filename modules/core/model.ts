import { ModelSymbol } from "./symbolic";
import { SQLSegment } from "./writes";

export type Model<T> = (new () => T) & {
  indexes?: SQLSegment[];
};
