import { decodeSelector, DecodeSelector, IDecodeSelector } from "./symbolic";
import { raw, sql, SQLSegment } from "./writes";
import * as uuid from "uuid";
import { getSelector } from "./query";

export class CountAggregation implements IDecodeSelector<number> {
  constructor(public source: DecodeSelector<any[]>) {}

  id = uuid.v4();

  get select() {
    const selector = getSelector(this.source);
    return sql`(select count(*) from ${selector.select} as ${raw(
      JSON.stringify(this.id)
    )})::integer`;
  }
}

export const count = (
  result: DecodeSelector<any[]>
): DecodeSelector<number> => {
  return new CountAggregation(result);
};
