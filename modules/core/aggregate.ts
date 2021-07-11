import * as uuid from "uuid";
import { DBQuery } from "./query";
import { DecodeSelector, IDecodeSelector } from "./symbolic";
import { raw, sql } from "./writes";

export class CountAggregation implements IDecodeSelector<number> {
  constructor(public source: DBQuery<any, any>) {}

  id = uuid.v4();

  get select() {
    const segment = this.source.toSegment();

    return sql`(select count(*) from (${segment}) as ${raw(
      JSON.stringify(this.id)
    )})::integer`;
  }
}

export const count = (result: DBQuery<any, any>): DecodeSelector<number> => {
  return new CountAggregation(result);
};
