import { generateSQLInsert } from "../generateSchema";
import * as t from "@/core/types";

describe("db/framework/generateSQLInsert()", () => {
  it("can generate sql insert for simple model", () => {
    class Model {
      id = t.generatedId;
      name = t.text;
    }

    expect(generateSQLInsert(Model)).toMatchSnapshot();
  });

  it("can generate sql insert for model with references", () => {
    class Refed {
      id = t.generatedId;
      name = t.text;
    }

    class Model {
      id = t.generatedId;
      name = t.text;
      ref = t.ref(Refed, "id");
    }

    expect(generateSQLInsert(Model)).toMatchSnapshot();
  });
});
