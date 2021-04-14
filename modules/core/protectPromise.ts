export class ProtectPromise {
  constructor(private expectedMethod: string) {}

  private then() {
    throw new Error(
      `Promise#then() was called on ${this.constructor.name}, did you forget to use \`${this.expectedMethod}\`?`
    );
  }

  private catch() {
    throw new Error(
      `Promise#catch() was called on ${this.constructor.name}, did you forget to use \`${this.expectedMethod}\`?`
    );
  }

  private finally() {
    throw new Error(
      `Promise#finally() was called on ${this.constructor.name}, did you forget to use \`${this.expectedMethod}\`?`
    );
  }
}
