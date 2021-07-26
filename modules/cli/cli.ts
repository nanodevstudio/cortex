#!/usr/bin/env node

import { Command } from "commander";
import * as pkg from "../../package.json";
import { join } from "path";
import * as fs from "fs";
import { migrationTemplate } from "./migrationTemplate";

const cli = new Command();

cli.name("cortex");
cli.version(pkg.version);

cli
  .command("migration:create <path> <name>")
  .description("Create a new migration in your migrations folder.")
  .action((path, name) => {
    const folderPath = join(process.cwd(), path);
    const filePath = join(
      folderPath,
      `${new Date().toISOString()}__${name}.ts`
    );

    try {
      fs.mkdirSync(folderPath, { recursive: true });
    } catch (e) {}

    fs.writeFileSync(filePath, migrationTemplate);
  });

cli.parse(process.argv);
