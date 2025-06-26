import * as fs from "node:fs/promises";
import * as path from "node:path";

import type { CompiledGraph, Graph } from "@langchain/langgraph";
import type { JSONSchema7 } from "json-schema";

export interface GraphSchema {
  state: JSONSchema7 | undefined;
  input: JSONSchema7 | undefined;
  output: JSONSchema7 | undefined;
  config: JSONSchema7 | undefined;
}

export interface GraphSpec {
  sourceFile: string;
  exportSymbol: string;
}

export type CompiledGraphFactory<T extends string> = (config: {
  configurable?: Record<string, unknown>;
}) => Promise<CompiledGraph<T>>;

export async function resolveGraph(
  spec: string,
  options?: { onlyFilePresence?: false },
): Promise<{
  sourceFile: string;
  exportSymbol: string;
  resolved: CompiledGraph<string> | CompiledGraphFactory<string>;
}>;

export async function resolveGraph(
  spec: string,
  options: { onlyFilePresence: true },
): Promise<{ sourceFile: string; exportSymbol: string; resolved: undefined }>;

export async function resolveGraph(
  spec: string,
  options?: { onlyFilePresence?: boolean },
) {
  const [userFile, exportSymbol] = spec.split(":", 2);
  const sourceFile = path.resolve(process.cwd(), userFile);

  // validate file exists
  await fs.stat(sourceFile);
  if (options?.onlyFilePresence) {
    return { sourceFile, exportSymbol, resolved: undefined };
  }

  type GraphLike = CompiledGraph<string> | Graph<string>;

  type GraphUnknown =
    | GraphLike
    | Promise<GraphLike>
    | ((config: {
        configurable?: Record<string, unknown>;
      }) => GraphLike | Promise<GraphLike>)
    | undefined;

  const isGraph = (graph: GraphLike): graph is Graph<string> => {
    if (typeof graph !== "object" || graph == null) return false;
    return "compile" in graph && typeof graph.compile === "function";
  };

  const graph: GraphUnknown = await import(sourceFile).then(
    (module) => module[exportSymbol || "default"],
  );

  // obtain the graph, and if not compiled, compile it
  const resolved: CompiledGraph<string> | CompiledGraphFactory<string> =
    await (async () => {
      if (!graph) throw new Error("Failed to load graph: graph is nullush");

      const afterResolve = (graphLike: GraphLike): CompiledGraph<string> => {
        const graph = isGraph(graphLike) ? graphLike.compile() : graphLike;
        return graph;
      };

      if (typeof graph === "function") {
        return async (config: { configurable?: Record<string, unknown> }) => {
          const graphLike = await graph(config);
          return afterResolve(graphLike);
        };
      }

      return afterResolve(await graph);
    })();

  return { sourceFile, exportSymbol, resolved };
}
