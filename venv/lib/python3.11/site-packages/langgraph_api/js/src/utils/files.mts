export function filterValidExportPath(path: string | undefined) {
  if (!path) return false;
  return !path.split(":")[0].endsWith(".py");
}
