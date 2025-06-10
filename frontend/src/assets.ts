import * as models from "./models";
import { pluralise, truncatePath } from "./utils";

export function getAssetName(asset: models.Asset) {
  const path =
    Object.keys(asset.entries).length == 1
      ? Object.keys(asset.entries)[0]
      : undefined;
  return path
    ? truncatePath(path)
    : pluralise(Object.keys(asset.entries).length, "file");
}
