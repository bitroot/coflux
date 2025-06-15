import * as models from "./models";
import { pluralise, truncatePath } from "./utils";

export function getAssetName(asset: models.AssetSummary) {
  if (asset.entry) {
    return truncatePath(asset.entry.path);
  } else {
    return pluralise(asset.totalCount, "file");
  }
}
