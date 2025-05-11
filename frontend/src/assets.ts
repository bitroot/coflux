import * as models from "./models";
import { humanSize, pluralise } from "./utils";

export function getAssetMetadata(asset: models.Asset) {
  const parts = [];
  switch (asset.type) {
    case 0:
      if ("size" in asset.metadata) {
        parts.push(humanSize(asset.metadata.size));
      }
      if ("type" in asset.metadata && asset.metadata.type) {
        parts.push(asset.metadata.type);
      }
      break;
    case 1:
      if ("count" in asset.metadata) {
        parts.push(pluralise(asset.metadata.count, "file"));
      }
      if ("totalSize" in asset.metadata) {
        parts.push(humanSize(asset.metadata.totalSize));
      }
      break;
  }
  return parts;
}
