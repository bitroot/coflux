import {
  IconFile,
  IconFileText,
  IconFolder,
  IconProps,
} from "@tabler/icons-react";

import * as models from "../models";

function iconForAsset(asset: models.Asset) {
  switch (asset.type) {
    case 0: {
      const type = asset.metadata["type"] as undefined | string;
      switch (type?.split("/")[0]) {
        case "text":
          return IconFileText;
        default:
          return IconFile;
      }
    }
    case 1:
      return IconFolder;
    default:
      throw new Error(`unrecognised asset type (${asset.type})`);
  }
}

type AssetIconProps = IconProps & {
  asset: models.Asset;
};

export default function AssetIcon({
  asset,
  size = 16,
  ...props
}: AssetIconProps) {
  const Icon = iconForAsset(asset);
  return <Icon size={size} {...props} />;
}
