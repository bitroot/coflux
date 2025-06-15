import {
  IconFile,
  IconFileText,
  IconFiles,
  IconProps,
} from "@tabler/icons-react";

import * as models from "../models";

function iconForAsset(asset: models.AssetSummary) {
  if (asset.entry) {
    const type = asset.entry.metadata["type"] as undefined | string;
    switch (type?.split("/")[0]) {
      case "text":
        return IconFileText;
      default:
        return IconFile;
    }
  } else {
    return IconFiles;
  }
}

type AssetIconProps = IconProps & {
  asset: models.AssetSummary;
};

export default function AssetIcon({
  asset,
  size = 16,
  ...props
}: AssetIconProps) {
  const Icon = iconForAsset(asset);
  return <Icon size={size} {...props} />;
}
