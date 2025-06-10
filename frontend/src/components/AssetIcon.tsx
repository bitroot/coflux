import {
  IconFile,
  IconFileText,
  IconFiles,
  IconProps,
} from "@tabler/icons-react";

import * as models from "../models";

function iconForAsset(asset: models.Asset, path: string | undefined) {
  const entry = path && asset.entries[path];
  if (entry) {
    const type = entry.metadata["type"] as undefined | string;
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
  asset: models.Asset;
  path?: string;
};

export default function AssetIcon({
  asset,
  path,
  size = 16,
  ...props
}: AssetIconProps) {
  const Icon = iconForAsset(asset, path);
  return <Icon size={size} {...props} />;
}
