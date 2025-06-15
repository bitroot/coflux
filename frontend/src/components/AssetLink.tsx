import { ReactNode, useCallback } from "react";
import * as models from "../models";
import { buildUrl, humanSize, pluralise } from "../utils";
import { useHoverContext } from "./HoverContext";
import classNames from "classnames";
import { Link, useLocation, useSearchParams } from "react-router-dom";

type Props = {
  asset: models.AssetSummary;
  assetId: string;
  className?: string;
  hoveredClassName?: string;
  children: ReactNode;
};

export default function AssetLink({
  asset,
  assetId,
  className,
  hoveredClassName,
  children,
}: Props) {
  const [searchParams] = useSearchParams();
  const { pathname } = useLocation();
  const { isHovered, setHovered } = useHoverContext();
  const handleMouseOver = useCallback(
    () => setHovered({ assetId }),
    [setHovered, assetId],
  );
  const handleMouseOut = useCallback(() => setHovered(undefined), [setHovered]);
  return (
    <Link
      to={buildUrl(pathname, {
        ...Object.fromEntries(searchParams),
        asset: asset.entry ? `${assetId}:${asset.entry.path}` : assetId,
      })}
      className={classNames(
        className,
        isHovered({ assetId }) && hoveredClassName,
      )}
      onMouseOver={handleMouseOver}
      onMouseOut={handleMouseOut}
      title={
        asset.entry
          ? `${asset.entry.path} (${humanSize(asset.entry.size)})`
          : `${pluralise(asset.totalCount, "file")} (${humanSize(asset.totalSize)})`
      }
    >
      {children}
    </Link>
  );
}
