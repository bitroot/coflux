import { Fragment, ReactNode, MouseEvent, useCallback, useState } from "react";
import {
  IconCornerLeftUp,
  IconDownload,
  IconFile,
  IconFolder,
} from "@tabler/icons-react";

import * as models from "../models";
import { getAssetName } from "../assets";
import { humanSize, pluralise } from "../utils";
import Dialog from "./common/Dialog";
import { uniq } from "lodash";
import { useHoverContext } from "./HoverContext";
import classNames from "classnames";
import { createBlobStore } from "../blobs";
import { useSetting } from "../settings";

function showPreview(mimeType: string | undefined) {
  const parts = mimeType?.split("/");
  switch (parts?.[0]) {
    case "image":
    case "text":
      return true;
    default:
      return false;
  }
}

function assetUrl(projectId: string, assetId: string, path?: string) {
  return `/assets/${projectId}/${assetId}${path ? `/${path}` : ""}`;
}

function pathParent(path: string) {
  return path.includes("/") ? path.substring(path.lastIndexOf("/") + 1) : "";
}

type FilePreviewProps = {
  open: boolean;
  projectId: string;
  assetId: string;
  path?: string;
  entry: models.AssetEntry;
  onClose: () => void;
};

function PreviewDialog({
  open,
  projectId,
  assetId,
  path,
  entry,
  onClose,
}: FilePreviewProps) {
  if (showPreview(entry.metadata["type"] as string | undefined)) {
    return (
      <Dialog open={open} size="xl" onClose={onClose}>
        <div className="h-[80vh] rounded-lg overflow-hidden flex flex-col min-w-2xl">
          <iframe
            src={assetUrl(projectId, assetId, path)}
            sandbox="allow-downloads allow-forms allow-modals allow-scripts"
            className="size-full"
          />
        </div>
      </Dialog>
    );
  } else {
    return (
      <Dialog open={open} size="sm" className="p-6" onClose={onClose}>
        <div className="flex justify-center">
          <a
            href={assetUrl(projectId, assetId, path)}
            className="flex flex-col items-center hover:bg-slate-100 rounded-md px-3 py-1"
            target="_blank"
            rel="noreferrer"
          >
            <IconDownload size={30} />
            {humanSize(entry.size)}
          </a>
        </div>
      </Dialog>
    );
  }
}

type EntriesTableProps = {
  entries: models.Asset["entries"];
  basePath: string;
  projectId: string;
  assetId: string;
  onSelect: (path: string) => void;
};

function EntriesTable({
  entries,
  basePath,
  projectId,
  assetId,
  onSelect,
}: EntriesTableProps) {
  const pathEntries = Object.keys(entries).filter((p) =>
    p.startsWith(basePath),
  );
  const directories = uniq(
    pathEntries
      .map((p) => p.substring(basePath.length))
      .filter((e) => e.includes("/"))
      .map((p) => p.substring(0, p.indexOf("/") + 1)),
  );
  const files = pathEntries.filter(
    (p) => !p.substring(basePath.length).includes("/"),
  );
  return (
    <table className="w-full">
      <tbody>
        {basePath && (
          <tr>
            <td>
              <button
                className="inline-flex gap-1 items-center mb-1 rounded-sm px-1 hover:bg-slate-100"
                onClick={() => onSelect(pathParent(basePath))}
              >
                <IconCornerLeftUp size={16} /> Up
              </button>
            </td>
            <td></td>
          </tr>
        )}
        {directories.map((directory) => (
          <tr key={directory}>
            <td>
              <button
                onClick={() => onSelect(`${basePath}${directory}`)}
                className="inline-flex gap-1 items-center rounded-sm px-1 hover:bg-slate-100"
              >
                <IconFolder size={16} />
                {directory}
              </button>
            </td>
            <td></td>
          </tr>
        ))}
        {files.map((path) => (
          <tr key={path}>
            <td>
              <a
                href={assetUrl(projectId, assetId, path)}
                className="inline-flex gap-1 items-center rounded-sm px-1 hover:bg-slate-100"
                onClick={(ev) => {
                  if (!ev.ctrlKey) {
                    ev.preventDefault();
                    onSelect(path);
                  }
                }}
              >
                <IconFile size={16} />
                {path.substring(basePath.length)}
              </a>
            </td>
            <td className="text-slate-500">{humanSize(entries[path].size)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

type DirectoryBrowserProps = {
  open: boolean;
  asset: models.Asset;
  projectId: string;
  assetId: string;
  onClose: () => void;
};

function DirectoryBrowser({
  open,
  asset,
  projectId,
  assetId,
  onClose,
}: DirectoryBrowserProps) {
  const [selected, setSelected] = useState<string>("");
  const handlePreviewClose = useCallback(
    () =>
      setSelected((s) =>
        s.includes("/") ? s.substring(0, s.lastIndexOf("/") + 1) : "",
      ),
    [],
  );
  return (
    <>
      <Dialog
        open={open}
        title={
          <div>
            <span className="text-slate-500 font-normal text-lg">
              {getAssetName(asset)}
            </span>
          </div>
        }
        size="sm"
        className="p-6"
        onClose={onClose}
      >
        <div className="flex flex-col">
          <EntriesTable
            entries={asset.entries}
            basePath={selected}
            projectId={projectId}
            assetId={assetId}
            onSelect={setSelected}
          />
        </div>
        {asset.entries[selected] && (
          <PreviewDialog
            open={!!asset.entries[selected]}
            projectId={projectId}
            assetId={assetId}
            path={selected}
            entry={asset.entries[selected]}
            onClose={handlePreviewClose}
          />
        )}
      </Dialog>
    </>
  );
}

function getAssetEntryMetadata(entry: models.AssetEntry) {
  const parts = [];
  parts.push(humanSize(entry.size));
  if ("type" in entry.metadata && entry.metadata["type"]) {
    parts.push(entry.metadata["type"]);
  }
  return parts;
}

type Props = {
  asset: models.Asset;
  projectId: string;
  assetId: string;
  path?: string;
  className?: string;
  hoveredClassName?: string;
  children: ReactNode;
};

export default function AssetLink({
  asset,
  projectId,
  assetId,
  path,
  className,
  hoveredClassName,
  children,
}: Props) {
  const [open, setOpen] = useState<boolean>(false);
  const { isHovered, setHovered } = useHoverContext();
  const handleLinkClick = useCallback((ev: MouseEvent) => {
    if (!ev.ctrlKey) {
      ev.preventDefault();
      setOpen(true);
    }
  }, []);
  const handleClose = useCallback(() => setOpen(false), []);
  const handleMouseOver = useCallback(
    () => setHovered({ assetId, path }),
    [setHovered, assetId, path],
  );
  const handleMouseOut = useCallback(() => setHovered(undefined), [setHovered]);
  const blobStoresSetting = useSetting(projectId, "blobStores");
  const primaryBlobStore = createBlobStore(blobStoresSetting[0]);
  if (path) {
    const entry = asset.entries[path];
    return (
      <Fragment>
        <PreviewDialog
          open={open}
          projectId={projectId}
          assetId={assetId}
          path={path}
          entry={entry}
          onClose={handleClose}
        />
        <a
          href={primaryBlobStore.url(entry.blobKey)}
          title={`${path}\n${getAssetEntryMetadata(entry).join("; ")}`}
          className={classNames(
            className,
            isHovered({ assetId, path }) && hoveredClassName,
          )}
          target="_blank"
          rel="noreferrer"
          onClick={handleLinkClick}
          onMouseOver={handleMouseOver}
          onMouseOut={handleMouseOut}
        >
          {children}
        </a>
      </Fragment>
    );
  } else {
    return (
      <Fragment>
        <DirectoryBrowser
          open={open}
          asset={asset}
          projectId={projectId}
          assetId={assetId}
          onClose={handleClose}
        />
        <button
          type="button"
          title={pluralise(Object.keys(asset.entries).length, "file")}
          className={classNames(
            className,
            "cursor-pointer",
            isHovered({ assetId }) && hoveredClassName,
          )}
          onClick={handleLinkClick}
          onMouseOver={handleMouseOver}
          onMouseOut={handleMouseOut}
        >
          {children}
        </button>
      </Fragment>
    );
  }
}
