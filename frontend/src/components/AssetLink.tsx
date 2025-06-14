import { Fragment, ReactNode, MouseEvent, useCallback, useState } from "react";
import {
  IconCaretRightFilled,
  IconDownload,
  IconFile,
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
  basePath?: string;
  projectId: string;
  assetId: string;
  onSelect: (path: string) => void;
  className?: string;
};

function EntriesTable({
  entries,
  basePath = "",
  projectId,
  assetId,
  onSelect,
  className,
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
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  return (
    <div className={className}>
      {directories.length || files.length ? (
        <table className="w-full">
          <tbody>
            {directories.map((directory) => (
              <Fragment key={directory}>
                <tr>
                  <td>
                    <button
                      onClick={() =>
                        setExpanded((e) => ({
                          ...e,
                          [directory]: !e[directory],
                        }))
                      }
                      className="inline-flex gap-1 items-center rounded-sm px-1 hover:bg-slate-100"
                    >
                      <IconCaretRightFilled
                        size={16}
                        className={classNames(
                          "transform transition-transform",
                          expanded[directory] ? "rotate-90" : "rotate-0",
                        )}
                      />
                      {directory}
                    </button>
                  </td>
                  <td></td>
                </tr>
                <tr>
                  <td colSpan={2}>
                    {expanded[directory] && (
                      <EntriesTable
                        entries={entries}
                        basePath={`${basePath}${directory}`}
                        projectId={projectId}
                        assetId={assetId}
                        onSelect={onSelect}
                        className="ml-3 pl-1 border-l border-slate-200"
                      />
                    )}
                  </td>
                </tr>
              </Fragment>
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
                <td className="text-slate-500 text-right">
                  {humanSize(entries[path].size)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="text-slate-500 italic">Empty</p>
      )}
    </div>
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
  const [selected, setSelected] = useState<string | null>(null);
  const handlePreviewClose = useCallback(() => setSelected(null), []);
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
        <div className="flex flex-col h-100 overflow-auto">
          <EntriesTable
            entries={asset.entries}
            projectId={projectId}
            assetId={assetId}
            onSelect={setSelected}
          />
        </div>
        {selected && asset.entries[selected] && (
          <PreviewDialog
            open={true}
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
