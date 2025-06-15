import Dialog from "./common/Dialog";
import * as models from "../models";
import * as api from "../api";
import {
  ComponentProps,
  Fragment,
  useCallback,
  useEffect,
  useState,
} from "react";
import {
  Link,
  useLocation,
  useNavigate,
  useSearchParams,
} from "react-router-dom";
import { buildUrl, humanSize, pluralise } from "../utils";
import { omit, sum, uniq } from "lodash";
import {
  IconAlertTriangle,
  IconChevronCompactRight,
  IconDownload,
  IconFile,
  IconFolder,
  IconLoader2,
  IconWindowMaximize,
} from "@tabler/icons-react";
import classNames from "classnames";
import Alert from "./common/Alert";
import { useSetting } from "../settings";
import { BlobStore, createBlobStore } from "../blobs";
import Button from "./common/Button";

function parseIdentifier(value: string | null): [string | undefined, string] {
  if (value) {
    const parts = value.split(":");
    return [parts[0], parts.length > 1 ? parts[1] : ""];
  } else {
    return [undefined, ""];
  }
}

function totalCount(entries: models.Asset["entries"], path?: string) {
  return Object.keys(entries).filter((p) => !path || p.startsWith(path)).length;
}

function totalSize(entries: models.Asset["entries"], path?: string) {
  return sum(
    Object.keys(entries)
      .filter((p) => !path || p.startsWith(path))
      .map((p) => entries[p].size),
  );
}

type LocationBarProps = {
  selected: string;
  assetId: string;
};

function LocationBar({ selected, assetId }: LocationBarProps) {
  const { pathname } = useLocation();
  const [searchParams] = useSearchParams();
  return (
    <ol className="flex items-center">
      <li>
        <Link
          to={buildUrl(pathname, {
            ...Object.fromEntries(searchParams),
            asset: assetId,
          })}
          className="p-1 hover:bg-slate-50 rounded hover:underline"
        >
          Asset
        </Link>
      </li>
      {selected &&
        (selected.endsWith("/") ? selected.slice(0, -1) : selected)
          .split("/")
          .map((p, i, ps) => (
            <Fragment key={i}>
              <li>
                <IconChevronCompactRight
                  size={16}
                  className="shrink-0 text-slate-300"
                />
              </li>
              <li>
                {i == ps.length - 1 ? (
                  <span className="p-1">{p}</span>
                ) : (
                  <Link
                    to={buildUrl(pathname, {
                      ...Object.fromEntries(searchParams),
                      asset: `${assetId}:${ps.slice(0, i + 1).join("/")}/`,
                    })}
                    className="p-1 hover:bg-slate-50 hover:underline rounded"
                  >
                    {p}
                  </Link>
                )}
              </li>
            </Fragment>
          ))}
    </ol>
  );
}

type ToolbarProps = {
  onMaximise: () => void;
};

function Toolbar({ onMaximise }: ToolbarProps) {
  return (
    <div>
      <Button variant="secondary" size="sm" outline={true} onClick={onMaximise}>
        <IconWindowMaximize size={16} className="shrink-0" />
      </Button>
    </div>
  );
}

type FileListItemProps = {
  prefix: string;
  item: string;
  selected?: boolean;
  assetId: string;
  count: number;
  size: number;
};

function FileListItem({
  prefix,
  item,
  selected,
  assetId,
  count,
  size,
}: FileListItemProps) {
  const { pathname } = useLocation();
  const [searchParams] = useSearchParams();
  return (
    <li>
      <Link
        to={buildUrl(pathname, {
          ...Object.fromEntries(searchParams),
          asset: `${assetId}:${prefix}${item}`,
        })}
        className={classNames(
          "flex items-center px-2 py-1 gap-1",
          selected ? "bg-slate-100" : "hover:bg-slate-50",
        )}
      >
        <span className="flex-1 flex items-center gap-1">
          {/* TODO: use icon for file type */}
          {item.endsWith("/") ? (
            <IconFolder size={16} className="shrink-0" />
          ) : (
            <IconFile size={16} className="shrink-0" />
          )}
          {item}
        </span>
        <span className="flex-1 text-slate-500 text-sm">
          {item.endsWith("/")
            ? `${pluralise(count, "file")}, ${humanSize(size)}`
            : humanSize(size)}
        </span>
      </Link>
    </li>
  );
}

type FilesListProps = {
  entries: models.Asset["entries"];
  assetId: string;
  selected: string;
};

function FilesList({ entries, assetId, selected }: FilesListProps) {
  const pathPrefix =
    !selected || selected.endsWith("/")
      ? selected
      : selected.substring(0, selected.lastIndexOf("/") + 1);
  const contents = uniq(
    Object.keys(entries)
      .filter((p) => p.startsWith(pathPrefix))
      .map((p) => p.substring(pathPrefix.length))
      .map((p) => {
        const i = p.indexOf("/");
        if (i >= 0) {
          return p.substring(0, i + 1);
        } else {
          return p;
        }
      }),
  );
  const sorted = [
    ...contents.filter((p) => p.endsWith("/")).sort(),
    ...contents.filter((p) => !p.endsWith("/")).sort(),
  ];
  return (
    <div className="flex flex-col">
      <div className="flex-1 overflow-auto">
        <ol>
          {sorted.map((path) => (
            <FileListItem
              key={path}
              prefix={pathPrefix}
              item={path}
              assetId={assetId}
              count={totalCount(entries, `${pathPrefix}${path}`)}
              size={totalSize(entries, `${pathPrefix}${path}`)}
            />
          ))}
        </ol>
      </div>
    </div>
  );
}

type FileInfoProps = {
  entry: models.AssetEntry;
  blobStore: BlobStore;
};

function FileInfo({ entry, blobStore }: FileInfoProps) {
  return (
    <div className="flex-1 overflow-auto p-5 flex flex-col items-center justify-center gap-5">
      <div className="flex flex-col items-center">
        <IconFile
          size={40}
          strokeWidth={1}
          className="shrink-0 text-slate-500 mb-2"
        />
        <h1>{entry.path}</h1>
        <p className="text-slate-500 text-sm">{humanSize(entry.size)}</p>
      </div>
      <Button
        as="a"
        variant="secondary"
        outline={true}
        href={blobStore.url(entry.blobKey)}
        download={true}
        className="text-sm m-1 p-1 rounded-md data-active:bg-slate-100 flex items-center gap-1"
      >
        <IconDownload size={16} />
        Download
      </Button>
    </div>
  );
}

type Props = {
  identifier: string | null;
  projectId: string;
};

export default function AssetDialog({ identifier, projectId }: Props) {
  const [searchParams] = useSearchParams();
  const { pathname } = useLocation();
  const navigate = useNavigate();
  const blobStoresSetting = useSetting(projectId, "blobStores");
  const primaryBlobStore = createBlobStore(blobStoresSetting[0]);
  const [assetId, selected] = parseIdentifier(identifier);
  const [asset, setAsset] = useState<models.Asset | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [size, setSize] = useState<ComponentProps<typeof Dialog>["size"]>("lg");
  useEffect(() => {
    setError(null);
    if (assetId) {
      setAsset(null);
      api
        .getAsset(projectId, assetId)
        .then((asset) => setAsset(asset))
        .catch((error) => setError(error));
    }
  }, [projectId, assetId]);
  const handleDialogClose = useCallback(() => {
    navigate(
      buildUrl(pathname, omit(Object.fromEntries(searchParams), "asset")),
    );
  }, [searchParams, pathname, navigate]);
  const handleMaximise = useCallback(() => {
    setSize((size) => (size == "lg" ? undefined : "lg"));
  }, []);
  const entry =
    selected && !selected.endsWith("/") ? asset?.entries[selected] : undefined;
  const type = entry && (entry.metadata["type"] as string);
  return (
    <Dialog open={!!assetId} onClose={handleDialogClose} size={size}>
      {error ? (
        <div className="p-3">
          <Alert icon={IconAlertTriangle} variant="danger">
            <p>Failed to load asset. Please try again.</p>
          </Alert>
        </div>
      ) : asset === null ? (
        <div
          className={classNames(
            "flex flex-col items-center justify-center gap-2",
            size ? "h-96" : "h-full",
          )}
        >
          <IconLoader2 size={24} className="animate-spin text-slate-300" />
          <p className="text-slate-500">Loading asset...</p>
        </div>
      ) : (
        <div className={classNames("flex flex-col", size ? "h-96" : "h-full")}>
          <div className="p-3 border-b border-slate-200 flex justify-between">
            <LocationBar selected={selected} assetId={assetId!} />
            <Toolbar onMaximise={handleMaximise} />
          </div>
          {entry &&
          (type?.startsWith("text/") || type?.startsWith("image/")) ? (
            <iframe
              src={primaryBlobStore.url(entry.blobKey)}
              className="flex-1"
            ></iframe>
          ) : entry ? (
            <FileInfo
              entry={{ ...entry, path: selected }}
              blobStore={primaryBlobStore}
            />
          ) : (
            <FilesList
              entries={asset.entries}
              assetId={assetId!}
              selected={selected}
            />
          )}
        </div>
      )}
    </Dialog>
  );
}
