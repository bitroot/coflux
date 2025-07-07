import Dialog from "./common/Dialog";
import * as models from "../models";
import * as api from "../api";
import {
  Fragment,
  ReactNode,
  RefObject,
  useCallback,
  useEffect,
  useRef,
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
  Icon,
  IconAlertTriangle,
  IconChevronDown,
  IconDownload,
  IconFiles,
  IconFolder,
  IconLoader2,
  IconWindowMaximize,
  IconWindowMinimize,
} from "@tabler/icons-react";
import classNames from "classnames";
import Alert from "./common/Alert";
import { useSetting } from "../settings";
import { BlobStore, createBlobStore } from "../blobs";
import Button from "./common/Button";
import { Menu, MenuButton, MenuItem, MenuItems } from "@headlessui/react";
import {
  getAssetName,
  getIconForFileType,
  resolveAssetsForRun,
} from "../assets";
import { micromark } from "micromark";
import BlobKey from "./BlobKey";

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

type AssetSelectorProps = {
  assetId: string;
  run: models.Run;
};

function AssetSelector({ assetId: selectedAssetId, run }: AssetSelectorProps) {
  const { pathname } = useLocation();
  const [searchParams] = useSearchParams();
  const activeStepId = searchParams.get("step") || undefined;
  const activeAttempt = searchParams.has("attempt")
    ? parseInt(searchParams.get("attempt")!)
    : undefined;
  const assets = resolveAssetsForRun(run, activeStepId, activeAttempt);
  return (
    <div className="flex relative">
      <Menu>
        <MenuButton as={Button} variant="secondary" outline={true}>
          <IconChevronDown size={16} className="shrink-0" />
        </MenuButton>
        <MenuItems
          transition
          anchor={{ to: "bottom start", gap: 4, padding: 20 }}
          className="absolute top-full left-0 bg-white flex flex-col overflow-y-scroll min-w-40 shadow-xl rounded-md origin-top transition duration-200 ease-out data-closed:scale-95 data-closed:opacity-0 outline-none"
        >
          {assets.map(([stepId, attempt, assetId]) => {
            const step = run.steps[stepId];
            const asset = step.executions[attempt].assets[assetId];
            return (
              <MenuItem key={`${stepId}-${attempt}-${assetId}`}>
                <Link
                  to={buildUrl(pathname, {
                    ...Object.fromEntries(searchParams),
                    asset: assetId,
                  })}
                  className="px-2 py-1 data-active:bg-slate-100 flex flex-col"
                >
                  <span
                    className={
                      assetId == selectedAssetId ? "font-bold" : undefined
                    }
                  >
                    {asset.name || (
                      <span className="italic text-slate-800">
                        {getAssetName(asset)}
                      </span>
                    )}
                  </span>
                  <span className="text-slate-500 text-sm">
                    {`${pluralise(asset.totalCount, "file")}, ${humanSize(asset.totalSize)}`}
                  </span>
                </Link>
              </MenuItem>
            );
          })}
        </MenuItems>
      </Menu>
    </div>
  );
}

type LocationBarProps = {
  asset: models.Asset;
  selected: string;
  assetId: string;
  run: models.Run;
};

function LocationBar({ asset, selected, assetId, run }: LocationBarProps) {
  const { pathname } = useLocation();
  const [searchParams] = useSearchParams();
  const parts = selected.match(/[^/]+\/?/g) || [];
  const segments: [string, Icon, ReactNode][] = ["", ...parts].map(
    (part, i, ps) => {
      if (part == "") {
        const child = asset.name || (
          <span className="italic text-slate-800">Untitled</span>
        );
        return ["", IconFiles, child];
      } else if (part.endsWith("/")) {
        return [ps.slice(0, i + 1).join(""), IconFolder, part.slice(0, -1)];
      } else {
        const path = ps.slice(0, i + 1).join("");
        const icon = getIconForFileType(
          asset.entries[path].metadata["type"] as string,
        );
        return [path, icon, part];
      }
    },
  );
  return (
    <div className="flex-1 p-3 flex items-center gap-2 min-w-0">
      <AssetSelector assetId={assetId} run={run} />
      <ol className="flex items-center gap-2 overflow-auto scrollbar-none">
        {segments.map(([path, icon, child], i) => {
          const Icon = icon;
          if (i == segments.length - 1) {
            return (
              <span
                key={i}
                className="p-1 flex items-center gap-1 whitespace-nowrap"
              >
                <Icon size={16} className="shrink-0" />
                {child}
              </span>
            );
          } else {
            return (
              <Button
                key={i}
                as={Link}
                variant="secondary"
                outline={true}
                to={buildUrl(pathname, {
                  ...Object.fromEntries(searchParams),
                  asset: `${assetId}:${path}`,
                })}
                left={<Icon size={16} className="shrink-0" />}
                className="whitespace-nowrap"
              >
                {child}
              </Button>
            );
          }
        })}
      </ol>
    </div>
  );
}

type ToolbarProps = {
  asset: models.Asset;
  assetId: string | undefined;
  selected: string;
  maximised: boolean;
  onToggleMaximise: () => void;
};

function Toolbar({
  asset,
  assetId,
  selected,
  maximised,
  onToggleMaximise,
}: ToolbarProps) {
  return (
    <div className="flex items-center gap-3 p-3">
      {selected == "" ? (
        assetId && (
          <span className="text-sm text-slate-600">
            <span className="text-slate-400">Asset ID:</span> {assetId}
          </span>
        )
      ) : selected && !selected.endsWith("/") ? (
        <BlobKey blobKey={asset.entries[selected].blobKey} />
      ) : null}
      <Button
        variant="secondary"
        outline={true}
        title={maximised ? "Restore dialog" : "Maxmise dialog"}
        onClick={onToggleMaximise}
      >
        {maximised ? (
          <IconWindowMinimize size={16} className="shrink-0" />
        ) : (
          <IconWindowMaximize size={16} className="shrink-0" />
        )}
      </Button>
    </div>
  );
}

type FileListItemProps = {
  prefix: string;
  item: string;
  icon: Icon;
  selected?: boolean;
  assetId: string;
  count: number;
  size: number;
};

function FileListItem({
  prefix,
  item,
  icon: Icon,
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
          "flex items-center px-3 py-2 gap-1",
          selected ? "bg-slate-100" : "hover:bg-slate-50",
        )}
      >
        <span className="flex-1 flex items-center gap-1">
          {item.endsWith("/") ? (
            <Fragment>
              <IconFolder size={20} strokeWidth={1.5} className="shrink-0" />
              {item.slice(0, -1)}
            </Fragment>
          ) : (
            <Fragment>
              <Icon size={20} strokeWidth={1.5} className="shrink-0" />
              {item}
            </Fragment>
          )}
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
  const collator = new Intl.Collator(undefined, {
    numeric: true,
    sensitivity: "base",
  });
  const sorted = [
    ...contents.filter((p) => p.endsWith("/")).sort(collator.compare),
    ...contents.filter((p) => !p.endsWith("/")).sort(collator.compare),
  ];
  return (
    <div className="flex-1 overflow-auto">
      <ol>
        {sorted.map((path) => (
          <FileListItem
            key={path}
            prefix={pathPrefix}
            item={path}
            icon={getIconForFileType(
              entries[`${pathPrefix}${path}`]?.metadata["type"] as string,
            )}
            assetId={assetId}
            count={totalCount(entries, `${pathPrefix}${path}`)}
            size={totalSize(entries, `${pathPrefix}${path}`)}
          />
        ))}
      </ol>
    </div>
  );
}

type FileInfoProps = {
  entry: models.AssetEntry;
  blobStore: BlobStore | undefined;
};

function FileInfo({ entry, blobStore }: FileInfoProps) {
  const Icon = getIconForFileType(entry.metadata["type"] as string);
  return (
    <div className="flex-1 overflow-auto p-5 flex flex-col items-center justify-center gap-6">
      <div className="flex flex-col items-center">
        <Icon
          size={100}
          strokeWidth={1}
          className="shrink-0 text-slate-200 mb-2"
        />
        <h1>{entry.path}</h1>
        <p className="text-slate-500 text-sm">{humanSize(entry.size)}</p>
      </div>
      {blobStore ? (
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
      ) : (
        <BlobKey blobKey={entry.blobKey} size="md" />
      )}
    </div>
  );
}

type MarkdownPreviewProps = {
  blobStore: BlobStore;
  blobKey: string;
  className?: string;
};

function MarkdownPreview({
  blobStore,
  blobKey,
  className,
}: MarkdownPreviewProps) {
  const [error, setError] = useState<unknown>();
  const [html, setHtml] = useState<string>();
  useEffect(() => {
    setHtml(undefined);
    setError(undefined);
    blobStore
      .load(blobKey)
      .then((source) => {
        if (!source) {
          throw new Error("Blob not found");
        }
        setHtml(micromark(source));
      })
      .catch(setError);
  }, [blobStore, blobKey]);
  return (
    <div className={className}>
      {error ? (
        <Alert icon={IconAlertTriangle} variant="danger">
          <p>Failed to load asset. Please try again.</p>
        </Alert>
      ) : html === undefined ? (
        <div>
          <IconLoader2 size={24} className="animate-spin text-slate-300" />
          <p className="text-slate-500">Loading...</p>
        </div>
      ) : (
        <div
          className="prose prose-slate"
          dangerouslySetInnerHTML={{ __html: html }}
        />
      )}
    </div>
  );
}

function useIsScaledDown(
  imgRef: RefObject<HTMLImageElement | null>,
  src: string,
): boolean {
  const [scaledDown, setScaledDown] = useState(false);

  useEffect(() => {
    const img = imgRef.current;
    if (!img) return;

    const check = () => {
      const { naturalWidth: nW, naturalHeight: nH } = img;
      const { width: rW, height: rH } = img.getBoundingClientRect();
      setScaledDown(rW < nW || rH < nH);
    };

    if (img.complete) {
      check();
    } else {
      img.addEventListener("load", check);
    }

    const ro = new ResizeObserver(check);
    ro.observe(img);

    return () => {
      img.removeEventListener("load", check);
      ro.disconnect();
    };
  }, [imgRef, src]);

  return scaledDown;
}

type ImagePreviewProps = {
  src: string;
};

function ImagePreview({ src }: ImagePreviewProps) {
  const [zoomed, setZoomed] = useState(false);
  const imgRef = useRef<HTMLImageElement>(null);
  const isScaledDown = useIsScaledDown(imgRef, src);
  const handleClick = useCallback(() => setZoomed((z) => !z), []);
  return (
    <div
      className={classNames(
        "flex-1",
        zoomed ? "overflow-auto" : "flex min-h-0 min-w-0",
      )}
    >
      <img
        src={src}
        ref={imgRef}
        className={classNames(
          zoomed ? "max-w-none m-auto" : "mx-auto object-scale-down",
          zoomed ? "cursor-zoom-out" : isScaledDown ? "cursor-zoom-in" : null,
        )}
        onClick={isScaledDown || zoomed ? handleClick : undefined}
      />
    </div>
  );
}

type Props = {
  identifier: string | null;
  projectId: string;
  run: models.Run;
};

export default function AssetDialog({ identifier, projectId, run }: Props) {
  const [searchParams] = useSearchParams();
  const { pathname } = useLocation();
  const navigate = useNavigate();
  const blobStoresSetting = useSetting(projectId, "blobStores");
  const blobStore = createBlobStore(blobStoresSetting[0]);
  const [assetId, selected] = parseIdentifier(identifier);
  const [asset, setAsset] = useState<models.Asset | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [maximised, setMaximised] = useState(false);
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
  const handleToggleMaximise = useCallback(
    () => setMaximised((maximised) => !maximised),
    [],
  );
  const entry =
    selected && !selected.endsWith("/") ? asset?.entries[selected] : undefined;
  const type = entry && (entry.metadata["type"] as string);
  return (
    <Dialog
      open={!!assetId}
      onClose={handleDialogClose}
      size={maximised ? undefined : "2xl"}
    >
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
            maximised ? "h-full" : "h-[50vh]",
          )}
        >
          <IconLoader2 size={24} className="animate-spin text-slate-300" />
          <p className="text-slate-500">Loading asset...</p>
        </div>
      ) : (
        <div
          className={classNames(
            "flex flex-col",
            maximised ? "h-full" : "h-[50vh]",
          )}
        >
          <div className="border-b border-slate-200 flex">
            <LocationBar
              asset={asset}
              selected={selected}
              assetId={assetId!}
              run={run}
            />
            <Toolbar
              asset={asset}
              assetId={assetId}
              selected={selected}
              maximised={maximised}
              onToggleMaximise={handleToggleMaximise}
            />
          </div>
          {entry ? (
            type == "text/markdown" && blobStore ? (
              <MarkdownPreview
                blobStore={blobStore}
                blobKey={entry.blobKey}
                className="flex-1 overflow-auto p-4"
              />
            ) : type == "application/pdf" && blobStore ? (
              <iframe
                src={blobStore.url(entry.blobKey)}
                className="flex-1"
              ></iframe>
            ) : type?.startsWith("text/") && blobStore ? (
              <iframe
                src={blobStore.url(entry.blobKey)}
                sandbox="allow-downloads allow-forms allow-modals allow-scripts"
                className="flex-1"
              ></iframe>
            ) : type?.startsWith("image/") && blobStore ? (
              <ImagePreview src={blobStore.url(entry.blobKey)} />
            ) : (
              <FileInfo
                entry={{ ...entry, path: selected }}
                blobStore={blobStore}
              />
            )
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
