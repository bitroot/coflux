import classNames from "classnames";
import { Fragment, useCallback, useEffect, useState } from "react";
import * as models from "../models";
import * as settings from "../settings";
import { chunk } from "lodash";
import {
  IconArrowRightBar,
  IconChevronDown,
  IconDownload,
  IconFunction,
} from "@tabler/icons-react";
import { createBlobStore } from "../blobs";
import {
  Menu,
  MenuButton,
  MenuItem,
  MenuItems,
  MenuSeparator,
} from "@headlessui/react";
import { humanSize } from "../utils";
import StepLink from "./StepLink";
import AssetLink from "./AssetLink";
import AssetIcon from "./AssetIcon";
import Alert from "./common/Alert";
import Button from "./common/Button";
import { useSetting } from "../settings";

type DataProps = {
  data: models.Data;
  references: models.Reference[];
  projectId: string;
};

function Data({ data, references, projectId }: DataProps) {
  const blobStoresSetting = useSetting(projectId, "blobStores");
  if (Array.isArray(data)) {
    return (
      <Fragment>
        [
        {data.map((item, index) => (
          <Fragment key={index}>
            <Data data={item} references={references} projectId={projectId} />
            {index < data.length - 1 && ",\u00a0"}
          </Fragment>
        ))}
        ]
      </Fragment>
    );
  } else if (data && typeof data == "object" && "type" in data) {
    switch (data.type) {
      case "set":
        if (!data.items.length) {
          return "∅";
        } else {
          return (
            <Fragment>
              {"{"}
              {data.items.map((item, index) => (
                <Fragment key={index}>
                  <Data
                    data={item}
                    references={references}
                    projectId={projectId}
                  />
                  {index < data.items.length - 1 && ",\u00a0"}
                </Fragment>
              ))}
              {"}"}
            </Fragment>
          );
        }
      case "tuple":
        return (
          <Fragment>
            (
            {data.items.map((item, index) => (
              <Fragment key={index}>
                <Data
                  data={item}
                  references={references}
                  projectId={projectId}
                />
                {index < data.items.length - 1 && ",\u00a0"}
              </Fragment>
            ))}
            )
          </Fragment>
        );
      case "dict":
        return (
          <Fragment>
            {"{"}
            {chunk(data.items, 2).map(([key, value], index) => (
              <div
                key={index}
                className="pl-4 border-l border-slate-100 ml-1 whitespace-nowrap"
              >
                <Data
                  data={key}
                  references={references}
                  projectId={projectId}
                />
                <IconArrowRightBar
                  size={20}
                  strokeWidth={1}
                  className="text-slate-400 mx-1 inline-block"
                />
                <Data
                  data={value}
                  references={references}
                  projectId={projectId}
                />
              </div>
            ))}
            {"}"}
          </Fragment>
        );
      case "ref": {
        const reference = references[data.index];
        switch (reference.type) {
          case "fragment": {
            const primaryBlobStore = createBlobStore(blobStoresSetting[0]);
            return (
              <Menu>
                <MenuButton className="bg-slate-100 rounded-sm px-1.5 py-0.5 text-xs font-sans inline-flex gap-1">
                  {reference.format}
                  <span className="text-slate-500">
                    ({humanSize(reference.size)})
                  </span>
                  <IconChevronDown
                    size={16}
                    className="text-slate-600"
                    strokeWidth={1.5}
                  />
                </MenuButton>
                <MenuItems
                  transition
                  anchor="bottom"
                  className="bg-white shadow-xl rounded-md origin-top transition duration-200 ease-out data-closed:scale-95 data-closed:opacity-0"
                >
                  <dl className="flex flex-col gap-1 p-2">
                    {Object.entries(reference.metadata).map(([key, value]) => (
                      <div key={key}>
                        <dt className="text-xs text-slate-500">{key}</dt>
                        <dd className="text-sm text-slate-900">
                          {typeof value == "string"
                            ? value
                            : JSON.stringify(value)}
                        </dd>
                      </div>
                    ))}
                  </dl>
                  <MenuSeparator className="my-1 h-px bg-slate-100" />
                  <MenuItem>
                    <a
                      href={primaryBlobStore.url(reference.blobKey)}
                      download
                      className="text-sm m-1 p-1 rounded-md data-active:bg-slate-100 flex items-center gap-1"
                    >
                      <IconDownload size={16} />
                      Download
                    </a>
                  </MenuItem>
                </MenuItems>
              </Menu>
            );
          }
          case "execution": {
            const execution = reference.execution;
            return (
              <StepLink
                runId={execution.runId}
                stepId={execution.stepId}
                attempt={execution.attempt}
                className="bg-slate-100 rounded-sm px-0.5 hover:bg-slate-200 ring-offset-1 ring-slate-400"
                hoveredClassName="ring-2"
              >
                <IconFunction size={16} className="inline-block" />
              </StepLink>
            );
          }
          case "asset":
            return (
              <AssetLink
                projectId={projectId}
                assetId={reference.assetId}
                asset={reference.asset}
                className="bg-slate-100 rounded-sm px-0.5 ring-offset-1 ring-slate-400"
                hoveredClassName="ring-2"
              >
                <AssetIcon asset={reference.asset} className="inline-block" />
              </AssetLink>
            );
        }
      }
    }
  } else if (typeof data == "string") {
    return (
      <span className="text-slate-400 whitespace-nowrap">
        "<span className="text-green-700">{data}</span>"
      </span>
    );
  } else if (typeof data == "number") {
    return <span className="text-purple-700">{data}</span>;
  } else if (data === true) {
    return <span className="text-orange-700">True</span>;
  } else if (data === false) {
    return <span className="text-orange-700">False</span>;
  } else if (data === null) {
    return <span className="text-orange-700 italic">None</span>;
  } else {
    throw new Error(`Unexpected data type: ${data}`);
  }
}

async function loadBlob(
  blobStoresSetting: settings.BlobStoreSettings[],
  blobKey: string,
) {
  for (const settings of blobStoresSetting) {
    const store = createBlobStore(settings);
    const result = await store.load(blobKey);
    if (result !== undefined) {
      return result;
    }
  }
  return undefined;
}

type LoadBlobLinkProps = {
  value: Extract<models.Value, { type: "blob" }>;
  projectId: string;
};

function LoadBlobLink({ value, projectId }: LoadBlobLinkProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>();
  const blobStoresSetting = useSetting(projectId, "blobStores");
  const handleLoadClick = useCallback(() => {
    setError(undefined);
    setLoading(true);
    loadBlob(blobStoresSetting, value.key)
      .then((data) => {
        if (data !== undefined) {
          sessionStorage.setItem(`blobs.${value.key}`, data);
          window.dispatchEvent(new Event("storage"));
        } else {
          setError("Not found");
        }
      })
      .catch(setError)
      .finally(() => setLoading(false));
  }, [value.key, blobStoresSetting]);
  return loading ? (
    <span className="italic text-slate-500 text-sm">Loading...</span>
  ) : error ? (
    // TODO: prompt to configure settings
    <Alert variant="danger">{error.toString()}</Alert>
  ) : (
    <Button size="sm" outline={true} onClick={handleLoadClick}>
      Load ({humanSize(value.size)})
    </Button>
  );
}

function getValueData(value: models.Value) {
  if (value.type == "raw") {
    return value.data;
  } else {
    const json = sessionStorage.getItem(`blobs.${value.key}`);
    if (json) {
      return JSON.parse(json);
    } else {
      return undefined;
    }
  }
}

type ValueProps = {
  value: models.Value;
  projectId: string;
  className?: string;
  block?: boolean;
};

export default function Value({
  value,
  projectId,
  className,
  block,
}: ValueProps) {
  const [, setCount] = useState(0);
  const handleUnloadClick = useCallback(() => {
    if (value.type == "blob") {
      sessionStorage.removeItem(`blobs.${value.key}`);
      window.dispatchEvent(new Event("storage"));
    }
  }, [value]);
  const data = getValueData(value);
  const handleStorageEvent = useCallback(() => setCount((c) => c + 1), []);
  useEffect(() => {
    window.addEventListener("storage", handleStorageEvent);
    return () => window.removeEventListener("storage", handleStorageEvent);
  }, [handleStorageEvent]);
  return (
    <span
      className={classNames(
        className,
        block ? "block" : "inline-block align-middle",
      )}
    >
      {data !== undefined ? (
        <Fragment>
          <div
            className={classNames(
              "bg-white rounded-sm border border-slate-200 font-mono text-sm leading-none",
              block ? "p-1 overflow-auto" : "p-0.5",
            )}
          >
            <Data
              data={data}
              references={value.references}
              projectId={projectId}
            />
          </div>
          {value.type == "blob" && (
            <Button
              size="sm"
              outline={true}
              onClick={handleUnloadClick}
              className="mt-1"
            >
              Unload
            </Button>
          )}
        </Fragment>
      ) : value.type == "blob" ? (
        <LoadBlobLink value={value} projectId={projectId} />
      ) : null}
    </span>
  );
}
