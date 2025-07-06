import {
  ComponentProps,
  FormEvent,
  Fragment,
  ReactNode,
  useCallback,
  useState,
} from "react";
import Button from "./common/Button";
import Dialog from "./common/Dialog";
import {
  Disclosure,
  DisclosureButton,
  DisclosurePanel,
  Menu,
  MenuButton,
  MenuItem,
  MenuItems,
} from "@headlessui/react";
import { insertAt, removeAt } from "../utils";
import Field from "./common/Field";
import Input from "./common/Input";
import Tabs, { Tab } from "./common/Tabs";
import classNames from "classnames";
import {
  IconArrowDown,
  IconArrowUp,
  IconBulb,
  IconChevronRight,
  IconX,
} from "@tabler/icons-react";
import Badge from "./Badge";
import { useSettings } from "../settings";
import { isEqual } from "lodash";
import Select from "./common/Select";
import * as settings from "../settings";
import Alert from "./common/Alert";

type BlobStoreSettingsProps = {
  index: number;
  count: number;
  onMove: (index: number, direction: number) => void;
  onRemove: (index: number) => void;
  title: ReactNode;
  children: ReactNode;
};

function BlobStoreSettings({
  index,
  count,
  onMove,
  onRemove,
  title,
  children,
}: BlobStoreSettingsProps) {
  const handleMoveUpClick = useCallback(
    () => onMove(index, -1),
    [onMove, index],
  );
  const handleMoveDownClick = useCallback(
    () => onMove(index, +1),
    [onMove, index],
  );
  const handleRemoveClick = useCallback(
    () => onRemove(index),
    [onRemove, index],
  );
  return (
    <Disclosure
      as="div"
      className="border border-slate-200 rounded-md bg-slate-50"
    >
      <div className="flex gap-1 items-center text-sm pr-2">
        <DisclosureButton className="flex gap-1 px-1 py-1.5 items-center group flex-1">
          <span className="group-hover:bg-slate-200 rounded-sm px-1 py-0.5 flex items-center gap-1">
            <IconChevronRight
              size={16}
              className="group-data-open:rotate-90 transition"
            />
            {title}
          </span>
          {index == 0 && <Badge label="Primary" intent="info" size="sm" />}
        </DisclosureButton>
        <button
          className="hover:enabled:bg-slate-200 rounded-sm p-1 disabled:text-slate-300"
          type="button"
          disabled={index == 0}
          onClick={handleMoveUpClick}
        >
          <IconArrowUp size={16} />
        </button>
        <button
          className="hover:enabled:bg-slate-200 rounded-sm p-1 disabled:text-slate-300"
          type="button"
          disabled={index == count - 1}
          onClick={handleMoveDownClick}
        >
          <IconArrowDown size={16} />
        </button>
        <button
          className="hover:enabled:bg-slate-200 rounded-sm p-1 disabled:text-slate-300"
          type="button"
          disabled={count <= 1}
          onClick={handleRemoveClick}
        >
          <IconX size={16} />
        </button>
      </div>
      <DisclosurePanel
        transition
        className="p-3 bg-white rounded-md origin-top transition duration-200 ease-out data-closed:opacity-0"
      >
        {children}
      </DisclosurePanel>
    </Disclosure>
  );
}

type HttpBlobStoreSettingsProps = {
  store: Extract<settings.BlobStoreSettings, { type: "http" }>;
  index: number;
  count: number;
  onMove: (index: number, direction: number) => void;
  onRemove: (index: number) => void;
  onChange: (index: number, store: settings.BlobStoreSettings) => void;
};

function HttpBlobStoreSettings({
  store,
  index,
  count,
  onMove,
  onRemove,
  onChange,
}: HttpBlobStoreSettingsProps) {
  const handleHostChange = useCallback(
    (host: string) => onChange(index, { ...store, host }),
    [store, index, onChange],
  );
  const handleProtocolChange = useCallback(
    (protocol: "http" | "https" | null) => {
      if (protocol) {
        onChange(index, { ...store, protocol });
      }
    },
    [store, index, onChange],
  );
  return (
    <BlobStoreSettings
      index={index}
      count={count}
      onMove={onMove}
      onRemove={onRemove}
      title={
        <span className="inline-flex gap-1">
          <span className="font-semibold">HTTP store</span>
          {store.host && <span className="text-slate-500">({store.host})</span>}
        </span>
      }
    >
      <div className="flex gap-3">
        <Field label="Protocol" className="w-32">
          <Select<"http" | "https">
            value={store.protocol}
            options={["http", "https"]}
            onChange={handleProtocolChange}
          />
        </Field>
        <Field label="Host" className="flex-1">
          <Input
            type="text"
            value={store.host}
            placeholder={window.location.host}
            onChange={handleHostChange}
          />
        </Field>
      </div>
    </BlobStoreSettings>
  );
}

type S3BlobStoreSettingsProps = {
  store: Extract<settings.BlobStoreSettings, { type: "s3" }>;
  index: number;
  count: number;
  onMove: (index: number, direction: number) => void;
  onRemove: (index: number) => void;
  onChange: (index: number, store: settings.BlobStoreSettings) => void;
};

function S3BlobStoreSettings({
  store,
  index,
  count,
  onMove,
  onRemove,
  onChange,
}: S3BlobStoreSettingsProps) {
  const handleRegionChange = useCallback(
    (region: string) => onChange(index, { ...store, region }),
    [store, onChange, index],
  );
  const handleBucketChange = useCallback(
    (bucket: string) => onChange(index, { ...store, bucket }),
    [store, onChange, index],
  );
  const handlePrefixChange = useCallback(
    (prefix: string) => onChange(index, { ...store, prefix }),
    [store, onChange, index],
  );
  const handleAccessKeyIdChange = useCallback(
    (accessKeyId: string) => onChange(index, { ...store, accessKeyId }),
    [store, onChange, index],
  );
  const handleSecretAccessKeyChange = useCallback(
    (secretAccessKey: string) => onChange(index, { ...store, secretAccessKey }),
    [store, onChange, index],
  );
  return (
    <BlobStoreSettings
      index={index}
      count={count}
      onMove={onMove}
      onRemove={onRemove}
      title={
        <span className="inline-flex gap-1">
          <span className="font-semibold">S3 store</span>
          {store.bucket && (
            <span className="text-slate-500">
              ({store.bucket + (store.prefix ? `/${store.prefix}` : "")})
            </span>
          )}
        </span>
      }
    >
      <div className="flex gap-3">
        <Field label="Region" className="flex-1">
          <Input
            type="text"
            value={store.region}
            onChange={handleRegionChange}
          />
        </Field>
        <Field label="Bucket name" className="flex-1">
          <Input
            type="text"
            value={store.bucket}
            onChange={handleBucketChange}
          />
        </Field>
        <Field label="Prefix" hint="optional" className="flex-1">
          <Input
            type="text"
            value={store.prefix}
            onChange={handlePrefixChange}
          />
        </Field>
      </div>
      <div className="flex gap-3">
        <Field label="Access key ID" className="flex-1">
          <Input
            type="text"
            value={store.accessKeyId}
            onChange={handleAccessKeyIdChange}
          />
        </Field>
        <Field label="Secret access key" className="flex-1">
          <Input
            type="text"
            value={store.secretAccessKey}
            onChange={handleSecretAccessKeyChange}
          />
        </Field>
      </div>
      <Alert variant="primary" size="sm" icon={IconBulb} className="mt-2">
        <p>You may need to configure CORS settings on your bucket.</p>
      </Alert>
    </BlobStoreSettings>
  );
}

type BlobStoresSettingsProps = {
  stores: settings.BlobStoreSettings[];
  onChange: (stores: settings.BlobStoreSettings[]) => void;
};

function BlobStoresSettings({ stores, onChange }: BlobStoresSettingsProps) {
  const handleStoreMove = useCallback(
    (index: number, delta: number) => {
      onChange(insertAt(removeAt(stores, index), index + delta, stores[index]));
    },
    [stores, onChange],
  );
  const handleStoreChange = useCallback(
    (index: number, store: settings.BlobStoreSettings) =>
      onChange(stores.with(index, store)),
    [stores, onChange],
  );
  const handleStoreRemove = useCallback(
    (index: number) => {
      onChange(removeAt(stores, index));
    },
    [stores, onChange],
  );
  const handleHttpAddClick = useCallback(
    () =>
      onChange([
        ...stores,
        {
          type: "http",
          protocol: window.location.protocol == "https:" ? "https" : "http",
          host: window.location.host,
        },
      ]),
    [stores, onChange],
  );
  const handleS3AddClick = useCallback(
    () =>
      onChange([
        ...stores,
        {
          type: "s3",
          bucket: "",
          prefix: "",
          region: "",
          accessKeyId: "",
          secretAccessKey: "",
        },
      ]),
    [stores, onChange],
  );
  return (
    <div className="flex flex-col gap-2">
      <p className="text-sm mb-2 text-slate-700">
        Configure blob stores to support downloading blobs in the browser. The
        primary store will be used to generate URLs. Loading blobs will attempt
        stores in turn. These settings are only stored in local storage. Loaded
        blob content is cached in session storage.
      </p>
      <div className="flex flex-col gap-1">
        {stores.map((store, index) => (
          <Fragment key={index}>
            {store.type == "http" ? (
              <HttpBlobStoreSettings
                store={store}
                index={index}
                count={stores.length}
                onMove={handleStoreMove}
                onRemove={handleStoreRemove}
                onChange={handleStoreChange}
              />
            ) : store.type == "s3" ? (
              <S3BlobStoreSettings
                store={store}
                index={index}
                count={stores.length}
                onMove={handleStoreMove}
                onRemove={handleStoreRemove}
                onChange={handleStoreChange}
              />
            ) : null}
          </Fragment>
        ))}
      </div>
      <div>
        <Menu>
          <MenuButton as={Button} size="sm" outline={true}>
            Add store...
          </MenuButton>
          <MenuItems
            transition
            anchor="bottom start"
            className="p-1 bg-white shadow-xl rounded-md flex flex-col text-sm origin-top transition duration-200 ease-out data-closed:scale-95 data-closed:opacity-0"
          >
            <MenuItem>
              <button
                onClick={handleHttpAddClick}
                className="p-1 rounded-sm text-start data-active:bg-slate-100"
              >
                Add HTTP store
              </button>
            </MenuItem>
            <MenuItem>
              <button
                onClick={handleS3AddClick}
                className="p-1 rounded-sm text-start data-active:bg-slate-100"
              >
                Add S3 store
              </button>
            </MenuItem>
          </MenuItems>
        </Menu>
      </div>
    </div>
  );
}

function SettingsTab({ className, ...props }: ComponentProps<typeof Tab>) {
  return (
    <Tab
      className={classNames("flex-1 flex flex-col gap-4 p-4", className)}
      {...props}
    />
  );
}

type Props = {
  projectId: string;
  open: boolean;
  onClose: () => void;
};

export default function SettingsDialog({ projectId, open, onClose }: Props) {
  const [savedSettings, saveSettings] = useSettings(projectId);
  const [state, setState] = useState<settings.Settings>();
  const settings = state || savedSettings;
  const handleBlobStoresChange = useCallback(
    (blobStores: settings.BlobStoreSettings[]) =>
      setState({ ...settings, blobStores }),
    [settings],
  );
  const handleResetClick = useCallback(() => setState(undefined), []);
  const handleSubmit = useCallback(
    (ev: FormEvent) => {
      ev.preventDefault();
      saveSettings(state);
      setState(undefined);
      onClose();
    },
    [saveSettings, state, onClose],
  );
  const changed = !isEqual(settings, savedSettings);
  return (
    <Dialog
      title={<div className="px-4 pt-4">Project settings</div>}
      open={open}
      onClose={onClose}
      size="lg"
    >
      <form onSubmit={handleSubmit}>
        <Tabs className="px-4">
          <SettingsTab label="Blob stores">
            <BlobStoresSettings
              stores={settings.blobStores}
              onChange={handleBlobStoresChange}
            />
          </SettingsTab>
        </Tabs>
        <div className="p-4 flex gap-2">
          <Button type="submit" disabled={!changed}>
            Save
          </Button>
          <Button
            type="button"
            variant="secondary"
            outline={true}
            disabled={!changed}
            onClick={handleResetClick}
          >
            Reset
          </Button>
        </div>
      </form>
    </Dialog>
  );
}
