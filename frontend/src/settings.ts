import useLocalStorage from "./hooks/useLocalStorage";

export type BlobStoreSettings =
  | {
      type: "http";
      protocol: "http" | "https";
      host: string;
    }
  | {
      type: "s3";
      bucket: string;
      prefix: string;
      region: string;
      accessKeyId: string;
      secretAccessKey: string;
    };

export type Settings = {
  blobStores: BlobStoreSettings[];
};

const defaultSettings: Settings = {
  blobStores: [
    {
      type: "http",
      protocol: window.location.protocol == "https:" ? "https" : "http",
      host: window.location.host,
    },
  ],
};

export function useSettings(projectId: string) {
  return useLocalStorage(`${projectId}/settings`, defaultSettings);
}

export function useSetting(
  projectId: string,
  key: keyof typeof defaultSettings,
) {
  const [settings] = useSettings(projectId);
  return settings[key];
}
