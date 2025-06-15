import { GetObjectCommand, NoSuchKey, S3Client } from "@aws-sdk/client-s3";
import * as settings from "./settings";

export interface BlobStore {
  load(blobKey: string): Promise<string | undefined>;
  url(blobKey: string): string;
}

class HttpBlobStore implements BlobStore {
  constructor(
    private settings: Extract<settings.BlobStoreSettings, { type: "http" }>,
  ) {}

  async load(blobKey: string) {
    const resp = await fetch(this.url(blobKey));
    if (resp.ok) {
      return resp.text();
    } else if (resp.status == 404) {
      return undefined;
    } else {
      throw new Error(`unexpected response code (${resp.status})`);
    }
  }

  url(blobKey: string) {
    const protocol =
      this.settings.protocol ||
      (window.location.protocol == "https:" ? "https" : "http");
    const host = this.settings.host || window.location.host;
    return `${protocol}://${host}/blobs/${blobKey}`;
  }
}

class S3BlobStore implements BlobStore {
  constructor(
    private settings: Extract<settings.BlobStoreSettings, { type: "s3" }>,
  ) {}

  private key(blobKey: string) {
    return `${this.settings.prefix ? this.settings.prefix + "/" : ""}${blobKey.slice(0, 2)}/${blobKey.slice(2, 4)}/${blobKey.slice(4)}`;
  }

  async load(blobKey: string) {
    const client = new S3Client({
      region: this.settings.region,
      credentials: {
        accessKeyId: this.settings.accessKeyId,
        secretAccessKey: this.settings.secretAccessKey,
      },
    });
    try {
      const response = await client.send(
        new GetObjectCommand({
          Bucket: this.settings.bucket,
          Key: this.key(blobKey),
        }),
      );
      return await response.Body!.transformToString();
    } catch (error) {
      if (error instanceof NoSuchKey) {
        return undefined;
      } else {
        throw error;
      }
    }
  }

  url(blobKey: string) {
    return `https://${this.settings.bucket}.s3.${this.settings.region}.amazonaws.com/${this.key(blobKey)}`;
  }
}

export function createBlobStore(settings: settings.BlobStoreSettings) {
  switch (settings.type) {
    case "http":
      return new HttpBlobStore(settings);
    case "s3":
      return new S3BlobStore(settings);
  }
}
