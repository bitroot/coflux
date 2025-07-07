import { useState, useCallback } from "react";
import { truncate } from "lodash";
import { IconCopy, IconCheck } from "@tabler/icons-react";

type BlobKeyProps = {
  blobKey: string;
};

export default function BlobKey({ blobKey }: BlobKeyProps) {
  const [copied, setCopied] = useState(false);
  const handleCopy = useCallback(async () => {
    await navigator.clipboard.writeText(blobKey);
    setCopied(true);
    setTimeout(() => setCopied(false), 1000);
  }, [blobKey]);
  return (
    <button
      className="flex gap-1 rounded-md bg-slate-50 items-center px-1.5 py-1 text-slate-600 group focus:outline-none"
      title="Blob key (click to copy)"
      onClick={handleCopy}
    >
      <span className="text-xs leading-none">
        {truncate(blobKey, { length: 12, omission: "…" })}
      </span>
      {copied ? (
        <IconCheck size={14} className="text-green-700" />
      ) : (
        <IconCopy size={14} className="opacity-50 group-hover:opacity-100" />
      )}
    </button>
  );
}
