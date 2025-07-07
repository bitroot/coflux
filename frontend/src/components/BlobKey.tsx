import { useState, useCallback } from "react";
import { truncate } from "lodash";
import { IconCopy, IconCheck } from "@tabler/icons-react";
import classNames from "classnames";

type BlobKeyProps = {
  blobKey: string;
  size?: "md" | "sm";
};

export default function BlobKey({ blobKey, size = "md" }: BlobKeyProps) {
  const [copied, setCopied] = useState(false);
  const handleCopy = useCallback(async () => {
    await navigator.clipboard.writeText(blobKey);
    setCopied(true);
    setTimeout(() => setCopied(false), 1000);
  }, [blobKey]);
  return (
    <button
      className="flex gap-1 rounded-md bg-slate-50 items-center px-1.5 py-1 text-slate-500 group focus:outline-none"
      title="Blob key (click to copy)"
      onClick={handleCopy}
    >
      <span
        className={classNames(
          size == "sm" ? "text-xs" : "text-sm",
          "leading-none",
        )}
      >
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
