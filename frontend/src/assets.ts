import * as models from "./models";
import { resolveSteps } from "./graph";
import { sortBy } from "lodash";
import {
  IconFile,
  IconFileCode,
  IconFileText,
  IconFileTypeBmp,
  IconFileTypeCss,
  IconFileTypeCsv,
  IconFileTypeDocx,
  IconFileTypeHtml,
  IconFileTypeJpg,
  IconFileTypeJs,
  IconFileTypePdf,
  IconFileTypePng,
  IconFileTypePpt,
  IconFileTypeSql,
  IconFileTypeSvg,
  IconFileTypeTs,
  IconFileTypeXls,
  IconFileTypeXml,
  IconFileTypeZip,
  IconGif,
} from "@tabler/icons-react";

type Item = [string, number, string];

export function resolveAssetsForRun(
  run: models.Run,
  activeStepId: string | undefined,
  activeAttempt: number | undefined,
): Item[] {
  const stepAttempts = resolveSteps(run, activeStepId, activeAttempt);
  return sortBy(
    Object.entries(stepAttempts).flatMap(([stepId, attempt]) =>
      Object.keys(run.steps[stepId].executions[attempt].assets).map(
        (assetId) => [stepId, attempt, assetId] as Item,
      ),
    ),
    (item) => run.steps[item[0]].executions[item[1]].createdAt,
  );
}

export function getAssetName(asset: models.AssetSummary | models.Asset) {
  if (asset.name) {
    return asset.name;
  } else if ("entry" in asset && asset.entry) {
    const parts = asset.entry.path.split("/");
    return parts[parts.length - 1];
  } else if ("entries" in asset && Object.keys(asset.entries).length == 1) {
    const parts = Object.keys(asset.entries)[0].split("/");
    return parts[parts.length - 1];
  } else {
    return "Untitled";
  }
}

export function getIconForFileType(type: string | undefined) {
  switch (type) {
    case "text/plain":
      return IconFileText;
    case "text/html":
      return IconFileTypeHtml;
    case "text/css":
      return IconFileTypeCss;
    case "application/javascript":
    case "application/x-javascript":
    case "text/javascript":
      return IconFileTypeJs;
    case "application/typescript":
      return IconFileTypeTs;
    case "application/json":
      return IconFileCode;
    case "application/xml":
    case "text/xml":
      return IconFileTypeXml;
    case "text/csv":
      return IconFileTypeCsv;
    case "application/sql":
      return IconFileTypeSql;
    case "image/png":
      return IconFileTypePng;
    case "image/jpeg":
      return IconFileTypeJpg;
    case "image/gif":
      return IconGif;
    case "image/bmp":
      return IconFileTypeBmp;
    case "image/svg+xml":
      return IconFileTypeSvg;
    case "application/pdf":
      return IconFileTypePdf;
    case "application/zip":
      return IconFileTypeZip;
    case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
    case "application/msword":
      return IconFileTypeDocx;
    case "application/vnd.openxmlformats-officedocument.presentationml.presentation":
    case "application/vnd.ms-powerpoint":
      return IconFileTypePpt;
    case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
    case "application/vnd.ms-excel":
      return IconFileTypeXls;
    default:
      return IconFile;
  }
}
