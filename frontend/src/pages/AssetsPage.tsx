import { useParams, useSearchParams } from "react-router-dom";

import { useContext } from "../layouts/RunLayout";
import AssetLink from "../components/AssetLink";
import StepLink from "../components/StepLink";
import { getAssetName, resolveAssetsForRun } from "../assets";
import AssetIcon from "../components/AssetIcon";
import { humanSize, pluralise } from "../utils";

export default function AssetsPage() {
  const { run } = useContext();
  const { run: runId } = useParams();
  const [searchParams] = useSearchParams();
  const activeStepId = searchParams.get("step") || undefined;
  const activeAttempt = searchParams.has("attempt")
    ? parseInt(searchParams.get("attempt")!)
    : undefined;
  const assets = resolveAssetsForRun(run, activeStepId, activeAttempt);
  return (
    <div className="p-5">
      {assets.length ? (
        <table className="w-full">
          <tbody className="divide-y divide-slate-100">
            {assets.map(([stepId, attempt, assetId]) => {
              const step = run.steps[stepId];
              const asset = step.executions[attempt].assets[assetId];
              return (
                <tr key={`${stepId}-${attempt}-${assetId}`}>
                  <td className="p-1">
                    <AssetLink
                      asset={asset}
                      assetId={assetId}
                      className="inline-flex items-center gap-1 whitespace-nowrap rounded-full px-1 ring-slate-400"
                      hoveredClassName="ring-2"
                    >
                      <AssetIcon asset={asset} size={18} className="shrink-0" />
                      <span className="flex flex-col min-w-0">
                        <span className="text-ellipsis overflow-hidden whitespace-nowrap">
                          {asset.name || (
                            <span className="italic text-slate-800">
                              {getAssetName(asset)}
                            </span>
                          )}
                        </span>
                      </span>
                    </AssetLink>
                  </td>
                  <td className="p-1">
                    <span className="text-slate-500 text-sm whitespace-nowrap">
                      {asset.totalCount > 1
                        ? `${pluralise(asset.totalCount, "file")}, ${humanSize(asset.totalSize)}`
                        : humanSize(asset.totalSize)}
                    </span>
                  </td>
                  <td className="p-1 text-right">
                    <StepLink
                      runId={runId!}
                      stepId={stepId}
                      attempt={attempt}
                      className="inline-block max-w-full rounded-sm leading-none text-sm ring-offset-1"
                      activeClassName="ring-2 ring-cyan-400"
                      hoveredClassName="ring-2 ring-slate-300"
                    >
                      <span className="font-mono">{step.target}</span>{" "}
                      <span className="text-slate-500 text-sm">
                        ({step.module})
                      </span>
                    </StepLink>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      ) : (
        <p className="italic">None</p>
      )}
    </div>
  );
}
