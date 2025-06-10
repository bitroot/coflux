import { useParams, useSearchParams } from "react-router-dom";
import { sortBy } from "lodash";

import { useContext } from "../layouts/RunLayout";
import AssetLink from "../components/AssetLink";
import StepLink from "../components/StepLink";
import { getAssetName } from "../assets";
import AssetIcon from "../components/AssetIcon";
import { resolveSteps } from "../graph";
import { humanSize } from "../utils";

type Item = [string, number, string];

export default function AssetsPage() {
  const { run } = useContext();
  const { run: runId, project: projectId } = useParams();
  const [searchParams] = useSearchParams();
  const activeStepId = searchParams.get("step") || undefined;
  const activeAttempt = searchParams.has("attempt")
    ? parseInt(searchParams.get("attempt")!)
    : undefined;
  const stepAttempts = resolveSteps(run, activeStepId, activeAttempt);
  const assets = sortBy(
    Object.entries(stepAttempts).flatMap(([stepId, attempt]) =>
      Object.keys(run.steps[stepId].executions[attempt].assets).map(
        (assetId) => [stepId, attempt, assetId] as Item,
      ),
    ),
    (item) => run.steps[item[0]].executions[item[1]].createdAt,
  );
  return (
    <div className="p-5">
      {assets.length ? (
        <table className="w-full">
          <tbody className="divide-y divide-slate-100">
            {assets.map(([stepId, attempt, assetId]) => {
              const step = run.steps[stepId];
              const asset = step.executions[attempt].assets[assetId];
              const path =
                Object.keys(asset.entries).length == 1
                  ? Object.keys(asset.entries)[0]
                  : undefined;
              const entry = path && asset.entries[path];
              return (
                <tr key={`${stepId}-${attempt}-${assetId}`}>
                  <td className="p-1">
                    <AssetLink
                      asset={asset}
                      projectId={projectId!}
                      assetId={assetId}
                      path={path}
                      className="inline-flex items-center gap-1 whitespace-nowrap rounded-full px-1 ring-slate-400"
                      hoveredClassName="ring-2"
                    >
                      <AssetIcon
                        asset={asset}
                        path={path}
                        size={18}
                        className="shrink-0"
                      />
                      <span className="flex flex-col min-w-0">
                        <span className="text-ellipsis overflow-hidden whitespace-nowrap">
                          {getAssetName(asset)}
                        </span>
                      </span>
                    </AssetLink>
                  </td>
                  <td className="p-1">
                    {entry && (
                      <span className="text-slate-500 text-sm whitespace-nowrap">
                        {humanSize(entry.size)}
                      </span>
                    )}
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
