import { useNavigate, useParams, useSearchParams } from "react-router-dom";
import { useEffect } from "react";
import { buildUrl } from "../utils";
import { useSpaces, useRun } from "../topics";
import { findKey } from "lodash";

export default function RunPage() {
  const navigate = useNavigate();
  const { project: projectId, run: runId } = useParams();
  const [searchParams] = useSearchParams();
  const spaces = useSpaces(projectId);
  const activeSpaceName = searchParams.get("space") || undefined;
  const activeSpaceId = findKey(
    spaces,
    (e) => e.name == activeSpaceName && e.state != "archived",
  );
  const run = useRun(projectId, runId, activeSpaceId);
  const initialStep = run && Object.values(run.steps).find((s) => !s.parentId)!;
  const type = initialStep?.type;
  useEffect(() => {
    if (type) {
      const page = type == "sensor" ? "children" : "graph";
      navigate(
        buildUrl(
          `/projects/${projectId}/runs/${runId}/${page}`,
          Object.fromEntries(searchParams),
        ),
        { replace: true },
      );
    }
  }, [projectId, runId, searchParams, type, navigate]);
  return <div></div>;
}
