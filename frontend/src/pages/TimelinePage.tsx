import { useParams } from "react-router-dom";

import RunTimeline from "../components/RunTimeline";
import { useContext } from "../layouts/RunLayout";

export default function TimelinePage() {
  const { run } = useContext();
  const { run: runId } = useParams();
  if (runId) {
    return <RunTimeline runId={runId} run={run} />;
  } else {
    return null;
  }
}
