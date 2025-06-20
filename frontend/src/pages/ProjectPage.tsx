import { ReactNode, useMemo } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import classNames from "classnames";
import { findKey } from "lodash";

import { randomName } from "../utils";
import CodeBlock from "../components/CodeBlock";
import { useSpaces, useProjects, useModules } from "../topics";
import {
  Disclosure,
  DisclosureButton,
  DisclosurePanel,
} from "@headlessui/react";
import { IconChevronDown, IconInfoCircle } from "@tabler/icons-react";

function generatePackageName(projectName: string | undefined) {
  return projectName?.replace(/[^a-z0-9_]/gi, "").toLowerCase() || "my_package";
}

type HintProps = {
  children: ReactNode;
};

function Hint({ children }: HintProps) {
  return (
    <div className="text-sm flex gap-1 text-slate-400 mb-2">
      <IconInfoCircle size={16} className="mt-0.5 shrink-0" />
      {children}
    </div>
  );
}

type StepProps = {
  title: string;
  children: ReactNode;
};

function Step({ title, children }: StepProps) {
  return (
    <Disclosure as="li" className="my-4" defaultOpen={true}>
      {({ open }) => (
        <div className="flex flex-col">
          <DisclosureButton className="text-left flex items-center gap-1">
            {title}
            <IconChevronDown
              size={16}
              className={classNames("text-slate-400", open && "rotate-180")}
            />
          </DisclosureButton>
          <DisclosurePanel>{children}</DisclosurePanel>
        </div>
      )}
    </Disclosure>
  );
}

type GettingStartedProps = {
  projectId: string;
  spaceId: string | undefined;
};

function GettingStarted({ projectId, spaceId }: GettingStartedProps) {
  const projects = useProjects();
  const project = (projectId && projects && projects[projectId]) || undefined;
  const packageName = generatePackageName(project?.name);
  const spaces = useSpaces(projectId);
  const spaceName = spaceId && spaces?.[spaceId].name;
  const exampleSpaceName = useMemo(() => randomName(), []);
  const exampleModuleName = `${packageName}.workflows`;
  const anySpaces =
    spaces &&
    Object.values(spaces).filter((e) => e.state != "archived").length > 0;
  if (anySpaces) {
    return (
      <div className="overflow-auto">
        <div className="bg-slate-50 border border-slate-100 rounded-lg mx-auto my-6 w-2/3 p-3 text-slate-600">
          <h1 className="text-3xl my-2">Your project is ready</h1>
          <p className="my-2">
            Follow these steps to create your first workflow:
          </p>
          <ol className="list-decimal pl-8 my-5">
            <Step title="Install the CLI">
              <CodeBlock
                className="bg-slate-100"
                prompt="$"
                code={["pip install coflux"]}
              />
            </Step>
            <Step title="Populate the configuration file">
              <CodeBlock
                className="bg-slate-100"
                prompt="$"
                code={[
                  `coflux configure \\\n  --host=${window.location.host} \\\n  --project=${projectId} \\\n  --space=${spaceName || exampleSpaceName}`,
                ]}
              />
              <Hint>
                <p>
                  This will create a configuration file at{" "}
                  <code className="bg-slate-100">coflux.yaml</code>. It's not
                  necessary, but avoids having to specify these settings with
                  every CLI command.
                </p>
              </Hint>
            </Step>
            <Step title="Initialise an empty module">
              <CodeBlock
                className="bg-slate-100"
                prompt="$"
                code={[
                  `mkdir -p ${packageName}`,
                  `touch ${packageName}/__init__.py`,
                  `touch ${packageName}/workflows.py`,
                ]}
              />
            </Step>
            <Step title="Run the agent">
              <CodeBlock
                className="bg-slate-100"
                prompt="$"
                code={[`coflux agent --dev ${exampleModuleName}`]}
              />
              <Hint>
                <p>
                  The <code className="bg-slate-100">--dev</code> flag enables
                  file watching and automatic module registration (equivalent to{" "}
                  <code className="bg-slate-100">--reload</code> and{" "}
                  <code className="bg-slate-100">--register</code>) - the agent
                  will automatically restart when changes to the source code are
                  detected, and automtically register workflow definitions.
                </p>
              </Hint>
            </Step>
            <Step title="Add a workflow to your module">
              <CodeBlock
                header={`${packageName}/workflows.py`}
                className="bg-slate-100"
                code={[
                  "import coflux as cf",
                  "",
                  "@cf.workflow()",
                  "def hello(name: str):",
                  '    cf.log_info("Hello, {name}", name=name)',
                  "    return 42",
                ]}
              />
              <Hint>
                <p>
                  When you save the file, the space will automatically appear in
                  the sidebar.
                </p>
              </Hint>
            </Step>
          </ol>
        </div>
      </div>
    );
  } else {
    return null;
  }
}

export default function ProjectPage() {
  const { project: projectId } = useParams();
  const [searchParams] = useSearchParams();
  const spaceName = searchParams.get("space") || undefined;
  const spaces = useSpaces(projectId);
  const spaceId = findKey(
    spaces,
    (e) => e.name == spaceName && e.state != "archived",
  );
  const modules = useModules(projectId, spaceId);
  if (
    projectId &&
    modules &&
    !Object.values(modules).some((r) => r.workflows.length || r.sensors.length)
  ) {
    return <GettingStarted projectId={projectId} spaceId={spaceId} />;
  } else {
    return <div></div>;
  }
}
