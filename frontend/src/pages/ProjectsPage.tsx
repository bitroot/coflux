import { Link, Outlet } from "react-router-dom";
import { IconPlus, IconPyramid } from "@tabler/icons-react";

import { useTitlePart } from "../components/TitleContext";
import { useProjects } from "../topics";
import Header from "../components/Header";
import { Fragment } from "react/jsx-runtime";

export default function ProjectsPage() {
  const projects = useProjects();
  useTitlePart("Projects");
  return (
    <Fragment>
      <Header />
      {projects && (
        <div className="bg-white flex-1 flex">
          <ul className="flex flex-wrap gap-4 p-4">
            {Object.entries(projects).map(([projectId, project]) => (
              <li key={projectId} className="flex w-40 h-44">
                <Link
                  to={`/projects/${projectId}`}
                  className="group flex-1 flex flex-col justify-center p-4 pt-6 border border-slate-200 rounded-lg items-center overflow-hidden hover:border-slate-300 hover:bg-slate-100/20"
                >
                  <IconPyramid
                    size={50}
                    stroke={1.5}
                    className="text-slate-500 group-hover:text-slate-700 mb-2"
                  />
                  <h3 className="font-mono font-bold text-slate-500 group-hover:text-slate-600 max-w-full overflow-hidden whitespace-nowrap text-ellipsis">
                    {project.name}
                  </h3>
                  <h4 className="font-mono text-slate-400 text-xs">
                    {projectId}
                  </h4>
                </Link>
              </li>
            ))}
            <li className="flex w-44 h-44">
              <Link
                to="/projects/new"
                className="group flex-1 flex gap-2 flex-col justify-center p-4 pt-8 border border-dashed border-slate-200 rounded-lg items-center hover:border-slate-300 hover:bg-slate-100/20"
              >
                <IconPlus
                  size={50}
                  stroke={1.5}
                  className="text-slate-500 group-hover:text-slate-700"
                />
                <h3 className="text-slate-500 group-hover:text-slate-600">
                  New project...
                </h3>
              </Link>
            </li>
          </ul>
        </div>
      )}
      <Outlet />
    </Fragment>
  );
}
