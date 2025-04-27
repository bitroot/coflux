import {
  Combobox,
  ComboboxInput,
  ComboboxOption,
  ComboboxOptions,
} from "@headlessui/react";
import {
  IconBox,
  IconCpu,
  IconLoader2,
  IconPoint,
  IconProps,
  IconSearch,
  IconSubtask,
} from "@tabler/icons-react";
import {
  ChangeEvent,
  ComponentType,
  Fragment,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import * as api from "../api";
import { useWorkspaces } from "../topics";
import { Link, useNavigate } from "react-router-dom";

type Run = {
  runId: string;
  stepId: string;
  attempt: number;
};

type Match = (
  | {
      type: "repository";
      name: string;
    }
  | {
      type: "workflow" | "sensor" | "task";
      repository: string;
      name: string;
      run: Run | null;
    }
) & { score: number };

type MatchOptionProps = {
  icon: ComponentType<IconProps>;
  name: string;
  hint?: string;
  href: string;
};

function MatchOption({ icon: Icon, name, hint, href }: MatchOptionProps) {
  return (
    <ComboboxOption
      as={Link}
      to={href}
      value={href}
      className="flex items-center gap-1 py-1 pl-1 pr-2 rounded data-[focus]:bg-slate-100"
    >
      <Icon size={20} strokeWidth={1} className="text-slate-400 shrink-0" />
      <div className="flex-1 text-sm whitespace-nowrap">
        {name}
        {hint && <span className="text-slate-400 text-xs ml-1">{hint}</span>}
      </div>
    </ComboboxOption>
  );
}

function buildRunUrl(projectId: string, workspaceName: string, run: Run) {
  return `/projects/${projectId}/runs/${run.runId}?workspace=${workspaceName}&step=${run.stepId}&attempt=${run.attempt}`;
}

type Props = {
  projectId: string;
  workspaceId: string;
};

export default function SearchInput({ projectId, workspaceId }: Props) {
  const workspaces = useWorkspaces(projectId);
  const navigate = useNavigate();
  const inputRef = useRef<HTMLInputElement>(null);
  const abortControllerRef = useRef<AbortController>();
  const debounceTimeoutRef = useRef<ReturnType<typeof setTimeout>>();
  const [loading, setLoading] = useState(false);
  const [query, setQuery] = useState("");
  const [error, setError] = useState<any>();
  const [matches, setMatches] = useState<Match[]>();
  const handleQueryChange = useCallback(
    (ev: ChangeEvent<HTMLInputElement>) => setQuery(ev.target.value),
    [],
  );
  const handleKeyDown = useCallback((ev: KeyboardEvent) => {
    if (ev.key == "k" && (ev.metaKey || ev.ctrlKey)) {
      ev.preventDefault();
      if (inputRef.current) {
        inputRef.current.focus();
        inputRef.current.select();
      }
    }
  }, []);
  const handleChange = useCallback(
    (value: string) => {
      if (value) {
        navigate(value);
        inputRef.current?.blur();
      }
    },
    [navigate],
  );
  const performSearch = useCallback(
    (projectId: string, workspaceId: string, query: string) => {
      abortControllerRef.current?.abort();
      const controller = new AbortController();
      abortControllerRef.current = controller;
      setError(undefined);
      setLoading(true);
      api
        .search(projectId, workspaceId, query)
        .then((data) => setMatches(data.matches))
        .catch(setError)
        .finally(() => setLoading(false));
    },
    [],
  );
  useEffect(() => {
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);
  useEffect(() => {
    clearTimeout(debounceTimeoutRef.current);
    if (query) {
      setLoading(true);
      debounceTimeoutRef.current = setTimeout(() => {
        performSearch(projectId, workspaceId, query);
      }, 200);
    } else {
      abortControllerRef.current?.abort();
      setLoading(false);
      setMatches(undefined);
    }
  }, [projectId, workspaceId, query, performSearch]);
  const workspaceName = workspaces?.[workspaceId].name;
  return (
    <Combobox immediate onChange={handleChange}>
      <div className="flex items-center py-1 mr-1 gap-2 bg-slate-100/90 hover:bg-slate-100 rounded-lg flex-1 max-w-56 text-left text-cyan-900 text-sm relative">
        <span className="absolute left-2 text-cyan-900/40">
          {loading ? (
            <IconLoader2 size={16} className="animate-spin" />
          ) : (
            <IconSearch size={16} />
          )}
        </span>
        <ComboboxInput
          value={query}
          onChange={handleQueryChange}
          ref={inputRef}
          placeholder="Go to..."
          className="border-none bg-transparent text-sm text-slate-900 focus:ring-0 py-0 pl-7 pr-14 min-w-0 w-full flex-1 placeholder:text-cyan-900/40"
        />
        <span className="rounded-md bg-cyan-900/10 text-cyan-900/30 text-xs font-semibold px-1.5 py-0.5 absolute right-1">
          Ctrl-K
        </span>
      </div>
      <ComboboxOptions
        transition
        anchor={{ to: "bottom end", gap: "8px" }}
        className="bg-white min-w-[var(--input-width)] shadow-xl rounded-md origin-top transition duration-200 ease-out data-[closed]:scale-95 data-[closed]:opacity-0"
      >
        {error ? (
          <p>Error</p>
        ) : matches?.length ? (
          <div className="p-1">
            {matches.map((match, index) => (
              <Fragment key={index}>
                {match.type == "repository" ? (
                  <MatchOption
                    icon={IconBox}
                    name={match.name}
                    href={`/projects/${projectId}/repositories/${match.name}?workspace=${workspaceName}`}
                  />
                ) : match.type == "workflow" ? (
                  <MatchOption
                    icon={IconSubtask}
                    name={match.name}
                    hint={match.repository}
                    href={
                      match.run
                        ? buildRunUrl(projectId, workspaceName!, match.run)
                        : `/projects/${projectId}/workflows/${match.repository}/${match.name}?workspace=${workspaceName}`
                    }
                  />
                ) : match.type == "sensor" ? (
                  <MatchOption
                    icon={IconCpu}
                    name={match.name}
                    hint={match.repository}
                    href={
                      match.run
                        ? buildRunUrl(projectId, workspaceName!, match.run)
                        : `/projects/${projectId}/sensors/${match.repository}/${match.name}?workspace=${workspaceName}`
                    }
                  />
                ) : match.type == "task" && match.run ? (
                  <MatchOption
                    icon={IconPoint}
                    name={match.name}
                    hint={match.repository}
                    href={buildRunUrl(projectId, workspaceName!, match.run)}
                  />
                ) : null}
              </Fragment>
            ))}
          </div>
        ) : matches ? (
          <p className="italic text-sm p-3 text-slate-500">No matches</p>
        ) : null}
      </ComboboxOptions>
    </Combobox>
  );
}
