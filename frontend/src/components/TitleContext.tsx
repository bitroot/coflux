import {
  Dispatch,
  ReactNode,
  SetStateAction,
  createContext,
  useContext,
  useEffect,
  useState,
} from "react";

const Context = createContext<Dispatch<SetStateAction<string[]>> | undefined>(
  undefined,
);

export function useTitlePart(part: string | undefined) {
  const setParts = useContext(Context);
  if (!setParts) {
    throw new Error("not in title context");
  }

  useEffect(() => {
    if (part) {
      setParts((parts) => [...parts, part]);
      return () => {
        setParts((parts) => parts.slice(0, -1));
      };
    }
  }, [part, setParts]);
}

function buildTitle(appName: string, parts: string[]) {
  if (parts.length) {
    return `${parts.slice().reverse().join(" - ")} - ${appName}`;
  } else {
    return appName;
  }
}

type Props = {
  appName: string;
  children: ReactNode;
};

export default function TitleContext({ children, appName }: Props) {
  const [parts, setParts] = useState<string[]>([]);
  useEffect(() => {
    const original = document.title;
    document.title = buildTitle(appName, parts);
    return () => {
      document.title = original;
    };
  }, [parts, appName]);
  return <Context.Provider value={setParts}>{children}</Context.Provider>;
}
