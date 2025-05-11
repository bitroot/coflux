import { ReactNode } from "react";
import * as models from "../models";

function interpolate(
  items: ReactNode[],
  separator: (i: number) => ReactNode,
): ReactNode[] {
  return items.flatMap((item, i) => (i > 0 ? [separator(i), item] : [item]));
}

type Props = {
  tagSet: models.TagSet;
};

export default function TagSet({ tagSet }: Props) {
  return (
    <ul className="list-disc ml-5 marker:text-slate-600">
      {Object.entries(tagSet).map(([key, values]) => (
        <li key={key}>
          {interpolate(
            values.map((v) => (
              <span key={v} className="rounded bg-slate-300/50 px-1 text-sm">
                <span className="text-slate-500">{key}</span>: {v}
              </span>
            )),
            (i) => (
              <span key={i} className="text-slate-500 text-sm">
                {" / "}
              </span>
            ),
          )}
        </li>
      ))}
    </ul>
  );
}
