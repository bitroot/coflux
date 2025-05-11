import classNames from "classnames";
import { ReactNode } from "react";

type Props = {
  header?: ReactNode;
  prompt?: string;
  code: string[];
  className?: string;
};

export default function CodeBlock({ header, prompt, code, className }: Props) {
  return (
    <div
      className={classNames(
        "shadow-inner rounded-md my-2 overflow-hidden",
        className,
      )}
    >
      {header && (
        <div className="bg-slate-200/40 border-b border-slate-200/60 px-2 py-1 text-sm text-slate-400">
          {header}
        </div>
      )}
      <div className="p-2">
        {code.map((line, index) => (
          <code key={index} className="block whitespace-pre text-sm">
            {prompt && (
              <span className="select-none mr-2 text-cyan-600">{prompt}</span>
            )}
            {line}
          </code>
        ))}
      </div>
    </div>
  );
}
