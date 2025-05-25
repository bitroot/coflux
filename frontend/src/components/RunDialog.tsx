import { FormEvent, useCallback, useState } from "react";

import Dialog from "./common/Dialog";
import * as models from "../models";
import Field from "./common/Field";
import Input from "./common/Input";
import Button from "./common/Button";
import { RequestError } from "../api";
import Alert from "./common/Alert";
import WorkspaceLabel from "./WorkspaceLabel";
import { micromark } from "micromark";

function translateArgumentError(error: string | undefined) {
  switch (error) {
    case "not_json":
      return "Not valid JSON";
    default:
      return error;
  }
}
type ArgumentProps = {
  parameter: models.Parameter;
  value: string | undefined;
  error?: string;
  onChange: (name: string, value: string) => void;
};

function Argument({ parameter, value, error, onChange }: ArgumentProps) {
  const handleChange = useCallback(
    (value: string) => onChange(parameter.name, value),
    [parameter, onChange],
  );
  return (
    <Field
      label={<span className="font-mono font-bold">{parameter.name}</span>}
      hint={parameter.annotation}
      error={translateArgumentError(error)}
    >
      <Input
        value={value ?? ""}
        placeholder={parameter.default}
        onChange={handleChange}
      />
    </Field>
  );
}

type Props = {
  projectId: string;
  module: string | undefined;
  target: string | undefined;
  parameters: models.Parameter[];
  instruction: string | null;
  activeWorkspaceId: string;
  open: boolean;
  onRun: (arguments_: ["json", string][]) => Promise<void>;
  onClose: () => void;
};

export default function RunDialog({
  projectId,
  module,
  target,
  parameters,
  instruction,
  activeWorkspaceId,
  open,
  onRun,
  onClose,
}: Props) {
  const [starting, setStarting] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>();
  const [values, setValues] = useState<Record<string, string>>({});
  const handleValueChange = useCallback(
    (name: string, value: string) =>
      setValues((vs) => ({ ...vs, [name]: value })),
    [],
  );
  const handleSubmit = useCallback(
    (ev: FormEvent) => {
      ev.preventDefault();
      setStarting(true);
      setErrors(undefined);
      onRun(parameters.map((p) => ["json", values[p.name] || p.default]))
        .catch((error) => {
          if (error instanceof RequestError) {
            setErrors(error.details);
          } else {
            // TODO
            setErrors({});
          }
        })
        .finally(() => {
          setStarting(false);
        });
    },
    [parameters, values, onRun],
  );
  return (
    <Dialog
      title={
        <div className="flex justify-between items-start font-normal text-base px-6 pt-6">
          <div className="flex flex-col">
            <span className="text-slate-400 text-sm">{module} /</span>
            <span className="font-mono font-bold text-xl leading-tight">
              {target}
            </span>
          </div>
          <WorkspaceLabel
            projectId={projectId}
            workspaceId={activeWorkspaceId}
          />
        </div>
      }
      size="lg"
      open={open}
      onClose={onClose}
    >
      {instruction && (
        <div
          className="bg-slate-50 border-slate-100 border-y px-6 py-4 shadow-inner prose prose-slate prose-sm max-h-64 max-w-none overflow-auto"
          dangerouslySetInnerHTML={{ __html: micromark(instruction) }}
        />
      )}
      <form onSubmit={handleSubmit} className="p-6">
        {errors && (
          <Alert variant="warning">
            <p>Failed to start run. Please check errors below.</p>
          </Alert>
        )}
        {parameters.length > 0 && (
          <div className="pb-4">
            {parameters.map((parameter, index) => (
              <Argument
                key={parameter.name}
                parameter={parameter}
                value={values[parameter.name]}
                error={errors?.[`arguments.${index}`]}
                onChange={handleValueChange}
              />
            ))}
          </div>
        )}
        <div className="flex gap-2">
          <Button type="submit" disabled={starting}>
            Run
          </Button>
          <Button
            type="button"
            outline={true}
            variant="secondary"
            onClick={onClose}
          >
            Cancel
          </Button>
        </div>
      </form>
    </Dialog>
  );
}
