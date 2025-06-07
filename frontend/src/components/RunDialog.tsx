import { ComponentProps, FormEvent, useCallback, useState } from "react";

import Dialog from "./common/Dialog";
import * as models from "../models";
import Field from "./common/Field";
import Input from "./common/Input";
import Button from "./common/Button";
import { RequestError } from "../api";
import Alert from "./common/Alert";
import WorkspaceLabel from "./WorkspaceLabel";
import { micromark } from "micromark";
import Select from "./common/Select";

type Value =
  | {
      type: "string";
      value: string;
    }
  | {
      type: "number";
      value: string;
    }
  | {
      type: "json";
      value: string;
    }
  | {
      type: "boolean";
      value: boolean | null;
    }
  | { type: "null" };

type StringInputProps = Omit<
  ComponentProps<typeof Input>,
  "value" | "onChange"
> & {
  value: Value & { type: "string" };
  onChange: (value: Value) => void;
};

function StringInput({ value, onChange, ...props }: StringInputProps) {
  const handleChange = useCallback(
    (value: string) => onChange({ type: "string", value }),
    [onChange],
  );
  return <Input value={value.value} onChange={handleChange} {...props} />;
}

type NumberInputProps = Omit<
  ComponentProps<typeof Input>,
  "value" | "onChange"
> & {
  value: Value & { type: "number" };
  onChange: (value: Value) => void;
};

function NumberInput({ value, onChange, ...props }: NumberInputProps) {
  const handleChange = useCallback(
    (value: string) => onChange({ type: "number", value }),
    [onChange],
  );
  return <Input value={value.value} onChange={handleChange} {...props} />;
}

type BooleanInputProps = Omit<
  ComponentProps<typeof Select>,
  "value" | "onChange" | "options" | "empty"
> & {
  value: Value & { type: "boolean" };
  onChange: (value: Value) => void;
};

function BooleanInput({ value, onChange, ...props }: BooleanInputProps) {
  const handleChange = useCallback(
    (value: "true" | "false" | null) =>
      onChange({
        type: "boolean",
        value: value == "true" ? true : value == "false" ? false : null,
      }),
    [onChange],
  );
  return (
    <Select<"true" | "false">
      value={value.value ? "true" : value.value === false ? "false" : null}
      options={{ true: "True", false: "False" }}
      empty="Select..."
      onChange={handleChange}
      {...props}
    />
  );
}

type JsonInputProps = Omit<
  ComponentProps<typeof Input>,
  "value" | "onChange"
> & {
  value: Value & { type: "json" };
  onChange: (value: Value) => void;
};

function JsonInput({ value, onChange, ...props }: JsonInputProps) {
  const handleChange = useCallback(
    (value: string) => onChange({ type: "json", value }),
    [onChange],
  );
  return <Input value={value.value} onChange={handleChange} {...props} />;
}

function serialiseValue(value: Value): ["json", string] {
  switch (value.type) {
    case "string":
    case "boolean":
      return ["json", JSON.stringify(value.value)];
    case "number":
      // TODO: parse?
      return ["json", value.value];
    case "null":
      return ["json", "null"];
    case "json":
      return ["json", value.value];
  }
}

function defaultValue(parameter: models.Parameter): Value {
  switch (parameter.annotation) {
    case "<class 'str'>":
      return {
        type: "string",
        value: parameter.default ? JSON.parse(parameter.default) : "",
      };
    case "<class 'int'>":
    case "<class 'float'>":
      return { type: "number", value: parameter.default || "" };
    case "<class 'bool'>":
      return {
        type: "boolean",
        value:
          parameter.default?.toLowerCase() == "true"
            ? true
            : parameter.default?.toLowerCase() == "false"
              ? false
              : null,
      };
    case "None":
    case "typing.Literal[None]":
      return { type: "null" };
    default:
      return { type: "json", value: parameter.default || "" };
  }
}

function tryParseJson(value: string) {
  try {
    return JSON.parse(value);
  } catch {
    return undefined;
  }
}

function coerceToString(value: Value | undefined): Value & { type: "string" } {
  switch (value?.type) {
    case "string":
      return value;
    case "number":
    case "json": {
      const value_ = tryParseJson(value.value);
      if (typeof value_ == "string") {
        return { type: "string", value: value_ };
      } else {
        return { type: "string", value: value.value };
      }
    }
    case "boolean":
      return {
        type: "string",
        value: value.value ? "true" : value.value === false ? "false" : "",
      };
    case "null":
    case undefined:
      return { type: "string", value: "" };
  }
}

function coerceToNumber(value: Value | undefined): Value & { type: "number" } {
  switch (value?.type) {
    case "string":
    case "json": {
      const float = parseFloat(value.value);
      if (!isNaN(float)) {
        return { type: "number", value: value.value.toString() };
      } else {
        return { type: "number", value: "" };
      }
    }
    case "number":
      return value;
    case "boolean":
      return {
        type: "number",
        value: value.value ? "1" : value.value === false ? "0" : "",
      };
    case "null":
    case undefined:
      return { type: "number", value: "" };
  }
}

function coerceToJson(value: Value | undefined): Value & { type: "json" } {
  switch (value?.type) {
    case "string":
    case "boolean":
      return { type: "json", value: JSON.stringify(value.value) };
    case "number":
      return { type: "json", value: value.value };
    case "json":
      return value;
    case "null":
      return { type: "json", value: "null" };
    case undefined:
      return { type: "json", value: "" };
  }
}

function coerceToBoolean(
  value: Value | undefined,
): Value & { type: "boolean" } {
  switch (value?.type) {
    case "string":
      return { type: "boolean", value: value.value.toLowerCase() == "true" };
    case "number":
      return { type: "boolean", value: parseFloat(value.value) > 0 };
    case "json":
      return {
        type: "boolean",
        value:
          value.value == "true" ? true : value.value == "false" ? false : null,
      };
    case "boolean":
      return value;
    case "null":
    case undefined:
      return { type: "boolean", value: null };
  }
}

function coerceType(value: Value | undefined, type: Value["type"]): Value {
  switch (type) {
    case "string":
      return coerceToString(value);
    case "number":
      return coerceToNumber(value);
    case "json":
      return coerceToJson(value);
    case "boolean":
      return coerceToBoolean(value);
    case "null":
      return { type: "null" };
  }
}

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
  value: Value | undefined;
  error?: string;
  onChange: (name: string, value: Value) => void;
};

function Argument({
  parameter,
  value: value_,
  error,
  onChange,
}: ArgumentProps) {
  const value = value_ || defaultValue(parameter);
  const handleTypeChange = useCallback(
    (type: Value["type"]) => onChange(parameter.name, coerceType(value, type)),
    [parameter, value, onChange],
  );
  const handleValueChange = useCallback(
    (value: Value) => onChange(parameter.name, value),
    [parameter, onChange],
  );
  return (
    <Field
      label={<span className="font-mono font-bold">{parameter.name}</span>}
      hint={parameter.annotation}
      error={translateArgumentError(error)}
    >
      <div className="flex gap-1.5">
        <Select<Value["type"]>
          value={value.type}
          options={{
            string: "String",
            number: "Number",
            boolean: "Boolean",
            null: "Null",
            json: "JSON",
          }}
          onChange={handleTypeChange}
        />
        {value.type == "string" ? (
          <StringInput
            value={value}
            onChange={handleValueChange}
            className="flex-1"
          />
        ) : value.type == "number" ? (
          <NumberInput
            value={value}
            onChange={handleValueChange}
            className="flex-1"
          />
        ) : value.type == "boolean" ? (
          <BooleanInput
            value={value}
            onChange={handleValueChange}
            className="flex-1"
          />
        ) : value.type == "json" ? (
          <JsonInput
            value={value}
            onChange={handleValueChange}
            className="flex-1"
          />
        ) : null}
      </div>
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
  const [values, setValues] = useState<Record<string, Value>>({});
  const handleValueChange = useCallback(
    (name: string, value: Value) =>
      setValues((vs) => ({ ...vs, [name]: value })),
    [],
  );
  const handleSubmit = useCallback(
    (ev: FormEvent) => {
      ev.preventDefault();
      setStarting(true);
      setErrors(undefined);
      onRun(
        parameters.map((p) =>
          serialiseValue(values[p.name] || defaultValue(p)),
        ),
      )
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
      <form onSubmit={handleSubmit} className="pt-2 px-6 pb-6">
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
          <Button type="submit" size="lg" disabled={starting}>
            Run
          </Button>
          <Button
            type="button"
            size="lg"
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
