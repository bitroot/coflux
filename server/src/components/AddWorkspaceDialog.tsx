import { FormEvent, useCallback, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";

import Dialog from "./common/Dialog";
import Input from "./common/Input";
import Field from "./common/Field";
import Button from "./common/Button";
import * as models from "../models";
import * as api from "../api";
import { RequestError } from "../api";
import Alert from "./common/Alert";
import Select from "./common/Select";
import { randomName } from "../utils";

function translateError(error: string | undefined) {
  switch (error) {
    case "invalid":
      return "Invalid workspace name";
    case "exists":
      return "Workspace already exists";
    default:
      return error;
  }
}

type Props = {
  workspaces: Record<string, models.Workspace>;
  open: boolean;
  hideCancel?: boolean;
  onClose: () => void;
};

export default function AddWorkspaceDialog({
  workspaces,
  open,
  hideCancel,
  onClose,
}: Props) {
  const { project: activeProjectId } = useParams();
  const [name, setName] = useState(() => randomName());
  const [baseId, setBaseId] = useState<string | null>(null);
  const [errors, setErrors] = useState<Record<string, string>>();
  const [adding, setAdding] = useState(false);
  const navigate = useNavigate();
  const handleSubmit = useCallback(
    (ev: FormEvent) => {
      ev.preventDefault();
      setAdding(true);
      setErrors(undefined);
      api
        .createWorkspace(activeProjectId!, name, baseId)
        .then(() => {
          navigate(`/projects/${activeProjectId}?workspace=${name}`);
          setName(randomName());
          setBaseId(null);
          onClose();
        })
        .catch((error) => {
          if (error instanceof RequestError) {
            setErrors(error.details);
          } else {
            // TODO
            setErrors({});
          }
        })
        .finally(() => {
          setAdding(false);
        });
    },
    [navigate, name, baseId],
  );
  const workspaceNames = Object.entries(workspaces)
    .filter(([_, e]) => e.status != "archived")
    .reduce((acc, [id, e]) => ({ ...acc, [id]: e.name }), {});
  return (
    <Dialog
      title="Add workspace"
      open={open}
      onClose={onClose}
      className="p-6 max-w-lg"
    >
      {errors && (
        <Alert variant="warning">
          <p>Failed to create workspace. Please check errors below.</p>
        </Alert>
      )}
      <form onSubmit={handleSubmit}>
        <Field label="Workspace name" error={translateError(errors?.workspace)}>
          <Input
            type="text"
            value={name}
            className="w-full"
            onChange={setName}
          />
        </Field>
        <Field label="Base workspace" error={translateError(errors?.base)}>
          <Select
            options={workspaceNames}
            empty="(None)"
            size="md"
            value={baseId}
            onChange={setBaseId}
          />
        </Field>
        <div className="mt-4 flex gap-2">
          <Button type="submit" disabled={adding}>
            Create
          </Button>
          {!hideCancel && (
            <Button
              type="button"
              outline={true}
              variant="secondary"
              onClick={onClose}
            >
              Cancel
            </Button>
          )}
        </div>
      </form>
    </Dialog>
  );
}
