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
      return "Invalid space name";
    case "exists":
      return "Space already exists";
    default:
      return error;
  }
}

type Props = {
  spaces: Record<string, models.Space>;
  open: boolean;
  hideCancel?: boolean;
  onClose: () => void;
};

export default function AddSpaceDialog({
  spaces,
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
        .createSpace(activeProjectId!, name, baseId)
        .then(() => {
          navigate(`/projects/${activeProjectId}?space=${name}`);
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
    [navigate, name, baseId, activeProjectId, onClose],
  );
  const spaceNames = Object.entries(spaces)
    .filter(([, e]) => e.state != "archived")
    .reduce((acc, [id, e]) => ({ ...acc, [id]: e.name }), {});
  return (
    <Dialog
      title="Add space"
      open={open}
      size="md"
      className="p-6"
      onClose={onClose}
    >
      {errors && (
        <Alert variant="warning">
          <p>Failed to create space. Please check errors below.</p>
        </Alert>
      )}
      <form onSubmit={handleSubmit}>
        <Field label="Space name" error={translateError(errors?.space)}>
          <Input
            type="text"
            value={name}
            className="w-full"
            onChange={setName}
          />
        </Field>
        <Field label="Base space" error={translateError(errors?.base)}>
          <Select
            options={spaceNames}
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
