import { FormEvent, useCallback, useState } from "react";
import { useNavigate } from "react-router-dom";

import Dialog from "./common/Dialog";
import Input from "./common/Input";
import Field from "./common/Field";
import Button from "./common/Button";
import * as api from "../api";
import { RequestError } from "../api";
import Alert from "./common/Alert";

function translateProjectNameError(error: string | undefined) {
  switch (error) {
    case "invalid":
      return "Invalid project name";
    default:
      return error;
  }
}

export default function NewProjectDialog() {
  const [projectName, setProjectName] = useState("My Project");
  const [errors, setErrors] = useState<Record<string, string>>();
  const [creating, setCreating] = useState(false);
  const navigate = useNavigate();
  const handleClose = useCallback(() => {
    navigate("/projects");
  }, [navigate]);
  const handleSubmit = useCallback(
    (ev: FormEvent) => {
      ev.preventDefault();
      setCreating(true);
      setErrors(undefined);
      api
        .createProject(projectName)
        .then(({ projectId }) => {
          navigate(`/projects/${projectId}`);
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
          setCreating(false);
        });
    },
    [navigate, projectName],
  );
  return (
    <Dialog
      title="New project"
      open={true}
      size="sm"
      className="p-6"
      onClose={handleClose}
    >
      {errors && (
        <Alert variant="warning">
          <p>Failed to create project. Please check errors below.</p>
        </Alert>
      )}
      <form onSubmit={handleSubmit}>
        <Field
          label="Project name"
          error={translateProjectNameError(errors?.projectName)}
        >
          <Input type="text" value={projectName} onChange={setProjectName} />
        </Field>
        <div className="mt-4 flex gap-2">
          <Button type="submit" disabled={creating}>
            Create
          </Button>
          <Button
            type="button"
            outline={true}
            variant="secondary"
            onClick={handleClose}
          >
            Cancel
          </Button>
        </div>
      </form>
    </Dialog>
  );
}
