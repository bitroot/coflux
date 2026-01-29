-- Rename 'space' to 'workspace'

-- Rename tables
ALTER TABLE spaces RENAME TO workspaces;
ALTER TABLE space_states RENAME TO workspace_states;
ALTER TABLE space_names RENAME TO workspace_names;
ALTER TABLE space_bases RENAME TO workspace_bases;
ALTER TABLE space_manifests RENAME TO workspace_manifests;

-- Rename columns in renamed tables
ALTER TABLE workspace_states RENAME COLUMN space_id TO workspace_id;
ALTER TABLE workspace_names RENAME COLUMN space_id TO workspace_id;
ALTER TABLE workspace_bases RENAME COLUMN space_id TO workspace_id;
ALTER TABLE workspace_bases RENAME COLUMN base_id TO base_workspace_id;
ALTER TABLE workspace_manifests RENAME COLUMN space_id TO workspace_id;

-- Rename columns in other tables
ALTER TABLE pools RENAME COLUMN space_id TO workspace_id;
ALTER TABLE sessions RENAME COLUMN space_id TO workspace_id;
ALTER TABLE executions RENAME COLUMN space_id TO workspace_id;
