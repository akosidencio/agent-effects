-- Existing payloads retain their version-1 interpretation.
ALTER TABLE effects ADD COLUMN input_schema_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE effects ADD COLUMN output_schema_version INTEGER NOT NULL DEFAULT 1;
