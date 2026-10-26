-- BIGINT represents the full Rust u32 version range.
ALTER TABLE effects ADD COLUMN input_schema_version BIGINT NOT NULL DEFAULT 1;
ALTER TABLE effects ADD COLUMN output_schema_version BIGINT NOT NULL DEFAULT 1;
