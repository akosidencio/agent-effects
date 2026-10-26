-- NULL denotes a legacy record: do not infer execution capabilities.
ALTER TABLE effects ADD COLUMN execution_contract JSONB;
