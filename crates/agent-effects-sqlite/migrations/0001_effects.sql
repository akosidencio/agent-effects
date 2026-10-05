-- Effect records and their audit trail. Part of the storage stability
-- contract: change only through new migrations.
--
-- Times are Unix milliseconds. Ids are lowercase hyphenated UUIDs, whose text
-- order equals their byte order, so `ORDER BY id` is creation order (v7).

CREATE TABLE effects (
    id                 TEXT    PRIMARY KEY NOT NULL,
    effect_name        TEXT    NOT NULL,
    logical_key        TEXT    NOT NULL,
    kind               TEXT    NOT NULL,
    status             TEXT    NOT NULL,
    input              TEXT,             -- JSON
    input_fingerprint  TEXT,
    output             TEXT,             -- JSON
    last_error         TEXT,             -- JSON ErrorRecord
    created_by         TEXT,
    attempt_count      INTEGER NOT NULL,
    next_attempt_at    INTEGER,
    attempt_started_at INTEGER,
    lease_owner        TEXT,
    lease_epoch        INTEGER NOT NULL,
    lease_expires_at   INTEGER,
    version            INTEGER NOT NULL,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL,
    committed_at       INTEGER,
    UNIQUE (effect_name, logical_key)
);

-- Recovery and pending scans filter by status and lease expiry.
CREATE INDEX effects_status_lease ON effects (status, lease_expires_at);

CREATE TABLE effect_events (
    effect_id   TEXT    NOT NULL REFERENCES effects (id),
    sequence    INTEGER NOT NULL,
    transition  TEXT    NOT NULL,
    from_status TEXT    NOT NULL,
    to_status   TEXT    NOT NULL,
    attempt     INTEGER NOT NULL,
    actor       TEXT,
    payload     TEXT,                    -- JSON
    at          INTEGER NOT NULL,
    PRIMARY KEY (effect_id, sequence)
);
