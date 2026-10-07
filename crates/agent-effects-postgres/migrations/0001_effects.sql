-- Effect records and their audit trail. Part of the storage stability
-- contract: change only through new migrations.
--
-- Times are stored at millisecond precision. Ids are UUIDv7, so `ORDER BY id`
-- is creation order.

CREATE TABLE effects (
    id                    UUID        PRIMARY KEY,
    effect_name           TEXT        NOT NULL,
    logical_key           TEXT        NOT NULL,
    kind                  TEXT        NOT NULL,
    status                TEXT        NOT NULL,
    input                 JSONB,
    input_fingerprint     TEXT,
    output                JSONB,
    last_error            JSONB,
    created_by            TEXT,
    attempt_count         INTEGER     NOT NULL,
    may_have_applied      BOOLEAN     NOT NULL,
    compensation_attempts INTEGER     NOT NULL,
    approved              BOOLEAN     NOT NULL,
    next_attempt_at       TIMESTAMPTZ,
    attempt_started_at    TIMESTAMPTZ,
    attempt_ended_at      TIMESTAMPTZ,
    lease_owner           TEXT,
    lease_epoch           BIGINT      NOT NULL,
    lease_expires_at      TIMESTAMPTZ,
    version               BIGINT      NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,
    committed_at          TIMESTAMPTZ,
    UNIQUE (effect_name, logical_key)
);

-- Recovery and pending scans filter by status and lease expiry.
CREATE INDEX effects_status_lease ON effects (status, lease_expires_at);
-- Retention prunes settled records by status and age.
CREATE INDEX effects_status_updated ON effects (status, updated_at);

CREATE TABLE effect_events (
    effect_id   UUID        NOT NULL REFERENCES effects (id),
    sequence    BIGINT      NOT NULL,
    transition  TEXT        NOT NULL,
    from_status TEXT        NOT NULL,
    to_status   TEXT        NOT NULL,
    attempt     INTEGER     NOT NULL,
    actor       TEXT,
    payload     JSONB,
    at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (effect_id, sequence)
);
