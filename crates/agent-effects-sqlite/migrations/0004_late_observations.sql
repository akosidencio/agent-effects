CREATE TABLE late_observations (
    id TEXT PRIMARY KEY NOT NULL,
    effect_id TEXT NOT NULL REFERENCES effects(id) ON DELETE CASCADE,
    observation TEXT NOT NULL
);
CREATE INDEX late_observations_effect ON late_observations(effect_id, id);
