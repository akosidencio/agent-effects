CREATE TABLE late_observations (
    id UUID PRIMARY KEY,
    effect_id UUID NOT NULL REFERENCES effects(id) ON DELETE CASCADE,
    observation JSONB NOT NULL
);
CREATE INDEX late_observations_effect ON late_observations(effect_id, id);
