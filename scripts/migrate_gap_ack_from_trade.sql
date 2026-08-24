-- Migrate Trade public.preference_data_gap_ack → Golden Source ops_jobs.data_source_void.
-- Applied 2026-08-23 from bifrost_dev export (6 rows).

CREATE TABLE IF NOT EXISTS ops_jobs.data_source_void (
  data_type        text        PRIMARY KEY,
  is_void          boolean     NOT NULL DEFAULT false,
  acked_gap_count  integer,
  note             text,
  updated_at       timestamptz NOT NULL DEFAULT now()
);

INSERT INTO ops_jobs.data_source_void (data_type, is_void, acked_gap_count, note, updated_at)
VALUES
  ('income_statements', TRUE, 3136, NULL, '2026-05-08 01:36:49.036096+00'),
  ('balance_sheets',    TRUE, 1877, NULL, '2026-05-08 01:36:54.30911+00'),
  ('cash_flows',        TRUE, 2275, NULL, '2026-05-08 01:36:57.087037+00'),
  ('ratios',            TRUE, 1971, NULL, '2026-05-08 01:37:00.402609+00'),
  ('short_interest',    TRUE,  614, NULL, '2026-05-08 02:30:52.043831+00'),
  ('short_volume',      TRUE,  709, NULL, '2026-05-08 02:53:36.522472+00')
ON CONFLICT (data_type) DO UPDATE SET
  is_void = EXCLUDED.is_void,
  acked_gap_count = EXCLUDED.acked_gap_count,
  note = EXCLUDED.note,
  updated_at = EXCLUDED.updated_at;
