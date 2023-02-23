-- noinspection SqlResolveForFile @ routine/"system_id"

CREATE SCHEMA pgqs;

BEGIN;

CREATE FUNCTION pgqs.create_role_if_not_exists(
  IN in_role_name TEXT)
RETURNS VOID LANGUAGE PLPGSQL AS $body$
BEGIN
  PERFORM TRUE
    FROM pg_catalog.pg_roles
    WHERE rolname = in_role_name;
  IF NOT FOUND THEN
    EXECUTE format('CREATE ROLE %I', in_role_name);
  END IF;
END
$body$;

SELECT pgqs.create_role_if_not_exists('pgqs_queue_worker');
SELECT pgqs.create_role_if_not_exists('pgqs_queue_admin');
SELECT pgqs.create_role_if_not_exists('pgqs_monitor');
SELECT pgqs.create_role_if_not_exists('pgqs_maintenance');
SELECT pgqs.create_role_if_not_exists('pgqs_admin');

GRANT pgqs_queue_worker, pgqs_queue_admin, pgqs_monitor, pgqs_maintenance TO pgqs_admin;
GRANT USAGE ON SCHEMA pgqs TO
  pgqs_queue_worker, pgqs_queue_admin, pgqs_monitor, pgqs_maintenance, pgqs_admin;

ALTER SCHEMA pgqs OWNER TO pgqs_admin;

CREATE FUNCTION pgqs.pgqs_version()
RETURNS TEXT IMMUTABLE
LANGUAGE SQL AS $body$
SELECT '0.1'
$body$;

CREATE TABLE pgqs.system (
  system_id UUID NOT NULL,
  system_created_at TIMESTAMPTZ NOT NULL
);
CREATE UNIQUE INDEX system_singleton_key ON pgqs.system ((true));

CREATE FUNCTION pgqs.system_singleton ()
RETURNS TRIGGER
LANGUAGE PLPGSQL AS $body$
BEGIN
  RAISE EXCEPTION 'system record cannot be deleted'
        USING ERRCODE = 'QS409';
END
$body$;

CREATE TRIGGER system_singleton
BEFORE DELETE ON pgqs.system
FOR EACH ROW EXECUTE PROCEDURE pgqs.system_singleton();

CREATE FUNCTION pgqs.init_system(
  OUT system_id UUID,
  OUT system_created_at TIMESTAMPTZ
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
BEGIN
  system_id := gen_random_uuid();
  system_created_at := CURRENT_TIMESTAMP;

  INSERT INTO pgqs.system (system_id, system_created_at)
    VALUES (gen_random_uuid(), CURRENT_TIMESTAMP);

  EXECUTE format($f$
  CREATE FUNCTION pgqs.system_id()
    RETURNS UUID
    IMMUTABLE
    LEAKPROOF
    PARALLEL SAFE
    LANGUAGE SQL AS $inner$SELECT CAST(%L AS UUID)$inner$
  $f$, system_id);

  RETURN;
END
$body$;

SELECT pgqs.init_system();

CREATE SEQUENCE pgqs.queues_queue_id_seq;

CREATE FUNCTION pgqs.new_queue_id()
RETURNS BIGINT
LANGUAGE SQL AS $body$
  SELECT nextval('pgqs.queues_queue_id_seq');
$body$;


CREATE FUNCTION pgqs.is_valid_queue_name(
  in_queue_name TEXT
) RETURNS BOOLEAN
LANGUAGE SQL AS $body$
  -- AWS SQS queue names have a max length of 80 characters
  SELECT in_queue_name ~ '^[a-zA-Z0-9_-]{1,80}$'
$body$;

CREATE TYPE pgqs.queue_state AS ENUM ('ACTIVE', 'DELETED');

CREATE FUNCTION pgqs.default_queue_maximum_message_size_bytes()
RETURNS INT LANGUAGE SQL AS $body$
  SELECT 262144
$body$;

CREATE FUNCTION pgqs.default_queue_visibility_timeout_seconds()
RETURNS INT LANGUAGE SQL AS $body$
  SELECT 30
$body$;

CREATE FUNCTION pgqs.default_queue_delay_seconds()
RETURNS INT LANGUAGE SQL AS $body$
  SELECT 0
$body$;

CREATE TABLE pgqs.queues (
  queue_id BIGINT PRIMARY KEY, -- internal key -- 8 bytes
  queue_created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- 8 bytes
  queue_updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- 8 bytes
  queue_state pgqs.queue_state NOT NULL DEFAULT 'ACTIVE',
  queue_delay_seconds INT NOT NULL
    DEFAULT pgqs.default_queue_delay_seconds()
    CHECK (queue_delay_seconds >= 0), -- mutable -- 4 bytes
  queue_visibility_timeout_seconds INT NOT NULL
    DEFAULT pgqs.default_queue_visibility_timeout_seconds()
    CHECK (queue_visibility_timeout_seconds >= 0), -- mutable -- 4 bytes
  queue_maximum_message_size_bytes INT NOT NULL
    DEFAULT pgqs.default_queue_maximum_message_size_bytes()
    CHECK (1024 <= queue_maximum_message_size_bytes
           AND queue_maximum_message_size_bytes <= 262144),
  queue_message_table_count SMALLINT NOT NULL DEFAULT 3 CHECK (queue_message_table_count >= 2), -- must be positive? > 2? -- 2 bytes
  queue_active_message_table_idx SMALLINT NOT NULL DEFAULT 1, -- mutable must be between 1 and queue_message_table_count -- 2 bytes
  queue_rotation_period_seconds INT NOT NULL DEFAULT 7200 CHECK (queue_rotation_period_seconds > 0), -- mutable, must be > 0s? -- 4 bytes
  queue_last_rotated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- mutable -- 8 bytes
  queue_name TEXT NOT NULL UNIQUE,  -- immutable -- 84 bytes varlena
  queue_parent_message_table_name TEXT NOT NULL UNIQUE, -- immutable  -- > 14 bytes
  queue_message_sequence_name TEXT NOT NULL UNIQUE, -- immutable  -- >
  queue_tags jsonb NOT NULL DEFAULT '{}'
);

ALTER TABLE pgqs.queues ADD CONSTRAINT queue_name_check CHECK (pgqs.is_valid_queue_name(queue_name));

CREATE FUNCTION pgqs.encode_queue_url(
  IN in_system_id UUID,
  IN in_queue_name TEXT,
  OUT queue_url TEXT
) RETURNS TEXT
LANGUAGE SQL AS $body$
  SELECT 'pgqs://pgqs.system:' || in_system_id || '/' || in_queue_name;
$body$;

CREATE FUNCTION pgqs.encode_queue_url(
  IN in_queue_name TEXT,
  OUT queue_url TEXT
) RETURNS TEXT
LANGUAGE SQL AS $body$
  SELECT pgqs.encode_queue_url(pgqs.system_id(), in_queue_name);
$body$;

CREATE FUNCTION pgqs.decode_queue_url(
  IN in_queue_url TEXT,
  OUT system_id UUID,
  OUT queue_name TEXT
) RETURNS RECORD
LANGUAGE SQL AS $body$
-- pgqs://pgqs.system:27697529-42e7-400a-adcd-20fe50d173d2/some-queue-name
-- 12345678911234567892123456789312345678941234567895123456
  SELECT CAST(substring(in_queue_url FROM 20 FOR 36) AS UUID),
         substring(in_queue_url FROM 57)
$body$;

CREATE FUNCTION pgqs.checked_decode_queue_url(
  IN in_queue_url TEXT,
  OUT system_id UUID,
  OUT queue_name TEXT
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  _decoded RECORD;
BEGIN
  _decoded := pgqs.decode_queue_url(in_queue_url);
  system_id := _decoded.system_id;
  queue_name := _decoded.queue_name;

  IF system_id IS NULL OR system_id <> pgqs.system_id() THEN
    RAISE 'Invalid QueueUrl'
          USING DETAIL = format('QueueUrl %L', in_queue_url);
  END IF;

  IF queue_name IS NULL OR NOT pgqs.is_valid_queue_name(queue_name) THEN
    RAISE 'Invalid QueueUrl'
          USING DETAIL = format('QueueUrl %L', in_queue_url);
  END IF;

  RETURN;

END
$body$;


CREATE FUNCTION
pgqs.queue(
  IN in_queue_name TEXT,
  OUT queue_id BIGINT,
  OUT queue_url TEXT,
  OUT queue_state pgqs.queue_state,
  OUT queue_delay_seconds INT,
  OUT queue_maximum_message_size_bytes INT,
  OUT queue_visibility_timeout_seconds INT,
  OUT queue_message_table_count SMALLINT,
  OUT queue_active_message_table_idx SMALLINT,
  OUT queue_active_message_table_name TEXT,
  OUT queue_rotation_period_seconds INT,
  OUT queue_last_rotated_at TIMESTAMPTZ,
  OUT queue_parent_message_table_name TEXT,
  OUT queue_message_sequence_name TEXT,
  OUT queue_next_rotation_due_at TIMESTAMPTZ,
  OUT queue_created_at TIMESTAMPTZ,
  OUT queue_updated_at TIMESTAMPTZ
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue RECORD;
BEGIN
  SELECT INTO _queue
           *
    FROM pgqs.queues AS q
    WHERE q.queue_name = in_queue_name;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'NonExistentQueue'
          USING ERRCODE = 'QS434',
                DETAIL = format('queue name %L', in_queue_name);
  END IF;

  queue_id := _queue.queue_id;
  queue_url := pgqs.encode_queue_url(pgqs.system_id(), _queue.queue_name);

  queue_active_message_table_idx := _queue.queue_active_message_table_idx;
  queue_active_message_table_name
    := _queue.queue_parent_message_table_name || '_' || _queue.queue_active_message_table_idx;
  queue_last_rotated_at := _queue.queue_last_rotated_at;
  queue_rotation_period_seconds := _queue.queue_rotation_period_seconds;
  queue_next_rotation_due_at := _queue.queue_last_rotated_at + _queue.queue_rotation_period_seconds * INTERVAL '1s';
  queue_message_table_count := _queue.queue_message_table_count;

  queue_visibility_timeout_seconds := _queue.queue_visibility_timeout_seconds;
  queue_delay_seconds := _queue.queue_delay_seconds;
  queue_maximum_message_size_bytes := _queue.queue_maximum_message_size_bytes;
  queue_state := _queue.queue_state;
  queue_message_sequence_name := _queue.queue_message_sequence_name;
  queue_parent_message_table_name := _queue.queue_parent_message_table_name;
  queue_created_at := _queue.queue_created_at;
  queue_updated_at := _queue.queue_updated_at;

  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.queue_by_queue_url(
  IN in_queue_url TEXT,
  OUT queue_id BIGINT,
  OUT queue_url TEXT,
  OUT queue_delay_seconds INT,
  OUT queue_state pgqs.queue_state,
  OUT queue_visibility_timeout_seconds INT,
  OUT queue_message_table_count SMALLINT,
  OUT queue_active_message_table_idx SMALLINT,
  OUT queue_active_message_table_name TEXT,
  OUT queue_rotation_period_seconds INT,
  OUT queue_last_rotated_at TIMESTAMPTZ,
  OUT queue_parent_message_table_name TEXT,
  OUT queue_message_sequence_name TEXT,
  OUT queue_next_rotation_due_at TIMESTAMPTZ
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue RECORD;
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
BEGIN
  SELECT INTO STRICT _queue
         *
    FROM pgqs.queue(_queue_name);
  RETURN;
END
$body$;


CREATE TYPE pgqs.message_table_state AS ENUM ('ACTIVE', 'DRAINING', 'PENDING_DRAINING', 'PENDING');

CREATE TABLE pgqs.message_tables (
  queue_id BIGINT NOT NULL
    REFERENCES pgqs.queues (queue_id),
  message_table_idx SMALLINT NOT NULL,
  message_table_name TEXT NOT NULL,
  message_table_state pgqs.message_table_state NOT NULL,
  PRIMARY KEY (queue_id, message_table_idx),
  UNIQUE (queue_id, message_table_name)
);


CREATE FUNCTION
pgqs.queue_next_message_table_idx(
  in_queue_message_table_count SMALLINT,
  in_queue_message_table_idx SMALLINT,
  OUT queue_next_message_table_idx SMALLINT
) RETURNS SMALLINT
IMMUTABLE
LEAKPROOF
PARALLEL SAFE
LANGUAGE SQL AS $body$
 SELECT CASE WHEN in_queue_message_table_idx = in_queue_message_table_count
             THEN 1
             ELSE in_queue_message_table_idx + 1
        END;
$body$;

CREATE FUNCTION
pgqs.queue_message_table_state_constraint()
RETURNS TRIGGER
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue RECORD;
  _expected_pending_message_table_idx SMALLINT;
  _message_table RECORD;
BEGIN
  CASE NEW.message_table_state
    WHEN 'ACTIVE' THEN
      SELECT INTO _queue *
        FROM pgqs.queues
        WHERE queue_id = NEW.queue_id;
      IF NEW.message_table_idx <> _queue.queue_active_message_table_idx  THEN
        RAISE EXCEPTION 'cannot set message_table ACTIVE when idx not active in queues'
              USING ERRCODE = 'QS500',
                    DETAIL = format('message_tables (queue_id, message_table_idx, message_table_state) = (%L, %L, %L); '
                                    || 'queues (queue_id, queue_active_message_table_idx) = (%L, %L)',
                                    NEW.queue_id, NEW.message_table_idx, NEW.message_table_state,
                                    _queue.queue_id, _queue.queue_active_message_table_idx);
      END IF;
    WHEN 'PENDING', 'PENDING_DRAINING' THEN
      SELECT INTO _queue *
        FROM pgqs.queues
        WHERE queue_id = NEW.queue_id;
      _expected_pending_message_table_idx
        := pgqs.queue_next_message_table_idx(_queue.queue_message_table_count,
                                             _queue.queue_active_message_table_idx);
      IF NEW.message_table_idx <> _expected_pending_message_table_idx THEN
        RAISE EXCEPTION 'cannot set message_table_idx to PENDING/PENDING_DRAINING: does not follow ACTIVE message_table_idx'
              USING ERRCODE = 'QS510',
                    DETAIL = format('queues (queue_id, queue_message_table_count, queue_active_message_table_idx) = (%L, %L, %L); '
                                    || 'message_tables (queue_id, message_table_idx, message_table_state) = (%L, %L, %L); '
                                    || 'expected (message_table_idx, message_table_state)',
                                     _queue.queue_id, _queue.queue_message_table_count, _queue.queue_active_message_table_idx,
                                     NEW.queue_id, NEW.message_table_idx, NEW.message_table_state,
                                     _expected_pending_message_table_idx, NEW.message_table_state);
      END IF;

      SELECT INTO _message_table
             mt.message_table_idx, mt.message_table_state
        FROM pgqs.message_tables AS mt
        WHERE mt.queue_id = NEW.queue_id
              AND mt.message_table_state IN ('PENDING', 'PENDING_DRAINING')
              AND mt.message_table_idx <> NEW.message_table_idx;
      IF FOUND THEN
        RAISE EXCEPTION 'cannot set multiple tables to PENDING/PENDING_DRAINING states'
              USING ERRCODE = 'QS520',
                    DETAIL = format('message_tables (queue_id, message_table_idx, message_table_state) rows conflict; ' ||
                                    'new row (%L, %L, %L) conflicts with existing (%L, %L, %L)',
                                    NEW.queue_id, NEW.message_table_idx, NEW.message_table_state,
                                    NEW.queue_id, _message_table.message_table_idx, _message_table.message_table_state);
      END IF;
    ELSE
      NULL;
  END CASE;

  RETURN NEW;
END
$body$;


CREATE TRIGGER queue_message_table_state_constraint
AFTER INSERT OR UPDATE ON pgqs.message_tables
FOR EACH ROW EXECUTE PROCEDURE pgqs.queue_message_table_state_constraint();

CREATE UNIQUE INDEX message_tables_queue_active_key ON pgqs.message_tables (queue_id) WHERE message_table_state = 'ACTIVE';

CREATE FUNCTION pgqs.message_tables(
  IN in_queue_id BIGINT,
  OUT message_table_idx SMALLINT,
  OUT message_table_name TEXT,
  OUT message_table_state pgqs.message_table_state
) RETURNS SETOF RECORD
LANGUAGE SQL AS $body$
  SELECT mt.message_table_idx, mt.message_table_name, mt.message_table_state
    FROM pgqs.message_tables AS mt
    JOIN pgqs.queues AS q USING (queue_id)
    WHERE mt.queue_id = in_queue_id;
$body$;


CREATE FUNCTION pgqs.visible_message_tables(
  IN in_queue_id BIGINT,
  OUT message_table_idx SMALLINT,
  OUT message_table_name TEXT,
  OUT message_table_state pgqs.message_table_state
) RETURNS SETOF RECORD
LANGUAGE SQL AS $body$
  SELECT mt.message_table_idx, mt.message_table_name, mt.message_table_state
    FROM pgqs.message_tables AS mt
    WHERE mt.queue_id = in_queue_id
          AND mt.message_table_state IN ('ACTIVE', 'DRAINING', 'PENDING_DRAINING')
    ORDER BY mt.message_table_state DESC,
             mt.message_table_idx;
$body$;


-- noinspection SpellCheckingInspection
CREATE TABLE pgqs.messages_template (
  queue_id BIGINT NOT NULL
    REFERENCES pgqs.queues (queue_id),
  message_id uuid PRIMARY KEY NOT NULL, -- 100 chars long max (looks like uuid?)
  message_sequence_number BIGINT NOT NULL,
  message_sent_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

  message_delay_seconds INT NOT NULL DEFAULT 0 CHECK (message_delay_seconds >= 0), -- mutable
  message_visible_at TIMESTAMPTZ NOT NULL,
  -- initially message_sent_at + delay_seconds -- mutable,
  -- subsequently message_last_received_at + visibility_timeout

  message_receive_count INT NOT NULL DEFAULT 0 CHECK (message_receive_count >= 0), -- natural number, mutable
  message_first_received_at TIMESTAMPTZ
    CHECK (message_first_received_at IS NOT NULL OR message_receive_count = 0),
  message_last_received_at TIMESTAMPTZ -- mutable
    CHECK (message_first_received_at IS NOT NULL OR message_receive_count = 0),
  message_receipt_token uuid NOT NULL DEFAULT gen_random_uuid(), -- mutable
  -- example of SQS receipt handle (172 chars long, looks like base64 encoded?)
  -- MbZj6wDWli+JvwwJaBV+3dcjk2YW2vA3+STFFljTM8tJJg6HRG6PYSasuWXPJB+CwLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ+QEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0=

  message_body_md5u UUID NOT NULL,  -- immutable
  message_attributes_md5u UUID NOT NULL,  -- immutable
  message_system_attributes_md5u UUID NOT NULL,  -- immutable

  -- Important
  -- A message can include only XML, JSON, and unformatted text. The following Unicode characters are allowed:
  -- #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF
-- In any case, the longest possible character string that can be stored is about 1 GB. (PostgreSQL TEXT type)

  message_body TEXT NOT NULL, -- max 256 KiB (SQS) -- immutable
  message_attributes jsonb NOT NULL DEFAULT '{}'::jsonb, -- SQS message can have up to 10 attributes -- immutable
  message_system_attributes jsonb NOT NULL DEFAULT '{}'::jsonb -- for otel -- immutable
  --
);


CREATE FUNCTION
pgqs.generate_message_tables(
  IN in_queue_id BIGINT,
  IN in_queue_message_table_count SMALLINT,
  OUT message_table_idx SMALLINT,
  OUT message_table_name TEXT
) RETURNS SETOF RECORD
LANGUAGE SQL AS $body$
  SELECT s.idx AS message_table_idx,
         'messages_' || in_queue_id || '_' || s.idx
    FROM generate_series(1, in_queue_message_table_count) AS s (idx);
$body$;


CREATE TABLE pgqs.queue_stats (
  queue_id BIGINT PRIMARY KEY
    REFERENCES pgqs.queues (queue_id),
  queue_stats_updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  message_count BIGINT NOT NULL DEFAULT 0,
  stored_message_count BIGINT NOT NULL DEFAULT 0,
  delayed_message_count BIGINT NOT NULL DEFAULT 0,
  in_flight_message_count BIGINT NOT NULL DEFAULT 0,
  invisible_message_count BIGINT NOT NULL DEFAULT 0
);

CREATE FUNCTION pgqs.update_queue_stats(
  IN in_queue_name TEXT,
  OUT queue_stats_updated_at TIMESTAMPTZ,
  OUT message_count BIGINT,
  OUT stored_message_count BIGINT,
  OUT delayed_message_count BIGINT,
  OUT in_flight_message_count BIGINT,
  OUT invisible_message_count BIGINT
) RETURNS RECORD
LANGUAGE PLPGSQL AS
$body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_stats_table_name CONSTANT TEXT := 'queue_stats';
  _queue RECORD;
  _as_of TIMESTAMPTZ := CURRENT_TIMESTAMP;
  k_partition_table_sql_fmt CONSTANT TEXT := 'SELECT message_sent_at, message_visible_at, message_receive_count FROM %I.%I';
  _partition_table_sql TEXT;
  _partition_table_sqls TEXT[];
  _union_table_sql TEXT;
  k_stats_sql_fmt CONSTANT TEXT
    := $fmt$
SELECT COUNT(*) AS message_count,
       COUNT(*) FILTER (WHERE message_receive_count = 0) AS stored_message_count,
       COUNT(*) FILTER (WHERE message_receive_count = 0 AND message_visible_at > $1) AS delayed_message_count,
       COUNT(*) FILTER (WHERE message_receive_count > 0) AS in_flight_message_count,
       COUNT(*) FILTER (WHERE message_receive_count > 0 AND message_visible_at <= $1) AS invisible_message_count
  FROM (%s) AS _
$fmt$;
  _stats_sql TEXT;
  k_update_sql_fmt CONSTANT TEXT
    := $fmt$
UPDATE %I.%I AS stats
  SET (queue_stats_updated_at, message_count,
       stored_message_count, delayed_message_count,
       in_flight_message_count, invisible_message_count)
        = ($1, _.message_count,
           _.stored_message_count, _.delayed_message_count,
           _.in_flight_message_count, _.invisible_message_count)
  FROM (%s) AS _
  WHERE stats.queue_id = $2
  RETURNING stats.*
$fmt$;
  _update_sql TEXT;
  _res RECORD;
BEGIN
  _queue := pgqs.queue(in_queue_name);

  FOR _res IN
     SELECT _.message_table_name
       FROM pgqs.visible_message_tables(_queue.queue_id) AS _
  LOOP
    _partition_table_sql := format(k_partition_table_sql_fmt, k_schema_name, _res.message_table_name);
    _partition_table_sqls := array_append(_partition_table_sqls, _partition_table_sql);
  END LOOP;

  _union_table_sql := array_to_string(_partition_table_sqls, ' UNION ALL ');
  _stats_sql := format(k_stats_sql_fmt, _union_table_sql);
  _update_sql := format(k_update_sql_fmt, k_schema_name, k_stats_table_name, _stats_sql);

  EXECUTE _update_sql INTO _res USING _as_of, _queue.queue_id;

  queue_stats_updated_at := _res.queue_stats_updated_at;
  message_count := _res.message_count;
  stored_message_count := _res.stored_message_count;
  delayed_message_count := _res.delayed_message_count;
  in_flight_message_count := _res.in_flight_message_count;
  invisible_message_count := _res.invisible_message_count;

  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.cached_queue_stats(
  IN in_queue_name TEXT,
  OUT queue_stats_updated_at TIMESTAMPTZ,
  OUT message_count BIGINT,
  OUT stored_message_count BIGINT,
  OUT delayed_message_count BIGINT,
  OUT in_flight_message_count BIGINT,
  OUT invisible_message_count BIGINT
) RETURNS RECORD
LANGUAGE SQL AS $body$
  SELECT queue_stats.queue_stats_updated_at,
         queue_stats.message_count,
         queue_stats.stored_message_count,
         queue_stats.delayed_message_count,
         queue_stats.in_flight_message_count,
         queue_stats.invisible_message_count
   FROM pgqs.queue_stats
   JOIN pgqs.queues USING (queue_id)
   WHERE queue_name = in_queue_name
$body$;


CREATE FUNCTION
pgqs.queue_stats_now (
  IN in_queue_name TEXT,
  OUT queue_depth BIGINT,
  OUT visible_queue_depth BIGINT,
  OUT delayed_queue_depth BIGINT,
  OUT last_1m_sent_count BIGINT,
  OUT last_5m_sent_count BIGINT,
  OUT earliest_sent_at TIMESTAMPTZ,
  OUT latest_sent_at TIMESTAMPTZ
 ) RETURNS SETOF RECORD
LANGUAGE PLPGSQL AS
$body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  _queue RECORD;
  _as_of TIMESTAMPTZ := CURRENT_TIMESTAMP;
  k_partition_table_sql_fmt CONSTANT TEXT := 'SELECT message_sent_at, message_visible_at, message_receive_count FROM %I.%I';
  _partition_table_sql TEXT;
  _partition_table_sqls TEXT[];
  _union_table_sql TEXT;
  k_stats_sql_fmt CONSTANT TEXT
    := $fmt$
SELECT COUNT(*) AS queue_depth,
       COUNT(*) FILTER (WHERE message_visible_at <= $1) AS visible_queue_depth,
       COUNT(*) FILTER (WHERE message_visible_at > $1 AND message_receive_count = 0) AS delayed_queue_depth,
       COUNT(*) FILTER (WHERE message_sent_at >= $1 - INTERVAL '1m') AS last_1m_sent_count,
       COUNT(*) FILTER (WHERE message_sent_at >= $1 - INTERVAL '5m') AS last_5m_sent_count,
       min(message_sent_at) AS earliest_sent_at,
       max(message_sent_at) AS latest_sent_at
  FROM (%s) AS _
$fmt$;
  _stats_sql TEXT;
  _res RECORD;
BEGIN
  _queue := pgqs.queue(in_queue_name);

  FOR _res IN
     SELECT _.message_table_name
       FROM pgqs.generate_message_tables(_queue.queue_id, _queue.queue_message_table_count) AS _
  LOOP
    _partition_table_sql := format(k_partition_table_sql_fmt, k_schema_name, _res.message_table_name);
    _partition_table_sqls := array_append(_partition_table_sqls, _partition_table_sql);

  END LOOP;
  _union_table_sql := array_to_string(_partition_table_sqls, ' UNION ALL ');
  _stats_sql := format(k_stats_sql_fmt, _union_table_sql);

  RETURN QUERY EXECUTE _stats_sql USING _as_of;

  RETURN;
END
$body$;


CREATE FUNCTION
pgqs.queue_table_stats (
  IN in_queue_name TEXT,
  OUT messages_table_idx INT,
  OUT messages_table_name TEXT,
  OUT table_state TEXT,
  OUT queue_depth BIGINT,
  OUT visible_queue_depth BIGINT,
  OUT earliest_sent_at TIMESTAMPTZ,
  OUT latest_sent_at TIMESTAMPTZ,
  OUT last_1m_sent_count BIGINT,
  OUT last_5m_sent_count BIGINT
 ) RETURNS SETOF RECORD
LANGUAGE PLPGSQL AS
$body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  _queue RECORD;
  _as_of TIMESTAMPTZ := CURRENT_TIMESTAMP;
  k_stats_sql_fmt CONSTANT TEXT
    := $fmt$
SELECT COUNT(*) AS queue_depth,
       COUNT(*) FILTER (WHERE message_visible_at <= $1) AS visible_queue_depth,
       COUNT(*) FILTER (WHERE message_sent_at >= $1 - INTERVAL '1m') AS last_1m_sent_count,
       COUNT(*) FILTER (WHERE message_sent_at >= $1 - INTERVAL '5m') AS last_5m_sent_count,
       min(message_sent_at) AS earliest_sent_at,
       max(message_sent_at) AS latest_sent_at
  FROM %I.%I
$fmt$;
  _stats_sql TEXT;
  _message_table RECORD;
  _stats RECORD;
BEGIN
  _queue := pgqs.queue(in_queue_name);

  FOR _message_table IN
     SELECT _.message_table_idx, _.message_table_name, _.message_table_state
       FROM pgqs.message_tables(_queue.queue_id) AS _
  LOOP
    _stats_sql := format(k_stats_sql_fmt, k_schema_name, _message_table.message_table_name);
    EXECUTE _stats_sql INTO _stats USING _as_of;
      messages_table_name := _message_table.message_table_name;
      messages_table_idx := _message_table.message_table_idx;
      table_state := _message_table.message_table_state::TEXT;
      queue_depth := _stats.queue_depth;
      visible_queue_depth := _stats.visible_queue_depth;
      earliest_sent_at := _stats.earliest_sent_at;
      latest_sent_at := _stats.latest_sent_at;
      last_1m_sent_count := _stats.last_1m_sent_count;
      last_5m_sent_count := _stats.last_5m_sent_count;
    RETURN NEXT;
  END LOOP;

  RETURN;
END
$body$;



CREATE FUNCTION
pgqs.maybe_prep_message_table_rotation(
  IN in_queue_id BIGINT,
  IN in_message_table_idx SMALLINT,
  IN in_message_table_name TEXT,
  OUT message_table_state pgqs.message_table_state
)
RETURNS pgqs.message_table_state
LANGUAGE PLPGSQL AS $body$
/*
  NOTE: We assume that message_table_idx and message_table_name are consistent for the given queue
 */
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_pending_draining CONSTANT pgqs.message_table_state := 'PENDING_DRAINING';
  k_pending CONSTANT pgqs.message_table_state := 'PENDING';
  k_select_messages_sql_fmt CONSTANT TEXT := 'SELECT TRUE FROM %I.%I LIMIT 1';
  _select_messages_sql TEXT;
  _message_table RECORD;
  _row_count INT;
BEGIN
  _select_messages_sql := format(k_select_messages_sql_fmt,
                                 k_schema_name, in_message_table_name);
  EXECUTE _select_messages_sql;
  GET DIAGNOSTICS _row_count = ROW_COUNT;
  IF _row_count > 0 THEN
    RAISE NOTICE '%.% is not empty. Skipping rotation.',
                  quote_ident(k_schema_name), quote_ident(in_message_table_name);
    message_table_state := k_pending_draining;
    RETURN;
  END IF;

  UPDATE pgqs.message_tables AS mt
    SET message_table_state = k_pending
    WHERE (mt.queue_id, mt.message_table_idx, mt.message_table_state)
            = (in_queue_id, in_message_table_idx, k_pending_draining);

  IF NOT FOUND THEN
    SELECT INTO _message_table
           mt.message_table_state
      FROM pgqs.message_tables mt
      WHERE (queue_id, message_table_idx) = (in_queue_id, in_message_table_idx);
    IF NOT FOUND THEN
      RAISE EXCEPTION 'no message table with idx found for queue'
            USING ERRCODE = 'QS540',
                  DETAIL = format('message_tables (queue_id, message_table_idx) = (%L, %L)',
                                  in_queue_id, in_message_table_idx);
    ELSE
      RAISE EXCEPTION 'message table in unexpected state'
            USING ERRCODE = 'QS550',
            DETAIL = format('message_tables (queue_id, message_table_idx, message_table_state) = (%L, %L, %L)',
                            in_queue_id, in_message_table_idx, _message_table.message_table_state);
    END IF;
  END IF;

  message_table_state := k_pending;
  RETURN;

END
$body$;

CREATE FUNCTION
pgqs.pending_message_table(
  IN in_queue_id BIGINT,
  OUT message_table_idx SMALLINT,
  OUT message_table_name TEXT,
  OUT message_table_state pgqs.message_table_state
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  _message_table RECORD;
BEGIN

  SELECT INTO STRICT _message_table
         mt.message_table_idx,
         mt.message_table_name,
         mt.message_table_state
    FROM pgqs.message_tables AS mt
    WHERE mt.queue_id = in_queue_id
          AND mt.message_table_state IN ('PENDING', 'PENDING_DRAINING');
  message_table_idx := _message_table.message_table_idx;
  message_table_name := _message_table.message_table_name;
  message_table_state := _message_table.message_table_state;

  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.maybe_rotate_message_tables(
  IN in_queue_id BIGINT,
  OUT message_table_state pgqs.message_table_state
)
RETURNS pgqs.message_table_state
LANGUAGE PLPGSQL
AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_active CONSTANT pgqs.message_table_state := 'ACTIVE';
  k_draining CONSTANT pgqs.message_table_state := 'DRAINING';
  k_pending_draining CONSTANT pgqs.message_table_state := 'PENDING_DRAINING';
  k_pending CONSTANT pgqs.message_table_state := 'PENDING';
  _pending_message_table RECORD;
  _next_pending_message_table_idx SMALLINT;
  _queue_message_table_count SMALLINT;
BEGIN

  _pending_message_table := pgqs.pending_message_table(in_queue_id);
  message_table_state := _pending_message_table.message_table_state;

  IF _pending_message_table.message_table_state = k_pending_draining THEN
    RETURN;
  ELSE -- message_table_state = PENDING
    -- there may be long lock on the table from pg_dump,
    -- detect it and skip rotate then
    BEGIN
        EXECUTE format('LOCK TABLE %I.%I NOWAIT',
                       k_schema_name, _pending_message_table.message_table_name);
        EXECUTE format('TRUNCATE %I.%I',
                       k_schema_name, _pending_message_table.message_table_name);
    EXCEPTION WHEN lock_not_available THEN
        -- cannot truncate, skipping rotate
      RAISE NOTICE '%.% is locked (maybe pg_dump is running). Skipping rotation.',
                    quote_ident(k_schema_name),
                    quote_ident(_pending_message_table.message_table_name);
      RETURN;
    END;

    UPDATE pgqs.queues
      SET queue_active_message_table_idx = _pending_message_table.message_table_idx,
          queue_last_rotated_at = CURRENT_TIMESTAMP
      WHERE queue_id = in_queue_id
      RETURNING queue_message_table_count INTO _queue_message_table_count;

    _next_pending_message_table_idx
      := pgqs.queue_next_message_table_idx(_queue_message_table_count,
                                           _pending_message_table.message_table_idx);


    UPDATE pgqs.message_tables AS mt
      SET message_table_state = CASE WHEN mt.message_table_state = k_active THEN k_draining
                                     WHEN mt.message_table_state = k_pending THEN k_active
                                     ELSE k_pending_draining
                                 END
      WHERE mt.queue_id = in_queue_id
            AND (mt.message_table_state IN ('ACTIVE', 'PENDING')
                 OR mt.message_table_idx = _next_pending_message_table_idx);
    message_table_state := k_pending_draining;
    RETURN;
  END IF;
END
$body$;

CREATE FUNCTION
pgqs.maybe_prep_or_rotate_message_tables(
  IN in_queue_id BIGINT,
  OUT message_table_state pgqs.message_table_state
)
RETURNS pgqs.message_table_state
LANGUAGE PLPGSQL
AS $body$
DECLARE
  k_pending_draining CONSTANT pgqs.message_table_state := 'PENDING_DRAINING';
  _pending_message_table RECORD;
BEGIN

  _pending_message_table := pgqs.pending_message_table(in_queue_id);
  message_table_state := _pending_message_table.message_table_state;

  IF _pending_message_table.message_table_state = k_pending_draining THEN
    message_table_state
      := pgqs.maybe_prep_message_table_rotation(in_queue_id,
                                                _pending_message_table.message_table_idx,
                                                _pending_message_table.message_table_name);
    -- maybe_prep_message_table_rotation will return k_pending if prepped
  ELSE -- message_table_state = PENDING
    message_table_state := pgqs.maybe_rotate_message_tables(in_queue_id);
  END IF;

  RETURN;
END
$body$;


CREATE FUNCTION
pgqs.maybe_rotate_message_tables(
  IN in_queue_name TEXT,
  IN as_of TIMESTAMPTZ,
  OUT message_table_state pgqs.message_table_state
)
RETURNS pgqs.message_table_state
LANGUAGE PLPGSQL
AS $body$
DECLARE
  _queue RECORD;
BEGIN
  _queue := pgqs.queue(in_queue_name);

  message_table_state := pgqs.maybe_prep_message_table_rotation(_queue.queue_id);

  -- check if we're due to rotate

  -- noinspection SqlResolve
  IF _queue.last_rotated_at + _queue.rotation_period > as_of THEN
    RAISE NOTICE 'Queue % is not due for rotation.',
                  quote_literal(in_queue_name);
    RETURN;
  END IF;

  message_table_state := pgqs.maybe_rotate_message_tables(_queue.queue_id);
  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.maybe_rotate_message_tables(
  IN in_queue_name TEXT,
  OUT message_table_state pgqs.message_table_state
)
RETURNS pgqs.message_table_state
LANGUAGE SQL AS $body$
  SELECT pgqs.maybe_rotate_message_tables(in_queue_name, CURRENT_TIMESTAMP);
$body$;


CREATE FUNCTION
pgqs.maybe_rotate_message_tables_now(
  IN in_queue_name TEXT,
  OUT message_table_state pgqs.message_table_state
)
RETURNS pgqs.message_table_state
LANGUAGE PLPGSQL
AS $body$
DECLARE
  _queue RECORD;
BEGIN
  _queue := pgqs.queue(in_queue_name);

  message_table_state := pgqs.maybe_prep_or_rotate_message_tables(_queue.queue_id);
  RETURN;
END
$body$;

CREATE FUNCTION pgqs.maintenance_operations(
  IN in_as_of TIMESTAMPTZ,
  OUT op TEXT,
  OUT request jsonb
) RETURNS SETOF RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  _op TEXT;
BEGIN

  _op := 'CleanUpDeletedQueue';
  RETURN QUERY
    SELECT _op, jsonb_build_object('QueueName', q.queue_name)
      FROM pgqs.queues AS q
      WHERE q.queue_state = 'DELETED'
      ORDER BY q.queue_name;

  _op := 'RotateMessageTables';
  RETURN QUERY
    SELECT _op, jsonb_build_object('QueueName', q.queue_name)
      FROM pgqs.queues AS q
      WHERE q.queue_state <> 'DELETED'
            AND (q.queue_last_rotated_at + q.queue_rotation_period_seconds * INTERVAL '1s') < in_as_of
      ORDER BY in_as_of - (q.queue_last_rotated_at + q.queue_rotation_period_seconds * INTERVAL '1s') DESC,
               q.queue_name;
END
$body$;


CREATE FUNCTION pgqs.maint_get_maintenance_operations(
  IN in_request jsonb, -- not used, but there for consistency with API
  OUT response jsonb
)
RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _operations jsonb;
BEGIN
  SELECT INTO _operations
         array_to_json(array_agg(jsonb_build_object('op', op, 'request', request)))
    FROM pgqs.maintenance_operations(CURRENT_TIMESTAMP) AS _ (op, request);
  response := jsonb_build_object('Operations', _operations);
  RAISE NOTICE '%', jsonb_pretty(response);
END
$body$;

CREATE FUNCTION
pgqs.maint_request_queue_name(
  IN in_request jsonb,
  OUT queue_name TEXT
) RETURNS TEXT
LANGUAGE SQL AS $body$
  SELECT in_request->>'QueueName';
$body$;

CREATE FUNCTION
pgqs.maint_rotate_message_tables(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.maint_request_queue_name(in_request);
BEGIN
  PERFORM pgqs.maybe_rotate_message_tables(_queue_name);
  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.maint_clean_up_deleted_queue(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.maint_request_queue_name(in_request);
BEGIN
  RAISE NOTICE '% queue_name %', in_request, _queue_name;
  PERFORM pgqs.delete_queue(_queue_name);
  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.invoke_maintenance(
  IN in_op TEXT,
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL
SET search_path TO 'pgqs'
AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_op_sql_fmt CONSTANT TEXT := 'SELECT %I.%I($1)';
  _op_func_name TEXT;
  _op_sql TEXT;
BEGIN
   _op_func_name := 'maint_' || lower(regexp_replace(in_op, '(?<!^)([A-Z])', '_\1', 'g'));
   _op_sql := format(k_op_sql_fmt, k_schema_name, _op_func_name);
   BEGIN
     EXECUTE _op_sql INTO response USING in_request;
     EXCEPTION WHEN SQLSTATE '42883' THEN
       IF SQLERRM LIKE format('function %s.%s(json%%) does not exist', k_schema_name, _op_func_name) THEN
         RAISE EXCEPTION 'Unknown operation'
               USING ERRCODE = 'QS400',
               DETAIL = format('operation %L is unknown', in_op);
       ELSE
         RAISE;
       END IF;
   END;
   RETURN;
END
$body$;

CREATE FUNCTION pgqs.attributes_md5(
  IN in_attrs jsonb,
  OUT attributes_md5 TEXT
) RETURNS TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_string_field_idx CONSTANT bytea := '\x01';
  k_binary_field_idx CONSTANT bytea := '\x02';
  _field_idx bytea;
  _attr_name TEXT;
  _attr_val jsonb;
  _attr_bytes bytea;
  _all_bytes bytea;
  _value_bytes bytea;
BEGIN
  _all_bytes := '';
  FOR _attr_name, _attr_val IN
    SELECT k, v
      FROM jsonb_each(in_attrs) AS _ (k, v)
      ORDER BY k
  LOOP
    IF _attr_val ? 'StringValue' THEN
      _field_idx := k_string_field_idx;
      _value_bytes := convert_to(_attr_val->>'StringValue', 'UTF8');
    ELSIF _attr_val ? 'BinaryValue' THEN
      _field_idx := k_binary_field_idx;
      _value_bytes := decode(_attr_val->>'BinaryValue', 'base64');
    ELSE
      RAISE 'Unsupported Attribute value type % %', _attr_name, _attr_val;
    END IF;
    _attr_bytes := int4send(octet_length(_attr_name)) || convert_to(_attr_name, 'UTF8');
    _attr_bytes := _attr_bytes || int4send(octet_length(_attr_val->>'DataType'))
                               || convert_to(_attr_val->>'DataType', 'UTF8');
    _attr_bytes := _attr_bytes || _field_idx;
    _attr_bytes := _attr_bytes || int4send(octet_length(_value_bytes)) || _value_bytes;

    _all_bytes := _all_bytes || _attr_bytes;
  END LOOP;

  attributes_md5 := md5(_all_bytes);
  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.attributes_md5u(
  IN in_attributes jsonb,
  OUT attributes_md5u UUID)
RETURNS UUID
LANGUAGE SQL AS $body$
SELECT CAST(pgqs.attributes_md5(in_attributes) AS UUID);
$body$;

-- PRIVATE
CREATE FUNCTION
pgqs.new_message(
  IN in_queue_name TEXT,
  IN in_message_body TEXT,
  IN in_message_delay_seconds INT,
  IN in_message_attributes JSONB,
  IN in_message_system_attributes JSONB,
  IN in_message_sent_at TIMESTAMPTZ,
  OUT message_id UUID,
  OUT message_body_md5u UUID,
  OUT message_attributes_md5u UUID,
  OUT message_system_attributes_md5u UUID,
  OUT message_sequence_number BIGINT
)
RETURNS RECORD
LANGUAGE PLPGSQL
AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_insert_message_sql_fmt CONSTANT TEXT
    := 'INSERT INTO %I.%I '
        || '(queue_id, message_id, message_sequence_number, '
        || 'message_sent_at, message_delay_seconds, message_visible_at, '
        || 'message_body, message_body_md5u, '
        || 'message_attributes, message_attributes_md5u, '
        || 'message_system_attributes, message_system_attributes_md5u) '
        || 'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)';
  _insert_message_sql TEXT;
  _queue RECORD;
  _message_visible_at TIMESTAMPTZ;
  _message_delay_seconds INT;
  _message_attributes jsonb := COALESCE(in_message_attributes, '{}'::jsonb);
  _message_system_attributes jsonb := COALESCE(in_message_system_attributes, '{}'::jsonb);
BEGIN
  _queue := pgqs.queue(in_queue_name);
  _message_delay_seconds := COALESCE(in_message_delay_seconds, _queue.queue_delay_seconds);
  _message_visible_at := in_message_sent_at + _message_delay_seconds * INTERVAL '1s';

  _insert_message_sql := format(k_insert_message_sql_fmt,
                                k_schema_name, _queue.queue_active_message_table_name);
  message_sequence_number := nextval(format('%I.%I', k_schema_name, _queue.queue_message_sequence_name));
  message_body_md5u := CAST(md5(in_message_body) AS UUID);
  message_attributes_md5u := pgqs.attributes_md5u(in_message_attributes);
  message_system_attributes_md5u := pgqs.attributes_md5u(in_message_system_attributes);
  message_id := gen_random_uuid();
  EXECUTE _insert_message_sql
          USING _queue.queue_id, message_id, message_sequence_number, in_message_sent_at,
                _message_delay_seconds, _message_visible_at,
                in_message_body, message_body_md5u,
                _message_attributes, message_attributes_md5u,
                _message_system_attributes, message_system_attributes_md5u;
  RETURN;
END
$body$;

-- PRIVATE
CREATE FUNCTION
pgqs.new_message(
  IN in_queue_name TEXT,
  IN in_message_body TEXT,
  IN in_message_delay_seconds INT,
  IN in_message_sent_at TIMESTAMPTZ,
  OUT message_id UUID,
  OUT message_body_md5u UUID,
  OUT message_sequence_number BIGINT
)
RETURNS RECORD
LANGUAGE PLPGSQL
AS $body$
DECLARE
  k_empty_message_attributes CONSTANT JSONB := '{}';
  k_empty_message_system_attributes CONSTANT JSONB := '{}';
  _message RECORD;
BEGIN
  _message := pgqs.new_message(in_queue_name, in_message_body,
                               in_message_delay_seconds,
                               k_empty_message_attributes,
                               k_empty_message_system_attributes,
                               in_message_sent_at);
  message_id := _message.message_id;
  message_body_md5u := _message.message_body_md5u;
  message_sequence_number := _message.message_sequence_number;
  RETURN;
END
$body$;


CREATE FUNCTION
pgqs.md5_str(
  IN in_md5_uuid UUID
) RETURNS TEXT
LANGUAGE SQL AS $body$
  SELECT replace(CAST(in_md5_uuid AS TEXT), '-', '')
$body$;

CREATE FUNCTION
pgqs.send_message(
  IN in_queue_name TEXT,
  IN in_message_body TEXT,
  IN in_delay_seconds INT,
  IN in_message_attributes JSONB,
  IN in_message_system_attributes JSONB,
  OUT message_id UUID,
  OUT message_body_md5 TEXT,
  OUT message_attributes_md5 TEXT,
  OUT message_system_attributes_md5 TEXT,
  OUT message_sequence_number BIGINT
)
RETURNS RECORD
LANGUAGE PLPGSQL
AS $body$
DECLARE
  _sent_at TIMESTAMPTZ := CURRENT_TIMESTAMP;
  _message RECORD;
BEGIN
  _message := pgqs.new_message(in_queue_name, in_message_body, in_delay_seconds,
                               in_message_attributes, in_message_system_attributes,
                               _sent_at);
  message_id := _message.message_id;
  message_body_md5 := pgqs.md5_str(_message.message_body_md5u);
  message_attributes_md5 := pgqs.md5_str(_message.message_attributes_md5u);
  message_system_attributes_md5 := pgqs.md5_str(_message.message_system_attributes_md5u);
  message_sequence_number := _message.message_sequence_number;
  RETURN;
END
$body$;


CREATE FUNCTION pgqs.encode_receipt_handle(
  IN in_system_id UUID,
  IN in_queue_name TEXT,
  IN in_message_id UUID,
  IN in_receipt_token UUID
) RETURNS TEXT
LANGUAGE SQL AS $body$
  SELECT encode(convert_to(array_to_string(ARRAY['pgqs', in_system_id::TEXT, in_queue_name,
                                                 in_message_id::TEXT, in_receipt_token::TEXT],
                                           ':'), 'UTF8'), 'base64');
$body$;


CREATE FUNCTION pgqs.encode_receipt_handle(
  IN in_queue_name TEXT,
  IN in_message_id UUID,
  IN in_receipt_token UUID
) RETURNS TEXT
LANGUAGE SQL AS $body$
  SELECT pgqs.encode_receipt_handle(pgqs.system_id(), in_queue_name, in_message_id, in_receipt_token);
$body$;


CREATE FUNCTION pgqs.decode_receipt_handle(
  IN in_receipt_handle TEXT,
  OUT system_id UUID,
  OUT queue_name TEXT,
  OUT message_id UUID,
  OUT message_receipt_token UUID
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_part_count CONSTANT INT := 5;
  k_prefix_idx CONSTANT INT := 1;
  k_system_id_idx CONSTANT INT := 2;
  k_queue_name_idx CONSTANT INT := 3;
  k_message_id_idx CONSTANT INT := 4;
  k_receipt_token_idx CONSTANT INT := 5;
  _parts TEXT[];
BEGIN
  /*
     https://en.wikipedia.org/wiki/SQLSTATE

     > Real DBMSs are free to define additional values for SQLSTATE to
     > handle those features which are beyond the standard. Such values
     > must use one of the characters [I-Z] or [5-9] as the first byte of
     > class (first byte of SQLSTATE) or subclass (third byte of SQLSTATE).

    See also: https://www.postgresql.org/message-id/20185.1359219138@sss.pgh.pa.us

  */
  BEGIN
    _parts := string_to_array(convert_from(decode(in_receipt_handle, 'base64'), 'UTF8'), ':');
  EXCEPTION WHEN SQLSTATE '22023' THEN
    RAISE EXCEPTION 'invalid receipt handle'
          USING ERRCODE = 'QS441';
  END;

  IF k_part_count <> array_length(_parts, 1)
     OR _parts[k_prefix_idx] <> 'pgqs' THEN
    RAISE EXCEPTION 'invalid receipt handle'
          USING ERRCODE = 'QS441';
  END IF;

  system_id := _parts[k_system_id_idx];
  queue_name := _parts[k_queue_name_idx];
  message_id := CAST(_parts[k_message_id_idx] AS UUID);
  message_receipt_token := CAST(_parts[k_receipt_token_idx] AS UUID);

  RETURN;
END
$body$;


CREATE FUNCTION pgqs.decode_receipt_handle(
  IN in_queue_name TEXT,
  IN in_receipt_handle TEXT,
  OUT message_id UUID,
  OUT message_receipt_token UUID
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  _decoded_handle RECORD;
BEGIN

  _decoded_handle := pgqs.decode_receipt_handle(in_receipt_handle);

  IF _decoded_handle.queue_name <> in_queue_name
     OR _decoded_handle.system_id <> pgqs.system_id()
  THEN
    RAISE EXCEPTION 'invalid receipt handle'
          USING ERRCODE = 'QS441';
  END IF;

  message_id := _decoded_handle.message_id;
  message_receipt_token := _decoded_handle.message_receipt_token;

  RETURN;
END
$body$;


CREATE FUNCTION
pgqs.delete_message(
  IN in_message_table_name TEXT,
  IN in_message_id UUID,
  IN in_message_receipt_token UUID)
RETURNS BOOLEAN
LANGUAGE PLPGSQL
AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_delete_message_sql_fmt CONSTANT TEXT
    := 'DELETE FROM %I.%I WHERE (message_id, message_receipt_token) = ($1, $2)';
  _delete_message_sql TEXT;
  _row_count INT;
BEGIN
  _delete_message_sql := format(k_delete_message_sql_fmt,
                                k_schema_name, in_message_table_name);

  EXECUTE _delete_message_sql USING in_message_id, in_message_receipt_token;

  GET DIAGNOSTICS _row_count = ROW_COUNT;

  RETURN _row_count > 0;
END
$body$;

CREATE FUNCTION
pgqs.delete_message(
  IN in_queue_id BIGINT,
  IN in_message_id UUID,
  IN in_message_receipt_token UUID
)
RETURNS BOOLEAN
LANGUAGE PLPGSQL
AS $body$
DECLARE
  _visible_table RECORD;
  _affected BOOLEAN DEFAULT FALSE;
BEGIN
  FOR _visible_table IN
    SELECT mt.message_table_name
      FROM pgqs.visible_message_tables(in_queue_id) mt
  LOOP
    _affected := pgqs.delete_message(_visible_table.message_table_name,
                                    in_message_id,
                                    in_message_receipt_token);
    IF _affected THEN
      EXIT;
    END IF;
  END LOOP;

  RETURN _affected;
END
$body$;


CREATE FUNCTION
pgqs.delete_message(
  IN in_queue_name TEXT,
  IN in_message_receipt_handle TEXT)
RETURNS BOOLEAN
LANGUAGE PLPGSQL
AS $body$
DECLARE
  _queue RECORD;
  _decoded_handle RECORD;
  _affected BOOLEAN DEFAULT FALSE;
BEGIN
  _queue := pgqs.queue(in_queue_name);

  _decoded_handle := pgqs.decode_receipt_handle(in_message_receipt_handle);

  _affected := pgqs.delete_message(_queue.queue_id,
                                   _decoded_handle.message_id,
                                   _decoded_handle.message_receipt_token);

  RETURN _affected;
END
$body$;

CREATE FUNCTION
pgqs.delete_message_batch(
  IN in_queue_name TEXT,
  IN in_message_receipt_handles TEXT[],
  OUT message_receipt_handle TEXT
)
RETURNS SETOF TEXT
LANGUAGE PLPGSQL
AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_delete_messages_sql_fmt CONSTANT TEXT
    := 'DELETE FROM %I.%I AS m WHERE (message_id, message_receipt_token) = ($1, $2) RETURNING message_id';
  _queue RECORD;
  _delete_message_sql TEXT;
  _res RECORD;
  _decoded RECORD;
BEGIN
  _queue := pgqs.queue(in_queue_name);

  _delete_message_sql := format(k_delete_messages_sql_fmt,
                                k_schema_name, _queue.queue_parent_message_table_name);

  << delete_receipt_handles >>
  FOREACH message_receipt_handle IN ARRAY in_message_receipt_handles
  LOOP

    BEGIN
      _decoded := pgqs.decode_receipt_handle(in_queue_name, message_receipt_handle);
      EXCEPTION WHEN SQLSTATE 'QS4X1' THEN
        CONTINUE delete_receipt_handles;
    END;

    EXECUTE _delete_message_sql INTO _res
      USING _decoded.message_id, _decoded.message_receipt_token;
    IF _res.message_id IS NOT NULL THEN
      RETURN NEXT;
    END IF;
  END LOOP;
END
$body$;


CREATE FUNCTION
pgqs.decode_sqs_request_queue_url(
  IN in_request jsonb,
  OUT queue_url TEXT,
  OUT queue_name TEXT
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  _decoded_queue_url RECORD;
BEGIN
  queue_url := in_request->>'QueueUrl';
  IF queue_url IS NULL THEN
    RAISE EXCEPTION 'Invalid request'
          USING DETAIL = 'Missing QueueUrl';
  END IF;
  _decoded_queue_url := pgqs.checked_decode_queue_url(queue_url);
  queue_name := _decoded_queue_url.queue_name;
  RETURN;
END
$body$;

/*  :AddPermission
{:op :AddPermission,
 :request
 {:QueueUrl string,
  :Label string,
  :AWSAccountIds [:seq-of string],
  :Actions [:seq-of string]},
 :response nil}

  WON'T IMPLEMENT
*/

/*  :ChangeMessageVisibility
{:op :ChangeMessageVisibility,
 :request
 {:QueueUrl string, :ReceiptHandle string, :VisibilityTimeout integer},
 :response nil}
*/

CREATE FUNCTION pgqs.change_message_visibility_1(
  IN in_message_table_name TEXT,
  in_message_id UUID,
  in_message_receipt_token UUID,
  in_visibility_timeout INTERVAL,
  in_as_of TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_update_sql_fmt CONSTANT TEXT
    := 'UPDATE %I.%I '
       || 'SET message_visible_at = $4 + $3 '
       || 'WHERE (message_id, message_receipt_token) = ($1, $2) '
       || 'AND $4 < message_visible_at';

  _update_sql TEXT;
  _row_count INT;
  k_select_sql_fmt CONSTANT TEXT := 'SELECT TRUE FROM %I.%I WHERE message_id = $1';
  _select_sql TEXT;
BEGIN
  _update_sql := format(k_update_sql_fmt, k_schema_name, in_message_table_name);
  EXECUTE _update_sql
    USING in_message_id, in_message_receipt_token,
          in_visibility_timeout, in_as_of;

  GET DIAGNOSTICS _row_count = ROW_COUNT;

  IF _row_count > 0 THEN
    RETURN TRUE;
  END IF;

  _select_sql := format(k_select_sql_fmt, k_schema_name, in_message_table_name);

  EXECUTE _select_sql USING in_message_id;

  GET DIAGNOSTICS _row_count = ROW_COUNT;

  IF _row_count > 0 THEN
    RAISE EXCEPTION 'MessageNotInflight';
  END IF;

  RETURN FALSE;
END
$body$;

CREATE FUNCTION pgqs.change_message_visibility(
  IN in_queue_id BIGINT,
  IN in_message_id UUID,
  IN in_message_receipt_token UUID,
  IN in_visibility_timeout_seconds INT
) RETURNS BOOLEAN
LANGUAGE PLPGSQL AS $body$
DECLARE
  _visible_table RECORD;
  _affected BOOLEAN DEFAULT FALSE;
  _as_of CONSTANT TIMESTAMPTZ := CURRENT_TIMESTAMP;
  _visibility_timeout INTERVAL := in_visibility_timeout_seconds * INTERVAL '1s';
BEGIN

  FOR _visible_table IN
    SELECT mt.message_table_name
      FROM pgqs.visible_message_tables(in_queue_id) mt
  LOOP
    _affected := pgqs.change_message_visibility_1(_visible_table.message_table_name,
                                                  in_message_id,
                                                  in_message_receipt_token,
                                                  _visibility_timeout,
                                                  _as_of);

    IF _affected THEN
      EXIT;
    END IF;
  END LOOP;

  RETURN _affected;
END
$body$;


CREATE FUNCTION pgqs.validate_sqs_change_message_visibility_request(
  IN in_request jsonb,
  OUT receipt_handle TEXT,
  OUT visibility_timeout_seconds INT
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
BEGIN
  IF in_request->'VisibilityTimeout' IS NULL THEN
    RAISE EXCEPTION 'Missing VisibilityTimeout';
  END IF;

  IF jsonb_typeof(in_request->'VisibilityTimeout') <> 'number' THEN
    RAISE EXCEPTION 'Invalid VisibilityTimeout'
          USING DETAIL = format('VisibilityTimeout: %L', in_request->'VisibilityTimeout');
  END IF;

  visibility_timeout_seconds := CAST(in_request->>'VisibilityTimeout' AS INT);
  IF visibility_timeout_seconds < 0 OR visibility_timeout_seconds > 43200 THEN
    RAISE EXCEPTION 'VisibilityTimeout out of range'
          USING DETAIL = format('VisibilityTimeout: %L', in_request->'VisibilityTimeout');
  END IF;

  RETURN;
END
$body$;


CREATE FUNCTION pgqs.sqs_change_message_visibility(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
  _queue RECORD;
  _receipt_handle TEXT;
  _decoded_handle RECORD;
  _res RECORD;
BEGIN
  _queue := pgqs.queue(_queue_name);

  _receipt_handle := in_request->>'ReceiptHandle';
  IF _receipt_handle IS NULL THEN
    RAISE EXCEPTION 'Invalid request'
          USING DETAIL = 'Missing ReceiptHandle';
  END IF;
  _decoded_handle := pgqs.decode_receipt_handle(_receipt_handle);

  _res := pgqs.validate_sqs_change_message_visibility_request(in_request);
  PERFORM pgqs.change_message_visibility(_queue.queue_id,
                                         _decoded_handle.message_id,
                                         _decoded_handle.message_receipt_token,
                                         _res.visibility_timeout_seconds);
  response := NULL;
  RETURN;
END
$body$;


CREATE FUNCTION pgqs.is_valid_batch_entry_id(
  in_batch_entry_id TEXT
) RETURNS BOOLEAN
LANGUAGE SQL AS $body$
SELECT in_batch_entry_id ~ '^[a-zA-Z0-9_-]{1,80}$'
$body$;


/*  :ChangeMessageVisibilityBatch
{:op :ChangeMessageVisibilityBatch,
 :request
 {:QueueUrl string,
  :Entries
  [:seq-of
   {:Id string, :ReceiptHandle string, :VisibilityTimeout integer}]},
 :response
 {:Successful [:seq-of {:Id string}],
  :Failed
  [:seq-of
   {:Id string, :SenderFault boolean, :Code string, :Message string}]}}
*/

CREATE FUNCTION
pgqs.validate_sqs_change_message_visibility_batch_request(
  IN in_request jsonb,
  OUT entries jsonb[],
  OUT failed jsonb[]
) LANGUAGE PLPGSQL AS $body$
DECLARE
  _entries jsonb;
  _entry jsonb;
  _ids TEXT[];
  _visibility_timeout_seconds INT;
BEGIN
  _entries := in_request->'Entries';
  IF _entries IS NULL THEN
    RAISE EXCEPTION 'EmptyBatchRequest'
          USING DETAIL = 'Missing Entries';
  END IF;

  IF jsonb_typeof(_entries) <> 'array' THEN
    RAISE EXCEPTION 'Invalid request'
          USING DETAIL = 'Entries is not an array';
  END IF;

  IF jsonb_array_length(_entries) = 0  THEN
    RAISE EXCEPTION 'EmptyBatchRequest'
          USING DETAIL = 'The batch request does not contain any entries.';
  END IF;

  IF jsonb_array_length(_entries) > 10  THEN
    RAISE EXCEPTION 'TooManyEntriesInBatchRequest'
          USING DETAIL = 'The batch request contains more entries than permissible.';
  END IF;

  -- check each entry for Id, ReceiptHandle
  FOR _entry IN
    SELECT * FROM jsonb_array_elements(_entries)
  LOOP
    IF _entry->'Id' IS NULL THEN
      RAISE EXCEPTION 'malformed send message batch entry, missing Id'
            USING DETAIL = format('%s', _entry);
    END IF;

    IF _entry->>'Id' = ANY(_ids) THEN
      RAISE EXCEPTION 'BatchEntryIdsNotDistinct'
            USING DETAIL = format('%s', _entry);
    END IF;

    _ids := array_append(_ids, _entry->>'Id');

    IF NOT pgqs.is_valid_batch_entry_id(_entry->>'Id') THEN
      failed := array_append(failed,
                             jsonb_build_object('Id', _entry->'Id',
                                                'SenderFault', true,
                                                'Message', 'InvalidBatchEntryId'));
      CONTINUE;
    END IF;

    IF _entry->'ReceiptHandle' IS NULL THEN
      failed := array_append(failed,
                             jsonb_build_object('Id', _entry->'Id',
                                                'SenderFault', true,
                                                'Message', 'Missing ReceiptHandle'));
      CONTINUE;
    END IF;

    IF _entry->'VisibilityTimeout' IS NULL THEN
      failed := array_append(failed,
                             jsonb_build_object('Id', _entry->'Id',
                                                'SenderFault', true,
                                                'Message', 'Missing VisibilityTimeout'));
      CONTINUE;
    END IF;

    IF jsonb_typeof(_entry->'VisibilityTimeout') <> 'number' THEN
      failed := array_append(failed,
                             jsonb_build_object('Id', _entry->'Id',
                                                'SenderFault', true,
                                                'Message', 'Invalid VisibilityTimeout value'));
      CONTINUE;
    END IF;

    _visibility_timeout_seconds = CAST(_entry->>'VisibilityTimeout' AS INT);
    IF _visibility_timeout_seconds < 0 OR 43200 < _visibility_timeout_seconds THEN
      failed := array_append(failed,
                             jsonb_build_object('Id', _entry->'Id',
                                                'SenderFault', true,
                                                'Message', 'VisibilityTimeout out of range'));
      CONTINUE;
    END IF;

    entries := array_append(entries, _entry);
  END LOOP;

  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.sqs_change_message_visibility_batch(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
  _queue RECORD;
  _request RECORD;
  _entry jsonb;
  _entries jsonb[];
  _failed jsonb[];
  _decoded_handle RECORD;
  _result_entry jsonb;
BEGIN
  _queue := pgqs.queue(_queue_name);
  _request := pgqs.validate_sqs_change_message_visibility_batch_request(in_request);

  _failed := _request.failed;
  IF _request.entries IS NOT NULL THEN
    FOREACH _entry IN ARRAY _request.entries
    LOOP
      _decoded_handle := pgqs.decode_receipt_handle(_queue_name, _entry->>'ReceiptHandle');
      PERFORM pgqs.change_message_visibility(_queue.queue_id,
                                             _decoded_handle.message_id,
                                             _decoded_handle.message_receipt_token,
                                             CAST(_entry->>'VisibilityTimeout' AS INT));
      _result_entry := jsonb_build_object('Id', _entry->'Id');
      _entries := array_append(_entries, _result_entry);
    END LOOP;
  END IF;

  response := jsonb_strip_nulls(jsonb_build_object('Successful', _entries,
                                                   'Failed', _failed));
  RETURN;
END
$body$;


/*  :CreateQueue
{:op :CreateQueue,
 :request
 {:QueueName string,
  :Attributes
  [:map-of
   [:one-of
    ["All"
     "Policy"
     "VisibilityTimeout"
     "MaximumMessageSize"
     "MessageRetentionPeriod"
     "ApproximateNumberOfMessages"
     "ApproximateNumberOfMessagesNotVisible"
     "CreatedTimestamp"
     "LastModifiedTimestamp"
     "QueueArn"
     "ApproximateNumberOfMessagesDelayed"
     "DelaySeconds"
     "ReceiveMessageWaitTimeSeconds"
     "RedrivePolicy"
     "FifoQueue"
     "ContentBasedDeduplication"
     "KmsMasterKeyId"
     "KmsDataKeyReusePeriodSeconds"
     "DeduplicationScope"
     "FifoThroughputLimit"
     "RedriveAllowPolicy"
     "SqsManagedSseEnabled"]]
   string],
  :tags [:map-of string string]},
 :response {:QueueUrl string}}

*/

CREATE FUNCTION
pgqs.new_queue(
  IN in_queue_name TEXT,
  IN in_queue_visibility_timeout_seconds INT,
  IN in_queue_delay_seconds INT,
  IN in_queue_maximum_message_size_bytes INT,
  OUT queue_id BIGINT
) RETURNS BIGINT
LANGUAGE PLPGSQL SECURITY DEFINER
AS $body$
DECLARE
  k_queue_created_at CONSTANT TIMESTAMPTZ := CURRENT_TIMESTAMP;
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_messages_template_table_name CONSTANT TEXT := 'messages_template';
  _queue_id BIGINT;
  _queue_parent_message_table_name TEXT;
  _queue_message_sequence_name TEXT;
  _create_parent_message_table_sql TEXT;
  _create_message_table_sql TEXT;
  _message_index_name TEXT;
  _create_message_index_sql TEXT;
  _create_message_table_sqls TEXT[];
  _sql TEXT;
  _message_table RECORD;
  _create_message_sequence_sql TEXT;
  _queue_message_table_count SMALLINT;
  _insert_message_table_sql TEXT;
  k_active CONSTANT pgqs.message_table_state := 'ACTIVE';
  k_pending CONSTANT pgqs.message_table_state := 'PENDING';
  k_draining CONSTANT pgqs.message_table_state := 'DRAINING';

  -- Note: These default constants should match the defaults in the pgqs.queues table.
  _queue_visibility_timeout_seconds INT := COALESCE(in_queue_visibility_timeout_seconds,
                                                    pgqs.default_queue_visibility_timeout_seconds());
  _queue_delay_seconds INT := COALESCE(in_queue_delay_seconds, pgqs.default_queue_delay_seconds());
  _queue_maximum_message_size_bytes INT := COALESCE(in_queue_maximum_message_size_bytes,
                                                    pgqs.default_queue_maximum_message_size_bytes());
BEGIN
  _queue_id := pgqs.new_queue_id();
  _queue_parent_message_table_name := 'messages_' || _queue_id::TEXT;
  _create_parent_message_table_sql := format('CREATE TABLE %I.%I () INHERITS (%I.%I)',
                                             k_schema_name, _queue_parent_message_table_name,
                                             k_schema_name, k_messages_template_table_name);
  _queue_message_sequence_name := 'messages_' || _queue_id::TEXT || '_message_seq';
  _create_message_sequence_sql := format('CREATE SEQUENCE %I.%I',
                                         k_schema_name, _queue_message_sequence_name);

  INSERT INTO pgqs.queues
    (queue_id, queue_name, queue_created_at, queue_last_rotated_at,
     queue_parent_message_table_name, queue_message_sequence_name,
     queue_visibility_timeout_seconds, queue_delay_seconds,
     queue_maximum_message_size_bytes)
    VALUES (_queue_id, in_queue_name, k_queue_created_at, k_queue_created_at,
            _queue_parent_message_table_name, _queue_message_sequence_name,
            _queue_visibility_timeout_seconds, _queue_delay_seconds,
            _queue_maximum_message_size_bytes)
    RETURNING queue_message_table_count INTO _queue_message_table_count;

  FOR _message_table IN
     SELECT t.message_table_idx, t.message_table_name
       FROM pgqs.generate_message_tables(_queue_id, _queue_message_table_count) AS t
  LOOP
    _create_message_table_sql := format('CREATE TABLE %I.%I () INHERITS (%I.%I)',
                                        k_schema_name, _message_table.message_table_name,
                                        k_schema_name, _queue_parent_message_table_name);
    _message_index_name := _message_table.message_table_name || '_sent_at_visible_at_idx';
    _create_message_index_sql
      := format('CREATE INDEX %I ON %I.%I (message_sent_at, message_visible_at)',
                _message_index_name, k_schema_name, _message_table.message_table_name);
    _insert_message_table_sql
      := format('INSERT INTO %I.message_tables '
                || '(queue_id, message_table_idx, message_table_name, message_table_state) '
                || ' VALUES (%L, %L, %L, %L)',
                k_schema_name, _queue_id,
                _message_table.message_table_idx, _message_table.message_table_name,
                CASE WHEN _message_table.message_table_idx = 1 THEN k_active
                     WHEN _message_table.message_table_idx = 2 THEN k_pending
                     ELSE k_draining END);
    _create_message_table_sqls := array_cat(_create_message_table_sqls,
                                            ARRAY[_create_message_table_sql,
                                                  _create_message_index_sql,
                                                  _insert_message_table_sql]);
  END LOOP;

  EXECUTE _create_message_sequence_sql;
  EXECUTE _create_parent_message_table_sql;

  INSERT INTO pgqs.queue_stats (queue_id) VALUES (_queue_id);

  FOREACH _sql IN ARRAY _create_message_table_sqls
  LOOP
    EXECUTE _sql;
  END LOOP;

  queue_id := _queue_id;

  RETURN;
END
$body$;


CREATE FUNCTION
pgqs.create_queue(
  IN in_queue_name TEXT,
  OUT queue_pgqsrn TEXT
) RETURNS TEXT
LANGUAGE PLPGSQL SECURITY DEFINER
AS $body$
DECLARE
  k_null_visibility_timeout_seconds CONSTANT INT := NULL;
  k_null_delay_seconds CONSTANT INT := NULL;
  k_null_maximum_message_size_bytes CONSTANT INT := NULL;
BEGIN
  PERFORM pgqs.new_queue(in_queue_name,
                         k_null_visibility_timeout_seconds,
                         k_null_delay_seconds,
                         k_null_maximum_message_size_bytes);
  queue_pgqsrn := 'pgqs:' || pgqs.system_id() || ':' || in_queue_name;

  RETURN;
END
$body$;

CREATE FUNCTION pgqs.validate_sqs_mutable_queue_attributes(
  IN in_request jsonb,
  OUT visibility_timeout_seconds INT,
  OUT delay_seconds INT,
  OUT maximum_message_size_bytes INT
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
BEGIN
  IF in_request->'DelaySeconds' IS NOT NULL
     AND jsonb_typeof(in_request->'DelaySeconds') <> 'number' THEN
    RAISE EXCEPTION 'Invalid DelaySeconds'
          USING DETAIL = format('DelaySeconds: %L', in_request->'DelaySeconds');
  END IF;

  delay_seconds := CAST(in_request->>'DelaySeconds' AS INT);
  IF delay_seconds < 0 OR delay_seconds > 900 THEN
    RAISE EXCEPTION 'DelaySeconds out of range'
          USING DETAIL = format('DelaySeconds: %L', in_request->'DelaySeconds');
  END IF;

  IF in_request->'VisibilityTimeout' IS NOT NULL
     AND jsonb_typeof(in_request->'VisibilityTimeout') <> 'number' THEN
    RAISE EXCEPTION 'Invalid VisibilityTimeout'
          USING DETAIL = format('VisibilityTimeout: %L', in_request->'VisibilityTimeout');
  END IF;

  visibility_timeout_seconds := CAST(in_request->>'VisibilityTimeout' AS INT);
  IF delay_seconds < 0 OR delay_seconds > 43200 THEN
    RAISE EXCEPTION 'VisibilityTimeout out of range'
          USING DETAIL = format('VisibilityTimeout: %L', in_request->'VisibilityTimeout');
  END IF;

  IF in_request->'MaximumMessageSize' IS NOT NULL
     AND jsonb_typeof(in_request->'MaximumMessageSize') <> 'number' THEN
    RAISE EXCEPTION 'Invalid MaximumMessageSize'
          USING DETAIL = format('MaximumMessageSize: %L', in_request->'MaximumMessageSize');
  END IF;

  maximum_message_size_bytes := CAST(in_request->>'MaximumMessageSize' AS INT);
  IF maximum_message_size_bytes < 1024 OR 262144 < maximum_message_size_bytes THEN
    RAISE EXCEPTION 'MaximumMessageSize out of range'
          USING DETAIL = format('MaximumMessageSize: %L', in_request->'MaximumMessageSize');
  END IF;

END
$body$;

CREATE FUNCTION pgqs.sqs_create_queue(
 IN in_request jsonb,
 OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _request RECORD;
  _queue_name TEXT;
  _queue_url TEXT;
BEGIN
  _queue_name := in_request->>'QueueName';
  IF _queue_name IS NULL OR NOT pgqs.is_valid_queue_name(_queue_name) THEN
    RAISE EXCEPTION 'Invalid QueueName %', in_request;
  END IF;

  _request := pgqs.validate_sqs_mutable_queue_attributes(in_request);

  PERFORM pgqs.new_queue(_queue_name,
                         _request.visibility_timeout_seconds,
                         _request.delay_seconds,
                         _request.maximum_message_size_bytes);
  _queue_url := pgqs.encode_queue_url(_queue_name);

  response := jsonb_build_object('QueueUrl', _queue_url);

  RETURN;
END
$body$;

/*  :DeleteMessage
{:op :DeleteMessage,
 :request {:QueueUrl string, :ReceiptHandle string},
 :response nil}
*/

CREATE FUNCTION pgqs.sqs_delete_message(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
  _queue RECORD;
  _receipt_handle TEXT;
  _decoded_handle RECORD;
BEGIN
  _queue := pgqs.queue(_queue_name);

  _receipt_handle := in_request->>'ReceiptHandle';
  IF _receipt_handle IS NULL THEN
    RAISE EXCEPTION 'Invalid request'
          USING DETAIL = 'Missing ReceiptHandle';
  END IF;
  _decoded_handle := pgqs.decode_receipt_handle(_receipt_handle);

  PERFORM pgqs.delete_message(_queue.queue_id,
                              _decoded_handle.message_id,
                              _decoded_handle.message_receipt_token);
  response := NULL;
  RETURN;
END
$body$;


/*  :DeleteMessageBatch
{:op :DeleteMessageBatch,
 :request
 {:QueueUrl string,
  :Entries [:seq-of {:Id string, :ReceiptHandle string}]},
 :response
 {:Successful [:seq-of {:Id string}],
  :Failed
  [:seq-of
   {:Id string, :SenderFault boolean, :Code string, :Message string}]}}
*/

CREATE FUNCTION
pgqs.validate_delete_message_batch_request(
  IN in_request jsonb,
  OUT entries jsonb[],
  OUT failed jsonb[]
) LANGUAGE PLPGSQL AS $body$
DECLARE
  _entries jsonb;
  _entry jsonb;
  _ids TEXT[];
BEGIN
  _entries := in_request->'Entries';
  IF _entries IS NULL THEN
    RAISE EXCEPTION 'EmptyBatchRequest'
          USING DETAIL = 'Missing Entries';
  END IF;

  IF jsonb_typeof(_entries) <> 'array' THEN
    RAISE EXCEPTION 'Invalid request'
          USING DETAIL = 'Entries is not an array';
  END IF;

  IF jsonb_array_length(_entries) = 0  THEN
    RAISE EXCEPTION 'EmptyBatchRequest'
          USING DETAIL = 'The batch request does not contain any entries.';
  END IF;

  IF jsonb_array_length(_entries) > 10  THEN
    RAISE EXCEPTION 'TooManyEntriesInBatchRequest'
          USING DETAIL = 'The batch request contains more entries than permissible.';
  END IF;

  -- check each entry for Id, ReceiptHandle
  FOR _entry IN
    SELECT * FROM jsonb_array_elements(_entries)
  LOOP
    IF _entry->'Id' IS NULL THEN
      RAISE EXCEPTION 'malformed send message batch entry, missing Id'
            USING DETAIL = format('%s', _entry);
    END IF;

    IF _entry->>'Id' = ANY(_ids) THEN
      RAISE EXCEPTION 'BatchEntryIdsNotDistinct'
            USING DETAIL = format('%s', _entry);
    END IF;

    _ids := array_append(_ids, _entry->>'Id');

    IF NOT pgqs.is_valid_batch_entry_id(_entry->>'Id') THEN
      failed := array_append(failed,
                             jsonb_build_object('Id', _entry->'Id',
                                                'SenderFault', true,
                                                'Message', 'InvalidBatchEntryId'));
      CONTINUE;
    END IF;

    IF _entry->'ReceiptHandle' IS NULL THEN
      failed := array_append(failed,
                             jsonb_build_object('Id', _entry->'Id',
                                                'SenderFault', true,
                                                'Message', 'Missing ReceiptHandle'));
      CONTINUE;
    END IF;

    entries := array_append(entries, _entry);
  END LOOP;

  RETURN;
END
$body$;


CREATE FUNCTION
pgqs.sqs_delete_message_batch(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
  _request RECORD;
  _entry jsonb;
  _entries jsonb[];
  _failed jsonb[];
  _result_entry jsonb;
BEGIN
  _request := pgqs.validate_delete_message_batch_request(in_request);

  _failed := _request.failed;

  FOREACH _entry IN ARRAY _request.entries
  LOOP
    BEGIN
      PERFORM pgqs.delete_message(_queue_name, _entry->>'ReceiptHandle');
      _result_entry := jsonb_build_object('Id', _entry->'Id');
      _entries := array_append(_entries, _result_entry);
    END;
  END LOOP;
  response := jsonb_strip_nulls(jsonb_build_object('Successful', _entries,
                                                   'Failed', _failed));
  RETURN;
END
$body$;


/*  :DeleteQueue
{:op :DeleteQueue, :request {:QueueUrl string}, :response nil}
*/

CREATE FUNCTION
pgqs.delete_queue(
  IN in_queue_id BIGINT
) RETURNS BOOLEAN -- TODO is this success or whether or not it was deleted?
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_drop_table_sql_fmt CONSTANT TEXT := 'DROP TABLE IF EXISTS %I.%I';
  _drop_table_sql TEXT;
  k_drop_sequence_sql_fmt CONSTANT TEXT := 'DROP SEQUENCE IF EXISTS %I.%I';
  _drop_sequence_sql TEXT;
  _queue RECORD;
  _message_table RECORD;
BEGIN

  SELECT INTO _queue
         *
    FROM pgqs.queues q
    WHERE q.queue_id = in_queue_id;

  IF NOT FOUND THEN
    RETURN FALSE;
  END IF;

  -- drop message table partitions
  FOR _message_table IN
    SELECT mt.message_table_name
      FROM pgqs.message_tables(_queue.queue_id) AS mt
  LOOP
    _drop_table_sql := format(k_drop_table_sql_fmt,
                              k_schema_name, _message_table.message_table_name);
    EXECUTE _drop_table_sql;
  END LOOP;

  -- drop parent message table
  _drop_table_sql := format(k_drop_table_sql_fmt,
                            k_schema_name, _queue.queue_parent_message_table_name);
  EXECUTE _drop_table_sql;

  _drop_sequence_sql := format(k_drop_sequence_sql_fmt,
                               k_schema_name, _queue.queue_message_sequence_name);
  EXECUTE _drop_sequence_sql;

  DELETE FROM pgqs.message_tables
    WHERE queue_id = _queue.queue_id;
  DELETE FROM pgqs.queue_stats
    WHERE queue_id = _queue.queue_id;
  DELETE FROM pgqs.queues
    WHERE queue_id = _queue.queue_id;

  RETURN FOUND;
END
$body$;


CREATE FUNCTION
pgqs.delete_queue(
  IN in_queue_name TEXT
) RETURNS BOOLEAN
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_deleted CONSTANT pgqs.queue_state := 'DELETED';
  _queue RECORD;
BEGIN
  RAISE NOTICE 'delete_queue %s',in_queue_name;
  SELECT INTO _queue
         *
    FROM pgqs.queues q
    WHERE q.queue_name = in_queue_name;

  IF NOT FOUND THEN
    RETURN TRUE;
  END IF;

  IF k_deleted = _queue.queue_state THEN
    RETURN TRUE;
  END IF;

  UPDATE pgqs.queues
    SET queue_state = k_deleted
    WHERE queue_id = _queue.queue_id
          AND queue_state <> k_deleted;

  RETURN TRUE;
END
$body$;

CREATE FUNCTION
pgqs.sqs_delete_queue(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
BEGIN
  PERFORM pgqs.delete_queue(_queue_name);

  response := 'null'::jsonb
  RETURN;
END
$body$;


/*  :GetQueueAttributes
{:op :GetQueueAttributes,
 :request
 {:QueueUrl string,
  :AttributeNames
  [:seq-of
   [:one-of
    ["All"
     "Policy"
     "VisibilityTimeout"
     "MaximumMessageSize"
     "MessageRetentionPeriod"
     "ApproximateNumberOfMessages"
     "ApproximateNumberOfMessagesNotVisible"
     "CreatedTimestamp"
     "LastModifiedTimestamp"
     "QueueArn"
     "ApproximateNumberOfMessagesDelayed"
     "DelaySeconds"
     "ReceiveMessageWaitTimeSeconds"
     "RedrivePolicy"
     "FifoQueue"
     "ContentBasedDeduplication"
     "KmsMasterKeyId"
     "KmsDataKeyReusePeriodSeconds"
     "DeduplicationScope"
     "FifoThroughputLimit"
     "RedriveAllowPolicy"
     "SqsManagedSseEnabled"]]]},
 :response
 {:Attributes
  [:map-of
   [:one-of
    ["All"
     "Policy"
     "VisibilityTimeout"
     "MaximumMessageSize"
     "MessageRetentionPeriod"
     "ApproximateNumberOfMessages"
     "ApproximateNumberOfMessagesNotVisible"
     "CreatedTimestamp"
     "LastModifiedTimestamp"
     "QueueArn"
     "ApproximateNumberOfMessagesDelayed"
     "DelaySeconds"
     "ReceiveMessageWaitTimeSeconds"
     "RedrivePolicy"
     "FifoQueue"
     "ContentBasedDeduplication"
     "KmsMasterKeyId"
     "KmsDataKeyReusePeriodSeconds"
     "DeduplicationScope"
     "FifoThroughputLimit"
     "RedriveAllowPolicy"
     "SqsManagedSseEnabled"]]
   string]}}
*/

CREATE FUNCTION
pgqs.validate_sqs_get_queue_attributes_request(
  IN in_request jsonb,
  OUT attribute_names TEXT[])
RETURNS TEXT[]
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_valid_attribute_names CONSTANT TEXT[]
   := ARRAY['All',
            'VisibilityTimeout',
            'MaximumMessageSize',
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible',
            'CreatedTimestamp',
            'LastModifiedTimestamp',
            'ApproximateNumberOfMessagesDelayed',
            'DelaySeconds'
            ];
  _attribute_name TEXT;
BEGIN
  IF in_request->'AttributeNames' IS NULL THEN
    RAISE 'Invalid Request'
          USING DETAIL = 'Missing AttributeNames';
  END IF;
  IF jsonb_typeof(in_request->'AttributeNames') <> 'array' THEN
    RAISE 'Invalid Request'
          USING DETAIL = 'AttributeNames is not an array';
  END IF;
  IF jsonb_array_length(in_request->'AttributeNames') = 0 THEN
    RAISE 'Invalid Request'
          USING DETAIL = 'AttributeNames is empty';
  END IF;
  FOR _attribute_name IN
    SELECT attribute_name
      FROM jsonb_array_elements_text(in_request->'AttributeNames') AS _ (attribute_name)
  LOOP
    IF _attribute_name <> ALL(k_valid_attribute_names) THEN
      RAISE 'InvalidAttributeName'
            USING DETAIL = format('AttributeName %L', _attribute_name);
    END IF;
    -- TODO confirm AWS behavior of including duplicate attribute names
    IF _attribute_name = ANY(attribute_names) THEN
      CONTINUE;
    END IF;

    attribute_names := array_append(attribute_names, _attribute_name);
  END LOOP;

END
$body$;


CREATE FUNCTION
pgqs.sqs_get_queue_attributes(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
  _queue RECORD;
  _attributes jsonb := '{}';
  _attribute_names TEXT[];
  _include_all BOOLEAN;
  k_stats_attribute_names CONSTANT TEXT []
    := ARRAY['All',
             'ApproximateNumberOfMessages',
             'ApproximateNumberOfMessagesDelayed',
             'ApproximateNumberOfMessagesNotVisible'];
  _queue_stats RECORD;
  _stat_attribute_name TEXT;
BEGIN
  _queue := pgqs.queue(_queue_name);
  _attribute_names := pgqs.validate_sqs_get_queue_attributes_request(in_request);
  _include_all := ('All' = ANY(_attribute_names));

  FOREACH _stat_attribute_name IN ARRAY k_stats_attribute_names
  LOOP
    IF _stat_attribute_name = ANY(_attribute_names) THEN
      _queue_stats := pgqs.cached_queue_stats(_queue_name);
      EXIT;
    END IF;
  END LOOP;

  IF _include_all OR 'ApproximateNumberOfMessages' = ANY(_attribute_names) THEN
    _attributes := _attributes || jsonb_build_object('ApproximateNumberOfMessages',
                                                     _queue_stats.message_count);
  END IF;

  If _include_all OR 'ApproximateNumberOfMessagesDelayed' = ANY(_attribute_names) THEN
    _attributes := _attributes || jsonb_build_object('ApproximateNumberOfMessagesDelayed',
                                                     _queue_stats.delayed_message_count);
  END IF;

  If _include_all OR 'ApproximateNumberOfMessagesNotVisible' = ANY(_attribute_names) THEN
    _attributes := _attributes || jsonb_build_object('ApproximateNumberOfMessagesNotVisible',
                                                     _queue_stats.invisible_message_count);
  END IF;

  If _include_all OR 'CreatedTimestamp' = ANY(_attribute_names) THEN
    _attributes := _attributes || jsonb_build_object('CreatedTimestamp',
                                                     CAST(extract(EPOCH FROM _queue.queue_created_at) AS INT));
  END IF;

  If _include_all OR 'DelaySeconds' = ANY(_attribute_names) THEN
    _attributes := _attributes || jsonb_build_object('DelaySeconds', _queue.queue_delay_seconds);
  END IF;

  If _include_all OR 'LastModifiedTimestamp' = ANY(_attribute_names) THEN
    _attributes := _attributes || jsonb_build_object('LastTimestamp',
                                                     CAST(extract(EPOCH FROM _queue.queue_updated_at) AS INT));
  END IF;

  If _include_all OR 'MaximumMessageSize' = ANY(_attribute_names) THEN
    _attributes := _attributes || jsonb_build_object('MaximumMessageSize', _queue.queue_maximum_message_size_bytes);
  END IF;

  If _include_all OR 'VisibilityTimeout' = ANY(_attribute_names) THEN
    _attributes := _attributes || jsonb_build_object('VisibilityTimeout', _queue.queue_visibility_timeout_seconds);
  END IF;

  response := jsonb_build_object('Attributes', _attributes);
  RETURN;
END
$body$;

/*  :GetQueueUrl
{:op :GetQueueUrl,
 :request {:QueueName string, :QueueOwnerAWSAccountId string},
 :response {:QueueUrl string}}
*/

CREATE FUNCTION
pgqs.sqs_get_queue_url(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT;
  _queue RECORD;
  k_deleted CONSTANT pgqs.queue_state := 'DELETED';
BEGIN
  _queue_name := in_request->>'QueueName';
  IF _queue_name IS NULL THEN
    RAISE EXCEPTION 'Invalid request (missing QueueName)';
  END IF;

  _queue := pgqs.queue(_queue_name);

  IF _queue.queue_state = k_deleted THEN
    RAISE EXCEPTION 'NonExistentQueue';
  END IF;

  response := jsonb_build_object('QueueUrl', _queue.queue_url);
  RETURN;

END
$body$;

/*  :ListDeadLetterSourceQueues
{:op :ListDeadLetterSourceQueues,
 :request {:QueueUrl string, :NextToken string, :MaxResults integer},
 :response {:queueUrls [:seq-of string], :NextToken string}}
*/

/*  :ListQueueTags
{:op :ListQueueTags,
 :request {:QueueUrl string},
 :response {:Tags [:map-of string string]}}
*/

CREATE FUNCTION pgqs.queue_tags(
  IN in_queue_name TEXT,
  OUT tags jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
BEGIN
  SELECT INTO tags
         q.queue_tags
    FROM pgqs.queues AS q
    WHERE q.queue_name = in_queue_name
          AND q.queue_state = 'ACTIVE';

  IF NOT FOUND THEN
    RAISE EXCEPTION 'NonExistentQueue'
          USING ERRCODE = 'QS434',
                DETAIL = format('queue name %L', in_queue_name);
  END IF;
  RETURN;
END;
$body$;


CREATE FUNCTION pgqs.sqs_list_queue_tags(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
BEGIN
  response := jsonb_build_object('Tags', pgqs.queue_tags(_queue_name));
  RETURN;
END
$body$;


/*  :ListQueues
{:op :ListQueues,
 :request
 {:QueueNamePrefix string, :NextToken string, :MaxResults integer},
 :response {:QueueUrls [:seq-of string], :NextToken string}}
*/

CREATE FUNCTION
pgqs.encode_list_queues_next_token(
  IN in_as_of TIMESTAMPTZ,
  IN in_next_queue_name TEXT,
  IN in_queue_name_prefix TEXT
) RETURNS TEXT
LANGUAGE PLPGSQL AS $body$
BEGIN
  RETURN encode(convert_to(array_to_string(ARRAY[in_as_of::TEXT,
                                                 in_next_queue_name,
                                                 COALESCE(in_queue_name_prefix, '')],
                                           ','), 'UTF8'), 'base64');
END
$body$;


CREATE FUNCTION
pgqs.decode_list_queues_next_token(
  IN in_list_queues_next_token TEXT,
  OUT as_of TIMESTAMPTZ,
  OUT queue_name_prefix TEXT,
  OUT next_queue_name TEXT
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_as_of_idx CONSTANT INT := 1;
  k_next_queue_name_idx CONSTANT INT := 2;
  k_queue_name_prefix_idx CONSTANT INT := 3;
  _parts TEXT[];
BEGIN
  BEGIN
    _parts := string_to_array(convert_from(decode(in_list_queues_next_token,
                                                  'base64'), 'UTF8'), ',');
  EXCEPTION WHEN SQLSTATE '22023' THEN
    RAISE EXCEPTION 'invalid next token'
          USING ERRCODE = 'QS451';
  END;

  BEGIN
    as_of := CAST(_parts[k_as_of_idx] AS TIMESTAMPTZ);
  EXCEPTION WHEN SQLSTATE '22023' THEN
    RAISE EXCEPTION 'invalid next token'
          USING ERRCODE = 'QS451';
  END;

  queue_name_prefix := NULLIF(_parts[k_queue_name_prefix_idx], '');
  IF queue_name_prefix IS NOT NULL AND NOT pgqs.is_valid_queue_name(queue_name_prefix) THEN
    RAISE EXCEPTION 'invalid next token'
          USING ERRCODE = 'QS451';
  END IF;

  next_queue_name := _parts[k_next_queue_name_idx];
  IF NOT pgqs.is_valid_queue_name(next_queue_name) THEN
    RAISE EXCEPTION 'invalid next token'
          USING ERRCODE = 'QS451';
  END IF;

  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.raw_list_queues(
  IN in_max_results INT,
  IN in_queue_name_prefix TEXT,
  IN in_next_queue_name TEXT,
  IN in_as_of TIMESTAMPTZ,
  OUT queue_names TEXT[],
  OUT next_token TEXT
)
RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  _max_results INT := COALESCE(in_max_results, 1000);
  _limit INT := _max_results + 1;
  _where_clauses TEXT[];
  k_queue_is_not_deleted_queue_state_where_clause CONSTANT TEXT
    := format('queue_state <> CAST(%L AS pgqs.queue_state)', CAST('DELETED' AS pgqs.queue_state));
  _queue_created_at_where_clause TEXT;
  _queue_name_prefix_where_clause TEXT;
  _next_queue_name_where_clause TEXT;
  _next_queue_name TEXT;
  _sql TEXT;
  _res RECORD;
BEGIN
  _queue_created_at_where_clause := format('queue_created_at <= %L', in_as_of);

  _where_clauses := ARRAY[k_queue_is_not_deleted_queue_state_where_clause, _queue_created_at_where_clause];

  IF in_queue_name_prefix IS NOT NULL THEN
    _queue_name_prefix_where_clause := format('queue_name LIKE %L', (in_queue_name_prefix || '%'));
    _where_clauses := array_append(_where_clauses, _queue_name_prefix_where_clause);
  END IF;

  IF in_next_queue_name IS NOT NULL THEN
    _next_queue_name_where_clause := format('queue_name >= %L', in_next_queue_name);
    _where_clauses := array_append(_where_clauses, _next_queue_name_where_clause);
  END IF;

  _sql := format('WITH results (queue_name) AS ('
                 || 'SELECT queue_name FROM pgqs.queues '
                 || 'WHERE %s '
                 || 'ORDER BY queue_name '
                 || 'LIMIT %s) '
                 || 'SELECT array_agg(queue_name) AS queue_names, COUNT(*) AS returned_count '
                 || 'FROM results',
                 array_to_string(_where_clauses, ' AND '), _limit);
   EXECUTE _sql INTO _res;

   IF _res.returned_count > _max_results
   THEN
     queue_names := _res.queue_names[1:_max_results];
     _next_queue_name := _res.queue_names[_res.returned_count];
     next_token := pgqs.encode_list_queues_next_token(in_as_of, _next_queue_name, in_queue_name_prefix);
   ELSE
     queue_names := _res.queue_names;
   END IF;
   RETURN;
END
$body$;


CREATE FUNCTION
pgqs.list_queues(
  IN in_max_results INT,
  IN in_queue_name_prefix TEXT,
  IN in_next_token TEXT,
  OUT queue_names TEXT[],
  OUT next_token TEXT
)
RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  _max_results INT := COALESCE(in_max_results, 1000);
  -- _limit INT;
  _as_of TIMESTAMPTZ := CURRENT_TIMESTAMP;
  _queue_name_prefix TEXT;
  _next_queue_name TEXT;
  _decoded_next_token RECORD;
  _res RECORD;
BEGIN
  IF in_next_token IS NOT NULL THEN
    _decoded_next_token := pgqs.decode_list_queues_next_token(in_next_token);
    _as_of := _decoded_next_token.as_of;
    _next_queue_name := _decoded_next_token.next_queue_name;
    _queue_name_prefix := _decoded_next_token.queue_name_prefix;
  ELSE
    _queue_name_prefix := in_queue_name_prefix; -- XXX need to sanitize this
  END IF;

  IF NOT pgqs.is_valid_queue_name(_next_queue_name) THEN
    RAISE EXCEPTION 'invalid next_queue_name';
  END IF;

  IF _queue_name_prefix IS NOT NULL AND
     NOT pgqs.is_valid_queue_name(_queue_name_prefix) THEN
    RAISE EXCEPTION 'invalid queue_name_prefix';
  END IF;

   SELECT INTO STRICT _res
          *
     FROM pgqs.raw_list_queues(_max_results, _queue_name_prefix, _next_queue_name, _as_of);
   queue_names := _res.queue_names;
   next_token := _res.next_token;
   RETURN;
END
$body$;

CREATE FUNCTION pgqs.validate_sqs_list_queues_request(
  IN in_request jsonb,
  OUT max_results INT,
  OUT queue_name_prefix TEXT,
  OUT next_queue_name TEXT,
  OUT as_of TIMESTAMPTZ
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  _decoded_next_token RECORD;
BEGIN
  IF in_request->'MaxResults' IS NOT NULL
     AND jsonb_typeof(in_request->'MaxResults') <> 'number' THEN
    RAISE 'Invalid MaxResults';
  END IF;

  max_results = CAST(in_request->>'MaxResults' AS INT);

  IF in_request->'NextToken' IS NOT NULL THEN
    _decoded_next_token := pgqs.decode_list_queues_next_token(in_request->>'NextToken');
    queue_name_prefix := _decoded_next_token.queue_name_prefix;
    next_queue_name := _decoded_next_token.next_queue_name;
    as_of := _decoded_next_token.as_of;
    IF NOT pgqs.is_valid_queue_name(next_queue_name) THEN
      RAISE 'Invalid NextToken';
    END IF;
  ELSE
    as_of := CURRENT_TIMESTAMP;
    IF in_request->'QueueNamePrefix' IS NOT NULL
       AND jsonb_typeof(in_request->'QueueNamePrefix') <> 'string' THEN
      RAISE 'Invalid QueueNamePrefix';
    END IF;

  END IF;

  queue_name_prefix := in_request->>'QueueNamePrefix';
  IF queue_name_prefix IS NOT NULL
     AND NOT pgqs.is_valid_queue_name(queue_name_prefix) THEN
    RAISE 'Invalid queue_name_prefix';
  END IF;

  RETURN;
END
$body$;

CREATE FUNCTION pgqs.sqs_list_queues(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _request RECORD;
  _res RECORD;
  _queue_urls TEXT[];
BEGIN
  _request := pgqs.validate_sqs_list_queues_request(in_request);
  _res := pgqs.raw_list_queues(_request.max_results,
                               _request.queue_name_prefix,
                               _request.next_queue_name,
                               _request.as_of);
  SELECT INTO _queue_urls
         array_agg(pgqs.encode_queue_url(queue_name) ORDER BY queue_name)
    FROM unnest(_res.queue_names) AS _ (queue_name);

  response := jsonb_build_object('QueueUrls', _queue_urls,
                                 'NextToken', _res.next_token)
  RETURN;
END
$body$;

/*  :PurgeQueue
{:op :PurgeQueue, :request {:QueueUrl string}, :response nil}
*/

CREATE FUNCTION
pgqs.purge_queue(
  IN in_queue_name TEXT
) RETURNS BOOLEAN
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  _sql TEXT;
  _queue RECORD;
BEGIN
  _queue := pgqs.queue(in_queue_name);
  _sql := format('TRUNCATE %I.%I *',
                 k_schema_name,
                 _queue.queue_parent_message_table_name);
  EXECUTE _sql;
  RETURN TRUE;
END
$body$;

CREATE FUNCTION
pgqs.sqs_purge_queue(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
BEGIN
  PERFORM pgqs.purge_queue(_queue_name);

  response := 'null'::jsonb
  RETURN;
END
$body$;

/*  :ReceiveMessage
{:op :ReceiveMessage,
 :request
 {:QueueUrl string,
  :AttributeNames
  [:seq-of
   [:one-of
    ["All"
     "Policy"
     "VisibilityTimeout"
     "MaximumMessageSize"
     "MessageRetentionPeriod"
     "ApproximateNumberOfMessages"
     "ApproximateNumberOfMessagesNotVisible"
     "CreatedTimestamp"
     "LastModifiedTimestamp"
     "QueueArn"
     "ApproximateNumberOfMessagesDelayed"
     "DelaySeconds"
     "ReceiveMessageWaitTimeSeconds"
     "RedrivePolicy"
     "FifoQueue"
     "ContentBasedDeduplication"
     "KmsMasterKeyId"
     "KmsDataKeyReusePeriodSeconds"
     "DeduplicationScope"
     "FifoThroughputLimit"
     "RedriveAllowPolicy"
     "SqsManagedSseEnabled"]]],
  :MessageAttributeNames [:seq-of string],
  :MaxNumberOfMessages integer,
  :VisibilityTimeout integer,
  :WaitTimeSeconds integer,
  :ReceiveRequestAttemptId string},
 :response
 {:Messages
  [:seq-of
   {:MessageId string,
    :ReceiptHandle string,
    :MD5OfBody string,
    :Body string,
    :Attributes
    [:map-of
     [:one-of
      ["SenderId"
       "SentTimestamp"
       "ApproximateReceiveCount"
       "ApproximateFirstReceiveTimestamp"
       "SequenceNumber"
       "MessageDeduplicationId"
       "MessageGroupId"
       "AWSTraceHeader"]]
     string],
    :MD5OfMessageAttributes string,
    :MessageAttributes
    [:map-of
     string
     {:StringValue string,
      :BinaryValue blob,
      :StringListValues [:seq-of string],
      :BinaryListValues [:seq-of blob],
      :DataType string}]}]}}
*/

CREATE FUNCTION
pgqs.fetch_message(
  IN in_message_table_name TEXT,
  IN in_number_of_messages INT,
  IN in_visibility_timeout INTERVAL,
  IN in_as_of TIMESTAMPTZ,
  OUT message_id UUID,
  OUT message_body TEXT,
  OUT message_body_md5u UUID,
  OUT message_receipt_token UUID,
  OUT message_sent_at TIMESTAMPTZ,
  OUT message_first_received_at TIMESTAMPTZ,
  OUT message_receive_count INT,
  OUT message_attributes jsonb,
  OUT message_attributes_md5u UUID,
  OUT message_system_attributes jsonb,
  OUT message_system_attributes_md5u UUID
)
RETURNS SETOF RECORD
LANGUAGE PLPGSQL
AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  _message RECORD;
  _sql_fmt TEXT;
  _sql TEXT;
BEGIN
  _sql_fmt
     := 'UPDATE %I.%I AS msg SET '
          || 'message_receipt_token = gen_random_uuid(), '
          || 'message_first_received_at = (CASE WHEN message_receive_count = 0 THEN CURRENT_TIMESTAMP ELSE message_first_received_at END), '
          || 'message_last_received_at = CURRENT_TIMESTAMP, '
          || 'message_receive_count = message_receive_count + 1, '
          || 'message_visible_at = $1 + $2 '
          || 'WHERE message_id IN ('
          || 'SELECT m.message_id '
          || 'FROM %I.%I AS m '
          || 'WHERE m.message_visible_at <= $1 '
          || 'ORDER BY m.message_sent_at '
          || 'FOR UPDATE SKIP LOCKED '
          || 'LIMIT $3'
          || ') '
          || 'RETURNING msg.*';

  _sql := format(_sql_fmt,
                 k_schema_name, in_message_table_name,
                 k_schema_name, in_message_table_name);

  -- noinspection SqlResolve
  FOR _message IN
    EXECUTE _sql USING in_as_of, in_visibility_timeout, in_number_of_messages
  LOOP
    message_id := _message.message_id;
    message_body := _message.message_body;
    message_body_md5u := _message.message_body_md5u;
    message_receipt_token := _message.message_receipt_token;
    message_sent_at := _message.message_sent_at;
    message_first_received_at := _message.message_first_received_at;
    message_receive_count := _message.message_receive_count;
    message_attributes := _message.message_attributes;
    message_system_attributes := _message.message_system_attributes;
    RETURN NEXT;
  END LOOP;

  RETURN;
END
$body$;


CREATE FUNCTION
pgqs.receive_message(
  IN in_queue_name TEXT,
  IN in_max_number_of_messages INT,
  IN in_visibility_timeout_seconds INT,
  IN in_as_of TIMESTAMPTZ,
  OUT message_id UUID,
  OUT message_body TEXT,
  OUT message_body_md5 TEXT,
  OUT message_receipt_handle TEXT,
  OUT message_sent_at TIMESTAMPTZ,
  OUT message_receive_count INT,
  OUT message_first_received_at TIMESTAMPTZ,
  OUT message_attributes jsonb,
  OUT message_attributes_md5 TEXT,
  OUT message_system_attributes jsonb,
  OUT message_system_attributes_md5 TEXT
)
RETURNS SETOF RECORD
LANGUAGE PLPGSQL
AS $body$
DECLARE
  _queue RECORD;
  _visibility_timeout INTERVAL;
  _as_of TIMESTAMPTZ;
  _message RECORD;
  _max_number_of_messages INT := COALESCE(in_max_number_of_messages, 1);
  _number_of_messages INT := _max_number_of_messages;
  _row_count INT DEFAULT 0;
  _visible_table RECORD;
BEGIN
  _queue := pgqs.queue(in_queue_name);
  _as_of := COALESCE(in_as_of, CURRENT_TIMESTAMP);
  _visibility_timeout := COALESCE(in_visibility_timeout_seconds,
                                  _queue.queue_visibility_timeout_seconds)
                         * INTERVAL '1s';

  FOR _visible_table IN
    SELECT message_table_name
      FROM pgqs.visible_message_tables(_queue.queue_id)
  LOOP
    IF _row_count >= _max_number_of_messages THEN
      EXIT;
    END IF;

    _number_of_messages := _max_number_of_messages - _row_count;

    FOR _message IN
      SELECT *
        FROM pgqs.fetch_message(_visible_table.message_table_name,
                                _number_of_messages, _visibility_timeout, _as_of)
    LOOP
      message_id := _message.message_id;
      message_body := _message.message_body;
      message_body_md5 := pgqs.md5_str(_message.message_body_md5u);
      message_receipt_handle := pgqs.encode_receipt_handle(in_queue_name,
                                                           _message.message_id,
                                                           _message.message_receipt_token);
      message_sent_at := _message.message_sent_at;
      message_receive_count := _message.message_receive_count;
      message_first_received_at := _message.message_first_received_at;
      message_attributes := _message.message_attributes;
      message_attributes_md5 := pgqs.md5_str(_message.message_attributes_md5u);
      message_system_attributes := _message.message_system_attributes;
      message_system_attributes_md5 := pgqs.md5_str(_message.message_system_attributes_md5u);
      _row_count := _row_count + 1;
      RETURN NEXT;
    END LOOP;
  END LOOP;

  RETURN;
END
$body$;


CREATE FUNCTION pgqs.validate_sqs_receive_message_request(
  IN in_request jsonb,
  OUT max_number_of_messages INT,
  OUT visibility_timeout_seconds INT,
  OUT attribute_names TEXT[],
  OUT message_system_attribute_names TEXT[],
  OUT message_attribute_names TEXT[]
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_valid_attribute_names CONSTANT TEXT[]
    := ARRAY['All',
             -- 'SenderId',
             'SentTimestamp',
             'ApproximateReceiveCount',
             'ApproximateFirstReceiveTimestamp',
             -- 'SequenceNumber',
             -- 'MessageDeduplicationId',
             -- 'MessageGroupId',
             -- 'AWSTraceHeader',
             'otel-trace-id'];
  k_system_attribute_names CONSTANT TEXT [] := ARRAY['otel-trace-id'];
  _attribute_name TEXT;
BEGIN

  IF in_request->'MaxNumberOfMessages' IS NOT NULL THEN
    IF jsonb_typeof(in_request->'MaxNumberOfMessages') <> 'number' THEN
      RAISE EXCEPTION 'Invalid MaxNumberOfMessages'
            USING DETAIL = format('MaxNumberOfMessages: %L', in_request->'MaxNumberOfMessages');
    END IF;

    max_number_of_messages := CAST(in_request->>'MaxNumberOfMessages' AS INT);
    IF max_number_of_messages < 0 OR max_number_of_messages > 10 THEN
      RAISE EXCEPTION 'MaxNumberOfMessages out of range'
            USING DETAIL = format('MaxNumberOfMessages: %L', in_request->'MaxNumberOfMessages');
    END IF;
  END IF;

  IF in_request->'VisibilityTimeout' IS NOT NULL
     AND jsonb_typeof(in_request->'VisibilityTimeout') <> 'number' THEN
    RAISE EXCEPTION 'Invalid VisibilityTimeout'
          USING DETAIL = format('VisibilityTimeout: %L', in_request->'VisibilityTimeout');
  END IF;

  visibility_timeout_seconds := CAST(in_request->>'VisibilityTimeout' AS INT);
  IF visibility_timeout_seconds < 0 OR visibility_timeout_seconds > 10 THEN
    RAISE EXCEPTION 'VisibilityTimeout out of range'
          USING DETAIL = format('VisibilityTimeout: %L', in_request->'VisibilityTimeout');
  END IF;

  IF in_request->'AttributeNames' IS NOT NULL
     AND jsonb_typeof(in_request->'AttributeNames') <> 'array' THEN
    RAISE 'Invalid Request'
          USING DETAIL = 'AttributeNames is not an array';
  END IF;

  FOR _attribute_name IN
    SELECT attribute_name
      FROM jsonb_array_elements_text(in_request->'AttributeNames') AS _ (attribute_name)
  LOOP
    IF _attribute_name <> ALL(k_valid_attribute_names) THEN
      RAISE 'InvalidAttributeName'
            USING DETAIL = format('AttributeName %L', _attribute_name);
    END IF;
    -- TODO confirm AWS behavior of including duplicate attribute names
    IF _attribute_name = ANY(attribute_names)
       OR _attribute_name = ANY(message_system_attribute_names) THEN
      CONTINUE;
    END IF;
    IF _attribute_name = ANY(k_system_attribute_names) THEN
      message_system_attribute_names
        := array_append(message_system_attribute_names, _attribute_name);
    ELSE
      attribute_names := array_append(attribute_names, _attribute_name);
    END IF;
  END LOOP;

  IF in_request->'MessageAttributeNames' IS NOT NULL
     AND jsonb_typeof(in_request->'MessageAttributeNames') <> 'array' THEN
    RAISE 'Invalid Request'
          USING DETAIL = 'MessageAttributeNames is not an array';
  END IF;

  FOR _attribute_name IN
    SELECT attribute_name
      FROM jsonb_array_elements_text(in_request->'MessageAttributeNames') AS _ (attribute_name)
  LOOP
    IF NOT pgqs.is_valid_message_attribute_name(_attribute_name) THEN
      RAISE 'InvalidMessageAttributeName'
            USING DETAIL = format('MessageAttributeName %L', _attribute_name);
    END IF;
    IF _attribute_name = ANY(message_attribute_names) THEN
      CONTINUE;
    END IF;

    message_attribute_names := array_append(message_attribute_names, _attribute_name);
  END LOOP;

  RETURN;
END
$body$;


CREATE FUNCTION pgqs.sqs_receive_message(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_as_of CONSTANT TIMESTAMPTZ := CURRENT_TIMESTAMP;
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
  _request RECORD;
  _rec RECORD;
  _message jsonb;
  _messages jsonb[];

  _include_all_attributes BOOLEAN;
  _include_all_msg_attributes BOOLEAN;
  _attr_name TEXT;
  _attr_val jsonb;
  _attribute_names TEXT[];
  _attributes jsonb;
  _message_attribute_names TEXT[];
  _message_attributes jsonb;
  _message_system_attributes jsonb;
  k_empty_object CONSTANT jsonb := '{}'::jsonb;
BEGIN
  _request := pgqs.validate_sqs_receive_message_request(in_request);
  _attribute_names := _request.attribute_names;
  _message_attribute_names := _request.message_attribute_names;
  _include_all_attributes := 'All' = ANY(_attribute_names);
  _include_all_msg_attributes := 'All' = ANY(_message_attribute_names);

  FOR _rec IN
    SELECT rm.message_id,
           rm.message_body,
           rm.message_body_md5,
           rm.message_receipt_handle,
           rm.message_sent_at,
           rm.message_first_received_at,
           rm.message_receive_count,
           rm.message_attributes,
           rm.message_attributes_md5,
           rm.message_system_attributes,
           rm.message_system_attributes_md5
      FROM pgqs.receive_message(_queue_name,
                                _request.max_number_of_messages,
                                _request.visibility_timeout_seconds,
                                k_as_of) AS rm
  LOOP
    _attributes := '{}';
    IF _include_all_attributes OR 'ApproximateReceiveCount' = ANY(_attribute_names) THEN
      _attributes := _attributes || jsonb_build_object('ApproximateReceiveCount',
                                                       _rec.message_receive_count);
    END IF;

    IF _include_all_attributes OR 'ApproximateFirstReceiveTimestamp' = ANY(_attribute_names) THEN
      _attributes := _attributes || jsonb_build_object('ApproximateFirstReceiveTimestamp',
                                                       CAST(extract(EPOCH FROM _rec.message_first_received_at) AS INT));
    END IF;

    IF _include_all_attributes OR 'SentTimestamp' = ANY(_attribute_names) THEN
      _attributes := _attributes || jsonb_build_object('SentTimestamp',
                                                       CAST(extract(EPOCH FROM _rec.message_sent_at) AS INT));
    END IF;

    IF (_include_all_attributes OR 'otel-trace-id' = ANY(_attribute_names))
       AND _rec.message_system_attributes ? 'otel-trace-id' THEN
      _attributes := _attributes || jsonb_build_object('otel-trace-id', _rec.message_system_attributes->'otel-trace-id');
    END IF;

    _message_attributes := k_empty_object;

    FOR _attr_name, _attr_val IN
      SELECT k, v
        FROM jsonb_each(_rec.message_attributes) AS _ (k, v)
    LOOP
      IF _include_all_msg_attributes
         OR _attr_name = ANY(_request.message_attribute_names) THEN
        _message_attributes := _message_attributes || jsonb_build_object(_attr_name, _attr_val);
      END IF;
    END LOOP;

    _message := jsonb_build_object('MessageId', _rec.message_id,
                                   'Body', _rec.message_body,
                                   'MD5OfBody', _rec.message_body_md5,
                                   'ReceiptHandle', _rec.message_receipt_handle);
    IF _attributes <> k_empty_object THEN
      _message := _message || jsonb_build_object('Attributes', _attributes);
    END IF;
    IF _message_attributes <> k_empty_object THEN
      _message := _message || jsonb_build_object('MessageAttributes', _message_attributes,
                                                 'MD5OfMessageAttributes', _rec.message_attributes_md5);
    END IF;

   IF _message_system_attributes <> k_empty_object THEN
     _message := _message || jsonb_build_object('MessageSystemAttributes', _message_system_attributes,
                                                'MD5OfMessageSystemAttributes', _rec.message_system_attributes_md5);
   END IF;
    _messages := array_append(_messages, _message);
  END LOOP;

  response := jsonb_strip_nulls(jsonb_build_object('Messages', _messages));
  RETURN;
END
$body$;


/*  :SendMessage
{:op :SendMessage,
 :request
 {:QueueUrl string,
  :MessageBody string,
  :DelaySeconds integer,
  :MessageAttributes
  [:map-of
   string
   {:StringValue string,
    :BinaryValue blob,
    :StringListValues [:seq-of string],
    :BinaryListValues [:seq-of blob],
    :DataType string}],
  :MessageSystemAttributes
  [:map-of
   [:one-of ["AWSTraceHeader"]]
   {:StringValue string,
    :BinaryValue blob,
    :StringListValues [:seq-of string],
    :BinaryListValues [:seq-of blob],
    :DataType string}],
  :MessageDeduplicationId string,
  :MessageGroupId string},
 :response
 {:MD5OfMessageBody string,
  :MD5OfMessageAttributes string,
  :MD5OfMessageSystemAttributes string,
  :MessageId string,
  :SequenceNumber string -- FIFO only
  }}

;; Binary attribute values are Base64-encoded binary data object
*/


-- {:StringValue string,
--  :BinaryValue blob,
--  :StringListValues [:seq-of string],
--  :BinaryListValues [:seq-of blob],
--  :DataType string}

CREATE TYPE pgqs.anomaly_category
  AS ENUM ('unavailable',  -- retriable, callee
           'interrupted',  -- retriable, caller?
           'incorrect',    -- not retriable, caller
           'forbidden',    -- not retriable, caller
           'unsupported',  -- not retriable, caller
           'not-found',    -- not retriable, caller
           'conflict',     -- not retriable, coordinate with callee
           'fault',        -- not retriable, callee
           'busy'          -- retriable, caller
           );

CREATE FUNCTION pgqs.is_valid_message_attribute_name(
  IN in_attribute_name TEXT
) RETURNS BOOLEAN
LANGUAGE SQL AS $body$
  SELECT in_attribute_name ~ '^[a-zA-Z0-9_-](([a-zA-Z0-9_.-]){0,254}[a-zA-Z0-9_-])?$'
         AND in_attribute_name !~* '^(aws|amazon)\.';
$body$;

CREATE FUNCTION pgqs.validate_sqs_message_attributes(
  IN in_message_attributes jsonb,
  OUT anomaly_category pgqs.anomaly_category,
  OUT detail TEXT
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_incorrect CONSTANT pgqs.anomaly_category := 'incorrect';
  k_max_attr_count CONSTANT INT := 10;
  _attr_name TEXT;
  _attr_val jsonb;
  _data_type TEXT;
  _attr_count INT := 0;
BEGIN
  IF jsonb_typeof(in_message_attributes) <> 'object' THEN
    anomaly_category := k_incorrect;
    detail := 'Malformed MessageAttributes';
    RETURN;
  END IF;

  FOR _attr_name, _attr_val IN
    SELECT k, v
      FROM jsonb_each(in_message_attributes) AS _ (k, v)
  LOOP
    _attr_count := _attr_count + 1;
    IF _attr_count > k_max_attr_count THEN
      anomaly_category := k_incorrect;
      detail := 'too many attributes';
    END IF;

    IF NOT pgqs.is_valid_message_attribute_name(_attr_name) THEN
      anomaly_category := k_incorrect;
      detail := format('Invalid attribute name %L', _attr_name);
      RETURN;
    END IF;

    IF jsonb_typeof(_attr_val) <> 'object' THEN
      anomaly_category := k_incorrect;
      detail := format('Invalid attribute %L, %s', _attr_name, _attr_val);
    END IF;

    IF NOT (_attr_val ? 'DataType') THEN
      anomaly_category := k_incorrect;
      detail := format('Missing DataType %L, %s', _attr_name, _attr_val);
    END IF;

    IF jsonb_typeof(_attr_val->'DataType') <> 'string' THEN
      anomaly_category := k_incorrect;
      detail := format('Invalid DataType %L, %s', _attr_name, _attr_val);
    END IF;

    _data_type := _attr_val->>'DataType';

--  Type – The message attribute data type. Supported types include
--  String, Number, and Binary. You can also add custom information
--  for any data type. The data type has the same restrictions as the
--  message body (for more information, see SendMessage in the Amazon
--  Simple Queue Service API Reference). In addition, the following
--  restrictions apply:

-- Can be up to 256 characters long

-- Is case-sensitive
    -- XXX This is more restrictive than what's allowed for message body

    IF _data_type ~ '^String(\.[a-zA-Z0-9]{0,249})?$' THEN
      -- check v has {StringValue,BinaryValue} and DataType keys with string values
      -- DataType is String(.custom), Number(.custom), Binary(.custom)
      -- XXX Does BinaryValue need to match Binary DataType?
      -- should we calculate md5 here, too?
      IF NOT _attr_val ? 'StringValue' THEN
        anomaly_category := k_incorrect;
        detail := format('DataType does not match Value %L, %L', _attr_name, _attr_val);
      END IF;
    ELSIF _data_type ~ '^Number(\.[a-zA-Z0-9]{0,249})?$' THEN
      IF NOT _attr_val ? 'StringValue' THEN
        anomaly_category := k_incorrect;
        detail := format('DataType does not match Value %L, %L', _attr_name, _attr_val);
      END IF;
       -- XXX How to test Number value? try to cast to numeric?

     -- Number – Number attributes can store positive or negative
     -- numerical values. A number can have up to 38 digits of precision,
     -- and it can be between 10^-128 and 10^+126.  Note Amazon SQS removes
     -- leading and trailing zeroes.
       NULL;
    ELSIF _data_type ~ '^Binary(\.[a-zA-Z0-9]{0,249})?$' THEN
      IF NOT _attr_val ? 'BinaryValue' THEN
        anomaly_category := k_incorrect;
        detail := format('DataType does not match Value %L, %L', _attr_name, _attr_val);
      END IF;
     -- BinaryValue needs to be base64 (should we try decoding to confirm?)
    ELSE
      anomaly_category := k_incorrect;
      detail := format('Invalid DataType %L, %L, %s', _attr_name, _data_type, _attr_val);
    END IF;
  END LOOP;
  RETURN;
END
$body$;

CREATE FUNCTION pgqs.is_valid_otel_trace_id(
  IN in_trace_id TEXT
) RETURNS BOOLEAN
LANGUAGE SQL AS $body$
  SELECT in_trace_id ~ '^[0-9a-zA-Z]{32}$';
$body$;

CREATE FUNCTION pgqs.validate_sqs_message_system_attributes(
  IN in_message_attributes jsonb,
  OUT anomaly_category pgqs.anomaly_category,
  OUT detail TEXT
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_incorrect CONSTANT pgqs.anomaly_category := 'incorrect';
  _attr_name TEXT;
  _attr_val jsonb;
  _data_type TEXT;
BEGIN
  IF jsonb_typeof(in_message_attributes) <> 'object' THEN
    anomaly_category := k_incorrect;
    detail := 'Malformed MessageSystemAttributes';
    RETURN;
  END IF;

  FOR _attr_name, _attr_val IN
    SELECT k, v
      FROM jsonb_each(in_message_attributes) AS _ (k, v)
  LOOP
    IF _attr_name = 'otel-trace-id' THEN
      IF jsonb_typeof(_attr_val) <> 'object' THEN
        anomaly_category := k_incorrect;
        detail := format('Malformed %L attribute %L', _attr_name, _attr_val);
        RETURN;
      END IF;

      IF NOT (_attr_val ? 'DataType') THEN
        anomaly_category := k_incorrect;
        detail := format('Missing DataType %L, %s', _attr_name, _attr_val);
        RETURN;
      END IF;

      IF jsonb_typeof(_attr_val->'DataType') <> 'string' THEN
        anomaly_category := k_incorrect;
        detail := format('Invalid DataType %L, %s', _attr_name, _attr_val);
        RETURN;
      END IF;

      IF _attr_val->>'DataType' <> 'String' THEN
        anomaly_category := k_incorrect;
        detail := format('MessageSystemAttribute %L: invalid DataType %L', _attr_name, _attr_val);
        RETURN;
      END IF;

      IF NOT pgqs.is_valid_otel_trace_id(_attr_val->>'StringValue') THEN
        anomaly_category := k_incorrect;
        detail := format('MessageSystemAttribute %L: invalid value %L',
                         _attr_name, _attr_val->>'StringValue');
        RETURN;
      END IF;

    ELSE
      anomaly_category := k_incorrect;
      detail := format('Unsupported MessageSystemMAttribute %L', _attr_name);
      RETURN;
    END IF;


    _data_type := _attr_val->>'DataType';

--  Type – The message attribute data type. Supported types include String, Number, and Binary. You can also add custom information for any data type. The data type has the same restrictions as the message body (for more information, see SendMessage in the Amazon Simple Queue Service API Reference). In addition, the following restrictions apply:
-- Can be up to 256 characters long
-- Is case-sensitive
    -- XXX This is more restrictive than what's allowed for message body
    IF _data_type ~ '^String(\.[a-zA-Z0-9]{0,249})?$' THEN
      -- check v has {StringValue,BinaryValue} and DataType keys with string values
      -- DataType is String(.custom), Number(.custom), Binary(.custom)
      -- XXX Does BinaryValue need to match Binary DataType?
      -- should we calculate md5 here, too?
       IF NOT _attr_val ? 'StringValue' THEN
         anomaly_category := k_incorrect;
         detail := format('DataType does not match Value %L, %L', _attr_name, _attr_val);
         RETURN;
       END IF;
     ELSIF _data_type ~ '^Number(\.[a-zA-Z0-9]{0,249})?$' THEN
       IF NOT _attr_val ? 'StringValue' THEN
         anomaly_category := k_incorrect;
         detail := format('DataType does not match Value %L, %L', _attr_name, _attr_val);
         RETURN;
       END IF;
       -- XXX How to test Number value? try to cast to numeric?
--        Number – Number attributes can store positive or negative numerical values. A number can have up to 38 digits of precision, and it can be between 10^-128 and 10^+126.
-- Note
-- Amazon SQS removes leading and trailing zeroes.
     ELSIF _data_type ~ '^Binary(\.[a-zA-Z0-9]{0,249})?$' THEN
       IF NOT _attr_val ? 'BinaryValue' THEN
         anomaly_category := k_incorrect;
         detail := format('DataType does not match Value %L, %L', _attr_name, _attr_val);
         RETURN;
       END IF;
     ELSE
       anomaly_category := k_incorrect;
       detail := format('Invalid DataType %L, %L, %s', _attr_name, _data_type, _attr_val);
       RETURN;
     END IF;
  END LOOP;
  RETURN;
END
$body$;

CREATE FUNCTION pgqs.validate_sqs_send_message_request(
  IN in_request jsonb,
  OUT message_body TEXT,
  OUT delay_seconds INT,
  OUT message_attributes jsonb,
  OUT message_system_attributes jsonb
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  _attrs_res RECORD;
BEGIN
  message_body := in_request->>'MessageBody';
  IF message_body IS NULL THEN
    RAISE EXCEPTION 'Invalid MessageBody'
          USING DETAIL = format('MessageBody %L', message_body);
  END IF;

  IF in_request->'DelaySeconds' IS NOT NULL
     AND jsonb_typeof(in_request->'DelaySeconds') <> 'number' THEN
    RAISE EXCEPTION 'Invalid DelaySeconds'
          USING DETAIL = format('DelaySeconds: %L', in_request->'DelaySeconds');
  END IF;

  delay_seconds := CAST(in_request->>'DelaySeconds' AS INT);
  IF delay_seconds < 0 OR delay_seconds > 900 THEN
    RAISE EXCEPTION 'DelaySeconds out of range'
          USING DETAIL = format('DelaySeconds: %L', in_request->'DelaySeconds');
  END IF;

  IF in_request->'MessageAttributes' IS NOT NULL THEN
    _attrs_res := pgqs.validate_sqs_message_attributes(in_request->'MessageAttributes');
    IF _attrs_res.anomaly_category IS NOT NULL THEN
      RAISE EXCEPTION 'Invalid MessageAttributes'
            USING DETAIL = _attrs_res.detail;
    END IF;
  END IF;

  IF in_request->'MessageSystemAttributes' IS NOT NULL THEN
    _attrs_res := pgqs.validate_sqs_message_system_attributes(in_request->'MessageSystemAttributes');
    IF _attrs_res.anomaly_category IS NOT NULL THEN
      RAISE EXCEPTION 'Invalid MessageSystemAttributes'
            USING DETAIL = _attrs_res.detail;
    END IF;
  END IF;

  message_attributes := in_request->'MessageAttributes';
  message_system_attributes := in_request->'MessageSystemAttributes';

  RETURN;
END
$body$;


CREATE FUNCTION pgqs.sqs_send_message(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
  _request RECORD;
  _message RECORD;
  k_empty_object_md5 CONSTANT TEXT := 'd41d8cd98f00b204e9800998ecf8427e';
BEGIN
  _request := pgqs.validate_sqs_send_message_request(in_request);
  _message := pgqs.send_message(_queue_name,
                                _request.message_body,
                                _request.delay_seconds,
                                _request.message_attributes,
                                _request.message_system_attributes);

  response := jsonb_build_object('MessageId', _message.message_id,
                                 'MD5OfMessageBody', _message.message_body_md5);

  IF _message.message_attributes_md5 <> k_empty_object_md5 THEN
    response := response || jsonb_build_object('MD5OfMessageAttributes',
                                                _message.message_attributes_md5);
  END IF;

  IF _message.message_system_attributes_md5 <> k_empty_object_md5  THEN
    response := response || jsonb_build_object('MD5OfMessageSystemAttributes',
                                                _message.message_system_attributes_md5);
  END IF;

  RETURN;
END
$body$;

/*  :SendMessageBatch
{:op :SendMessageBatch,
 :request
 {:QueueUrl string,
  :Entries
  [:seq-of
   {:Id string,
    :MessageBody string,
    :DelaySeconds integer,
    :MessageAttributes
    [:map-of
     string
     {:StringValue string,
      :BinaryValue blob,
      :StringListValues [:seq-of string],
      :BinaryListValues [:seq-of blob],
      :DataType string}],
    :MessageSystemAttributes
    [:map-of
     [:one-of ["AWSTraceHeader"]]
     {:StringValue string,
      :BinaryValue blob,
      :StringListValues [:seq-of string],
      :BinaryListValues [:seq-of blob],
      :DataType string}],
    :MessageDeduplicationId string,
    :MessageGroupId string}]},
 :response
 {:Successful
  [:seq-of
   {:Id string,
    :MessageId string,
    :MD5OfMessageBody string,
    :MD5OfMessageAttributes string,
    :MD5OfMessageSystemAttributes string,
    :SequenceNumber string -- FIFO only
    }],
  :Failed
  [:seq-of
   {:Id string, :SenderFault boolean, :Code string, :Message string}]}}

*/


CREATE FUNCTION
pgqs.validate_send_message_batch_request(
  IN in_request jsonb,
  OUT entries jsonb[],
  OUT failed jsonb[]
) LANGUAGE PLPGSQL AS $body$
DECLARE
  _entries jsonb;
  _entry jsonb;
  _ids TEXT[];
BEGIN
  _entries := in_request->'Entries';
  IF _entries IS NULL THEN
    RAISE EXCEPTION 'EmptyBatchRequest'
          USING DETAIL = 'Missing Entries';
  END IF;

  IF jsonb_typeof(_entries) <> 'array' THEN
    RAISE EXCEPTION 'Invalid request'
          USING DETAIL = 'Entries is not an array';
  END IF;

  IF jsonb_array_length(_entries) = 0  THEN
    RAISE EXCEPTION 'EmptyBatchRequest'
          USING DETAIL = 'The batch request does not contain any entries.';
  END IF;

  IF jsonb_array_length(_entries) > 10  THEN
    RAISE EXCEPTION 'TooManyEntriesInBatchRequest'
          USING DETAIL = 'The batch request contains more entries than permissible.';
  END IF;

  -- check each entry for Id, MessageBody, DelaySeconds
  FOR _entry IN
    SELECT * FROM jsonb_array_elements(_entries)
  LOOP
    IF _entry->'Id' IS NULL THEN
      RAISE EXCEPTION 'malformed send message batch entry, missing Id'
            USING DETAIL = format('%s', _entry);
    END IF;

    IF _entry->>'Id' = ANY(_ids) THEN
      RAISE EXCEPTION 'BatchEntryIdsNotDistinct'
            USING DETAIL = format('%s', _entry);
    END IF;

    _ids := array_append(_ids, _entry->>'Id');

    IF NOT pgqs.is_valid_batch_entry_id(_entry->>'Id') THEN
      failed := array_append(failed,
                             jsonb_build_object('Id', _entry->'Id',
                                                'SenderFault', true,
                                                'Message', 'InvalidBatchEntryId'));
      CONTINUE;
    END IF;

    IF _entry->'MessageBody' IS NULL THEN
      failed := array_append(failed,
                             jsonb_build_object('Id', _entry->'Id',
                                                'SenderFault', true,
                                                'Message', 'Missing MessageBody'));
      CONTINUE;
    END IF;

    IF _entry->'DelaySeconds' IS NOT NULL
       AND jsonb_typeof(_entry->'DelaySeconds') <> 'number' THEN
      failed := array_append(failed,
                             jsonb_build_object('Id', _entry->'Id',
                                                'SenderFault', true,
                                                'Message', 'Invalid DelaySeconds'));
      CONTINUE;
    END IF;

    IF CAST(_entry->>'DelaySeconds' AS INT) < 0
       OR CAST(_entry->>'DelaySeconds' AS INT) > 900 THEN
      failed := array_append(failed,
                             jsonb_build_object('Id', _entry->'Id',
                                                'SenderFault', true,
                                                'Message', 'Invalid DelaySeconds'));
      CONTINUE;
    END IF;

    entries := array_append(entries, _entry);
  END LOOP;

  RETURN;
END
$body$;


CREATE FUNCTION
pgqs.sqs_send_message_batch(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
  _request RECORD;
  _entry jsonb;
  _entries jsonb[];
  _failed jsonb[];
  _message RECORD;
  _result_entry jsonb;
  k_empty_message_attributes CONSTANT jsonb := '{}';
  k_empty_message_system_attributes CONSTANT jsonb := '{}';
BEGIN
  _request := pgqs.validate_send_message_batch_request(in_request);

  _failed := _request.failed;

  FOREACH _entry IN ARRAY _request.entries
  LOOP
    BEGIN
      _message := pgqs.send_message(_queue_name,
                                    _entry->>'MessageBody',
                                    CAST(_entry->'DelaySeconds' AS INT),
                                    k_empty_message_attributes,
                                    k_empty_message_system_attributes);
      _result_entry := jsonb_build_object('Id', _entry->'Id',
                                          'MD5OfMessageBody', _message.message_body_md5);
      _entries := array_append(_entries, _result_entry);
    END;
  END LOOP;
  response := jsonb_strip_nulls(jsonb_build_object('Successful', _entries,
                                                   'Failed', _failed));
  RETURN;
END
$body$;

/*  :SetQueueAttributes
{:op :SetQueueAttributes,
 :request
 {:QueueUrl string,
  :Attributes
  [:map-of
   [:one-of
    ["All"
     "Policy"
     "VisibilityTimeout"
     "MaximumMessageSize"
     "MessageRetentionPeriod"
     "ApproximateNumberOfMessages"
     "ApproximateNumberOfMessagesNotVisible"
     "CreatedTimestamp"
     "LastModifiedTimestamp"
     "QueueArn"
     "ApproximateNumberOfMessagesDelayed"
     "DelaySeconds"
     "ReceiveMessageWaitTimeSeconds"
     "RedrivePolicy"
     "FifoQueue"
     "ContentBasedDeduplication"
     "KmsMasterKeyId"
     "KmsDataKeyReusePeriodSeconds"
     "DeduplicationScope"
     "FifoThroughputLimit"
     "RedriveAllowPolicy"
     "SqsManagedSseEnabled"]]
   string]},
 :response nil}
*/

CREATE FUNCTION
pgqs.set_queue_attributes(
  IN in_queue_name TEXT,
  IN in_queue_visibility_timeout_seconds INT,
  IN in_queue_delay_seconds INT,
  IN in_queue_maximum_message_size_bytes INT
) RETURNS VOID
LANGUAGE PLPGSQL AS $body$
DECLARE
  _as_of TIMESTAMPTZ := CURRENT_TIMESTAMP;
  _queue RECORD;
  _updated_at_set_expr TEXT;

  _set_expr TEXT;
  _set_exprs TEXT[];
  _set_exprs_sql TEXT;
  k_update_sql_fmt CONSTANT TEXT := 'UPDATE pgqs.queues AS q SET %s WHERE q.queue_id = $1';
  _update_sql TEXT;
BEGIN
  _queue := pgqs.queue(in_queue_name);

  _updated_at_set_expr := format('queue_updated_at = %L', _as_of);
  _set_exprs := ARRAY[_updated_at_set_expr];

  IF in_queue_visibility_timeout_seconds IS NOT NULL THEN
    _set_expr := format('queue_visibility_timeout_seconds = %L',
                       in_queue_visibility_timeout_seconds);
    _set_exprs := array_append(_set_exprs, _set_expr);
  END IF;

  IF in_queue_delay_seconds IS NOT NULL THEN
    _set_expr := format('queue_delay_seconds = %L', in_queue_delay_seconds);
    _set_exprs := array_append(_set_exprs, _set_expr);
  END IF;

  IF in_queue_maximum_message_size_bytes IS NOT NULL THEN
    _set_expr := format('queue_maximum_message_size_bytes = %L', in_queue_maximum_message_size_bytes);
    _set_exprs := array_append(_set_exprs, _set_expr);
  END IF;

  _set_exprs_sql := array_to_string(_set_exprs, ', ');
  _update_sql := format(k_update_sql_fmt, _set_exprs_sql);
  EXECUTE _update_sql USING _queue.queue_id;
  RETURN;
END
$body$;


CREATE FUNCTION
pgqs.validate_sqs_set_queue_attributes_request(
  IN in_request jsonb,
  OUT delay_seconds INT,
  OUT visibility_timeout_seconds INT,
  OUT maximum_message_size_bytes INT)
RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_valid_attribute_names CONSTANT TEXT[]
   := ARRAY['VisibilityTimeout',
            'DelaySeconds',
            'MaximumMessageSize'];
  _attributes RECORD;
  _attribute_name TEXT;
  _attribute_names TEXT[] = '{}';
  _request RECORD;
BEGIN
  IF in_request->'Attributes' IS NULL THEN
    RAISE 'Invalid Request'
          USING DETAIL = 'Missing AttributeNames';
  END IF;

  IF jsonb_typeof(in_request->'Attributes') <> 'object' THEN
    RAISE 'Invalid Request'
          USING DETAIL = 'Attributes is not an object';
  END IF;

  SELECT INTO _attributes
         count(*), array_agg(attribute_name) AS attribute_names
    FROM jsonb_object_keys(in_request->'Attributes') AS _ (attribute_name);

  IF _attributes.count = 0 THEN
    RAISE 'Invalid Request'
          USING DETAIL = 'Attributes is empty';
  END IF;

  FOREACH _attribute_name IN ARRAY _attributes.attribute_names
  LOOP
    IF _attribute_name <> ALL(k_valid_attribute_names) THEN
      RAISE 'InvalidAttributeName'
            USING DETAIL = format('AttributeName %L', _attribute_name);
    END IF;
    -- TODO confirm AWS behavior of including duplicate attribute names
    IF _attribute_name = ANY(_attribute_names) THEN
      CONTINUE;
    END IF;

    _attribute_names := array_append(_attribute_names, _attribute_name);
  END LOOP;

  _request := pgqs.validate_sqs_mutable_queue_attributes(in_request->'Attributes');

  delay_seconds := _request.delay_seconds;
  visibility_timeout_seconds := _request.visibility_timeout_seconds;
  maximum_message_size_bytes := _request.maximum_message_size_bytes;

  RETURN;
END
$body$;


CREATE FUNCTION
pgqs.sqs_set_queue_attributes(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
  _request RECORD;
BEGIN
  _request := pgqs.validate_sqs_set_queue_attributes_request(in_request);
  PERFORM pgqs.set_queue_attributes(_queue_name,
                                    _request.visibility_timeout_seconds,
                                    _request.delay_seconds,
                                    _request.maximum_message_size_bytes);
  RETURN;
END
$body$;


/*  :TagQueue
{:op :TagQueue,
 :request {:QueueUrl string, :Tags [:map-of string string]},
 :response nil}

*/

CREATE FUNCTION pgqs.sqs_request_queue_name (
  IN in_request jsonb,
  OUT queue_name TEXT
  )
RETURNS TEXT
LANGUAGE SQL AS $body$
  SELECT (pgqs.decode_sqs_request_queue_url(in_request)).queue_name
$body$;


CREATE FUNCTION pgqs.empty_value_tags (
  IN in_tags jsonb,
  OUT tag_keys TEXT[]
) LANGUAGE SQL AS $body$
  SELECT COALESCE(array_agg(_key), '{}'::TEXT[])
    FROM jsonb_each(in_tags) AS _ (_key, _val)
    WHERE _val = '""'::jsonb;
$body$;


CREATE FUNCTION pgqs.merge_queue_tags (
  IN in_queue_name TEXT,
  IN in_tags jsonb,
  OUT anomaly_category pgqs.anomaly_category,
  OUT anomaly_message TEXT
  ) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  -- if queue values are empty strings, delete
  -- otherwise merge
  _tags_to_delete TEXT[] := pgqs.empty_value_tags(in_tags);
  _tags_to_merge jsonb := in_tags - _tags_to_delete;
  k_incorrect CONSTANT pgqs.anomaly_category := 'incorrect';
  _tags jsonb;
  k_max_tag_count CONSTANT INT := 50;
  _tag_count INT;
BEGIN
  SELECT INTO _tags
         q.queue_tags
    FROM pgqs.queues q
    WHERE q.queue_name = in_queue_name
          AND q.queue_state = 'ACTIVE';

  IF NOT FOUND THEN
    anomaly_category := k_incorrect;
    anomaly_message := 'NonExistentQueue';
    RETURN;
  END IF;

  _tags := _tags - _tags_to_delete;
  _tags := _tags || _tags_to_merge;

  SELECT INTO _tag_count
         count(TRUE)
    FROM jsonb_object_keys(_tags);

  IF _tag_count > k_max_tag_count THEN
    anomaly_category := k_incorrect;
    anomaly_message := 'TooManyTags';
    RETURN;
  END IF;

  UPDATE pgqs.queues
    SET queue_tags = _tags
    WHERE queue_name = in_queue_name;

   RETURN;
END
$body$;


CREATE FUNCTION pgqs.is_valid_queue_tag_key(
  IN in_tag_key TEXT
) RETURNS BOOLEAN
LANGUAGE SQL AS $body$
  SELECT in_tag_key ~ '^[a-zA-Z0-9 _.:/=@+-]+$' AND char_length(in_tag_key) <= 128;
$body$;


CREATE FUNCTION pgqs.is_valid_queue_tag_value(
  IN in_tag_value TEXT
) RETURNS BOOLEAN
LANGUAGE SQL AS $body$
  SELECT in_tag_value ~ '^[a-zA-Z0-9 _.:/=@+-]*$' AND char_length(in_tag_value) <= 256;
$body$;


CREATE FUNCTIOn pgqs.validate_sqs_tag_queue_request(
  IN in_request jsonb,
  OUT tags jsonb,
  OUT anomaly_category pgqs.anomaly_category,
  OUT anomaly_message TEXT
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  _tag TEXT;
  _val jsonb;
  _tag_count INT := 0;
  k_max_tag_count CONSTANT INT := 50;
  k_incorrect CONSTANT pgqs.anomaly_category := 'incorrect';
BEGIN
  FOR _tag, _val IN
    SELECT k, v
      FROM jsonb_each(in_request->'Tags') AS _ (k, v)
  LOOP
    _tag_count := _tag_count + 1;
    IF k_max_tag_count < _tag_count THEN
      anomaly_category := k_incorrect;
      anomaly_message := 'TooManyTags';
      RETURN;
    END IF;

    IF jsonb_typeof(_val) <> 'string' THEN
      anomaly_category := k_incorrect;
      anomaly_message := format('Malformed tag value, %, %', _tag, _val);
      RETURN;
    END IF;

    IF NOT pgqs.is_valid_queue_tag_key(_tag) THEN
      anomaly_category := k_incorrect;
      anomaly_message := format('Invalid tag key value, %, %', _tag);
      RETURN;
    END IF;

    IF NOT pgqs.is_valid_queue_tag_value(_val #>> '{}') THEN
      anomaly_category := k_incorrect;
      anomaly_message := format('Invalid tag key value, %, %', _tag);
      RETURN;
    END IF;

  END LOOP;

  tags := in_request->'Tags';

  RETURN;
END
$body$;


CREATE FUNCTION
pgqs.sqs_tag_queue(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
  _request RECORD;
  _merge_res RECORD;
BEGIN
  _request := pgqs.validate_sqs_tag_queue_request(in_request);
  IF _request.anomaly_category IS NOT NULL THEN
    RAISE EXCEPTION 'Invalid TagQueue request'
          USING DETAIL = _request.anomaly_message;
  END IF;

  _merge_res := pgqs.merge_queue_tags(_queue_name, _request.tags);
  IF _merge_res.anomaly_category IS NOT NULL THEN
    RAISE EXCEPTION 'QueueName % %', _queue_name, _merge_res.anomaly_message;
  END IF;
  RETURN;
END
$body$;

/*  :UntagQueue
{:op :UntagQueue,
 :request {:QueueUrl string, :TagKeys [:seq-of string]},
 :response nil}
*/


CREATE FUNCTION
pgqs.remove_queue_tags(
  IN in_queue_name TEXT,
  IN in_tag_keys TEXT[],
  OUT anomaly_category pgqs.anomaly_category,
  OUT anomaly_message TEXT
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
BEGIN
  UPDATE pgqs.queues
    SET queue_tags = queue_tags - in_tag_keys
    WHERE queue_name = in_queue_name
          AND queue_state = 'ACTIVE';
  IF NOT FOUND THEN
    anomaly_category := 'incorrect';
    anomaly_message := 'NonExistentQueue';
  END IF;
  RETURN;
END
$body$;


CREATE FUNCTION
pgqs.validate_sqs_untag_queue_request(
  IN in_request jsonb,
  OUT tag_keys TEXT[],
  OUT anomaly_category pgqs.anomaly_category,
  OUT anomaly_message TEXT
) RETURNS RECORD
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_incorrect CONSTANT pgqs.anomaly_category := 'incorrect';
  _invalid_tag_keys TEXT[];
BEGIN
  IF in_request->'TagKeys' IS NULL THEN
    anomaly_category := k_incorrect;
    anomaly_message := 'Missing TagKeys';
    RETURN;
  END IF;

  IF jsonb_typeof(in_request->'TagKeys') <> 'array' THEN
    anomaly_category := k_incorrect;
    anomaly_message := 'Malformed TagKeys value: TagKeys is not an array';
    RETURN;
  END IF;

  SELECT INTO _invalid_tag_keys
         array_agg(tag_key #>> '{}')
    FROM jsonb_array_elements(in_request->'TagKeys') AS _ (tag_key)
    WHERE jsonb_typeof(tag_key) <> 'string'
          OR NOT pgqs.is_valid_queue_tag_key(tag_key #>> '{}');

  IF array_length(_invalid_tag_keys, 1) > 0 THEN
    anomaly_category := k_incorrect;
    anomaly_message := format('Malformed TagKeys values %L', in_request->'TagKeys');
  END IF;

  -- Let's just ignore dupes
  tag_keys := ARRAY(SELECT DISTINCT jsonb_array_elements_text(in_request->'TagKeys'));

  IF array_length(tag_keys, 1) = 0 THEN
    anomaly_category := k_incorrect;
    anomaly_message := 'Empty TagKeys';
  END IF;

  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.sqs_untag_queue(
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
SECURITY DEFINER
SET search_path TO 'pgqs'
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_name TEXT := pgqs.sqs_request_queue_name(in_request);
  _request RECORD;
  _untag_res RECORD;
BEGIN
  _request := pgqs.validate_sqs_untag_queue_request(in_request);
  IF _request.anomaly_category IS NOT NULL THEN
    RAISE EXCEPTION 'Invalid UntagQueue request'
          USING DETAIL = _request.anomaly_message;
  END IF;
  _untag_res := pgqs.remove_queue_tags(_queue_name, _request.tag_keys);
  IF _untag_res.anomaly_category IS NOT NULL THEN
    RAISE EXCEPTION 'Invalid UntagQueue request'
          USING DETAIL = _untag_res.anomaly_message;
  END IF;

  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.invoke(
  IN in_op TEXT,
  IN in_request jsonb,
  OUT response jsonb
) RETURNS jsonb
LANGUAGE PLPGSQL
SET search_path TO 'pgqs'
AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_op_sql_fmt CONSTANT TEXT := 'SELECT %I.%I($1)';
  _op_func_name TEXT;
  _op_sql TEXT;
BEGIN
   _op_func_name := 'sqs_' || lower(regexp_replace(in_op, '(?<!^)([A-Z])', '_\1', 'g'));
   _op_sql := format(k_op_sql_fmt, k_schema_name, _op_func_name);
   BEGIN
     EXECUTE _op_sql INTO response USING in_request;
     EXCEPTION WHEN SQLSTATE '42883' THEN
       IF SQLERRM LIKE format('function %s.%s(json%%) does not exist', k_schema_name, _op_func_name) THEN
         RAISE EXCEPTION 'Unknown operation'
               USING ERRCODE = 'QS400',
               DETAIL = format('operation %L is unknown', in_op);
       ELSE
         RAISE;
       END IF;
   END;
   RETURN;
END
$body$;


ALTER TABLE pgqs.system OWNER TO pgqs_admin;
ALTER TABLE pgqs.queues OWNER TO pgqs_admin;
ALTER TABLE pgqs.message_tables OWNER TO pgqs_admin;
ALTER TABLE pgqs.messages_template OWNER TO pgqs_admin;
ALTER TABLE pgqs.queue_stats OWNER TO pgqs_admin;

ALTER SEQUENCE pgqs.queues_queue_id_seq OWNER TO pgqs_admin;
ALTER TYPE pgqs.queue_state OWNER TO pgqs_admin;

-- ALTER FUNCTION pgqs.attributes_md5(in_attrs jsonb, OUT attributes_md5 text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.attributes_md5u(in_attributes jsonb, OUT attributes_md5u uuid) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.cached_queue_stats(in_queue_name text, OUT queue_stats_updated_at timestamp with time zone, OUT message_count bigint, OUT stored_message_count bigint, OUT delayed_message_count bigint, OUT in_flight_message_count bigint, OUT invisible_message_count bigint) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.change_message_visibility(in_queue_id bigint, in_message_id uuid, in_message_receipt_token uuid, in_visibility_timeout_seconds integer) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.change_message_visibility_1(in_message_table_name text, in_message_id uuid, in_message_receipt_token uuid, in_visibility_timeout interval, in_as_of timestamp with time zone) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.checked_decode_queue_url(in_queue_url text, OUT system_id uuid, OUT queue_name text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.create_queue(in_queue_name text, OUT queue_pgqsrn text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.create_role_if_not_exists(in_role_name text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.decode_list_queues_next_token(in_list_queues_next_token text, OUT as_of timestamp with time zone, OUT queue_name_prefix text, OUT next_queue_name text) OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.decode_queue_url(in_queue_url text, OUT system_id uuid, OUT queue_name text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.decode_receipt_handle(in_queue_name text, in_receipt_handle text, OUT message_id uuid, OUT message_receipt_token uuid) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.decode_receipt_handle(in_receipt_handle text, OUT system_id uuid, OUT queue_name text, OUT message_id uuid, OUT message_receipt_token uuid) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.decode_sqs_request_queue_url(in_request jsonb, OUT queue_url text, OUT queue_name text) OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.default_queue_delay_seconds() OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.default_queue_maximum_message_size_bytes() OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.default_queue_visibility_timeout_seconds() OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.delete_message(in_message_table_name text, in_message_id uuid, in_message_receipt_token uuid) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.delete_message(in_queue_id bigint, in_message_id uuid, in_message_receipt_token uuid) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.delete_message(in_queue_name text, in_message_receipt_handle text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.delete_message_batch(in_queue_name text, in_message_receipt_handles text[], OUT message_receipt_handle text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.delete_queue(in_queue_id bigint) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.delete_queue(in_queue_name text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.empty_value_tags(in_tags jsonb, OUT tag_keys text[]) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.encode_list_queues_next_token(in_as_of timestamp with time zone, in_next_queue_name text, in_queue_name_prefix text) OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.encode_queue_url(in_queue_name text, OUT queue_url text) OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.encode_queue_url(in_system_id uuid, in_queue_name text, OUT queue_url text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.encode_receipt_handle(in_queue_name text, in_message_id uuid, in_receipt_token uuid) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.encode_receipt_handle(in_system_id uuid, in_queue_name text, in_message_id uuid, in_receipt_token uuid) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.fetch_message(in_message_table_name text, in_number_of_messages integer, in_visibility_timeout interval, in_as_of timestamp with time zone, OUT message_id uuid, OUT message_body text, OUT message_body_md5u uuid, OUT message_receipt_token uuid, OUT message_sent_at timestamp with time zone, OUT message_first_received_at timestamp with time zone, OUT message_receive_count integer, OUT message_attributes jsonb, OUT message_attributes_md5u uuid, OUT message_system_attributes jsonb, OUT message_system_attributes_md5u uuid) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.generate_message_tables(in_queue_id bigint, in_queue_message_table_count smallint, OUT message_table_idx smallint, OUT message_table_name text) OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.init_system(OUT system_id uuid, OUT system_created_at timestamp with time zone) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.invoke(in_op text, in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.is_valid_batch_entry_id(in_batch_entry_id text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.is_valid_message_attribute_name(in_attribute_name text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.is_valid_otel_trace_id(in_trace_id text) OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.is_valid_queue_name(in_queue_name text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.is_valid_queue_tag_key(in_tag_key text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.is_valid_queue_tag_value(in_tag_value text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.list_queues(in_max_results integer, in_queue_name_prefix text, in_next_token text, OUT queue_names text[], OUT next_token text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.maintenance_operations(OUT op text, OUT request jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.maintenance_operations(in_as_of timestamp with time zone, OUT op text, OUT request jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.maybe_prep_message_table_rotation(in_queue_id bigint, in_message_table_idx smallint, in_message_table_name text, OUT message_table_state pgqs.message_table_state) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.maybe_prep_or_rotate_message_tables(in_queue_id bigint, OUT message_table_state pgqs.message_table_state) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.maybe_rotate_message_tables(in_queue_id bigint, OUT message_table_state pgqs.message_table_state) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.maybe_rotate_message_tables(in_queue_name text, OUT message_table_state pgqs.message_table_state) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.maybe_rotate_message_tables(in_queue_name text, as_of timestamp with time zone, OUT message_table_state pgqs.message_table_state) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.maybe_rotate_message_tables_now(in_queue_name text, OUT message_table_state pgqs.message_table_state) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.md5_str(in_md5_uuid uuid) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.merge_queue_tags(in_queue_name text, in_tags jsonb, OUT anomaly_category pgqs.anomaly_category, OUT anomaly_message text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.message_tables(in_queue_id bigint, OUT message_table_idx smallint, OUT message_table_name text, OUT message_table_state pgqs.message_table_state) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.new_message(in_queue_name text, in_message_body text, in_message_delay_seconds integer, in_message_attributes jsonb, in_message_system_attributes jsonb, in_message_sent_at timestamp with time zone, OUT message_id uuid, OUT message_body_md5u uuid, OUT message_attributes_md5u uuid, OUT message_system_attributes_md5u uuid, OUT message_sequence_number bigint) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.new_message(in_queue_name text, in_message_body text, in_message_delay_seconds integer, in_message_sent_at timestamp with time zone, OUT message_id uuid, OUT message_body_md5u uuid, OUT message_sequence_number bigint) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.new_queue(in_queue_name text, in_queue_visibility_timeout_seconds integer, in_queue_delay_seconds integer, in_queue_maximum_message_size_bytes integer, OUT queue_id bigint) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.new_queue_id() OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.pending_message_table(in_queue_id bigint, OUT message_table_idx smallint, OUT message_table_name text, OUT message_table_state pgqs.message_table_state) OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.pgqs_version() OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.purge_queue(in_queue_name text) OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.queue(in_queue_name text, OUT queue_id bigint, OUT queue_url text, OUT queue_state pgqs.queue_state, OUT queue_delay_seconds integer, OUT queue_maximum_message_size_bytes integer, OUT queue_visibility_timeout_seconds integer, OUT queue_message_table_count smallint, OUT queue_active_message_table_idx smallint, OUT queue_active_message_table_name text, OUT queue_rotation_period_seconds integer, OUT queue_last_rotated_at timestamp with time zone, OUT queue_parent_message_table_name text, OUT queue_message_sequence_name text, OUT queue_next_rotation_due_at timestamp with time zone, OUT queue_created_at timestamp with time zone, OUT queue_updated_at timestamp with time zone) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.queue_by_queue_url(in_queue_url text, OUT queue_id bigint, OUT queue_url text, OUT queue_delay_seconds integer, OUT queue_state pgqs.queue_state, OUT queue_visibility_timeout_seconds integer, OUT queue_message_table_count smallint, OUT queue_active_message_table_idx smallint, OUT queue_active_message_table_name text, OUT queue_rotation_period_seconds integer, OUT queue_last_rotated_at timestamp with time zone, OUT queue_parent_message_table_name text, OUT queue_message_sequence_name text, OUT queue_next_rotation_due_at timestamp with time zone) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.queue_message_table_state_constraint() OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.queue_next_message_table_idx(in_queue_message_table_count smallint, in_queue_message_table_idx smallint, OUT queue_next_message_table_idx smallint) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.queue_stats_now(in_queue_name text, OUT queue_depth bigint, OUT visible_queue_depth bigint, OUT delayed_queue_depth bigint, OUT last_1m_sent_count bigint, OUT last_5m_sent_count bigint, OUT earliest_sent_at timestamp with time zone, OUT latest_sent_at timestamp with time zone) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.queue_table_stats(in_queue_name text, OUT messages_table_idx integer, OUT messages_table_name text, OUT table_state text, OUT queue_depth bigint, OUT visible_queue_depth bigint, OUT earliest_sent_at timestamp with time zone, OUT latest_sent_at timestamp with time zone, OUT last_1m_sent_count bigint, OUT last_5m_sent_count bigint) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.queue_tags(in_queue_name text, OUT tags jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.raw_list_queues(in_max_results integer, in_queue_name_prefix text, in_next_queue_name text, in_as_of timestamp with time zone, OUT queue_names text[], OUT next_token text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.receive_message(in_queue_name text, in_max_number_of_messages integer, in_visibility_timeout_seconds integer, in_as_of timestamp with time zone, OUT message_id uuid, OUT message_body text, OUT message_body_md5 text, OUT message_receipt_handle text, OUT message_sent_at timestamp with time zone, OUT message_receive_count integer, OUT message_first_received_at timestamp with time zone, OUT message_attributes jsonb, OUT message_attributes_md5 text, OUT message_system_attributes jsonb, OUT message_system_attributes_md5 text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.remove_queue_tags(in_queue_name text, in_tag_keys text[], OUT anomaly_category pgqs.anomaly_category, OUT anomaly_message text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.send_message(in_queue_name text, in_message_body text, in_delay_seconds integer, in_message_attributes jsonb, in_message_system_attributes jsonb, OUT message_id uuid, OUT message_body_md5 text, OUT message_attributes_md5 text, OUT message_system_attributes_md5 text, OUT message_sequence_number bigint) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.set_queue_attributes(in_queue_name text, in_queue_visibility_timeout_seconds integer, in_queue_delay_seconds integer, in_queue_maximum_message_size_bytes integer) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_change_message_visibility(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_change_message_visibility_batch(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_create_queue(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_delete_message(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_delete_message_batch(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_delete_queue(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_get_queue_attributes(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_get_queue_url(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_list_queue_tags(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_list_queues(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_purge_queue(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_receive_message(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_request_queue_name(in_request jsonb, OUT queue_name text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_send_message(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_send_message_batch(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_set_queue_attributes(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_tag_queue(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.sqs_untag_queue(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.system_id() OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.system_singleton() OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.update_queue_stats(in_queue_name text, OUT queue_stats_updated_at timestamp with time zone, OUT message_count bigint, OUT stored_message_count bigint, OUT delayed_message_count bigint, OUT in_flight_message_count bigint, OUT invisible_message_count bigint) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_delete_message_batch_request(in_request jsonb, OUT entries jsonb[], OUT failed jsonb[]) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_send_message_batch_request(in_request jsonb, OUT entries jsonb[], OUT failed jsonb[]) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_sqs_change_message_visibility_batch_request(in_request jsonb, OUT entries jsonb[], OUT failed jsonb[]) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_sqs_change_message_visibility_request(in_request jsonb, OUT receipt_handle text, OUT visibility_timeout_seconds integer) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_sqs_get_queue_attributes_request(in_request jsonb, OUT attribute_names text[]) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_sqs_list_queues_request(in_request jsonb, OUT max_results integer, OUT queue_name_prefix text, OUT next_queue_name text, OUT as_of timestamp with time zone) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_sqs_message_attributes(in_message_attributes jsonb, OUT anomaly_category pgqs.anomaly_category, OUT detail text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_sqs_message_system_attributes(in_message_attributes jsonb, OUT anomaly_category pgqs.anomaly_category, OUT detail text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_sqs_mutable_queue_attributes(in_request jsonb, OUT visibility_timeout_seconds integer, OUT delay_seconds integer, OUT maximum_message_size_bytes integer) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_sqs_receive_message_request(in_request jsonb, OUT max_number_of_messages integer, OUT visibility_timeout_seconds integer, OUT attribute_names text[], OUT message_system_attribute_names text[], OUT message_attribute_names text[]) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_sqs_send_message_request(in_request jsonb, OUT message_body text, OUT delay_seconds integer, OUT message_attributes jsonb, OUT message_system_attributes jsonb) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_sqs_set_queue_attributes_request(in_request jsonb, OUT delay_seconds integer, OUT visibility_timeout_seconds integer, OUT maximum_message_size_bytes integer) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_sqs_tag_queue_request(in_request jsonb, OUT tags jsonb, OUT anomaly_category pgqs.anomaly_category, OUT anomaly_message text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.validate_sqs_untag_queue_request(in_request jsonb, OUT tag_keys text[], OUT anomaly_category pgqs.anomaly_category, OUT anomaly_message text) OWNER TO pgqs_admin;
-- ALTER FUNCTION pgqs.visible_message_tables(in_queue_id bigint, OUT message_table_idx smallint, OUT message_table_name text, OUT message_table_state pgqs.message_table_state) OWNER TO pgqs_admin;

GRANT EXECUTE ON FUNCTION pgqs.invoke(TEXT, jsonb) TO pgqs_queue_worker;

GRANT EXECUTE ON FUNCTION pgqs.sqs_change_message_visibility(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_change_message_visibility_batch(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_create_queue(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_delete_message(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_delete_message_batch(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_delete_queue(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_get_queue_attributes(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_get_queue_url(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_list_queue_tags(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_list_queues(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_purge_queue(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_receive_message(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_request_queue_name(in_request jsonb, OUT queue_name text) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_send_message(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_send_message_batch(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_set_queue_attributes(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_tag_queue(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;
GRANT EXECUTE ON FUNCTION pgqs.sqs_untag_queue(in_request jsonb, OUT response jsonb) TO pgqs_queue_worker;

ALTER FUNCTION pgqs.sqs_change_message_visibility(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_change_message_visibility_batch(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_create_queue(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_delete_message(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_delete_message_batch(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_delete_queue(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_get_queue_attributes(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_get_queue_url(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_list_queue_tags(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_list_queues(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_purge_queue(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_receive_message(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_send_message(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_send_message_batch(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_set_queue_attributes(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_tag_queue(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.sqs_untag_queue(in_request jsonb, OUT response jsonb) SECURITY DEFINER;


ALTER FUNCTION pgqs.sqs_change_message_visibility(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_change_message_visibility_batch(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_create_queue(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_delete_message(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_delete_message_batch(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_delete_queue(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_get_queue_attributes(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_get_queue_url(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_list_queue_tags(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_list_queues(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_purge_queue(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_receive_message(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_send_message(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_send_message_batch(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_set_queue_attributes(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_tag_queue(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.sqs_untag_queue(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';

ALTER FUNCTION pgqs.maint_clean_up_deleted_queue(in_request jsonb) OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.maint_get_maintenance_operations(in_request jsonb, OUT response jsonb) OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.maint_request_queue_name(in_request jsonb, OUT queue_name text) OWNER TO pgqs_admin;
ALTER FUNCTION pgqs.maint_rotate_message_tables(in_request jsonb) OWNER TO pgqs_admin;

ALTER FUNCTION pgqs.maint_clean_up_deleted_queue(in_request jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.maint_get_maintenance_operations(in_request jsonb, OUT response jsonb) SECURITY DEFINER;
ALTER FUNCTION pgqs.maint_request_queue_name(in_request jsonb, OUT queue_name text) SECURITY DEFINER;
ALTER FUNCTION pgqs.maint_rotate_message_tables(in_request jsonb) SECURITY DEFINER;

ALTER FUNCTION pgqs.maint_clean_up_deleted_queue(in_request jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.maint_get_maintenance_operations(in_request jsonb, OUT response jsonb) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.maint_request_queue_name(in_request jsonb, OUT queue_name text) SET search_path TO 'pgqs';
ALTER FUNCTION pgqs.maint_rotate_message_tables(in_request jsonb) SET search_path TO 'pgqs';

GRANT EXECUTE ON FUNCTION pgqs.invoke_maintenance(TEXT, jsonb) TO pgqs_maintenance;

COMMIT;
