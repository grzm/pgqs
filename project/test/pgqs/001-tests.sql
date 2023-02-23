BEGIN;

-- PRIVATE? convenience for testing?

-- PRIVATE
CREATE FUNCTION
pgqs.message(
  IN in_queue_name TEXT,
  IN in_message_id UUID,
  OUT message_id UUID,
  OUT message_body TEXT,
  OUT message_sent_at TIMESTAMPTZ,
  OUT message_delay_seconds INT,
  OUT message_visible_at TIMESTAMPTZ,
  OUT message_attributes JSONB,
  OUT message_system_attributes JSONB,
  OUT message_receipt_token UUID
)
LANGUAGE PLPGSQL
AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_message_sql_fmt CONSTANT TEXT := 'SELECT * FROM %I.%I WHERE message_id = $1';
  _queue RECORD;
  _message_sql TEXT;
  _message RECORD;
BEGIN
  _queue := pgqs.queue(in_queue_name);
  _message_sql := format(k_message_sql_fmt, k_schema_name, _queue.queue_parent_message_table_name);
  set client_min_messages TO DEBUG;

  EXECUTE _message_sql INTO _message USING in_message_id;

  IF _message.message_id IS NULL THEN
    RAISE EXCEPTION 'message_id % for queue % not found',
                    quote_literal(in_message_id), quote_literal(in_queue_name)
          USING ERRCODE = 'QS444';
  END IF;

  message_id := in_message_id;
  message_body := _message.message_body;
  message_sent_at := _message.message_sent_at;
  message_delay_seconds := _message.message_delay_seconds;
  message_visible_at := _message.message_visible_at;
  message_attributes := _message.message_attributes;
  message_system_attributes := _message.message_system_attributes;
  message_receipt_token := _message.message_receipt_token;

  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.message(
  IN in_queue_name TEXT,
  IN in_message_id TEXT,
  OUT message_id UUID,
  OUT message_body TEXT,
  OUT message_sent_at TIMESTAMPTZ,
  OUT message_delay_seconds INT,
  OUT message_visible_at TIMESTAMPTZ,
  OUT message_attributes JSONB,
  OUT message_system_attributes JSONB,
  OUT message_receipt_token UUID
)
LANGUAGE PLPGSQL
AS $body$
DECLARE
  _message RECORD;
BEGIN
  _message := pgqs.message(in_queue_name, in_message_id::UUID);

  message_id := in_message_id;
  message_body := _message.message_body;
  message_sent_at := _message.message_sent_at;
  message_delay_seconds := _message.message_delay_seconds;
  message_visible_at := _message.message_visible_at;
  message_attributes := _message.message_attributes;
  message_system_attributes := _message.message_system_attributes;
  message_receipt_token := _message.message_receipt_token;

  RETURN;
END
$body$;

CREATE FUNCTION
pgqs.send_message(
  IN in_queue_name TEXT,
  IN in_message_body TEXT,
  OUT message_id UUID,
  OUT message_body_md5 TEXT,
  OUT message_sequence_number BIGINT)
RETURNS RECORD
LANGUAGE PLPGSQL
AS $body$
DECLARE
  _queue RECORD;
  _sent_at TIMESTAMPTZ := CURRENT_TIMESTAMP;
  k_empty_message_attributes CONSTANT JSONB := '{}';
  k_empty_message_system_attributes CONSTANT JSONB := '{}';
  _message RECORD;
BEGIN
  _queue := pgqs.queue(in_queue_name);

  _message := pgqs.new_message(in_queue_name, in_message_body, _queue.queue_delay_seconds,
                                 k_empty_message_attributes,
                                 k_empty_message_system_attributes,
                                 _sent_at);
  message_id := _message.message_id;
  message_body_md5 := pgqs.md5_str(_message.message_body_md5u);
  message_sequence_number := _message.message_sequence_number;
  RETURN;
END
$body$;


CREATE FUNCTION
pgqs_test.test_create_queue()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_schema_name CONSTANT NAME := 'pgqs';
  _queue_name TEXT := 'my-queue';
  _queue_pgqsrn TEXT;
  _queue RECORD;
  _queue_res RECORD;
BEGIN

  RETURN NEXT table_is_empty('pgqs', 'queues', 'queues is initially empty');

  _queue_pgqsrn := pgqs.create_queue(_queue_name);
  RETURN NEXT is(_queue_pgqsrn, 'pgqs:' || pgqs.system_id() || ':' || _queue_name,
                 'have expected pgqsrn');

  SELECT INTO STRICT _queue -- STRICT to confirm we have only one row at this point
         *
    FROM pgqs.queues q;

  RETURN NEXT is(_queue.queue_name, _queue_name, 'Have expected queue_name');
  RETURN NEXT is(_queue.queue_message_table_count, 3::SMALLINT,
                 'have expected (default) message table count');
  RETURN NEXT has_table(k_schema_name, 'messages_template'::NAME);
  RETURN NEXT has_table(k_schema_name, CAST('messages_' || _queue.queue_id AS NAME));
  RETURN NEXT hasnt_table(k_schema_name, CAST('messages_' || _queue.queue_id || '_0' AS NAME));
  RETURN NEXT has_sequence(k_schema_name, CAST('messages_' || _queue.queue_id || '_message_seq' AS NAME));
  RETURN NEXT has_table('pgqs'::NAME, CAST('messages_' || _queue.queue_id || '_1' AS NAME));
  RETURN NEXT has_table('pgqs'::NAME, CAST('messages_' || _queue.queue_id || '_2' AS NAME));
  RETURN NEXT has_table('pgqs'::NAME, CAST('messages_' || _queue.queue_id || '_3' AS NAME));
  RETURN NEXT hasnt_table('pgqs'::NAME, CAST('messages_' || _queue.queue_id || '_4' AS NAME));

  _queue_res := pgqs.queue(_queue_name);
  RETURN NEXT matches(_queue_res.queue_url, '^pgqs://pgqs.system:[a-z0-9-]+/' || _queue_name, 'queue_url looks well-formed');


  RETURN NEXT ok(pgqs.delete_queue(_queue_name), 'can delete queue');
  -- queue should be marked deleted,

  SELECT INTO _queue
         *
    FROM pgqs.queues
    WHERE queue_name = _queue_name;

  RETURN NEXT is('DELETED'::pgqs.queue_state::TEXT, _queue.queue_state::TEXT, 'queue is now marked DELETED');
  -- no longer insert
  -- no longer fetch rows
  -- no longer returned in lists

  RETURN NEXT is(TRUE, pgqs.delete_queue(_queue.queue_id),
                 'deleted queue by id to actually drop tables and delete queue meta');

  PERFORM TRUE FROM pgqs.queues WHERE queue_name = _queue_name;
  RETURN NEXT ok(NOT FOUND, 'no row for queue in pgqs.queues');

END
$body$;

CREATE FUNCTION
pgqs_test.test_new_message()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_name CONSTANT TEXT := 'my-queue';
  k_msg_body CONSTANT TEXT := 'some example body';
  res RECORD;
  _message_id UUID;
  _message RECORD;
  _message_res RECORD;
  _queue RECORD;
BEGIN
  PERFORM pgqs.create_queue(k_queue_name);

  _queue := pgqs.queue(k_queue_name);

  _message := pgqs.new_message(k_queue_name, k_msg_body, 10, CURRENT_TIMESTAMP);

  _message_res := pgqs.message(k_queue_name, _message.message_id);

  RETURN NEXT is(_message_res.message_body, k_msg_body, 'table has expected body');

END
$body$;

CREATE FUNCTION
pgqs_test.test_sqs_send_message()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_name CONSTANT TEXT := 'my-queue';
  _queue_url TEXT;
  _queue RECORD;
  _create_res jsonb;
  _send_req jsonb;
  _send_res jsonb;
  _recv_res jsonb;
  _recv_msg jsonb;
  _message RECORD;
BEGIN
  set role to pgqs_queue_admin;
  _create_res := pgqs.invoke('CreateQueue', jsonb_build_object('QueueName', k_queue_name));
  reset role;

  _queue_url := _create_res->>'QueueUrl';

  _send_req := jsonb_build_object('QueueUrl', _queue_url,
                                  'MessageBody', 'some body');

  set role to pgqs_queue_worker;
  _send_res := pgqs.invoke('SendMessage', _send_req);
  reset role;

  RETURN NEXT ok(NOT _send_res ? 'MessageAttributes',
                 'message with no attributes has no attributes in send response');

  RETURN NEXT ok(NOT _send_res ? 'MD5OfMessageAttributes',
                 'message with no attributes has no attributes md5 in send response');

  RETURN NEXT ok(NOT _send_res ? 'MD5OfMessageSystemAttributes',
                 'message with no system attributes has no system attributes md5 in send response');

  _message := pgqs.message(k_queue_name, CAST(_send_res->>'MessageId' AS UUID));
  RETURN NEXT is(_message.message_attributes, '{}'::jsonb,
                 'table has expected message attributes');
  RETURN NEXT is(_message.message_system_attributes, '{}'::jsonb,
                 'table has expected message system attributes');

  set role to pgqs_queue_worker;
  _recv_res := pgqs.invoke('ReceiveMessage',
                        jsonb_build_object('QueueUrl', _queue_url,
                                           'MaxNumberOfMessages', 1));
  reset role;

  _recv_msg := jsonb_path_query(_recv_res, '$.Messages[0]');
  RETURN NEXT ok(NOT (_recv_msg ? 'MessageAttributes'),
                 'message with no message attributes has no message attributes');

  RETURN NEXT ok(NOT (_recv_msg ? 'MD5OfMessageAttributes'),
                 'message with no message attributes has no MD5 of message attributes');

  RETURN NEXT ok(NOT (_recv_msg ? 'MD5OfMessageSystemAttributes'),
                 'message with system attributes attributes has no MD5 of message system attributes');

END
$body$;


CREATE FUNCTION
pgqs_test.test_sqs_send_message_with_message_attributes()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_name CONSTANT TEXT := 'my-queue';
  _queue_url TEXT;
  _queue RECORD;
  _create_res jsonb;
  _send_req jsonb;
  _send_res jsonb;
  _recv_res jsonb;
  _recv_msg jsonb;
  _message_attrs jsonb;
  _message_system_attrs jsonb;
  _message RECORD;
BEGIN
  set role to pgqs_queue_admin;
  _create_res := pgqs.invoke('CreateQueue', jsonb_build_object('QueueName', k_queue_name));
  reset role;

  _queue_url := _create_res->>'QueueUrl';

  _message_attrs := jsonb_build_object('MyKey',
                                       '{"StringValue": "MyStringValue", "DataType": "String"}'::jsonb,

                                       'MyBinaryValue',
                                       '{"BinaryValue": "bXktc3RyaW5n", "DataType": "Binary"}'::jsonb);
  _message_system_attrs
    := jsonb_build_object('otel-trace-id',
                          '{"StringValue": "4650b69b04574f71ae14d2033af4c4fe", "DataType": "String"}'::jsonb);

  _send_req := jsonb_build_object('QueueUrl', _queue_url,
                                  'MessageBody', 'some body',
                                  'MessageAttributes', _message_attrs,
                                  'MessageSystemAttributes', _message_system_attrs);
  set role to pgqs_queue_worker;
  _send_res := pgqs.invoke('SendMessage', _send_req);
  reset role;

  _message := pgqs.message(k_queue_name, CAST(_send_res->>'MessageId' AS UUID));
  RETURN NEXT is(_message.message_attributes, _message_attrs,
                 'table has expected message attributes');
  RETURN NEXT is(_message.message_system_attributes, _message_system_attrs,
                 'table has expected message system attributes');

  set role to pgqs_queue_worker;
  _recv_res := pgqs.invoke('ReceiveMessage',
                        jsonb_build_object('QueueUrl', _queue_url,
                                           'MaxNumberOfMessages', 1,
                                           'AttributeNames', ARRAY['All'],
                                           'MessageAttributeNames', ARRAY['All']));
  reset role;

  _recv_msg := jsonb_path_query(_recv_res, '$.Messages[0]');
  RETURN NEXT is(_recv_msg->'MessageAttributes', _message_attrs,
                 'have expected MessageAttributes when receiving message with message attributes');

  -- RETURN NEXT is(_recv_msg->'Attributes', _message_system_attrs,
  --                'have expected MessageAttributes when receiving message with message attributes');

  -- RETURN NEXT is((select array_agg(k ORDER BY k) FROM json_each(_recv_msg->'Attributes'), _message_system_attrs,
  --                'have expected MessageAttributes when receiving message with message attributes');

END
$body$;


CREATE FUNCTION
pgqs_test.test_attributes_md5()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
BEGIN
  RETURN QUERY
    SELECT is(pgqs.attributes_md5(attrs), expected, description)
      FROM (VALUES (jsonb_build_object('AWSTraceHeader',
                                      jsonb_build_object('BinaryValue', 'Zm9v',
                                                         'DataType', 'Binary')),
                    'bf2e06bc41ed0341574d06761977682a'::TEXT, 'have expected md5 for binary "foo"'),

                    (jsonb_build_object('some-key', jsonb_build_object('DataType', 'String', 'StringValue', 'some-val')),
                    '77c0282df3ce7098193f0f08c935cf7b', 'have expected md5 for simple string map'))
           AS _ (attrs, expected, description);
END
$body$;



CREATE FUNCTION
pgqs_test.test_sqs_send_message_batch()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_name CONSTANT TEXT := 'my-queue';
  _queue RECORD;
  _entries jsonb;
  _res jsonb;
  _req jsonb;
  _batch_results jsonb;
  _have REFCURSOR;
  _want REFCURSOR;
BEGIN
  PERFORM pgqs.create_queue(k_queue_name);
  _queue := pgqs.queue(k_queue_name);
  RETURN NEXT ok(_queue.queue_url IS NOT NULL, 'have queue_url');

  WITH entries ("Id", "MessageBody", "DelaySeconds")
    AS (VALUES ('a', 'some-body-a', 3),
               ('b', 'some-body-b', NULL)),
       objs (o) AS (SELECT jsonb_strip_nulls(row_to_json(e.*)::jsonb)
                  FROM entries AS e)
    SELECT INTO STRICT _entries
         array_to_json(array_agg(o))
    FROM objs;

  _req := jsonb_build_object('QueueUrl', _queue.queue_url, 'Entries', _entries);
  _res := pgqs.invoke('SendMessageBatch', _req);

  _batch_results
    := jsonb_build_object('Successful',
                          ARRAY[jsonb_build_object('Id', 'a',
                                                   'MD5OfMessageBody', 'c656d1e1229e559489f5fb167fdb8ba1'),
                                jsonb_build_object('Id', 'b',
                                                   'MD5OfMessageBody', '12124c2d42d27d5117a539dcab17b729')]);
  RETURN NEXT is(_res, _batch_results, 'have expected message batch results');

  PERFORM pgqs.update_queue_stats(k_queue_name);
  OPEN _have FOR
    SELECT _.message_count,
           _.stored_message_count, _.delayed_message_count,
           _.in_flight_message_count, _.invisible_message_count
      FROM pgqs.queue_stats AS _
      WHERE _.queue_id = _queue.queue_id;
  OPEN _want FOR
     VALUES (2::BIGINT,
             2::BIGINT, 1::BIGINT,
             0::BIGINT, 0::BIGINT);

  RETURN NEXT results_eq(_have, _want, 'have expected queue stats after send batch');


END
$body$;

CREATE FUNCTION
pgqs_test.test_sqs_validate_send_message_batch_request()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_name CONSTANT TEXT := 'my-queue';
  k_queue_url CONSTANT TEXT := pgqs.encode_queue_url(gen_random_uuid(), k_queue_name);
  _entries jsonb;
  _res RECORD;
  _req jsonb;
  _want jsonb;
  k_sql_fmt CONSTANT TEXT := 'SELECT pgqs.validate_send_message_batch_request(%L)';
  _sql TEXT;
BEGIN

  WITH
    entries ("Id", "MessageBody", "DelaySeconds")
            AS (VALUES ('a'::TEXT, 'some-body-a'::TEXT, 3::INT),
                       ('a', 'some-body-b', NULL)),
    objs (o) AS (SELECT jsonb_strip_nulls(row_to_json(e.*)::jsonb)
                   FROM entries AS e)
    SELECT INTO STRICT _entries
         array_to_json(array_agg(o))
    FROM objs;

  _req := jsonb_build_object('QueueUrl', k_queue_url, 'Entries', _entries);
  _sql := format(k_sql_fmt, _req);
  RETURN NEXT throws_matching(_sql, 'BatchEntryIdsNotDistinct');

  _req := jsonb_build_object('QueueUrl', k_queue_url, 'Entries', NULL::jsonb);
  _sql := format(k_sql_fmt, _req);
  RETURN NEXT throws_matching(_sql, 'Invalid');

  _req := jsonb_build_object('QueueUrl', k_queue_url, 'Entries', '[]'::jsonb);
  _sql := format(k_sql_fmt, _req);
  RETURN NEXT throws_matching(_sql, 'EmptyBatchRequest');

  WITH
    entries ("Id", "MessageBody", "DelaySeconds")
            AS (SELECT n::TEXT, n::TEXT, n
                  FROM generate_series(1,11) AS x(n)),
    objs (o) AS (SELECT jsonb_strip_nulls(row_to_json(e.*)::jsonb)
                   FROM entries AS e)
    SELECT INTO STRICT _entries
         array_to_json(array_agg(o))
    FROM objs;

  _req := jsonb_build_object('QueueUrl', k_queue_url, 'Entries', _entries);
  _sql := format(k_sql_fmt, _req);
  RETURN NEXT throws_matching(_sql, 'TooManyEntriesInBatchRequest');

  _req := jsonb_build_object('QueueUrl', k_queue_url, 'Entries', '[{"MessageBody":"foo"}]'::jsonb);
  _sql := format(k_sql_fmt, _req);
  RETURN NEXT throws_matching(_sql, 'malformed.*missing Id', 'have no Id');

  _req := jsonb_build_object('QueueUrl', k_queue_url, 'Entries', '[{"Id": "some-id"}]'::jsonb);
  _res := pgqs.validate_send_message_batch_request(_req);
  RETURN NEXT is(_res.entries, NULL::jsonb[], 'no entries with missing MessageBody');
  RETURN NEXT is(_res.failed[1],
                 '{"Id": "some-id", "Message": "Missing MessageBody", "SenderFault": true}'::jsonb,
                 'failed: missing MessageBody');

  _req := jsonb_build_object('QueueUrl', k_queue_url,
                             'Entries',
                             '[{"Id": "some-id", "MessageBody": "my message", "DelaySeconds": "a"}]'::jsonb);
  _res := pgqs.validate_send_message_batch_request(_req);
  RETURN NEXT is(_res.failed[1],
                 '{"Id": "some-id", "Message": "Invalid DelaySeconds", "SenderFault": true}'::jsonb,
                 'failed: missing MessageBody');

  _req := jsonb_build_object('QueueUrl', k_queue_url,
                             'Entries',
                             '[{"Id": "some-id", "MessageBody": "my message", "DelaySeconds": -4}]'::jsonb);
  _res := pgqs.validate_send_message_batch_request(_req);
  RETURN NEXT is(_res.failed[1],
                 '{"Id": "some-id", "Message": "Invalid DelaySeconds", "SenderFault": true}'::jsonb,
                 'failed: missing MessageBody');

  _req := jsonb_build_object('QueueUrl', k_queue_url,
                             'Entries',
                             '[{"Id": "some-id", "MessageBody": "my message", "DelaySeconds": 902}]'::jsonb);
  _res := pgqs.validate_send_message_batch_request(_req);
  RETURN NEXT is(_res.failed[1],
                 '{"Id": "some-id", "Message": "Invalid DelaySeconds", "SenderFault": true}'::jsonb,
                 'failed: missing MessageBody');

END
$body$;


CREATE FUNCTION
pgqs_test.test_sqs_create_queue()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_name CONSTANT TEXT := 'my-queue';
  _sqs_response jsonb;
  k_custom_queue_name CONSTANT TEXT := 'my-wild-queue';
  _custom_sqs_response jsonb;
  k_custom_queue_delay_seconds CONSTANT INT := 300;
  k_custom_queue_visibility_timeout_seconds CONSTANT INT := 600;
  k_custom_queue_maximum_message_size_bytes CONSTANT INT := 2048;
  _queue RECORD;
  _custom_queue RECORD;
  k_default_queue_visibility_timeout_seconds CONSTANT INT := pgqs.default_queue_visibility_timeout_seconds();
  k_default_queue_delay_seconds CONSTANT INT := pgqs.default_queue_delay_seconds();
  k_default_queue_maximum_message_size_bytes CONSTANT INT := pgqs.default_queue_maximum_message_size_bytes();
  _custom_queue_attributes jsonb;
BEGIN
  _sqs_response := pgqs.invoke('CreateQueue', jsonb_build_object('QueueName', k_queue_name));
  RETURN NEXT is(_sqs_response->>'QueueUrl', pgqs.encode_queue_url(k_queue_name),
                 'get expected queue-url in response');
  _queue := pgqs.queue(k_queue_name);

  RETURN NEXT is(_queue.queue_visibility_timeout_seconds, k_default_queue_visibility_timeout_seconds,
                 'have default visibility timeout seconds');
  RETURN NEXT is(_queue.queue_delay_seconds, k_default_queue_delay_seconds,
                 'have default delay seconds');
  RETURN NEXT is(_queue.queue_maximum_message_size_bytes, k_default_queue_maximum_message_size_bytes,
                 'have default maximum message size');

  _custom_sqs_response := pgqs.invoke('CreateQueue',
                                   jsonb_build_object('QueueName', k_custom_queue_name,
                                                      'DelaySeconds', k_custom_queue_delay_seconds,
                                                      'VisibilityTimeout', k_custom_queue_visibility_timeout_seconds,
                                                      'MaximumMessageSize', k_custom_queue_maximum_message_size_bytes));
  _custom_queue := pgqs.queue(k_custom_queue_name);
  RETURN NEXT isnt(_custom_queue.queue_visibility_timeout_seconds, k_default_queue_visibility_timeout_seconds,
                   'custom queue visibility timeout isn''t the default');
  RETURN NEXT isnt(_custom_queue.queue_delay_seconds, k_default_queue_delay_seconds,
                   'custom queue delay seconds isn''t the default');
  RETURN NEXT isnt(_custom_queue.queue_maximum_message_size_bytes, k_default_queue_maximum_message_size_bytes,
                   'custom queue maximum message size isn''t the default');

  RETURN NEXT is(_custom_queue.queue_visibility_timeout_seconds, k_custom_queue_visibility_timeout_seconds,
                 'have specified visibility timeout seconds');
  RETURN NEXT is(_custom_queue.queue_delay_seconds, k_custom_queue_delay_seconds,
                 'have specified delay seconds');
  RETURN NEXT is(_custom_queue.queue_maximum_message_size_bytes, k_custom_queue_maximum_message_size_bytes,
                 'have specified maximum message size');

END
$body$;

CREATE FUNCTION
pgqs_test.test_sqs_get_set_queue_attributes()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_name CONSTANT TEXT := 'my-wild-queue';
  _sqs_response jsonb;
  k_queue_delay_seconds CONSTANT INT := 302;
  k_queue_visibility_timeout_seconds CONSTANT INT := 602;
  k_queue_maximum_message_size_bytes CONSTANT INT := 2048;
  _queue RECORD;
  _queue_attributes jsonb;
  k_get_attributes_fmt CONSTANT TEXT := format('SELECT pgqs.invoke(%L,%%L)', 'GetQueueAttributes');
  _sql TEXT;
  _delay_seconds INT;
  _visibility_timeout_seconds INT;
  _maximum_message_size_bytes INT;
BEGIN
  _sqs_response := pgqs.invoke('CreateQueue',
                            jsonb_build_object('QueueName', k_queue_name,
                                               'DelaySeconds', k_queue_delay_seconds,
                                               'VisibilityTimeout', k_queue_visibility_timeout_seconds,
                                               'MaximumMessageSize', k_queue_maximum_message_size_bytes));
  _queue := pgqs.queue(k_queue_name);

  _sql := format(k_get_attributes_fmt, jsonb_build_object('QueueUrl', _queue.queue_url));
  RETURN NEXT throws_ok(_sql, 'Invalid Request', 'throws with no Attributes property');


  _sql := format(k_get_attributes_fmt, jsonb_build_object('QueueUrl', _queue.queue_url,
                                                          'AttributeNames', '{}'));
  RETURN NEXT throws_ok(_sql, 'Invalid Request', 'throws with empty attributes');

  _sql := format(k_get_attributes_fmt, jsonb_build_object('QueueUrl', _queue.queue_url,
                                                          'AttributeNames', 1));
  RETURN NEXT throws_ok(_sql, 'Invalid Request', 'throws when AttributeNames is not an array');

  _sql := format(k_get_attributes_fmt, jsonb_build_object('QueueUrl', _queue.queue_url,
                                                          'AttributeNames', ARRAY['Nonesuch']));
  RETURN NEXT throws_ok(_sql, 'InvalidAttributeName', 'throws when throwing garbage at GetQueueAttributes');

  _queue_attributes := pgqs.invoke('GetQueueAttributes',
                                       jsonb_build_object('QueueUrl', _queue.queue_url,
                                                          'AttributeNames', ARRAY['All']));
  RETURN NEXT ok(_queue_attributes->'Attributes'
                 @> jsonb_build_object('VisibilityTimeout', k_queue_visibility_timeout_seconds,
                                       'MaximumMessageSize', k_queue_maximum_message_size_bytes,
                                       'ApproximateNumberOfMessages', 0,
                                       'ApproximateNumberOfMessagesNotVisible', 0,
                                       -- these will vary by test run
                                       -- 'CreatedTimestamp', 0, -- epoch seconds
                                       -- 'LastModifiedTimestamp', 0, -- epoch seconds
                                       'ApproximateNumberOfMessagesDelayed', 0,
                                       'DelaySeconds', k_queue_delay_seconds),
                 'have expected queue attributes');

  RETURN NEXT ok(_queue_attributes->'Attributes' ? 'CreatedTimestamp', 'attributes include CreatedTimestamp');
  RETURN NEXT ok(_queue_attributes->'Attributes' ? 'LastTimestamp', 'attributes include LastTimestamp');

  _queue_attributes := pgqs.invoke('GetQueueAttributes',
                                       jsonb_build_object('QueueUrl', _queue.queue_url,
                                                          'AttributeNames', ARRAY['VisibilityTimeout', 'DelaySeconds']));
  RETURN NEXT is(_queue_attributes,
                 jsonb_build_object('Attributes',
                                    jsonb_build_object('VisibilityTimeout', k_queue_visibility_timeout_seconds,
                                                       'DelaySeconds', k_queue_delay_seconds)),
                 'have expected queue attributes when specifying a subset');

  _delay_seconds := 35;
  _visibility_timeout_seconds := 65;
  _maximum_message_size_bytes := 2048;
  PERFORM pgqs.invoke('SetQueueAttributes',
                   jsonb_build_object('QueueUrl', _queue.queue_url,
                                     'Attributes',
                                     jsonb_build_object('DelaySeconds', _delay_seconds,
                                                        'VisibilityTimeout', _visibility_timeout_seconds,
                                                        'MaximumMessageSize', _maximum_message_size_bytes)));
  _queue_attributes := pgqs.invoke('GetQueueAttributes',
                                 jsonb_build_object('QueueUrl', _queue.queue_url,
                                                    'AttributeNames',
                                                     ARRAY['VisibilityTimeout',
                                                           'DelaySeconds',
                                                           'MaximumMessageSize']));
  RETURN NEXT is(_queue_attributes,
                 jsonb_build_object('Attributes',
                                    jsonb_build_object('DelaySeconds', _delay_seconds,
                                                       'VisibilityTimeout', _visibility_timeout_seconds,
                                                       'MaximumMessageSize', _maximum_message_size_bytes)),
                 'have expected queue attributes after updating subset');
END
$body$;



CREATE FUNCTION
pgqs_test.test_sqs_change_message_visibility()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_name CONSTANT TEXT := 'my-queue';
  k_queue_url CONSTANT TEXT := pgqs.encode_queue_url(k_queue_name);
  _sqs_response jsonb;
  _receive_message_response jsonb;
  _received_message jsonb;
  _change_visibility_response jsonb;
  _sent_message_response jsonb;
  _received_receipt_token UUID;
  _message_after_received RECORD;
  _message_after_changed RECORD;
  _change_sql TEXT;
BEGIN
  _sqs_response := pgqs.invoke('CreateQueue', jsonb_build_object('QueueName', k_queue_name));
  _sent_message_response := pgqs.invoke('SendMessage',
                                      jsonb_build_object('QueueUrl', k_queue_url,
                                                         'MessageBody', 'some body'));
  _receive_message_response
     := pgqs.invoke('ReceiveMessage',
                 jsonb_build_object('QueueUrl', k_queue_url,
                                    'MaxNumberOfMessages', 1));
  _received_message := jsonb_path_query(_receive_message_response, '$.Messages[0]');
  _received_receipt_token := (pgqs.decode_receipt_handle(_received_message->>'ReceiptHandle')).message_receipt_token;
  _message_after_received := pgqs.message(k_queue_name, _sent_message_response->>'MessageId');

  RETURN NEXT is(_message_after_received.message_receipt_token, _received_receipt_token,
                 'received token is same as sent message');

  _change_visibility_response
     := pgqs.invoke('ChangeMessageVisibility',
                 jsonb_build_object('QueueUrl', k_queue_url,
                                    'ReceiptHandle', _received_message->>'ReceiptHandle',
                                    'VisibilityTimeout', 0));
  _message_after_changed := pgqs.message(k_queue_name, _sent_message_response->>'MessageId');

  RETURN NEXT isnt(_message_after_changed.message_visible_at,
                   _message_after_received.message_visible_at, 'message_visible_at changed after changing visibility');

  _change_sql := format('SELECT * FROM pgqs.invoke(%L, CAST(%L AS jsonb))',
                        'ChangeMessageVisibility',
                        jsonb_build_object('QueueUrl', k_queue_url,
                                           'ReceiptHandle', _received_message->>'ReceiptHandle',
                                           'VisibilityTimeout', 100));
  RETURN NEXT throws_matching(_change_sql, 'MessageNotInflight',
                              'throws when message is no longer in flight');
END
$body$;

CREATE FUNCTION
pgqs_test.test_sqs_change_message_visibility_batch()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_name CONSTANT TEXT := 'my-queue';
  _queue RECORD;
  _entries jsonb;
  _entry_rec RECORD;
  _send_batch_req jsonb;
  _send_batch_res jsonb;
  _receive_req jsonb;
  _receive_res jsonb;
  _change_msg_batch_req jsonb;
  _change_msg_batch_res jsonb;
  k_change_msg_batch_fmt TEXT := format('SELECT pgqs.invoke(%L,%%L)', 'ChangeMessageVisibilityBatch');
  _sql TEXT;
  _have REFCURSOR;
  _want REFCURSOR;
BEGIN
  PERFORM pgqs.create_queue(k_queue_name);
  _queue := pgqs.queue(k_queue_name);
  RETURN NEXT ok(_queue.queue_url IS NOT NULL, 'have queue_url');

  WITH entries ("Id", "MessageBody")
    AS (VALUES ('a', 'some-body-a'),
               ('b', 'some-body-b')),
       objs (o) AS (SELECT jsonb_strip_nulls(row_to_json(e.*)::jsonb)
                  FROM entries AS e)
    SELECT INTO STRICT _entries
         array_to_json(array_agg(o))
    FROM objs;

  _send_batch_req := jsonb_build_object('QueueUrl', _queue.queue_url, 'Entries', _entries);
  _send_batch_res := pgqs.invoke('SendMessageBatch', _send_batch_req);

  _receive_res := pgqs.invoke('ReceiveMessage',
                           jsonb_build_object('QueueUrl', _queue.queue_url,
                                              'MaxNumberOfMessages', 10));

  -- PERFORM pgqs.update_queue_stats(k_queue_name);
  -- OPEN _have FOR
  --   SELECT _.message_count,
  --          _.stored_message_count, _.delayed_message_count,
  --          _.in_flight_message_count, _.invisible_message_count
  --     FROM pgqs.queue_stats AS _
  --     WHERE _.queue_id = _queue.queue_id;
  -- OPEN _want FOR
  --    VALUES (2::BIGINT,
  --            0::BIGINT, 0::BIGINT,
  --            2::BIGINT, 0::BIGINT);


  -- RETURN NEXT results_eq(_have, _want, 'have expected initial message stats');

  -- CLOSE _have;
  -- CLOSE _want;

  WITH entries ("ReceiptHandle", "Id", "VisibilityTimeout")
         AS (SELECT e->>'ReceiptHandle', ROW_NUMBER() OVER (), 30
               FROM jsonb_array_elements(_receive_res->'Messages') AS _ (e)),
       objs (o) AS (SELECT row_to_json(e.*)::jsonb
                     FROM entries AS e)
    SELECT INTO STRICT _entries
           array_to_json(array_agg(o))
      FROM objs;

  _change_msg_batch_req := jsonb_build_object('QueueUrl', _queue.queue_url,
                                          'Entries', _entries);
  _change_msg_batch_res := pgqs.invoke('ChangeMessageVisibilityBatch', _change_msg_batch_req);
  RETURN NEXT is(_change_msg_batch_res, '{"Successful": [{"Id": 1}, {"Id": 2}]}'::jsonb,
                 'have expected ChangeMessageVisibilityBatch results');

END
$body$;


CREATE FUNCTION
pgqs_test.test_sqs_get_queue_url()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_queue_name CONSTANT TEXT := 'my-queue';
  k_msg_body CONSTANT TEXT := 'some example body';
  _queue RECORD;
  _count_sql TEXT;
  _count BIGINT;
  _message RECORD;
  _create_queue_response jsonb;
  _get_queue_url_response jsonb;
  k_sqs_sql_fmt CONSTANT TEXT := 'SELECT pgqs.invoke(%L, %L)';
  _sql TEXT;
BEGIN
  _create_queue_response := pgqs.invoke('CreateQueue', jsonb_build_object('QueueName', k_queue_name));

  _get_queue_url_response := pgqs.invoke('GetQueueUrl', jsonb_build_object('QueueName', k_queue_name));

   PERFORM pgqs.invoke('DeleteQueue',
                    jsonb_build_object('QueueUrl', _get_queue_url_response->>'QueueUrl'));

  _sql := format(k_sqs_sql_fmt, 'GetQueueUrl', jsonb_build_object('QueueName', k_queue_name));
  RETURN NEXT throws_matching(_sql, 'NonExistentQueue', 'getting url of deleted queue throws');

  _sql := format(k_sqs_sql_fmt, 'GetQueueUrl', jsonb_build_object('QueueName', 'no-such-queue'));
  RETURN NEXT throws_matching(_sql, 'NonExistentQueue', 'getting url of non-existing queue throws');

END
$body$;


CREATE FUNCTION
pgqs_test.test_sqs_receive_message()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_queue_name CONSTANT TEXT := 'my-queue';
  k_msg_body CONSTANT TEXT := 'some example body';
  _queue RECORD;
  _count_sql TEXT;
  _visible_count_sql TEXT;
  _count BIGINT;
  _new_message RECORD;
  _message jsonb;
  _send_message_response jsonb;
  _receive_message_response jsonb;
BEGIN
  PERFORM pgqs.invoke('CreateQueue', jsonb_build_object('QueueName', k_queue_name));

  _queue := pgqs.queue(k_queue_name);

  _send_message_response
    := pgqs.invoke('SendMessage',
                jsonb_build_object('QueueUrl', _queue.queue_url,
                                   'MessageBody', k_msg_body));

  _count_sql := format('SELECT COUNT(*) FROM %I.%I',
                       k_schema_name, _queue.queue_active_message_table_name);

  EXECUTE _count_sql INTO _count;

  RETURN NEXT is(_count, 1::BIGINT, 'have expected inserted message count');

  _receive_message_response
    := pgqs.invoke('ReceiveMessage', jsonb_build_object('QueueUrl', _queue.queue_url,
                                                     'AttributeNames', ARRAY['All'],
                                                     'MaxNumberOfMessages', 1));

  _message := jsonb_path_query(_receive_message_response, '$.Messages[0]');

  RETURN NEXT is((SELECT array_agg(a ORDER BY a)
                    FROM jsonb_object_keys(_message->'Attributes') AS _(a)),
                 ARRAY['ApproximateFirstReceiveTimestamp',
                       'ApproximateReceiveCount',
                       'SentTimestamp'],
                'have expected message with attributes');

  PERFORM pgqs.delete_message(k_queue_name, _message->>'ReceiptHandle');

  PERFORM pgqs.invoke('SendMessage',
                   jsonb_build_object('QueueUrl', _queue.queue_url,
                                      'MessageBody', k_msg_body));

  _receive_message_response
    := pgqs.invoke('ReceiveMessage', jsonb_build_object('QueueUrl', _queue.queue_url,
                                                     'AttributeNames', ARRAY['SentTimestamp'],
                                                     'MaxNumberOfMessages', 1));
  _message := jsonb_path_query(_receive_message_response, '$.Messages[0]');
  RETURN NEXT is((SELECT array_agg(a ORDER BY a)
                    FROM jsonb_object_keys(_message->'Attributes') AS _(a)),
                 ARRAY['SentTimestamp'],
                'have expected message with specified attributes');

END
$body$;

CREATE FUNCTION
pgqs_test.test_sqs_delete_message()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_queue_name CONSTANT TEXT := 'my-queue';
  k_msg_body CONSTANT TEXT := 'some example body';
  _queue RECORD;
  _count_sql TEXT;
  _visible_count_sql TEXT;
  _count BIGINT;
  _new_message RECORD;
  _message jsonb;
  _send_message_response jsonb;
  _receive_message_response jsonb;
BEGIN
  PERFORM pgqs.invoke('CreateQueue', jsonb_build_object('QueueName', k_queue_name));

  _queue := pgqs.queue(k_queue_name);

  _send_message_response
    := pgqs.invoke('SendMessage',
                jsonb_build_object('QueueUrl', _queue.queue_url,
                                   'MessageBody', k_msg_body));

  _count_sql := format('SELECT COUNT(*) FROM %I.%I',
                       k_schema_name, _queue.queue_active_message_table_name);

  EXECUTE _count_sql INTO _count;

  RETURN NEXT is(_count, 1::BIGINT, 'have expected inserted message count');

  _receive_message_response
    := pgqs.invoke('ReceiveMessage', jsonb_build_object('QueueUrl', _queue.queue_url,
                                                     'MaxNumberOfMessages', 1));

  _message := jsonb_path_query(_receive_message_response, '$.Messages[0]');

  _visible_count_sql := format('SELECT COUNT(*) FROM %I.%I WHERE message_visible_at <= $1',
                       k_schema_name, _queue.queue_active_message_table_name);
  EXECUTE _visible_count_sql INTO _count USING CURRENT_TIMESTAMP;
  RETURN NEXT is(_count, 0::BIGINT, 'have no visible messages');

  PERFORM pgqs.invoke('DeleteMessage',
                   jsonb_build_object('QueueUrl', _queue.queue_url,
                                      'ReceiptHandle', _message->'ReceiptHandle'));

  EXECUTE _count_sql INTO _count;
  RETURN NEXT is(_count, 0::BIGINT, 'no messages after deleted');
END
$body$;

CREATE FUNCTION
pgqs_test.test_sqs_delete_message_batch()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  -- k_schema_name CONSTANT TEXT := 'pgqs';
  k_queue_name CONSTANT TEXT := 'my-queue';
  -- k_msg_body CONSTANT TEXT := 'some example body';
  _queue RECORD;
  _entries jsonb;
  _entry_rec RECORD;
  _send_batch_req jsonb;
  _send_batch_res jsonb;
  _receive_req jsonb;
  _receive_res jsonb;
  _delete_batch_req jsonb;
  _delete_batch_res jsonb;
  k_delete_batch_fmt TEXT := format('SELECT pgqs.invoke(%L,%%L)', 'DeleteMessageBatch');
  _sql TEXT;
  _have REFCURSOR;
  _want REFCURSOR;
BEGIN
  PERFORM pgqs.create_queue(k_queue_name);
  _queue := pgqs.queue(k_queue_name);
  RETURN NEXT ok(_queue.queue_url IS NOT NULL, 'have queue_url');

  WITH entries ("Id", "MessageBody")
    AS (VALUES ('a', 'some-body-a'),
               ('b', 'some-body-b')),
       objs (o) AS (SELECT jsonb_strip_nulls(row_to_json(e.*)::jsonb)
                  FROM entries AS e)
    SELECT INTO STRICT _entries
         array_to_json(array_agg(o))
    FROM objs;

  _send_batch_req := jsonb_build_object('QueueUrl', _queue.queue_url, 'Entries', _entries);
  _send_batch_res := pgqs.invoke('SendMessageBatch', _send_batch_req);

  _receive_res := pgqs.invoke('ReceiveMessage',
                           jsonb_build_object('QueueUrl', _queue.queue_url,
                                              'MaxNumberOfMessages', 10));

  PERFORM pgqs.update_queue_stats(k_queue_name);
  OPEN _have FOR
    SELECT _.message_count,
           _.stored_message_count, _.delayed_message_count,
           _.in_flight_message_count, _.invisible_message_count
      FROM pgqs.queue_stats AS _
      WHERE _.queue_id = _queue.queue_id;
  OPEN _want FOR
     VALUES (2::BIGINT,
             0::BIGINT, 0::BIGINT,
             2::BIGINT, 0::BIGINT);

  WITH entries ("ReceiptHandle", "Id")
         AS (SELECT e->>'ReceiptHandle', ROW_NUMBER() OVER ()
               FROM jsonb_array_elements(_receive_res->'Messages') AS _ (e)),
       objs (o) AS (SELECT row_to_json(e.*)::jsonb
                     FROM entries AS e)
    SELECT INTO STRICT _entries
           array_to_json(array_agg(o))
      FROM objs;

  RETURN NEXT results_eq(_have, _want, 'have expected initial message stats');

  CLOSE _have;
  CLOSE _want;

  _delete_batch_req := jsonb_build_object('QueueUrl', _queue.queue_url,
                                          'Entries', _entries);
  _delete_batch_res := pgqs.invoke('DeleteMessageBatch', _delete_batch_req);
  RETURN NEXT is(_delete_batch_res, '{"Successful": [{"Id": 1}, {"Id": 2}]}'::jsonb,
                 'have expected DeleteMessageBatch results');

  PERFORM pgqs.update_queue_stats(k_queue_name);
  OPEN _have FOR
    SELECT _.message_count,
           _.stored_message_count, _.delayed_message_count,
           _.in_flight_message_count, _.invisible_message_count
      FROM pgqs.queue_stats AS _
      WHERE _.queue_id = _queue.queue_id;
  OPEN _want FOR
     VALUES (0::BIGINT,
             0::BIGINT, 0::BIGINT,
             0::BIGINT, 0::BIGINT);

  WITH entries ("ReceiptHandle", "Id")
         AS (SELECT e->>'ReceiptHandle', ROW_NUMBER() OVER ()
               FROM jsonb_array_elements(_receive_res->'Messages') AS _ (e)),
       objs (o) AS (SELECT row_to_json(e.*)::jsonb
                     FROM entries AS e)
    SELECT INTO STRICT _entries
           array_to_json(array_agg(o))
      FROM objs;

  RETURN NEXT results_eq(_have, _want, 'all messages are gone');

  CLOSE _have;
  CLOSE _want;


  _delete_batch_req := json_build_object('QueueUrl', _queue.queue_url);
  _sql := format(k_delete_batch_fmt, _delete_batch_req);
  RETURN NEXT throws_ok(_sql, 'EmptyBatchRequest', 'throws with no Entries property');

  _delete_batch_req := json_build_object('QueueUrl', _queue.queue_url,
                                         'Entries', '{}'::jsonb);
  _sql := format(k_delete_batch_fmt, _delete_batch_req);
  RETURN NEXT throws_ok(_sql, 'Invalid request', 'throws when Entries is not an array');

  _delete_batch_req := json_build_object('QueueUrl', _queue.queue_url,
                                         'Entries', '[]'::jsonb);
  _sql := format(k_delete_batch_fmt, _delete_batch_req);
  RETURN NEXT throws_ok(_sql, 'EmptyBatchRequest', 'throws with empty Entries array');

  WITH entries ("ReceiptHandle", "Id")
         AS (VALUES ('dummy-1', 1), ('dummy-2', 1)),
       objs (o) AS (SELECT row_to_json(e.*)::jsonb
                     FROM entries AS e)
    SELECT INTO STRICT _entries
           array_to_json(array_agg(o))
      FROM objs;
  _delete_batch_req := json_build_object('QueueUrl', _queue.queue_url,
                                         'Entries', _entries);
  _sql := format(k_delete_batch_fmt, _delete_batch_req);
  RETURN NEXT throws_ok(_sql, 'BatchEntryIdsNotDistinct', 'throws with non-distinct batch entry ids');

END
$body$;

CREATE FUNCTION
pgqs_test.test_empty_value_tags ()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
BEGIN
  RETURN NEXT is(pgqs.empty_value_tags('{"foo": "bar", "baz": "", "quux": ""}'::jsonb), ARRAY['baz', 'quux']);
  RETURN NEXT is(pgqs.empty_value_tags('{}'::jsonb), '{}'::TEXT[]);
  RETURN NEXT is(pgqs.empty_value_tags('{"foo": "bar", "baz": "bat"}'::jsonb), '{}'::TEXT[]);
END
$body$;

CREATE FUNCTION
pgqs_test.test_tag_queue()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_name CONSTANT TEXT := 'this-queue';
  _create_queue_res jsonb;
  _queue_url TEXT;
  _tag_queue_req jsonb;
  _tag_queue_res jsonb;
  _list_tags_res jsonb;
  _untag_queue_req jsonb;
BEGIN
  _create_queue_res := pgqs.invoke('CreateQueue', jsonb_build_object('QueueName', k_queue_name));
  _queue_url := _create_queue_res->>'QueueUrl';
  _list_tags_res := pgqs.invoke('ListQueueTags', jsonb_build_object('QueueUrl', _queue_url));
  RETURN NEXT is(_list_tags_res->'Tags', '{}'::jsonb, 'start out with empty tags');
  _tag_queue_req := jsonb_build_object('QueueUrl', _queue_url,
                                       'Tags', '{"foo": "bar", "baz": "bat"}'::jsonb);
  _tag_queue_res := pgqs.invoke('TagQueue', _tag_queue_req);
  _list_tags_res := pgqs.invoke('ListQueueTags', jsonb_build_object('QueueUrl', _queue_url));
  RETURN NEXT is(_list_tags_res->'Tags', _tag_queue_req->'Tags', 'have tags after tagging queue');

  _tag_queue_req := jsonb_build_object('QueueUrl', _queue_url,
                                       'Tags', '{"foo": "bar", "baz": "", "quux": "blurfl"}'::jsonb);
  _tag_queue_res := pgqs.invoke('TagQueue', _tag_queue_req);
  _list_tags_res := pgqs.invoke('ListQueueTags', jsonb_build_object('QueueUrl', _queue_url));
  RETURN NEXT is(_list_tags_res->'Tags', '{"foo":"bar", "quux":"blurfl"}'::jsonb,
                 'have tags after tagging queue with empty-string value');
  _untag_queue_req := jsonb_build_object('QueueUrl', _queue_url,
                                         'TagKeys', jsonb_build_array('foo', 'quux'));
  PERFORM pgqs.invoke('UntagQueue', _untag_queue_req);
  _list_tags_res := pgqs.invoke('ListQueueTags', jsonb_build_object('QueueUrl', _queue_url));
  RETURN NEXT is(_list_tags_res->'Tags', '{}'::jsonb, 'No more tags after untagging!');

END
$body$;

CREATE FUNCTION
pgqs_test.test_list_queues()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_names CONSTANT TEXT[] := ARRAY['delta', 'alpha', 'charles', 'bravo', 'charlie'];
  _queue_name TEXT;
  k_null_max_results CONSTANT INT := NULL;
  k_null_next_token CONSTANT TEXT := NULL;
  k_null_queue_name_prefix CONSTANT TEXT := NULL;
  _res RECORD;
BEGIN
  FOREACH _queue_name IN ARRAY k_queue_names
  LOOP
    PERFORM pgqs.create_queue(_queue_name);
  END LOOP;

  _res := pgqs.list_queues(2, k_null_queue_name_prefix, k_null_next_token);

  RETURN NEXT is(_res.queue_names, ARRAY['alpha', 'bravo'], 'have expected queue names');

  _res := pgqs.list_queues(4, k_null_queue_name_prefix, _res.next_token);

  RETURN NEXT is(_res.queue_names, ARRAY['charles', 'charlie', 'delta'], 'have expected queue names');

  _res := pgqs.list_queues(k_null_max_results, 'char', k_null_next_token);

  RETURN NEXT is(_res.queue_names, ARRAY['charles', 'charlie'], 'have expected queue names');

  PERFORM pgqs.delete_queue('charles');

 SELECT INTO STRICT _res
        *
    FROM pgqs.queues
    WHERE queue_name = 'charles';
  RETURN NEXT is('DELETED'::pgqs.queue_state::TEXT, _res.queue_state::TEXT, 'queue is marked deleted, but still present');

  _res := pgqs.list_queues(k_null_max_results, k_null_queue_name_prefix, k_null_next_token);

  RETURN NEXT is(_res.queue_names, ARRAY['alpha', 'bravo', 'charlie', 'delta'], 'queue marked deleted is not returned');

END
$body$;

CREATE FUNCTION
pgqs_test.test_sqs_list_queues()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_names CONSTANT TEXT[] := ARRAY['delta', 'alpha', 'charles', 'bravo', 'charlie'];
  _queue_name TEXT;
  k_null_max_results CONSTANT INT := NULL;
  k_null_next_token CONSTANT TEXT := NULL;
  k_null_queue_name_prefix CONSTANT TEXT := NULL;
  _res RECORD;
  _list_queues_response jsonb;
BEGIN
  FOREACH _queue_name IN ARRAY k_queue_names
  LOOP
    PERFORM pgqs.create_queue(_queue_name);
  END LOOP;

  _list_queues_response := pgqs.invoke('ListQueues', '{"MaxResults": 2}');

  RETURN NEXT is(_list_queues_response->'QueueUrls',
                 (SELECT array_to_json(array_agg(pgqs.encode_queue_url(queue_name)))::jsonb
                    FROM unnest(ARRAY['alpha', 'bravo']) as _ (queue_name)),
                 'have expected queue names first fetch (returning NextToken)');

  _list_queues_response
    := pgqs.invoke('ListQueues',
                jsonb_build_object('MaxResults', 4,
                                   'NextToken', _list_queues_response->>'NextToken'));
  RETURN NEXT is(_list_queues_response->'QueueUrls',
                 (SELECT array_to_json(array_agg(pgqs.encode_queue_url(queue_name)))::jsonb
                    FROM unnest(ARRAY['charles', 'charlie', 'delta']) as _ (queue_name)),
                 'have expected queue names using NextToken');

  _list_queues_response := pgqs.invoke('ListQueues', '{"MaxResults": 10}');
  RETURN NEXT is(_list_queues_response->'QueueUrls',
                 (SELECT array_to_json(array_agg(pgqs.encode_queue_url(queue_name)))::jsonb
                    FROM unnest(ARRAY['alpha', 'bravo', 'charles', 'charlie', 'delta']) as _ (queue_name)),
                 'have expected queue names with MaxResults more than total number of queues)');

  _list_queues_response := pgqs.invoke('ListQueues', '{}');
  RETURN NEXT is(_list_queues_response->'QueueUrls',
                 (SELECT array_to_json(array_agg(pgqs.encode_queue_url(queue_name)))::jsonb
                    FROM unnest(ARRAY['alpha', 'bravo', 'charles', 'charlie', 'delta']) as _ (queue_name)),
                 'have expected queue names when omitting MaxResults)');

  _list_queues_response := pgqs.invoke('ListQueues', jsonb_build_object('QueueNamePrefix', 'char'));

  RETURN NEXT is(_list_queues_response->'QueueUrls',
                 (SELECT array_to_json(array_agg(pgqs.encode_queue_url(queue_name)))::jsonb
                    FROM unnest(ARRAY['charles', 'charlie']) as _ (queue_name)),
                 'have expected queue names using QueueNamePrefix');

  PERFORM pgqs.invoke('DeleteQueue',
                   jsonb_build_object('QueueUrl', (pgqs.queue('charles')).queue_url));

  SELECT INTO STRICT _res
         *
    FROM pgqs.queues
    WHERE queue_name = 'charles';
  RETURN NEXT is('DELETED'::pgqs.queue_state::TEXT,
                 _res.queue_state::TEXT, 'queue is marked deleted, but still present');

  _list_queues_response := pgqs.invoke('ListQueues', '{}'::jsonb);

  RETURN NEXT is(_list_queues_response->'QueueUrls',
                 (SELECT array_to_json(array_agg(pgqs.encode_queue_url(queue_name)))::jsonb
                    FROM unnest(ARRAY['alpha', 'bravo', 'charlie', 'delta']) as _ (queue_name)),
                 'queue marked deleted is not returned');

END
$body$;


CREATE FUNCTION
pgqs_test.test_maintenance_ops()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  _as_of TIMESTAMPTZ := CURRENT_TIMESTAMP + interval '3 hours';
  _create_res jsonb;
  _have REFCURSOR;
  _want REFCURSOR;
  _response jsonb;
  _op_map jsonb;
BEGIN
  _create_res := pgqs.invoke('CreateQueue', '{"QueueName": "my-queue"}'::jsonb);

  PERFORM pgqs.invoke('CreateQueue', '{"QueueName": "do-not-delete"}'::jsonb);

  PERFORM pgqs.invoke('DeleteQueue', jsonb_build_object('QueueUrl', _create_res->>'QueueUrl'));

  OPEN _want FOR
    VALUES ('CleanUpDeletedQueue', '{"QueueName": "my-queue"}'::jsonb),
           ('RotateMessageTables', '{"QueueName": "do-not-delete"}');

  OPEN _have FOR
    SELECT op, request
      FROM pgqs.maintenance_operations(_as_of);

  RETURN NEXT results_eq(_have, _want, 'have expected maintenance operations');

  set role to pgqs_maintenance;
  _response := pgqs.invoke_maintenance('GetMaintenanceOperations', NULL::jsonb);
  FOR _op_map IN
    SELECT * FROM jsonb_array_elements(_response->'Operations')
  LOOP
    RAISE NOTICE '% % % %', _op_map->>'op', _op_map->'request', pg_typeof((_op_map->'request')), _op_map->'request'->>'QueueName';
    PERFORM pgqs.invoke_maintenance(_op_map->>'op', _op_map->'request');
  END LOOP;

END
$body$;

CREATE FUNCTION
pgqs_test.test_rotation()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_queue_name CONSTANT TEXT := 'my-queue';
  -- k_active CONSTANT TEXT := 'ACTIVE';
  -- k_draining CONSTANT TEXT := 'DRAINING';
  -- k_pending_draining CONSTANT TEXT := 'PENDING_DRAINING';
  -- k_pending CONSTANT TEXT := 'PENDING';
  _queue RECORD;
  _have REFCURSOR;
  _want REFCURSOR;
BEGIN
  PERFORM pgqs.create_queue(k_queue_name);

  _queue := pgqs.queue(k_queue_name);

  -- initial message table states

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.message_tables
      WHERE queue_id = _queue.queue_id
      ORDER BY message_table_idx;
  OPEN _want FOR
    VALUES (1, 'ACTIVE'),
           (2, 'PENDING'),
           (3, 'DRAINING');
  RETURN NEXT results_eq(_have, _want, 'have expected initial message table states');
  CLOSE _have;
  CLOSE _want;

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.visible_message_tables(_queue.queue_id);
  OPEN _want FOR
    VALUES (3, 'DRAINING'),
           (1, 'ACTIVE');
  RETURN NEXT results_eq(_have, _want, 'have expected initial visible message table states');
  CLOSE _have;
  CLOSE _want;

  -- 1 - first rotation after initial set up
  RETURN NEXT is(pgqs.maybe_rotate_message_tables_now(k_queue_name)::TEXT, 'PENDING_DRAINING',
                 'did rotate once');

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.message_tables
      WHERE queue_id = _queue.queue_id
      ORDER BY message_table_idx;
  OPEN _want FOR
    VALUES (1, 'DRAINING'),
           (2, 'ACTIVE'),
           (3, 'PENDING_DRAINING');
  RETURN NEXT results_eq(_have, _want,
                         'have expected message table states after one rotate call');
  CLOSE _have;
  CLOSE _want;

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.visible_message_tables(_queue.queue_id);
  OPEN _want FOR
    VALUES (3, 'PENDING_DRAINING'),
           (1, 'DRAINING'),
           (2, 'ACTIVE');

  RETURN NEXT results_eq(_have, _want,
                         'have expected visible message table states after one rotate call');
  CLOSE _have;
  CLOSE _want;

  RETURN NEXT is((pgqs.queue(k_queue_name)).queue_active_message_table_idx,
                 2::SMALLINT, 'have expected next table');


  -- 2 - second rotation call
  RETURN NEXT is(pgqs.maybe_rotate_message_tables_now(k_queue_name)::TEXT, 'PENDING',
                 'PENDING after second rotation call');

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.message_tables
      WHERE queue_id = _queue.queue_id
      ORDER BY message_table_idx;
  OPEN _want FOR
    VALUES (1, 'DRAINING'),
           (2, 'ACTIVE'),
           (3, 'PENDING');
  RETURN NEXT results_eq(_have, _want,
                         '2 - have expected message table states after 2nd rotation call');
  CLOSE _have;
  CLOSE _want;

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.visible_message_tables(_queue.queue_id);
  OPEN _want FOR
    VALUES (1, 'DRAINING'),
           (2, 'ACTIVE');

  RETURN NEXT results_eq(_have, _want,
                         '2 - have expected visible message table states after 2nd rotation');
  CLOSE _have;
  CLOSE _want;

  RETURN NEXT is((pgqs.queue(k_queue_name)).queue_active_message_table_idx,
                 2::SMALLINT, 'have unchanged next table');

  -- 3 - third rotation call
  RETURN NEXT is(pgqs.maybe_rotate_message_tables_now(k_queue_name), 'PENDING_DRAINING',
                 '3 - did third rotation call');

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.message_tables
      WHERE queue_id = _queue.queue_id
      ORDER BY message_table_idx;
  OPEN _want FOR
    VALUES (1, 'PENDING_DRAINING'),
           (2, 'DRAINING'),
           (3, 'ACTIVE');
  RETURN NEXT results_eq(_have, _want,
                         '3 - have expected message table states after third rotation call');
  CLOSE _have;
  CLOSE _want;

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.visible_message_tables(_queue.queue_id);
  OPEN _want FOR
    VALUES (1, 'PENDING_DRAINING'),
           (2, 'DRAINING'),
           (3, 'ACTIVE');

  RETURN NEXT results_eq(_have, _want,
                         '3 - have expected visible message table states after third rotation call');
  CLOSE _have;
  CLOSE _want;

  RETURN NEXT is((pgqs.queue(k_queue_name)).queue_active_message_table_idx,
                 3::SMALLINT, 'have expected next table');

  -- 4 - fourth rotation call
  RETURN NEXT is(pgqs.maybe_rotate_message_tables_now(k_queue_name), 'PENDING',
                 '4 - did fourth rotation call');

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.message_tables
      WHERE queue_id = _queue.queue_id
      ORDER BY message_table_idx;
  OPEN _want FOR
    VALUES (1, 'PENDING'),
           (2, 'DRAINING'),
           (3, 'ACTIVE');
  RETURN NEXT results_eq(_have, _want,
                         '4 - have expected message table states after fourth rotation call');
  CLOSE _have;
  CLOSE _want;

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.visible_message_tables(_queue.queue_id);
  OPEN _want FOR
    VALUES (2, 'DRAINING'),
           (3, 'ACTIVE');

  RETURN NEXT results_eq(_have, _want,
                         '4 - have expected visible message table states after fourth rotation call');
  CLOSE _have;
  CLOSE _want;

  RETURN NEXT is((pgqs.queue(k_queue_name)).queue_active_message_table_idx,
                 3::SMALLINT, 'have expected next table');


  -- 5 - fifth rotation call
  RETURN NEXT is(pgqs.maybe_rotate_message_tables_now(k_queue_name), 'PENDING_DRAINING',
                 '5 - did fifth rotation call');

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.message_tables
      WHERE queue_id = _queue.queue_id
      ORDER BY message_table_idx;
  OPEN _want FOR
    VALUES (1, 'ACTIVE'),
           (2, 'PENDING_DRAINING'),
           (3, 'DRAINING');
  RETURN NEXT results_eq(_have, _want,
                         '5 - have expected message table states after fifth rotation call');
  CLOSE _have;
  CLOSE _want;

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.visible_message_tables(_queue.queue_id);
  OPEN _want FOR
    VALUES (2, 'PENDING_DRAINING'),
           (3, 'DRAINING'),
           (1, 'ACTIVE');

  RETURN NEXT results_eq(_have, _want,
                         '5 - have expected visible message table states after fifth rotate call');
  CLOSE _have;
  CLOSE _want;

  RETURN NEXT is((pgqs.queue(k_queue_name)).queue_active_message_table_idx,
                 1::SMALLINT, 'have expected next table');

  -- 6 - sixth rotation call
  RETURN NEXT is(pgqs.maybe_rotate_message_tables_now(k_queue_name), 'PENDING',
                 '6 - did sixth rotation call');

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.message_tables
      WHERE queue_id = _queue.queue_id
      ORDER BY message_table_idx;
  OPEN _want FOR
    VALUES (1, 'ACTIVE'),
           (2, 'PENDING'),
           (3, 'DRAINING');
  RETURN NEXT results_eq(_have, _want,
                         '6 - have expected message table states after sixth rotation call');
  CLOSE _have;
  CLOSE _want;

  OPEN _have FOR
    SELECT message_table_idx::INT, message_table_state::TEXT
      FROM pgqs.visible_message_tables(_queue.queue_id);
  OPEN _want FOR
    VALUES (3, 'DRAINING'),
           (1, 'ACTIVE');

  RETURN NEXT results_eq(_have, _want,
                         '6 - have expected visible message table states after sixth rotate call');
  CLOSE _have;
  CLOSE _want;

  RETURN NEXT is((pgqs.queue(k_queue_name)).queue_active_message_table_idx,
                 1::SMALLINT, 'have expected next table');

END
$body$;

CREATE FUNCTION
pgqs_test.test_purge_queue()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_queue_name CONSTANT TEXT := 'some-queue';
  _queue RECORD;
  _count BIGINT;
  _res RECORD;
  _sql TEXT;
  _active_table_name TEXT;
  _previous_table_name TEXT;
  _as_of TIMESTAMPTZ;
  k_pending CONSTANT pgqs.message_table_state := 'PENDING';
BEGIN
  PERFORM pgqs.create_queue(k_queue_name);

  _queue := pgqs.queue(k_queue_name);
  _active_table_name := _queue.queue_active_message_table_name;

  PERFORM * FROM pgqs.send_message(k_queue_name, 'some-body');
  PERFORM * FROM pgqs.send_message(k_queue_name, 'some-body 2');

  RETURN NEXT is(pgqs.maybe_rotate_message_tables_now(k_queue_name)::TEXT, 'PENDING_DRAINING',
                 'rotated queue tables');
  _queue := pgqs.queue(k_queue_name);
  RETURN NEXT isnt(_active_table_name, _queue.queue_active_message_table_name,
                   'have new active table after rotation');
  _previous_table_name := _active_table_name;
  _active_table_name := _queue.queue_active_message_table_name;

  PERFORM * FROM pgqs.send_message(k_queue_name, 'some-body 3');
  PERFORM * FROM pgqs.send_message(k_queue_name, 'some-body 4');
  _sql := format('SELECT COUNT(*) FROM %I.%I', k_schema_name, _previous_table_name);
  EXECUTE _sql INTO STRICT _res;
  RETURN NEXT is(_res.count, 2::BIGINT, 'have messages in previous table');

  _sql := format('SELECT COUNT(*) FROM %I.%I', k_schema_name, _active_table_name);
  EXECUTE _sql INTO STRICT _res;
  RETURN NEXT is(_res.count, 2::BIGINT, 'have messages in active table');

  RETURN NEXT ok(pgqs.purge_queue(k_queue_name), 'purged queue');

  _sql := format('SELECT COUNT(*) FROM %I.%I', k_schema_name, _queue.queue_parent_message_table_name);
  EXECUTE _sql INTO STRICT _res;
  RETURN NEXT is(_res.count, 0::BIGINT, 'have no messages in queue tables');

END
$body$;

CREATE FUNCTION
pgqs_test.test_sqs_purge_queue()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'pgqs';
  k_queue_name CONSTANT TEXT := 'some-queue';
  _queue RECORD;
  _count BIGINT;
  _res RECORD;
  _sql TEXT;
  _active_table_name TEXT;
  _previous_table_name TEXT;
  _as_of TIMESTAMPTZ;
  k_pending CONSTANT pgqs.message_table_state := 'PENDING';
  _request jsonb;
BEGIN
  PERFORM pgqs.create_queue(k_queue_name);

  _queue := pgqs.queue(k_queue_name);
  _active_table_name := _queue.queue_active_message_table_name;

  PERFORM * FROM pgqs.send_message(k_queue_name, 'some-body');
  PERFORM * FROM pgqs.send_message(k_queue_name, 'some-body 2');

  RETURN NEXT is(pgqs.maybe_rotate_message_tables_now(k_queue_name)::TEXT, 'PENDING_DRAINING',
                 'rotated queue tables');
  _queue := pgqs.queue(k_queue_name);
  RETURN NEXT isnt(_active_table_name, _queue.queue_active_message_table_name,
                   'have new active table after rotation');
  _previous_table_name := _active_table_name;
  _active_table_name := _queue.queue_active_message_table_name;

  PERFORM * FROM pgqs.send_message(k_queue_name, 'some-body 3');
  PERFORM * FROM pgqs.send_message(k_queue_name, 'some-body 4');
  _sql := format('SELECT COUNT(*) FROM %I.%I', k_schema_name, _previous_table_name);
  EXECUTE _sql INTO STRICT _res;
  RETURN NEXT is(_res.count, 2::BIGINT, 'have messages in previous table');

  _sql := format('SELECT COUNT(*) FROM %I.%I', k_schema_name, _active_table_name);
  EXECUTE _sql INTO STRICT _res;
  RETURN NEXT is(_res.count, 2::BIGINT, 'have messages in active table');

  _request := jsonb_build_object('QueueUrl', _queue.queue_url);
  _sql := format('SELECT TRUE FROM pgqs.invoke(%L, %L)', 'PurgeQueue', _request);
  RETURN NEXT lives_ok(_sql, 'purged queue');

  _sql := format('SELECT COUNT(*) FROM %I.%I', k_schema_name, _queue.queue_parent_message_table_name);
  EXECUTE _sql INTO STRICT _res;
  RETURN NEXT is(_res.count, 0::BIGINT, 'have no messages in queue tables');
END
$body$;

CREATE FUNCTION
pgqs_test.test_receipt_handle()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  _handle TEXT;
  _decoded RECORD;
  _parts TEXT[];
  k_invalid_receipt_handle CONSTANT TEXT := 'QS441';
  k_queue_name CONSTANT TEXT := 'my-queue';
  k_bad_queue_name CONSTANT TEXT := 'bad-queue';
  k_bad_system_id CONSTANT UUID := '00000000-ffff-0000-0000-000000000000';
  k_message_id CONSTANT UUID :=    '00000000-aaaa-0000-0000-000000000000';
  k_receipt_token CONSTANT UUID := '00000000-bbbb-0000-0000-000000000000';
  _sql TEXT;
BEGIN
  _handle := pgqs.encode_receipt_handle(k_queue_name, k_message_id, k_receipt_token);
  _decoded := pgqs.decode_receipt_handle(_handle);
  RETURN NEXT is(_decoded.system_id, pgqs.system_id(), 'round-trip system_id');
  RETURN NEXT is(_decoded.queue_name, k_queue_name, 'round-trip queue_name');
  RETURN NEXT is(_decoded.message_id, k_message_id, 'round-trip message_id');
  RETURN NEXT is(_decoded.message_receipt_token, k_receipt_token, 'round-trip receipt_token');

  _sql := format('SELECT pgqs.decode_receipt_handle(%L, %L)', k_queue_name, _handle);
  RETURN NEXT lives_ok(_sql, 'valid receipt handle is valid');

  PREPARE not_b64 AS SELECT pgqs.decode_receipt_handle('this is not base64-encoded');
  RETURN NEXT throws_ok('not_b64', k_invalid_receipt_handle, 'invalid receipt handle',
                        'non-base64-encoded handle fails');

  _handle := pgqs.encode_receipt_handle(pgqs.system_id(), k_bad_queue_name,
                                        k_message_id, k_receipt_token);
  _sql := format('SELECT pgqs.decode_receipt_handle(%L, %L)', k_queue_name, _handle);
  RETURN NEXT throws_ok(_sql, k_invalid_receipt_handle, 'invalid receipt handle',
                        'validating decode fails with wrong queue name');

  _handle := pgqs.encode_receipt_handle(k_bad_system_id, k_queue_name,
                                        k_message_id, k_receipt_token);
  _sql := format('SELECT pgqs.decode_receipt_handle(%L, %L)', k_queue_name, _handle);
  RETURN NEXT throws_ok(_sql, k_invalid_receipt_handle, 'invalid receipt handle',
                        'validating decode fails with wrong system id');

  _parts := ARRAY['pgqsx',
                  pgqs.system_id()::TEXT, k_queue_name,
                  k_message_id::TEXT, k_receipt_token::TEXT];
  _handle :=  encode(convert_to(array_to_string(_parts, ':'), 'UTF8'), 'base64');
  _sql := format('SELECT pgqs.decode_receipt_handle(%L)', _handle);
  RETURN NEXT throws_ok(_sql, k_invalid_receipt_handle,
                        'invalid receipt handle', 'decode fails wrong prefix');

  _parts := ARRAY[pgqs.system_id()::TEXT, k_queue_name,
                  k_message_id::TEXT, k_receipt_token::TEXT];
  _handle :=  encode(convert_to(array_to_string(_parts, ':'), 'UTF8'), 'base64');
  _sql := format('SELECT pgqs.decode_receipt_handle(%L)', _handle);
  RETURN NEXT throws_ok(_sql, k_invalid_receipt_handle, 'invalid receipt handle',
                        'decode fails with wrong number of parts');

END
$body$;

CREATE FUNCTION
pgqs_test.test_queue_stats()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  _queue_stats RECORD;
  _queue RECORD;
  k_queue_name CONSTANT TEXT := 'my-queue';
  _have REFCURSOR;
  _want REFCURSOR;
  _sql TEXT;
BEGIN
  PERFORM pgqs.create_queue(k_queue_name);
  _queue := pgqs.queue(k_queue_name);
  _queue_stats := pgqs.queue_stats_now(k_queue_name);

  RETURN NEXT is(_queue_stats.queue_depth, 0::BIGINT, 'empty table is empty');

  OPEN _have FOR
    SELECT messages_table_name, queue_depth
      FROM pgqs.queue_table_stats(k_queue_name)
      ORDER BY messages_table_name;
  OPEN _want FOR
    SELECT message_table_name, 0::BIGINT AS queue_depth
      FROM pgqs.generate_message_tables(_queue.queue_id, _queue.queue_message_table_count)
      ORDER BY message_table_name;

  RETURN NEXT results_eq(_have, _want, 'have expected table stats');

  CLOSE _have;
  CLOSE _want;

  OPEN _have FOR
    SELECT _.message_count,
           _.stored_message_count, _.delayed_message_count,
           _.in_flight_message_count, _.invisible_message_count
      FROM pgqs.queue_stats AS _
      WHERE _.queue_id = _queue.queue_id;
  OPEN _want FOR
     VALUES (0::BIGINT,
             0::BIGINT, 0::BIGINT,
             0::BIGINT, 0::BIGINT);

  RETURN NEXT results_eq(_have, _want, 'have expected initial cached queue stats');

  CLOSE _have;
  CLOSE _want;

  PERFORM pgqs.invoke('SendMessage', jsonb_build_object('QueueUrl', _queue.queue_url,
                                                        'MessageBody', 'foo'));
  PERFORM pgqs.invoke('SendMessage', jsonb_build_object('QueueUrl', _queue.queue_url,
                                                        'MessageBody', 'bas'));
  PERFORM pgqs.invoke('SendMessage', jsonb_build_object('QueueUrl', _queue.queue_url,
                                                        'MessageBody', 'bat'));
  PERFORM pgqs.invoke('SendMessage', jsonb_build_object('QueueUrl', _queue.queue_url,
                                                        'MessageBody', 'bar',
                                                        'DelaySeconds', 10));

  PERFORM pgqs.invoke('ReceiveMessage', jsonb_build_object('QueueUrl', _queue.queue_url));
  _sql := format('SELECT * FROM pgqs.update_queue_stats(%L)', k_queue_name);
  RETURN NEXT lives_ok(_sql, 'Can refresh cached pgqs.queue_stats table');

  OPEN _have FOR
    SELECT _.message_count,
           _.stored_message_count, _.delayed_message_count,
           _.in_flight_message_count, _.invisible_message_count
      FROM pgqs.queue_stats AS _
      WHERE _.queue_id = _queue.queue_id;
  OPEN _want FOR
     VALUES (4::BIGINT,
             3::BIGINT, 1::BIGINT,
             1::BIGINT, 0::BIGINT);

  RETURN NEXT results_eq(_have, _want, 'have expected cached queue stats after refresh');

END
$body$;

COMMIT;
