CREATE SCHEMA pgqs_test;

CREATE FUNCTION
pgqs_test.test_truth()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
BEGIN
  RETURN NEXT ok(TRUE, 'test truth');
END
$body$;
