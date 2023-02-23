# pgqs - PostgreSQL Queue Service

A PostgreSQL-backed queue system with AWS SQS-like actions.

This is alpha, and some function signatures are likely to change
(particularly around maintenance, monitoring, and error messages).

## Desiredata

- At least once deliverability
- Batch message request
- Batch message acknowledgement
- No need to keep transactions open while processing messages
- Message table rotation/truncation to manage PostgreSQL table bloat
- Minimal blocking of concurrent readers and writers
- Provide features similar to AWS SQS
- Be deployable where compiled PostgreSQL extensions are not allowed

## Why do this?

My intent was not to partially replicate SQS in PostgreSQL. Two of the
initial motivations were to (a) provide a function API rather than
direct table access to abstract the schema from the user interface;
and (b) to support enqueing batches of messages. To do that with
functions requires serializing the messages into a function
argument. Having worked with SQS, I started out using the AWS
SendMessageBatch data structure, more as a placeholder during
development. As there were other features I wanted that SQS provides,
it started to make sense to mimic the SQS API rather than come up with
my own: if anyone other than me ends up using it, they may already
have experience with SQS that they can leverage when picking up
pgqs. And if not, they'll have to learn something anyway.

The intent is not to replace or be a full implementation of AWS SQS in
PostgreSQL. If you need AWS SQS, use AWS SQS. If you want sometime
with some SQS features with a SQS-like interface but don't or can't
run on AWS SQS, maybe give pgqs a go.

## Usage

```sql
SELECT pgqs.invoke('CreateQueue', '{"QueueName": "my-queue"}');
-- =>  {"QueueUrl": "pgqs://pgqs.system:9efcf974-44ab-4ac1-8a84-3897ee337d83/my-queue"}

SELECT pgqs.invoke('GetQueueUrl', '{"QueueName": "my-queue"}');
-- => "pgqs://pgqs.system:9efcf974-44ab-4ac1-8a84-3897ee337d83/my-queue"

SELECT pgqs.invoke('SendMessage', '{"QueueUrl": "pgqs://pgqs.system:9efcf974-44ab-4ac1-8a84-3897ee337d83/my-queue", "MessageBody": "Hello, world!"}');

-- => SELECT pgqs.invoke('SendMessage', '{"QueueUrl": "pgqs://pgqs.system:9efcf974-44ab-4ac1-8a84-3897ee337d83/my-queue", "MessageBody": "Hello, world!"}');

SELECT pgqs.invoke('ReceiveMessage', '{"QueueUrl": "pgqs://pgqs.system:9efcf974-44ab-4ac1-8a84-3897ee337d83/my-queue", "MaxNumberOfMessages": 1}');
-- => {"Messages": [{"Body": "Hello, world!", "MD5OfBody": "6cd3556deb0da54bca060b4c39479839", "MessageId": "069a9ead-a340-4f2d-9ec6-8cf02fbd0f8f", "ReceiptHandle": "cGdzcTo5ZWZjZjk3NC00NGFiLTRhYzEtOGE4NC0zODk3ZWUzMzdkODM6bXktcXVldWU6MDY5YTll\nYWQtYTM0MC00ZjJkLTllYzYtOGNmMDJmYmQwZjhmOjAzNWJjNGMwLTRmOGQtNDZhMS1hYWNlLTFi\nMTYxZGVhZDAzNg=="}]}

SELECT pgqs.invoke('DeleteMessage', '{"QueueUrl":"pgqs://pgqs.system:9efcf974-44ab-4ac1-8a84-3897ee337d83/my-queue", "ReceiptHandle": "cGdzcTo5ZWZjZjk3NC00NGFiLTRhYzEtOGE4NC0zODk3ZWUzMzdkODM6bXktcXVldWU6MDY5YTll\nYWQtYTM0MC00ZjJkLTllYzYtOGNmMDJmYmQwZjhmOjAzNWJjNGMwLTRmOGQtNDZhMS1hYWNlLTFi\nMTYxZGVhZDAzNg=="}');

```
See [Actions](actions.markdown) for a complete list of supported
operations.

### Differences from AWS SQS

Restrictions on message length, character sets, and other quotas may
not be enforced (and not consistently across features, yet).

### Unsupported features
 * FIFO queues
 * Deadletter Queues (and no RedrivePolicies)
 * Retention periods (message expiry)
 * ReceiveMessageWait periods (no long-polling)
 * AddPermission/RemovePermission

And since it's PostgreSQL, not AWS, some AWS-specific featueres are
likewise omitted.

 * AWSTraceHeader MessageSystemAttribute
 * KMS keys
 * SSE integration

pgqs does have the concept of a `QueueUrl`, with each pgqs system being
assigned a unique system identifier. pgqs queues do not have QueueArns.

## Performance

pgqs hasn't been performance optimized. In limited laptop local
testing with parallelism of writes and reads around 10 workers, writes
are north of 5000/second, and reads/deletes on the order of
1500/second, which is fast enough for me. All of this is going to vary
by workload and size of hardware like any other database system.

## Implementation

### Message table rotation and bloat

During UPDATE and DELETE operations, PostgreSQL marks old rows as
dead. Manual VACUUM and autovacuum attempt to reclaim these tables,
but depending on the activity of the table, some rows can be difficult
to reclaim. The TRUNCATE operation detaches all rows from a table,
including dead rows, but requires a lock, preventing concurrent
operations on the table for the duration of the TRUNCATE operation.

For each queue, pgqs generates a number of message tables to allow
rotating which tables are being inserted into and queried from, and
which are inactive. The pgqs maintenance operations only truncate
tables when they are inactive, so we don't interrupt the processing of
queue messages. pgq uses a similar strategy.

The number of tables is currently hard-wired to be three, which allows
a freshly truncated table to become the table where inserts
happen. Two may be better as it reduces the range of tables that need
to be potetially searched for messages to SELECT, UPDATE, or DELETE
from 2-3 to 1-2, and more may be better if you want to have a large
number of messages retained over a longer period of time. I haven't
done testing to confirm what would be optimally here, and anything more
than two is in some ways attempting to replicate the relation
pagination PostgreSQL already does when writing data to disk.

I don't see a reason to make the number of message tables a
user-tweakable setting at this time, and those saavy enough to have
meaningful opinions on the subject are also saavy enough to know how
to make the changes to allow this to be tweaked.

## Copyright and License
Copyright © 2023, Michael Glaesemann

This software is provided under the PostgreSQL License. See LICENSE
file for details.
