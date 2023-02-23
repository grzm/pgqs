# pgqs Actions


## Usage


```sql
SELECT pgqs.invoke(:action, :request)
```


### ChangeMessageVisibility


Changes the visibility timeout of a specified message in a queue to
a new value. The default visibility timeout for a message is 30
seconds. The minimum is 0 seconds. The maximum is 12 hours. For more
information, see [Visibility
Timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
in the *Amazon SQS Developer Guide*.

For example, you have a message with a visibility timeout of 5
minutes. After 3 minutes, you call `ChangeMessageVisibility` with a
timeout of 10 minutes. You can continue to call
`ChangeMessageVisibility` to extend the visibility timeout to the
maximum allowed time. If you try to extend the visibility timeout
beyond the maximum, your request is rejected.

An Amazon SQS message has three basic states:

 1. Sent to a queue by a producer.
 2. Received from the queue by a consumer.
 3. Deleted from the queue.

A message is considered to be *stored* after it is sent to a queue by
a producer, but not yet received from the queue by a consumer (that
is, between states 1 and 2). There is no limit to the number of stored
messages. A message is considered to be *in flight* after it is
received from a queue by a consumer, but not yet deleted from the
queue (that is, between states 2 and 3). There is a limit to the
number of inflight messages.

 <important>
If you attempt to set the `VisibilityTimeout` to a value greater than
the maximum time left, Amazon SQS returns an error. Amazon SQS doesn't
automatically recalculate and increase the timeout to the maximum
remaining time.

Unlike with a queue, when you change the visibility timeout for a
specific message the timeout value is applied immediately but isn't
saved in memory for that message. If you don't delete a message after
it is received, the visibility timeout for the message reverts to the
original timeout value (not to the value you set using the
`ChangeMessageVisibility` action) the next time the message is
received.

 </important>


#### Request
```json
{"QueueUrl":"string",
 "ReceiptHandle":"string",
 "VisibilityTimeout":"integer"}

```


### ChangeMessageVisibilityBatch


Changes the visibility timeout of multiple messages. This is a
  batch version of `ChangeMessageVisibility`. The result of the action
  on each message is reported individually in the response. You can
  send up to 10 `ChangeMessageVisibility` requests with each
  `ChangeMessageVisibilityBatch` action.

 <important>

 Because the batch request can result in a combination of successful
 and unsuccessful actions, you should check for batch errors even when
 the call does not throw an exception.

</important>

Some actions take lists of parameters. These lists are specified using
arrays. For example, a parameter list with two elements looks like
this: `["first", "second"]`


#### Request
```json
{"QueueUrl":"string",
 "Entries":
 ["seq-of",
  {"Id":"string",
   "ReceiptHandle":"string",
   "VisibilityTimeout":"integer"}]}

```


#### Response
```json
{"Successful":["seq-of", {"Id":"string"}],
 "Failed":
 ["seq-of",
  {"Id":"string",
   "SenderFault":"boolean",
   "Code":"string",
   "Message":"string"}]}

```


### CreateQueue


Creates a queue. You can pass one or more attributes in the
  request. Keep the following in mind:

 * If you don't provide a value for an attribute, the queue is created
   with the default value for the attribute.

 * If you delete a queue, you must wait at least 60 seconds before
   creating a queue with the same name.

To successfully create a new queue, you must provide a queue name that
adheres to the [limits related to
queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/limits-queues.html)
and is unique within the scope of your queues.

To get the queue URL, use the `GetQueueUrl` action. `GetQueueUrl`
requires only the `QueueName` parameter.

Some actions take lists of parameters. These lists are specified
using an array. For example, a parameter list with two elements looks
like this: `["first", "second"]`


#### Request
```json
{"QueueName":"string",
 "Attributes":
 ["map-of",
  ["one-of",
   ["All", "VisibilityTimeout", "MaximumMessageSize",
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible", "CreatedTimestamp",
    "LastModifiedTimestamp", "ApproximateNumberOfMessagesDelayed",
    "DelaySeconds"]], "string"],
 "tags":["map-of", "string", "string"]}

```


#### Response
```json
{"QueueUrl":"string"}

```


### DeleteMessage


Deletes the specified message from the specified queue. To select
the message to delete, use the `ReceiptHandle` of the message (*not*
the `MessageId` which you receive when you send the message). Amazon
SQS can delete a message from a queue even if a visibility timeout
setting causes the message to be locked by another consumer. Amazon
SQS automatically deletes messages left in a queue longer than the
retention period configured for the queue.

 <note>The `ReceiptHandle` is associated with a *specific instance* of
 receiving a message. If you receive a message more than once, the
 `ReceiptHandle` is different each time you receive a message. When
 you use the `DeleteMessage` action, you must provide the most
 recently received `ReceiptHandle` for the message (otherwise, the
 request succeeds, but the message might not be deleted).

You should ensure that your application is idempotent, so that
receiving a message more than once does not cause issues. </note>


#### Request
```json
{"QueueUrl":"string", "ReceiptHandle":"string"}

```


### DeleteMessageBatch


Deletes up to ten messages from the specified queue. This is a
batch version of `DeleteMessage`. The result of the action on each
message is reported individually in the response.

<important>Because the batch request can result in a combination of
 successful and unsuccessful actions, you should check for batch
 errors even when the call returns an HTTP status code of `200`.
 </important>

Some actions take lists of parameters. These lists are specified using
an array. For example, a parameter list with two elements looks like
this: `["first", "second"]`


#### Request
```json
{"QueueUrl":"string",
 "Entries":["seq-of", {"Id":"string", "ReceiptHandle":"string"}]}

```


#### Response
```json
{"Successful":["seq-of", {"Id":"string"}],
 "Failed":
 ["seq-of",
  {"Id":"string",
   "SenderFault":"boolean",
   "Code":"string",
   "Message":"string"}]}

```


### DeleteQueue


Deletes the queue specified by the `QueueUrl`, regardless of the queue's contents.

<important> Be careful with the `DeleteQueue` action: When you delete
a queue, any messages in the queue are no longer available.
</important>

When you delete a queue, the background deletion process is not
immediate.

When you delete a queue, you must wait at least 60 seconds before
creating a queue with the same name.


#### Request
```json
{"QueueUrl":"string"}

```


### GetQueueAttributes


Gets attributes for the specified queue.


#### Request
```json
{"QueueUrl":"string",
 "AttributeNames":
 ["seq-of",
  ["one-of",
   ["All", "VisibilityTimeout", "MaximumMessageSize",
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible", "CreatedTimestamp",
    "LastModifiedTimestamp", "ApproximateNumberOfMessagesDelayed",
    "DelaySeconds", "RedrivePolicy", "FifoQueue"]]]}

```


#### Response
```json
{"Attributes":
 ["map-of",
  ["one-of",
   ["All", "Policy", "VisibilityTimeout", "MaximumMessageSize",
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible", "CreatedTimestamp",
    "LastModifiedTimestamp", "ApproximateNumberOfMessagesDelayed",
    "DelaySeconds"]], "string"]}

```


### GetQueueUrl


Returns the URL of an existing Amazon SQS queue.


#### Request
```json
{"QueueName":"string"}

```


#### Response
```json
{"QueueUrl":"string"}

```


### ListQueueTags


 List all tags added to the specified pgqs queue. For an overview,
see [Tagging Your Amazon SQS
Queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-tags.html)
in the *Amazon SQS Developer Guide*.


#### Request
```json
{"QueueUrl":"string"}

```


#### Response
```json
{"Tags":["map-of", "string", "string"]}

```


### ListQueues


Returns a list of your queues in the current pgqs system. The
response includes a maximum of 1,000 results. If you specify a value
for the optional `QueueNamePrefix` parameter, only queues with a name
that begins with the specified value are returned.

The `ListQueues` methods supports pagination. Set parameter
`MaxResults` in the request to specify the maximum number of results
to be returned in the response. If you do not set `MaxResults`, the
response includes a maximum of 1,000 results. If you set `MaxResults`
and there are additional results to display, the response includes a
value for `NextToken`. Use `NextToken` as a parameter in your next
request to `ListQueues` to receive the next page of results.


#### Request
```json
{"QueueNamePrefix":"string",
 "NextToken":"string",
 "MaxResults":"integer"}

```


#### Response
```json
{"QueueUrls":["seq-of", "string"], "NextToken":"string"}

```


### PurgeQueue


Deletes the messages in a queue specified by the `QueueURL`
parameter.

<important>When you use the `PurgeQueue` action, you can't retrieve
any messages deleted from a queue.  </important>



#### Request
```json
{"QueueUrl":"string"}

```


### ReceiveMessage


Retrieves one or more messages (up to 10), from the specified queue.

For each message returned, the response includes the following:

 * The message body.
 * An MD5 digest of the message body. For information about MD5, see [RFC1321](https://www.ietf.org/rfc/rfc1321.txt]
 * The `MessageId` you received when you sent the message to the queue.
 * The receipt handle.
 * The message attributes.
 * An MD5 digest of the message attributes.

The receipt handle is the identifier you must provide when deleting
the message. For more information, see [Queue and Message
Identifiers](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html)
in the *Amazon SQS Developer Guide*.

You can provide the `VisibilityTimeout` parameter in your request. The
parameter is applied to the messages that Amazon SQS returns in the
response. If you don't include the parameter, the overall visibility
timeout for the queue is used for the returned messages. For more
information, see [Visibility
Timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
in the *Amazon SQS Developer Guide*.

A message that isn't deleted or a message whose visibility isn't
extended before the visibility timeout expires counts as a failed
receive. Depending on the configuration of the queue, the message
might be sent to the dead-letter queue.

<note>In the future, new attributes might be added. If you write code
that calls this action, we recommend that you structure your code so
that it can handle new attributes gracefully.</note>


#### Request
```json
{"QueueUrl":"string",
 "AttributeNames":
 ["seq-of",
  ["one-of",
   ["All", "VisibilityTimeout", "MaximumMessageSize",
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible", "CreatedTimestamp",
    "LastModifiedTimestamp", "ApproximateNumberOfMessagesDelayed",
    "DelaySeconds"]]],
 "MessageAttributeNames":["seq-of", "string"],
 "MaxNumberOfMessages":"integer",
 "VisibilityTimeout":"integer",
 "WaitTimeSeconds":"integer",
 "ReceiveRequestAttemptId":"string"}

```


#### Response
```json
{"Messages":
 ["seq-of",
  {"MessageId":"string",
   "ReceiptHandle":"string",
   "MD5OfBody":"string",
   "Body":"string",
   "Attributes":
   ["map-of",
    ["one-of",
     ["SentTimestamp", "ApproximateReceiveCount",
      "ApproximateFirstReceiveTimestamp"]],
    "string"],
   "MD5OfMessageAttributes":"string",
   "MessageAttributes":
   ["map-of", "string",
    {"StringValue":"string",
     "BinaryValue":"blob",
     "DataType":"string"}]}]}

```


### SendMessage


Delivers a message to the specified queue.

<important>A message can include only XML, JSON, and unformatted text. The following Unicode characters are allowed:

 `#x9` | `#xA` | `#xD` | `#x20` to `#xD7FF` | `#xE000` to `#xFFFD`

Any characters not included in this list will be rejected. For more information, see the [W3C specification for characters](http://www.w3.org/TR/REC-xml/#charsets)
</important>


#### Request
```json
{"QueueUrl":"string",
 "MessageBody":"string",
 "DelaySeconds":"integer",
 "MessageAttributes":
 ["map-of", "string",
  {"StringValue":"string", "BinaryValue":"blob", "DataType":"string"}]}

```


#### Response
```json
{"MD5OfMessageBody":"string",
 "MD5OfMessageAttributes":"string",
 "MessageId":"string"}

```


### SendMessageBatch


Delivers up to ten messages to the specified queue. This is a batch version of `SendMessage`.

The result of sending each message is reported individually in the
response. Because the batch request can result in a combination of
successful and unsuccessful actions, you should check for batch errors
even when the call returns without throwing an exception

<important> A message can include only XML, JSON, and unformatted
text. The following Unicode characters are allowed:

`#x9` | `#xA` | `#xD | `#x20` to `#xD7FF` | `#xE000` to `#xFFFD`

Any characters not included in this list will be rejected. For more
information, see the [W3C specification for
characters](http://www.w3.org/TR/REC-xml/#charsets).  </important>

If you don't specify the `DelaySeconds` parameter for an entry, pgqs
uses the default value for the queue.

Some actions take lists of parameters. These lists are specified using
arrays. For example, a parameter list with two elements looks like
this: `{"AttributeName: ["first", "second"]}`.


#### Request
```json
{"QueueUrl":"string",
 "Entries":
 ["seq-of",
  {"Id":"string",
   "MessageBody":"string",
   "DelaySeconds":"integer",
   "MessageAttributes":
   ["map-of", "string",
    {"StringValue":"string",
     "BinaryValue":"blob",
     "DataType":"string"}]}]}

```


#### Response
```json
{"Successful":
 ["seq-of",
  {"Id":"string",
   "MessageId":"string",
   "MD5OfMessageBody":"string",
   "MD5OfMessageAttributes":"string"}],
 "Failed":
 ["seq-of",
  {"Id":"string",
   "SenderFault":"boolean",
   "Code":"string",
   "Message":"string"}]}

```


### SetQueueAttributes


Sets the value of one or more queue attributes.

In the future, new attributes might be added. If you write code that
calls this action, we recommend that you structure your code so that
it can handle new attributes gracefully.


#### Request
```json
{"QueueUrl":"string",
 "Attributes":
 ["map-of",
  ["one-of",
   ["All", "VisibilityTimeout", "MaximumMessageSize",
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible", "CreatedTimestamp",
    "LastModifiedTimestamp", "ApproximateNumberOfMessagesDelayed",
    "DelaySeconds"]], "string"]}

```


### TagQueue


Add tags to the specified queue.

When you use queue tags, keep the following guidelines in mind:

 * Adding more than 50 tags to a queue isn't allowed.
 * Tags don't have any semantic meaning. Pgqs interprets tags as character strings.
 * Tags are case-sensitive.
 * A new tag with a key identical to that of an existing tag overwrites the existing tag.

For a full list of tag restrictions, see [Quotas related to queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-limits.html#limits-queues) in the *Amazon SQS Developer Guide*.


#### Request
```json
{"QueueUrl":"string", "Tags":["map-of", "string", "string"]}

```


### UntagQueue


Remove tags from the specified queue.


#### Request
```json
{"QueueUrl":"string", "TagKeys":["seq-of", "string"]}

```