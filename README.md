[README.md](https://github.com/user-attachments/files/32481982/README.md)
# Generic Multi-Tenant Scheduler Service --- Solution Design

## 1. Document Purpose

This document captures the architecture brainstorming and solution
design for a generic, highly reliable, multi-tenant scheduler service
capable of scheduling arbitrary trigger types.

The design is intentionally presented with **two scheduling options**
for stakeholder evaluation:

1.  **Option 1 --- Database as Scheduler**
2.  **Option 2 --- Microservice Pooling**

The downstream execution architecture is common to both options.

The objective is to allow stakeholders to choose the scheduling model
based on reliability, scalability, operational complexity,
extensibility, and organizational constraints.

------------------------------------------------------------------------

# 2. Goals

## 2.1 Primary Goals

### Failproof scheduling

The platform must never silently lose a scheduled execution.

For every schedule occurrence that becomes due:

``` text
Scheduled occurrence
        |
        v
Durable execution record
        |
        +----> Dispatched
        |
        +----> Retried
        |
        +----> Successful
        |
        +----> Explicitly failed / terminal state
```

A downstream system being unavailable is **not a scheduler failure**.
The scheduler records the failure and applies the configured retry
policy.

### At-most-once scheduler execution

The platform should ensure that one scheduled occurrence has one
authoritative execution.

For example:

``` text
Schedule: SCH-123
Occurrence: 2026-09-21 10:00 UTC

              +----------------+
              |   EXE-98765    |
              +----------------+
```

Multiple scheduler pods/clusters may discover the same due schedule, but
only one execution should be created/claimed for that occurrence.

External side effects use idempotency mechanisms where required.

### Scale

The platform is designed for:

-   1M+ schedules
-   Multiple scheduler pods
-   Multiple active-active production clusters
-   High execution volume
-   Horizontal scaling

### Scheduling

-   Quartz-compatible cron expressions
-   Time-zone aware schedules
-   UTC-based execution timestamps
-   Target scheduling precision: approximately ±1 minute
-   Missed executions must be recovered
-   Multiple schedules per job
-   Different variables/configuration per schedule

### Generic triggers

The scheduler must support pluggable trigger implementations.

Initial trigger types:

-   REST API
-   Kafka message

Future trigger types may include:

-   Email
-   Webhook
-   gRPC
-   SQS
-   RabbitMQ
-   Database operation
-   Other enterprise integrations

Adding a new trigger type must not require modification to the Scheduler
Core.

### Traceability

Every execution must be traceable from:

``` text
Tenant
  |
Job
  |
Schedule
  |
Execution
  |
Attempt
  |
Trigger
  |
External correlation ID
```

### Fire-and-forget API

Job creation/configuration APIs should acknowledge acceptance without
waiting for the scheduled trigger to execute.

Example:

``` http
POST /jobs
```

Response:

``` json
{
  "jobId": "JOB-123",
  "status": "ACCEPTED",
  "traceId": "TRC-456"
}
```

The client can subsequently query execution status.

------------------------------------------------------------------------

# 3. Non-Goals

The following are not part of the current scope.

-   Building the UI in this design phase
-   Selecting the final database product
-   Implementing code
-   Guaranteeing exactly-once side effects for arbitrary external
    systems
-   Tenant-specific quotas/throttling in the first version
-   Long-running workflow orchestration
-   Building a general-purpose workflow engine
-   Replacing upstream Kafka infrastructure
-   Trigger execution longer than necessary; target trigger duration is
    ideally less than one minute

Tenant-level resource isolation and quotas are considered future scope.

------------------------------------------------------------------------

# 4. Key Design Principles

## 4.1 One Scheduler Database

There is **one Scheduler Database** in the logical architecture.

It is the persistent source of truth for:

-   Jobs
-   Schedules
-   Triggers
-   Executions
-   Retry state
-   Execution history
-   Scheduler state required for recovery

Internal Kafka is **not a second database**. It is the execution
transport/backbone.

``` text
                    +----------------------+
                    |    Scheduler DB      |
                    |                      |
                    | Jobs                 |
                    | Schedules            |
                    | Triggers             |
                    | Executions           |
                    +----------+-----------+
                               |
                               | Execution
                               v
                    +----------------------+
                    |    Internal Kafka    |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Trigger Executors    |
                    +----------------------+
```

## 4.2 Scheduler Core does not know trigger implementation details

The scheduler knows only:

``` text
TriggerExecutor.execute(execution)
```

It must not contain trigger-specific logic such as:

-   HTTP implementation
-   Kafka producer implementation
-   Email implementation
-   SQS implementation
-   Payload formatting specific to a trigger type

Those responsibilities belong to trigger plugins/executors.

## 4.3 Scheduling and execution are separated

The scheduler determines:

> "It is time to execute this schedule."

It does not perform the actual external operation.

``` text
Schedule
   |
   v
Execution
   |
   v
Internal Kafka
   |
   v
Trigger Executor
   |
   +----> REST
   |
   +----> Kafka
   |
   +----> Future Trigger
```

This separation is essential for scalability and fault isolation.

------------------------------------------------------------------------

# 5. Domain Model

The business model deliberately separates **Job Type**, **Schedule**,
and **Trigger Type**.

## 5.1 Job

A Job represents the business operation.

Example:

``` text
Job Type:
TriggerAIWorkflow
```

A job can have multiple schedules.

``` text
                       JOB
                        |
             +----------+----------+
             |          |          |
         Schedule A  Schedule B  Schedule C
```

## 5.2 Schedule

A schedule determines when a job should execute.

Example:

``` text
Schedule A
-----------
Cron: 0 9 * * MON
Timezone: Asia/Kolkata
Variables:
    workflow = "customer-sync"

Schedule B
-----------
Cron: 0 9 * * WED
Timezone: Asia/Kolkata
Variables:
    workflow = "customer-report"
```

The same job can therefore execute with different variables on different
schedules.

## 5.3 Trigger

A trigger defines how a job execution is delivered to an external
system.

Example:

``` text
Trigger Name:
TriggerAIWorkflow

Type:
REST

Configuration:
    URL
    HTTP Method
    Headers
    Payload
    Authentication
    Timeout
    Retry Policy
```

Another:

``` text
Trigger Name:
TriggerEvent

Type:
Kafka

Configuration:
    Topic
    Headers
    Payload
    Upstream Kafka connection
```

Triggers can be reused by multiple jobs.

``` text
TriggerAIWorkflow
       |
       +---- Job A
       +---- Job B
       +---- Job C
```

------------------------------------------------------------------------

# 6. Runtime Variables

Trigger configurations may contain placeholders.

Example:

``` text
URL:
https://ai.example.com/workflow/${workflowId}

Headers:
Authorization: ${authToken}
X-Correlation-ID: ${executionId}

Payload:
{
  "customerId": "${customerId}",
  "executionId": "${executionId}"
}
```

At execution time:

``` text
Execution
    |
    +-- Schedule Variables
    +-- Runtime Variables
    +-- System Variables
             |
             v
      Variable Resolver
             |
             v
      Resolved Trigger
             |
             v
      TriggerExecutor
```

Examples of system-generated variables may include:

-   executionId
-   jobId
-   scheduleId
-   traceId
-   scheduledFireTime

Trigger versioning is **not required** in the current design.

------------------------------------------------------------------------

# 7. Time Zones and UTC

Schedules are time-zone aware.

The original cron expression and timezone remain the source of truth.

Example:

``` text
Cron:
0 9 * * *

Timezone:
America/New_York
```

The system may calculate and persist:

``` text
nextFireTimeUtc:
2026-09-22T13:00:00Z
```

However, the cron expression should **not** simply be permanently
converted to UTC.

This is important because daylight-saving transitions can change the UTC
equivalent of a local-time schedule.

Therefore:

``` text
cron expression + timezone
            |
            v
      next UTC fire time
```

The `nextFireTimeUtc` value is optimized for efficient scheduling
queries.

------------------------------------------------------------------------

# 8. Common High-Level Architecture

Both scheduling options share the same overall execution architecture.

``` text
                         +---------------------+
                         |    Scheduler API    |
                         +----------+----------+
                                    |
                                    v
                         +---------------------+
                         |    Scheduler DB     |
                         |                     |
                         | Jobs                |
                         | Schedules           |
                         | Triggers            |
                         | Executions          |
                         +----------+----------+
                                    |
                         Scheduling mechanism
                         /                   \
                        /                     \
           Option 1: DB Scheduler       Option 2:
                                      Microservice Pool
                        \                     /
                         \                   /
                          +-------+---------+
                                  |
                           Execution Created
                                  |
                                  v
                       +----------------------+
                       |    Internal Kafka    |
                       +----------+-----------+
                                  |
                    +-------------+-------------+
                    |             |             |
                    v             v             v
              REST Executor  Kafka Executor  Future Executor
                    |             |
                    v             v
               REST API     Upstream Kafka
```

------------------------------------------------------------------------

# 9. Option 1 --- Database as Scheduler

## 9.1 Concept

In this option, the database's native scheduling capability determines
when scheduled jobs become due.

The application remains responsible for everything related to business
execution.

``` text
                      Scheduler API
                           |
                           v
                  +------------------+
                  |   Scheduler DB   |
                  |                  |
                  | Jobs             |
                  | Schedules        |
                  | Triggers         |
                  | Executions       |
                  +--------+---------+
                           |
                    Native DB Scheduler
                           |
                    Find due schedule
                           |
                    Create/claim execution
                           |
                           v
                  +------------------+
                  | Internal Kafka   |
                  +--------+---------+
                           |
                           v
                  Trigger Executors
```

## 9.2 DB responsibilities

The database scheduler is responsible for:

1.  Evaluating schedules.
2.  Determining due schedules.
3.  Handling scheduling persistence.
4.  Creating/initiating the execution.
5.  Coordinating scheduled occurrence ownership according to the DB
    scheduler's capabilities.

## 9.3 Application responsibilities

The application services remain responsible for:

-   API handling
-   Job management
-   Trigger management
-   Runtime variable resolution
-   Execution lifecycle
-   Kafka dispatch
-   Trigger plugins
-   REST calls
-   Upstream Kafka publishing
-   Retry handling
-   Error handling
-   Observability
-   Troubleshooting

The DB scheduler does **not** directly call external REST APIs.

## 9.4 Advantages

-   Scheduling state and scheduling engine are colocated.
-   Durable scheduling state is naturally persisted.
-   Database transactions and concurrency controls can be used.
-   Potentially less application-level scheduling code.
-   Database-native recovery mechanisms may simplify certain failure
    scenarios.
-   Fewer scheduler application components to operate.
-   Can leverage existing enterprise DB capabilities.

## 9.5 Disadvantages

-   Strong dependency on the selected database's scheduling
    capabilities.
-   DB vendor-specific behavior may be required.
-   Active-active behavior must be validated for the selected DB
    scheduler.
-   Scaling characteristics for 1M+ schedules are dependent on DB
    scheduler implementation.
-   Scheduling and application infrastructure become more tightly
    coupled to the database.
-   Portability between MongoDB, Oracle, SQL Server, etc. is reduced.
-   Sophisticated scheduling behavior may require application-side
    extensions anyway.
-   Operational troubleshooting may span DB scheduler + application +
    Kafka.
-   Adding advanced scheduler capabilities can become DB-specific.
-   Database becomes responsible for a potentially high-frequency
    scheduling workload.

## 9.6 Important consideration

The DB scheduler is responsible only for **scheduling**.

It does not replace the application execution platform.

The architecture remains:

``` text
DB Scheduler
     |
     v
Execution
     |
     v
Internal Kafka
     |
     v
Trigger Executor
```

------------------------------------------------------------------------

# 10. Option 2 --- Microservice Pooling

## 10.1 Concept

In this option, Spring Boot scheduler pods periodically poll the
Scheduler DB for schedules that are due.

The DB remains the source of truth.

``` text
                         Scheduler DB
                              ^
                              |
             +----------------+----------------+
             |                |                |
             |                |                |
       +-----+------+   +-----+------+   +-----+------+
       | Scheduler  |   | Scheduler  |   | Scheduler  |
       |   Pod 1    |   |   Pod 2    |   |   Pod N    |
       +------------+   +------------+   +------------+
             |                |                |
             +----------------+----------------+
                              |
                         Atomic Claim
                              |
                              v
                      Internal Kafka
```

The scheduler pods are stateless with respect to durable scheduling
state.

## 10.2 Polling

Each scheduler pod periodically queries for schedules where:

``` text
nextFireTimeUtc <= currentTimeUtc
AND
status = ACTIVE
```

The query must be optimized using an appropriate index.

At 1M+ schedules, this must not be implemented as a full-table scan.

## 10.3 Batch processing

Scheduler pods should claim work in batches.

Example:

``` text
Pod 1 -> claim batch of 100
Pod 2 -> claim batch of 100
Pod 3 -> claim batch of 100
Pod 4 -> claim batch of 100
```

The batch size is configurable.

This prevents a single scheduler pod from monopolizing a large number of
due jobs.

------------------------------------------------------------------------

# 11. Atomic Claiming

Atomic claiming is one of the most important mechanisms in the
Microservice Pooling option.

A naive implementation would be unsafe:

``` text
Pod A:
SELECT due jobs

Pod B:
SELECT same due jobs

Pod A:
process job

Pod B:
process same job
```

This could create duplicate executions.

## 11.1 Required behavior

Discovery and claiming must be performed atomically.

``` text
                Due Schedule
                     |
            +--------+--------+
            |                 |
          Pod A             Pod B
            |                 |
       Attempt claim     Attempt claim
            |                 |
         SUCCESS             FAIL
            |
            v
        Execution
```

## 11.2 Relational database implementation

Depending on the selected database, mechanisms may include:

``` sql
SELECT ...
FOR UPDATE SKIP LOCKED
```

or an atomic conditional update:

``` sql
UPDATE schedule
SET ...
WHERE schedule_id = ?
  AND next_fire_time <= ?
  AND status = 'ACTIVE';
```

The application verifies that exactly one row was affected.

The exact implementation is database-specific and should be finalized
after selecting the DB.

## 11.3 MongoDB implementation

An equivalent atomic `findAndModify` / conditional update approach can
be used.

The operation must atomically establish ownership and update the
schedule state.

## 11.4 Execution uniqueness

A further protection should enforce uniqueness around:

``` text
(scheduleId, scheduledFireTime)
```

Conceptually:

``` text
UNIQUE(
    scheduleId,
    scheduledFireTime
)
```

This provides a durable boundary ensuring that one schedule occurrence
has one authoritative execution.

------------------------------------------------------------------------

# 12. Claim Lease and Recovery

A scheduler pod can crash after claiming an execution.

Example:

``` text
10:00:01
Pod A claims EXE-123

10:00:02
Pod A crashes

10:00:03
EXE-123 is still CLAIMED
```

The system must not leave the execution permanently stuck.

The execution/claim model should therefore support a lease:

``` text
executionId
claimOwner
claimedAt
claimLeaseUntil
```

Example:

``` text
claimLeaseUntil = 10:01:00
```

If the owning scheduler disappears and the lease expires:

``` text
Lease expires
      |
      v
Another scheduler discovers recoverable execution
      |
      v
Reclaims / resumes processing
```

This mechanism is required to support the failproof/no-silent-loss goal.

------------------------------------------------------------------------

# 13. Load Distribution

The architecture must support scenarios such as:

> 100 jobs become due at exactly 10:00 AM.

We must not overload a single scheduler pod.

``` text
                    100 due jobs
                         |
              +----------+----------+
              |          |          |
             Pod 1      Pod 2      Pod 3
              |          |          |
           Batch       Batch      Batch
              \          |          /
               +---------+---------+
                         |
                         v
                  Internal Kafka
                         |
              +----------+----------+
              |          |          |
           Worker 1   Worker 2   Worker N
```

There are two levels of load distribution.

### Scheduler load

Multiple scheduler pods discover and claim due schedules.

### Trigger execution load

Trigger executions are distributed through Internal Kafka to scalable
workers.

This is important because:

``` text
Scheduler capacity != Trigger execution capacity
```

The scheduler should not spend its time performing 100 REST calls.

It should rapidly create/dispatch executions.

------------------------------------------------------------------------

# 14. Internal Kafka

Internal Kafka is the execution backbone.

``` text
Scheduler
    |
    v
Execution DB state
    |
    v
Internal Kafka
    |
    +---- REST Executor
    |
    +---- Kafka Executor
    |
    +---- Future Executors
```

The internal Kafka is controlled by the scheduler platform.

It is different from an upstream Kafka used as a trigger destination.

For example:

``` text
Internal Kafka
----------------
Scheduler -> Trigger Worker


Upstream Kafka
----------------
Kafka Trigger -> Customer/Upstream Kafka
```

------------------------------------------------------------------------

# 15. Trigger Plugin Architecture

The scheduler core should depend only on an abstraction:

``` text
TriggerExecutor.execute(execution)
```

Conceptually:

``` text
                     Scheduler Core
                           |
                           v
                   TriggerExecutor
                           |
          +----------------+----------------+
          |                |                |
          v                v                v
    REST Executor    Kafka Executor    Future Executor
```

## REST Executor

``` text
Execution
   |
Variable Resolver
   |
Resolved URL
Resolved Headers
Resolved Payload
   |
REST call
   |
2xx -> SUCCESS
Non-2xx -> FAILED / RETRY
```

## Kafka Executor

``` text
Execution
   |
Variable Resolver
   |
Resolved topic
Resolved headers
Resolved payload
   |
Upstream Kafka
```

The scheduler core does not know how either implementation works.

------------------------------------------------------------------------

# 16. REST Idempotency

External REST calls cannot guarantee exactly-once side effects purely
from the scheduler.

Example:

``` text
Scheduler
    |
    | POST request
    v
External REST API
    |
    | Processes request
    |
    X Network failure
    |
Scheduler does not receive response
```

A retry could cause duplicate processing.

Therefore the scheduler should send:

``` text
Idempotency-Key: <executionId>
```

Example:

``` text
Idempotency-Key: EXE-98765
```

The downstream API should use the key to prevent duplicate side effects.

The scheduler considers a REST trigger successful when the upstream
endpoint returns a 2xx response.

------------------------------------------------------------------------

# 17. Execution Lifecycle

The lifecycle should be explicit and queryable.

``` text
                    SCHEDULED
                        |
                        v
                      DUE
                        |
                        v
                     CLAIMED
                        |
                        v
                EXECUTION_CREATED
                        |
                        v
                   DISPATCHED
                        |
                        v
                     RUNNING
                        |
                +-------+-------+
                |               |
                v               v
             SUCCESS          FAILED
                                |
                         Retry available?
                          /           \
                        YES            NO
                         |              |
                         v              v
                 RETRY_SCHEDULED   FAILED_FINAL
                         |
                         v
                      RUNNING
```

`DUE` may be treated as a calculated condition rather than a persisted
state.

The durable lifecycle can therefore be:

``` text
SCHEDULED
   |
CLAIMED
   |
DISPATCHED
   |
RUNNING
   |
SUCCESS / FAILED
   |
RETRY_SCHEDULED
   |
FAILED_FINAL
```

------------------------------------------------------------------------

# 18. Execution IDs

Every scheduled occurrence should receive a unique Execution ID.

Example:

``` text
Job ID:
JOB-100

Schedule ID:
SCH-200

Execution ID:
EXE-300

Attempt:
2
```

The Execution ID should propagate through the entire execution path:

``` text
Execution ID
     |
     +---- Scheduler DB
     |
     +---- Internal Kafka
     |
     +---- Trigger Executor
     |
     +---- REST Idempotency-Key
     |
     +---- Logs
     |
     +---- Distributed Trace
     |
     +---- Upstream Correlation ID
```

This is the primary troubleshooting key.

------------------------------------------------------------------------

# 19. Retry Model

A failed trigger uses a configurable retry policy.

Example:

``` text
maxRetries = 5
backoff = exponential
initialDelay = 30 seconds
maxDelay = 10 minutes
```

Example lifecycle:

``` text
Attempt 1
   |
 HTTP 503
   |
Retry #1
   |
 HTTP 503
   |
Retry #2
   |
 HTTP 200
   |
SUCCESS
```

If all retries fail:

``` text
FAILED_FINAL
```

The failed execution must remain queryable for troubleshooting.

------------------------------------------------------------------------

# 20. Concurrent Executions

Concurrent executions are allowed.

Example:

``` text
Schedule:
Every 10 minutes

10:00 execution -> running
10:10 execution -> starts
```

The 10:10 execution does not need to wait for the 10:00 execution.

A future enhancement may introduce configurable concurrency policies
such as:

``` text
ALLOW_OVERLAP
FORBID_OVERLAP
```

but the current requirement is:

> Allow concurrent executions.

------------------------------------------------------------------------

# 21. Job Deletion

Deleting a job should not terminate an already running execution.

Example:

``` text
Job A
  |
  +---- Execution EXE-1 -> RUNNING
  |
  +---- Future execution
```

If Job A is deleted:

``` text
EXE-1 -> continues
Future executions -> stopped
```

Logical deletion/inactivation is preferable to physical deletion so
historical executions remain traceable.

------------------------------------------------------------------------

# 22. Missed Executions

The scheduler must recover executions missed because of:

-   scheduler pod failure
-   cluster failure
-   temporary database/application outage
-   deployment/restart
-   scheduling delay

Example:

``` text
10:00 scheduled
10:00 scheduler unavailable

10:05 scheduler recovers
        |
        v
Find missed schedule
        |
        v
Create/claim execution
```

The exact misfire/catch-up behavior should be configurable as part of
the final implementation policy.

The current business requirement is:

> A missed job must not silently disappear.

------------------------------------------------------------------------

# 23. Active-Active Production

Both production clusters are expected to actively schedule.

``` text
                       Scheduler DB
                            |
             +--------------+--------------+
             |                             |
      Production Cluster A          Production Cluster B
             |                             |
       +-----+-----+                 +-----+-----+
       |     |     |                 |     |     |
      Pod   Pod   Pod               Pod   Pod   Pod
       |     |     |                 |     |     |
       +-----+-----+                 +-----+-----+
             |                             |
             +--------------+--------------+
                            |
                       Atomic Claim
                            |
                            v
                     Internal Kafka
```

Both clusters may discover the same due schedules, but database-level
ownership/uniqueness guarantees must ensure only one authoritative
execution is created for each scheduled occurrence.

------------------------------------------------------------------------

# 24. Troubleshooting and Observability

The platform must answer:

> "Why did my job not run?"

and:

> "What happened to execution EXE-123?"

Each execution should record information such as:

``` text
Execution ID
Job ID
Schedule ID
Trigger ID
Tenant ID
Trace ID
Attempt Number
Scheduled Time
Claimed At
Started At
Completed At
Duration
Scheduler Cluster
Scheduler Pod
Status
Retry Count
Next Retry Time
Error
HTTP Status
Upstream Correlation ID
```

Example timeline:

``` text
10:00:00  Schedule became due
10:00:01  Execution claimed
10:00:01  Execution created
10:00:02  Kafka message published
10:00:03  REST executor started
10:00:04  HTTP 503
10:00:04  Retry scheduled
10:00:34  REST executor retry
10:00:35  HTTP 200
10:00:35  Execution SUCCESS
```

Important operational metrics:

-   Number of due executions
-   Claim rate
-   Dispatch rate
-   Kafka queue depth
-   Kafka consumer lag
-   Execution success rate
-   Execution failure rate
-   Retry rate
-   Failed-final count
-   Average execution latency
-   Oldest pending execution
-   Scheduler pod health
-   Scheduler DB latency

The **oldest pending execution** metric is particularly important for
determining whether the platform is falling behind.

------------------------------------------------------------------------

# 25. Failure Scenarios

## Scheduler Pod Crash

``` text
Pod A claims execution
       |
Pod A crashes
       |
Claim lease expires
       |
Pod B recovers execution
       |
Execution continues
```

## Entire Cluster Crash

``` text
Cluster A unavailable
       |
Cluster B continues scheduling
       |
Pending durable executions remain in DB
       |
Cluster B recovers/dispatches them
```

## Database Failure

The scheduler cannot safely create/claim new executions while the
authoritative DB is unavailable.

Once DB connectivity recovers, the scheduler must discover missed
schedules and recover them.

No execution should be silently discarded.

## Internal Kafka Failure

Execution state remains durable.

The scheduler must not mark an execution successful merely because it
attempted to publish.

Publishing success/failure must be observable, and un-dispatched
executions must remain recoverable.

## REST Failure

``` text
REST -> 503
       |
FAILED
       |
Retry Policy
       |
Retry
```

After exhausting retries:

``` text
FAILED_FINAL
```

## Network failure after REST request

The external service may have processed the request even though the
scheduler did not receive the response.

Therefore:

``` text
Execution ID
      |
      v
Idempotency-Key
```

must be used for REST triggers where the downstream supports
idempotency.

------------------------------------------------------------------------

# 26. DB Scheduler vs Microservice Pooling

## Comparison

  -----------------------------------------------------------------------
  Dimension               DB as Scheduler         Microservice Pooling
  ----------------------- ----------------------- -----------------------
  Scheduling engine       Database native         Spring Boot scheduler
                          scheduler               pods

  Source of truth         Scheduler DB            Scheduler DB

  Cron                    DB-specific             Quartz-compatible
                          implementation          

  Scheduling state        DB                      DB

  Execution transport     Internal Kafka          Internal Kafka

  Plugin architecture     Application layer       Application layer

  REST execution          Trigger executor        Trigger executor

  Kafka execution         Trigger executor        Trigger executor

  Horizontal scheduler    DB scheduler dependent  Application pods
  scaling                                         

  Active-active           DB scheduler dependent  Explicitly designed

  1M+ schedule scaling    Dependent on DB         Application + DB
                          scheduler               scaling

  Atomic claiming         DB-native mechanism     Explicit DB atomic
                                                  claim

  DB dependency           Higher                  Lower at
                                                  scheduling-engine level

  DB vendor portability   Lower                   Higher

  Application control     Lower                   Higher

  Scheduler customization DB dependent            High

  Operational simplicity  Potentially simpler     More application
                          initially               components

  Observability           More work               Strong control
  customization                                   

  Plugin extensibility    Requires application    Natural
                          layer                   

  Retry integration       Application layer       Application layer

  Runtime variable        Application layer       Application layer
  resolution                                      

  Failure recovery        DB-specific +           Explicit application
  control                 application             design

  Cloud portability       Depends on DB           Better

  Vendor lock-in          Higher                  Lower

  Development effort      Potentially lower       Higher initially
                          initially               

  Long-term scheduler     More constrained        More flexible
  customization                                   
  -----------------------------------------------------------------------

------------------------------------------------------------------------

# 27. Option 1 --- DB Scheduler: Key Pros

1.  Mature database scheduling capabilities may already exist.
2.  Durable scheduling state is naturally colocated with the scheduler.
3.  Strong transactional and locking primitives.
4.  Potentially fewer application scheduler components.
5.  Database-native recovery may reduce some application code.
6.  Existing enterprise database operations may already be well
    understood.

# 28. Option 1 --- DB Scheduler: Key Cons

1.  Strong dependency on database-specific scheduler behavior.
2.  Scaling to 1M+ schedules depends heavily on the selected DB
    scheduler.
3.  Active-active behavior must be validated carefully.
4.  More difficult to maintain database portability.
5.  Advanced scheduler behavior can become vendor-specific.
6.  Troubleshooting may cross DB scheduler and application boundaries.
7.  Database becomes responsible for a large scheduling workload.
8.  Application teams have less control over scheduling behavior.

------------------------------------------------------------------------

# 29. Option 2 --- Microservice Pooling: Key Pros

1.  Horizontal scheduler scaling through multiple pods.
2.  Explicit control over active-active behavior.
3.  Database remains the source of truth without making it the
    scheduler.
4.  Natural fit for plugin-based architecture.
5.  Strong application-level observability.
6.  Easier integration with Kafka.
7.  Easier integration with retry and execution lifecycle.
8.  Scheduler behavior can evolve independently of DB vendor features.
9.  Better control over batch size, polling rate and backpressure.
10. Better portability across MongoDB, Oracle, SQL Server, etc.

# 30. Option 2 --- Microservice Pooling: Key Cons

1.  More application code.
2.  Polling strategy must be carefully designed.
3.  Atomic claiming must be implemented correctly.
4.  Claim leases and recovery require explicit handling.
5.  Scheduler pods introduce additional operational components.
6.  Incorrect polling/indexing could create significant DB load.
7.  The team owns scheduler correctness instead of delegating it to a DB
    scheduler.
8.  More testing is required for concurrency and failure scenarios.

------------------------------------------------------------------------

# 31. Technology Direction

The scheduling service is primarily an enterprise application with:

-   APIs
-   persistent state
-   distributed coordination
-   Kafka integration
-   plugin architecture
-   retries
-   observability
-   authentication/authorization integration
-   execution tracking

For the Microservice Pooling option, **Spring Boot/Java** is the natural
candidate given the enterprise integration requirements and the existing
ecosystem.

Python remains technically possible, but language selection should be
considered secondary to the scheduling architecture.

The critical decision is:

``` text
Who determines and claims due schedules?
```

not simply:

``` text
Java vs Python
```

------------------------------------------------------------------------

# 32. Database Considerations

Candidate databases currently available:

-   MongoDB
-   Oracle
-   SQL Server

The database should be selected based on the scheduler's actual access
patterns rather than based solely on whether the database provides a
native scheduler.

Important evaluation criteria:

-   Indexed due-time lookup
-   Atomic claiming
-   Concurrency behavior
-   Transaction support
-   Locking semantics
-   Partitioning/sharding
-   Write throughput
-   Read throughput
-   Storage growth
-   High availability
-   Backup/recovery
-   Operational expertise
-   Cost
-   Enterprise standards

The logical design remains database-neutral.

------------------------------------------------------------------------

# 33. Recommended Logical Data Relationships

``` text
+---------+
|   Job   |
+----+----+
     |
     | 1:N
     v
+------------+
|  Schedule  |
+------+-----+
       |
       | 1:N
       v
+------------+
| Execution  |
+------------+

+------------+
|  Trigger   |
+------+-----+
       ^
       |
       | referenced by Job / execution configuration
       |
+------+-----+
|    Job     |
+------------+
```

A more complete conceptual model:

``` text
                 +----------------+
                 |      Job       |
                 +-------+--------+
                         |
                +--------+--------+
                |        |        |
                v        v        v
             Schedule Schedule Schedule
                |        |        |
                +--------+--------+
                         |
                         v
                    Execution
                         |
                         v
                  Trigger Reference
                         |
                         v
                      Trigger
```

------------------------------------------------------------------------

# 34. Execution Distribution Example

Suppose 100 jobs become due at 10:00.

We must not overload one scheduler pod.

``` text
                        100 Due Jobs
                             |
               +-------------+-------------+
               |             |             |
               v             v             v
            Pod 1          Pod 2          Pod 3
          Claim batch     Claim batch     Claim batch
               |             |             |
               +-------------+-------------+
                             |
                             v
                     Internal Kafka
                             |
             +---------------+---------------+
             |               |               |
             v               v               v
          Worker 1        Worker 2        Worker N
             |               |               |
             v               v               v
           REST            Kafka          REST
```

Batch claiming prevents one scheduler pod from monopolizing all due
work.

Kafka allows trigger execution capacity to scale independently from
scheduler capacity.

------------------------------------------------------------------------

# 35. API Concept

The API is asynchronous.

Example:

``` http
POST /jobs
```

Response:

``` json
{
  "jobId": "JOB-12345",
  "status": "ACCEPTED",
  "traceId": "TRC-98765"
}
```

Execution lookup:

``` http
GET /jobs/JOB-12345
```

Execution lookup:

``` http
GET /executions/EXE-98765
```

The API does not wait for the scheduled trigger to execute.

------------------------------------------------------------------------

# 36. Important Invariants

The following invariants should be treated as non-negotiable:

### Invariant 1

A scheduled occurrence must not silently disappear.

### Invariant 2

One schedule occurrence must have one authoritative Execution ID.

### Invariant 3

Multiple scheduler pods/clusters may discover a schedule, but only one
may successfully claim/create the authoritative execution.

### Invariant 4

A scheduler pod failure must not permanently strand an execution.

### Invariant 5

Trigger execution must be separated from scheduling.

### Invariant 6

A trigger plugin must be independently extensible.

### Invariant 7

The scheduler must remain unaware of trigger implementation details.

### Invariant 8

Every execution must be traceable using an Execution ID.

### Invariant 9

REST calls should use the Execution ID as an Idempotency-Key where
supported.

### Invariant 10

Running executions continue even if their parent job is deleted; future
executions are stopped.

### Invariant 11

Multiple executions may run concurrently.

------------------------------------------------------------------------

# 37. Stakeholder Decision

The key decision to be made is:

## Option 1 --- Database as Scheduler

``` text
DB determines what is due
        |
        v
Application handles execution
```

versus:

## Option 2 --- Microservice Pooling

``` text
Spring Boot pods determine what is due
        |
        v
DB provides durable state + atomic claiming
        |
        v
Application handles execution
```

The execution architecture after scheduling is intentionally the same.

This allows the organization to make the decision specifically around
the **scheduling responsibility** without redesigning the rest of the
platform.

------------------------------------------------------------------------

# 38. Future Scope

Potential future enhancements:

-   Tenant-level quotas
-   Tenant-level rate limiting
-   Tenant concurrency limits
-   Trigger versioning
-   Configurable concurrency policies
-   Richer misfire policies
-   Additional trigger plugins
-   UI dashboards
-   Execution replay
-   Manual retry
-   Run-now functionality
-   Pause/resume
-   Schedule history
-   Advanced SLA monitoring
-   Multi-region scheduling
-   Cross-region disaster recovery
-   Dynamic worker autoscaling
-   Scheduler capacity management

------------------------------------------------------------------------

# 39. Summary

The proposed scheduler is a durable, distributed scheduling platform
designed for 1M+ schedules and active-active production deployments.

The core architecture separates:

``` text
Scheduling
    |
    v
Durable Execution
    |
    v
Execution Transport
    |
    v
Trigger Plugin
    |
    v
External System
```

The two candidate scheduling models are:

``` text
Option 1:
Database as Scheduler

Option 2:
Microservice Pooling
```

Both use:

``` text
One Scheduler DB
+
Internal Kafka
+
Trigger Plugin Architecture
+
Durable Execution State
+
Execution IDs
+
Retry Policies
+
Observability
```

The primary stakeholder decision is therefore not whether the overall
platform should use a database or microservices. Both options use both.

The decision is:

> **Should the database's native scheduling engine determine and
> initiate due executions, or should horizontally scalable Spring Boot
> scheduler pods poll the database and atomically claim due schedules?**
