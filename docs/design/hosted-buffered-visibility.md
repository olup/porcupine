# Hosted buffered receipt visibility

Two independent hosted workers shared local PostgreSQL 17.11 and AWS
Emulator 1.0.497 on a GitHub-hosted Linux runner. The test sent six
small native track events to each worker's project. Requests alternated
between workers and paused 200 ms after each pair. Both projects stayed
below the size threshold.

After the last receipt, the test queried each project on the opposite
worker about every 250 ms and matched rows to receipt UUIDs. Latency runs
from the client's receipt of the buffered acknowledgment to its first
observation of the row in a query response. The measurements include
polling and query time.

| Flush interval | Receipts observed | Minimum | Median | Maximum |
| --- | ---: | ---: | ---: | ---: |
| 2 s default | 12/12 | 1.221 s | 1.738 s | 2.257 s |
| 60 s fixture | 12/12 | 59.728 s | 60.291 s | 60.810 s |

The fixture killed a writer immediately after it acknowledged a fresh
receipt. The surviving worker and a new worker with fresh scratch space
each read all six rows already published by the killed writer. Both
read zero rows for the fresh receipt at both flush settings. The kills
completed 16 ms and 14 ms after acknowledgment, respectively.

For a separate storage fault at the default timer, the fixture sent
HTTP 503 responses for S3 PUT requests through a loopback proxy.
S3 reads continued. A row from an event acknowledged during the fault
remained absent for 5.034 s, then became visible 0.570 s after writes
recovered. The client did not resend the event.

The next scenario killed a writer 5.046 s after it acknowledged a
receipt during a new S3 write fault. After writes recovered, a cold
worker read zero rows for the interrupted receipt and one row for the
earlier recovered receipt.
[Completed run on hosted/profile base d57e618](https://github.com/olup/porcupine/actions/runs/44180049629).

An earlier run passed the healthy timing, healthy kill and live writer
recovery checks. The cold worker then aborted in the native allocator
during the query after the outage kill. The current run completed that
query. The cause of the earlier abort remains open.
[Earlier run on hosted/profile base 60cd46b](https://github.com/olup/porcupine/actions/runs/44179083685/attempts/1).

The fixture covers two small projects and one steady-path sample at each
timer. The healthy kill sampled a receipt at the start of its timer
window. S3 errors kept an acknowledged event outstanding beyond the
default interval. A worker failure can lose memory-only receipts at
any age. Production visibility under sustained load, maintenance, and
remote S3 latency requires separate measurements.

## Reproduction

Use the isolated AWS Emulator runner on a machine with Docker and the AWS
CLI. The script starts the emulator and PostgreSQL locally.

```sh
PORCUPINE_TEST_HOSTED_BUFFERED_VISIBILITY=1 \
  bash scripts/aws-emulator-test.sh '^TestDuckLakeS3SharedPostgresHostedBufferedVisibility$'
```

The test prints every observed latency and the age of each interrupted
receipt. The 60-second timer is private to the test process.
