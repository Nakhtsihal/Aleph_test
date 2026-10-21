# Async Log Processing Pipeline

A containerized Python system that retrieves logs through the provided Aleph API, normalizes their fields, enriches them using Redis, and sends the results to a local JSON file and NATS.

NATS replaces Kafka according to the amended assignment.

## Architecture

The system contains five Docker Compose services:

| Service | Purpose |
|---|---|
| `redis` | Stores enrichment metadata keyed by IP address |
| `nats` | Stores enriched log messages using JetStream |
| `redis-loader` | Loads `data/enrichment.json` into Redis and exits |
| `aleph-api` | Returns regular and syslog records filtered by time |
| `processor` | Periodically retrieves, parses, enriches, publishes, and saves logs |

Processing flow:

```text
enrichment.json → redis-loader → Redis
                                  ↓
Aleph API → processor → parsing → enrichment → NATS JetStream
                                           → data/output.json
```

The processor waits for the Redis preload job to complete successfully before starting.

## Requirements

- Docker and Docker Compose.
- Python 3.12+ for running the optional local NATS subscriber.

Python services run in a Python 3.12 Docker image.

## Input files

The following files must be present in `data/`:

- `enrichment.json`: IP addresses mapped to enrichment metadata.
- `ha_logs.json`: regular logs.
- `ha_syslogs.json`: syslog records.

Both log datasets are retrieved through the API.

## Run the system

Run commands from the project root.

For a clean run, move any existing `data/output.json` to a backup location before starting the processor. Otherwise, new results will be appended to the existing records.

Start the infrastructure and API:

```bash
docker compose up -d --build redis nats aleph-api
```

The processor creates the JetStream stream on its first run. After that, the subscriber can read stored messages even if it was not running when they were published.

Start the processor and its Redis preload dependency:

```bash
docker compose up -d --build processor
```

Check processing messages:

```bash
docker compose logs --follow processor
```

For the supplied datasets and configured start time, the first successful batch contains 1,000 records: 500 regular logs and 500 syslog records. Subsequent successful polls report `0` while no new input records are available.

The Redis loader exiting after printing `Loaded` is expected: it is a one-time job.

Stop the system:

```bash
docker compose down
```

The output file remains in the local `data/` directory. JetStream messages and durable consumer progress remain in the `nats_data` Docker volume. Running `docker compose down -v` deletes that volume.

## Observe NATS messages

The subscriber connects to `localhost:4222`, exposed by Docker Compose.

For local execution, create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

After the processor has created the stream, run the subscriber in a separate terminal:

```bash
python src/nats_subscriber.py
```

Messages are published to the `enriched_logs` subject and stored in the `ENRICHED_LOGS` JetStream stream using file storage.

The subscriber uses the durable consumer `log_viewer` and explicitly acknowledges each message after printing it. Restarting the subscriber with the same consumer resumes from its saved progress; acknowledged messages are not normally delivered again. Unacknowledged messages can be redelivered.

Acknowledgements track consumer progress. They do not delete messages from the stream under the current retention policy.

JetStream files are stored under `/data` inside the NATS container, backed by the `nats_data` Docker volume.

## Verify the output

From the project root, run the following in a Python console:

```python
import json
from pathlib import Path

with open("data/output.json", encoding="utf-8") as file:
    output = json.load(file)

print("Records:", len(output))
print("Unique IDs:", len({log["audit_id"] for log in output}))
print("Enriched:", sum(bool(log["findings"]) for log in output))
print("Temporary file exists:", Path("data/output.json.tmp").exists())
```

After one complete run against a clean output file, expected values are:

```text
Records: 1000
Unique IDs: 1000
Enriched: 1000
Temporary file exists: False
```

All client IP addresses in the supplied datasets have matching enrichment metadata. Unknown or missing IP addresses produce an empty `findings` object.

To check HTTP error recovery, temporarily stop the API:

```bash
docker compose stop aleph-api
```

The processor logs the request error and retries after 30 seconds without advancing the start of the polling range.

Restore the API:

```bash
docker compose up -d aleph-api
```

The processor resumes requests without being restarted.

## Configuration

The processor reads these environment variables:

| Variable | Purpose |
|---|---|
| `REDIS_HOST` | Redis hostname |
| `NATS_URL` | NATS connection URL |
| `ALEPH_API_URL` | Log retrieval endpoint |
| `START_TIMESTAMP` | Initial Unix timestamp in seconds |

Compose sets `START_TIMESTAMP` to `1640995200`, the beginning of 2022, so the supplied historical records are retrieved. If this variable is absent, the processor starts from the current time.

The processor pauses for 30 seconds after each successful batch or failed HTTP request.

## Parsing and output decisions

- `ClientIP` or `ipadress` becomes `client_ip`.
- `Messages` or `details` becomes `message`.
- `sourceofdata` becomes `source`.
- `Severity` becomes `severity`.
- For the supplied syslog format, source and severity are extracted from the header.
- Fields without a corresponding input value remain `null`. In particular, the supplied regular records do not contain `sourceofdata` or `Severity`.
- Original timestamp values are preserved in the output. API filtering supports numeric Unix timestamps in milliseconds and ISO 8601 strings, converting both to seconds for comparison.
- ISO 8601 timestamps without a timezone are interpreted as UTC. Explicit timezone offsets are respected.
- Normalized fields remain at the top level.
- `aleph` contains `source` and `message`.
- `findings` maps the client IP to its full Redis metadata.
- Other available fields handled by the parser, such as `audit_id` and `action`, are retained.

HTTP, Redis, and NATS operations use asynchronous clients. Blocking file operations run through `asyncio.to_thread()`. A semaphore limits concurrent Redis enrichment lookups to 20.

The output is a JSON array. Each non-empty batch is added to the existing records, written to a temporary file, and then replaces the main file using `os.replace()`.

## Scope and limitations

The syslog parser supports the format supplied with the assignment.

The polling position is stored in memory. Restarting the processor uses `START_TIMESTAMP` again and can append previously processed records.

File output and NATS publication are separate operations. They are not a single transaction, and the system does not provide exactly-once delivery across both outputs.
