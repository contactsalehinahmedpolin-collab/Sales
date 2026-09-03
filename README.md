# Email verification (MillionVerifier)

Takes a CSV of leads, verifies every email against MillionVerifier, and splits the list
into `clean` / `risky` / `invalid` / `error` — without spending a credit twice.

Two backends, picked automatically by list size:

- **single** — one HTTP request per address against the realtime v3 API. Immediate,
  good up to a few thousand.
- **bulk** — one file upload for the whole list against the bulk v2 API, then poll and
  download. The right choice from a few thousand upward; it replaces N round trips with
  three.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env          # then put your key in it
export MILLIONVERIFIER_API_KEY=...   # key from https://app.millionverifier.com/api
```

## Use

```bash
# See what it would cost before spending anything
python verify_emails.py leads.csv --dry-run

# Verify (picks single or bulk by size)
python verify_emails.py leads.csv --out-dir out

# Force a backend
python verify_emails.py leads.csv --mode bulk
python verify_emails.py leads.csv --mode single --workers 30
```

Outputs in `out/`:

| File | Contents |
| --- | --- |
| `clean.csv` | `result=ok` — safe to send |
| `risky.csv` | `catch_all`, `unknown` — send only from a domain you can afford to burn |
| `invalid.csv` | `invalid`, `disposable` — never send |
| `error.csv` | rows with no email, or that could not be verified |
| `all_results.csv` | every input row, with the verification columns appended |

Every output file carries the original columns plus `mv_result`, `mv_quality`,
`mv_subresult`, `mv_free`, `mv_role`, `mv_error`, `mv_checked_at`.

Exit codes: `0` all rows verified, `1` some rows landed in `error.csv`, `2` the run did
not complete (bad key, no credits, failed bulk job).

### Options

```
--email-column NAME     email column name (default: email)
--mode {auto,single,bulk}   backend (default: auto)
--cache PATH            result cache (default: .mv_cache.jsonl)
--max-retries N         attempts per request (default: 4)
--dry-run               report credit cost, verify nothing

single mode:
--workers N             concurrent requests (default: 20)
--rate N                requests per second ceiling (default: 50; API limit is 160)

bulk mode:
--bulk-threshold N      auto switches to bulk at this many pending emails (default: 1000)
--chunk-size N          emails per uploaded file (default: 50000)
--bulk-state PATH       in-flight bulk file ids (default: .mv_bulk.json)
--poll-interval N       seconds between status polls (default: 15)
--bulk-timeout N        max seconds to wait per bulk file (default: 3600)
--delete-remote         delete the uploaded file from MillionVerifier after download
```

## Why it's built this way

Credits are the constraint, so the design is about not wasting them.

**Dedup before spending.** Emails are lowercased and deduplicated; three rows with the
same address cost one credit and still all appear in the output.

**Checkpointed result cache.** Each result is appended to `.mv_cache.jsonl` the moment it
lands. Kill the run at row 9,000 of 10,000 and the next run resumes from 9,000 instead of
re-buying them. Delete the file to force a re-verify. `error` results are never cached, so
failures retry rather than sticking.

**Resumable bulk jobs.** A bulk job is paid for at upload time and then runs server-side,
so the file id is written to `.mv_bulk.json` *before* the first status poll. If the
process dies while waiting, the next run re-attaches to the running job and downloads it
— re-running never re-uploads a chunk you already paid for. The id is cleared only once
that chunk's results are delivered.

**Fail fast on account errors.** MillionVerifier returns HTTP 200 with an `error` field
for a bad key or exhausted credits. In single mode that trips a shared abort flag and the
remaining workers stop immediately; in bulk mode it raises before the next chunk uploads.
Either way you don't march the whole list through a guaranteed failure.

**Real rate limiting.** Pacing happens in the request path via a shared token bucket.
Sleeping in the result loop after `ThreadPoolExecutor.map` throttles nothing — `map` has
already submitted every request.

**The download layout is resolved, not assumed.** The bulk results CSV is parsed by
locating the email and result columns from the file itself (by header name, or by
inspecting values when there is no header). A schema change on their side produces a loud
error instead of a silently mis-parsed list.

**Nothing is silently dropped.** Every input row lands in exactly one bucket, including
emails the bulk download omits entirely; the run asserts the counts reconcile before it
exits.

**Retries with backoff** on 429, 5xx, network failures, and non-JSON bodies, in both
backends.

## Result meanings

| `mv_result` | Meaning | Bucket |
| --- | --- | --- |
| `ok` | Mailbox exists and accepts mail | clean |
| `catch_all` | Domain accepts everything; the mailbox is unconfirmed | risky |
| `unknown` | Server did not give a definitive answer | risky |
| `disposable` | Temporary/throwaway address | invalid |
| `invalid` | Does not exist | invalid |

`catch_all` is the judgement call. Sending to catch-alls inflates your list and your
bounce risk is unmeasured — treat `risky.csv` as a separate, slower campaign on a domain
you are willing to lose, not as extra volume for your primary sending domain.

## Layout

```
verify_emails.py       entry point
mvverify/
  models.py            result vocabulary, Verification record
  cache.py             append-only result cache
  realtime.py          single-email backend + rate limiter
  bulk.py              bulk file backend + download parser
  csvio.py             CSV read/write
  cli.py               argument parsing and orchestration
tests/                 73 tests, API stubbed, no network
```

## Limits worth knowing

- Bulk mode is asynchronous by nature: you pay at upload and the answer arrives when the
  job finishes. For a list you need answered *now*, force `--mode single`.
- Verification proves an address is deliverable today. It says nothing about whether the
  person still works there or wants to hear from you.
- `.gitignore` excludes `leads.csv`, `out/`, `.env`, and both state files. Lead data does
  not belong in git.

## Tests

```bash
pip install pytest
python -m pytest tests -q
```

No network access required — both APIs are stubbed.
