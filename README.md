# Email verification (MillionVerifier)

Takes a CSV of leads, verifies every email against the MillionVerifier v3 API, and
splits the list into `clean` / `risky` / `invalid` / `error` — without spending a credit
twice.

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

# Verify
python verify_emails.py leads.csv --out-dir out
```

Outputs in `out/`:

| File | Contents |
| --- | --- |
| `clean.csv` | `result=ok` — safe to send |
| `risky.csv` | `catch_all`, `unknown` — send only from a warmed domain you can afford to burn |
| `invalid.csv` | `invalid`, `disposable` — never send |
| `error.csv` | rows with no email, or that could not be verified |
| `all_results.csv` | every input row, with the verification columns appended |

Every output file carries the original columns plus `mv_result`, `mv_quality`,
`mv_subresult`, `mv_free`, `mv_role`, `mv_error`, `mv_checked_at`.

Exit codes: `0` all rows verified, `1` some rows landed in `error.csv`, `2` the run
aborted on an account problem (bad key, no credits).

### Options

```
--email-column NAME   email column name (default: email)
--workers N           concurrent requests (default: 20)
--rate N              requests per second ceiling (default: 50; API limit is 160)
--max-retries N       attempts per email (default: 4)
--cache PATH          result cache (default: .mv_cache.jsonl)
--dry-run             report credit cost, verify nothing
```

## Why it's built this way

Credits are the constraint, so the design is about not wasting them:

- **Dedup before spending.** Emails are lowercased and deduplicated; three rows with the
  same address cost one credit and still all appear in the output.
- **Checkpointed cache.** Each result is appended to `.mv_cache.jsonl` the moment it
  lands. Kill the run at row 9,000 of 10,000 and the next run resumes from 9,000 instead
  of re-buying them. Delete the file to force a re-verify. `error` results are never
  cached, so failures retry rather than sticking.
- **Fail fast on account errors.** MillionVerifier returns HTTP 200 with an `error` field
  for a bad key or exhausted credits. Hitting one trips an abort flag and the remaining
  workers stop immediately rather than marching through the whole list.
- **Real rate limiting.** Pacing happens in the request path via a shared token bucket.
  Sleeping in the result loop after `ThreadPoolExecutor.map` throttles nothing — `map`
  has already submitted every request.
- **Nothing is silently dropped.** Every input row lands in exactly one bucket; the run
  asserts the counts reconcile before it exits.
- **Retries with backoff** on 429, 5xx, network failures, and non-JSON bodies.

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

## Limits worth knowing

- This uses the single-email endpoint, which is one HTTP request per address. For lists
  in the hundreds of thousands, MillionVerifier's bulk file-upload API is cheaper in
  wall-clock time; this tool does not use it.
- Verification proves an address is deliverable today. It says nothing about whether the
  person still works there or wants to hear from you.
- `.gitignore` excludes `leads.csv`, `out/`, `.env`, and the cache. Lead data does not
  belong in git.

## Tests

```bash
pip install pytest
python -m pytest tests -q
```

No network access required — the API is stubbed.
