# RavenDB Development Workflow

Reference for the full development cycle: worktrees → code → validate → push → PR → CI → review.

---

## Repository layout

| Path | Purpose |
|------|---------|
| `/mnt/shared/ravendb/repo/`           | Main checkout — **never edit files here directly** |
| `/mnt/shared/ravendb/RavenDB-XXXXX/`  | Per-issue worktree — all work happens here |

---

## Starting a new issue

### 1. Create a worktree

Branches are named after the YouTrack issue number. Create the worktree from the main           repo:
  
```bash
cd /mnt/shared/ravendb/repo
git worktree add /mnt/shared/ravendb/RavenDB-XXXXX -b RavenDB-mnt/shared originmnt/sharedmaster
```

All subsequent work is done in `/mnt/shared/ravendb/RavenDB-XXXXX/`. Never commit or edit in the main repo.

---

## Commit discipline

- **Never `--amend`.** Always create a new commit. If a pre-commit hook fails, fix the issue, re-stage, and create a fresh commit — do not amend.
- **Commit message format required by CI:** `RavenDB-XXXXX Description` (the conventions check enforces this).

Example:
```
RavenDB-25281 Fix score ordering direction in QueryPlanBuilder
```

---

## Pre-push validation

All four steps must pass before pushing.

### 1. Build the solution
```bash
dotnet build RavenDB.sln
```

### 2. Build the Studio
```bash
cd src/Raven.Studio
npm ci
npm run restore
npm run compile
```

### 3. Run all fast tests
```bash
dotnet test test/FastTests --configuration Release
```

### 4. Run tests for the current issue

Issue tests live under `test/FastTests/Issues/` and `test/SlowTests/Issues/`, named `RavenDB-XXXXX.cs` or `RavenDB_XXXXX.cs`.

```bash
dotnet test test/FastTests --configuration Release \
  --filter "FullyQualifiedName~FastTests.Issues.RavenDB_XXXXX"

dotnet test test/SlowTests --configuration Release \
  --filter "FullyQualifiedName~SlowTests.Issues.RavenDB_XXXXX"
```

Also run any tests for files that were modified in this change.

## Pushing and Creating PRs

- **Push to `origin`** (`<username>/ravendb` fork):
  ```bash
  git push origin <branch-name>
  ```

- **Create the PR on `<username>/ravendb`**:
  ```bash
  gh pr create --repo <username>/ravendb --title "<branch-name> Description" --body "..."
  ```
  Use `.github/pull_request_template.md` for the body structure.

- **PRs on `ravendb/ravendb`** — we do not have permissions to create them directly. Instead, generate a pre-filled compare URL:
  ```
  https://github.com/ravendb/ravendb/compare/BASE...<username>:ravendb:<branch-name>?expand=1&title=...&body=...
  ```
  Open in a browser and submit manually.

- **URL shortening** — use [is.gd](https://is.gd):
  ```bash
  curl "https://is.gd/create.php?format=json&url=<URL>"
  ```
---

## CI monitoring

After pushing, monitor CI with `gh pr checks`. The checks that matter:

| Check | What it validates |
|-------|------------------|
| `compile / Compile Server & Studio` | Full .NET + Studio build |
| `conventions / Commit Conventions` | Commit message format |
| `tests/fast` | Fast test suite (any OS/config) |

### CI wait script

* Mind the `<username>` in the script you need to change

Save as `/tmp/scripts/ci-wait.py` and run with `python3 /tmp/scripts/ci-wait.py <PR_NUMBER>`:

```python
#!/usr/bin/env python3

import subprocess
import sys
import time
import json
import re
from datetime import datetime

# Configuration
REPO = "<username>/ravendb"
TIMEOUT_SECONDS = 7200
INTERVAL_SECONDS = 60

def run_command(command):
    """Executes a shell command and returns the stdout, return code, and stderr."""
    result = subprocess.run(
        command, 
        shell=True, 
        capture_output=True, 
        text=True
    )
    return result.stdout, result.returncode, result.stderr

def get_pr_checks(pr_number):
    """Fetches the PR checks from the GitHub CLI."""
    cmd = f"gh pr checks {pr_number} --repo {REPO} --json name,status,conclusion"
    stdout, return_code, stderr = run_command(cmd)
    
    if return_code != 0:
        print(f"Error fetching PR checks: {stderr}", file=sys.stderr)
        return None
    
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        print(f"Failed to parse JSON output: {stdout}", file=sys.stderr)
        return []

def main():
    if len(sys.argv) < 2:
        print("Usage: python check_pr.py <PR_NUMBER>", file=sys.stderr)
        sys.exit(2)
        
    pr_number = sys.argv[1]
    elapsed = 0

    print(f"Monitoring checks for PR #{pr_number} on {REPO}...")

    while elapsed < TIMEOUT_SECONDS:
        checks = get_pr_checks(pr_number)
        
        if checks is None:
            sys.exit(1)

        # Initialize tracking variables
        compile_status = "N/A:N/A"
        conventions_status = "N/A:N/A"
        fast_statuses = []
        pending_count = 0

        # Process checks
        for check in checks:
            name = check.get("name", "")
            status = check.get("status", "unknown")
            conclusion = check.get("conclusion", "none")

            if "compile / Compile Server" in name:
                compile_status = f"{status}:{conclusion}"
            elif "conventions / Commit Conventions" in name:
                conventions_status = f"{status}:{conclusion}"
            elif "tests/fast" in name:
                fast_statuses.append(f"{status}:{conclusion}")

            # Count pending checks
            if re.search("compile / Compile Server|conventions / Commit Conventions|tests/fast", name):
                if status != "completed":
                    pending_count += 1

        fast_str = ",".join(fast_statuses)
        current_time = datetime.now().strftime("%H:%M:%S")
        print(f"[{current_time}] compile={compile_status} conventions={conventions_status} fast={fast_str}")

        # If all relevant checks are completed, evaluate conclusions
        if pending_count == 0:
            failures = []
            
            for check in checks:
                name = check.get("name", "")
                conclusion = check.get("conclusion", "none")
                
                if re.search("compile / Compile Server|conventions / Commit Conventions|tests/fast", name):
                    if conclusion != "success":
                        failures.append(f"{name}: {conclusion}")

            if not failures:
                print("\nAll checks passed.")
                sys.exit(0)
            else:
                print("\nFAILURES:")
                for failure in failures:
                    print(failure)
                sys.exit(1)

        time.sleep(INTERVAL_SECONDS)
        elapsed += INTERVAL_SECONDS

    print("\nTimed out after 2 hours.")
    sys.exit(2)

if __name__ == "__main__":
    main()
```

---

## Copilot review cycle

After CI passes (maximum 10 rounds):

### 1. Request review
```bash
gh pr edit "$PR_NUMBER" --repo <username>/ravendb --add-reviewer @copilot
```

### 2. Wait for review

* Mind the `<username>` in the script that you need to change when you write it. 

```python
#!/usr/bin/env python3

import sys
import time
import json
import subprocess
from datetime import datetime

# Configuration
REPO = "<username>/ravendb"
TIMEOUT_SECONDS = 7200
INTERVAL_SECONDS = 60

def run_gh_api(endpoint):
    """Executes a `gh api` command and returns the parsed JSON response or an error."""
    cmd = ["gh", "api", endpoint]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        return None, result.stderr
    
    try:
        return json.loads(result.stdout), None
    except json.JSONDecodeError:
        return None, "Failed to parse JSON response"

def main():
    if len(sys.argv) < 2:
        print("Usage: python check_copilot_review.py <PR_NUMBER>", file=sys.stderr)
        sys.exit(2)

    pr_number = sys.argv[1]
    elapsed = 0

    print(f"Monitoring Copilot reviews for PR #{pr_number}...")

    while elapsed < TIMEOUT_SECONDS:
        endpoint = f"repos/{REPO}/pulls/{pr_number}/reviews"
        reviews, error = run_gh_api(endpoint)

        if error:
            print(f"Error fetching reviews: {error}", file=sys.stderr)
            state = "none"
        else:
            # Filter reviews by the 'copilot' user
            copilot_reviews = [r for r in reviews if r.get("user", {}).get("login") == "copilot"]
            
            if copilot_reviews:
                # Sort reviews by 'submitted_at' and grab the last one
                copilot_reviews.sort(key=lambda x: x.get("submitted_at", ""))
                state = copilot_reviews[-1].get("state", "none")
            else:
                state = "none"

        current_time = datetime.now().strftime("%H:%M:%S")
        print(f"[{current_time}] Copilot review: {state}")

        if state not in ("none", "null", "PENDING"):
            print(f"Review received: {state}")
            
            comments_endpoint = f"repos/{REPO}/pulls/{pr_number}/comments"
            comments, comments_error = run_gh_api(comments_endpoint)
            
            if comments_error:
                print(f"Error fetching comments: {comments_error}", file=sys.stderr)
            elif comments:
                for comment in comments:
                    if comment.get("user", {}).get("login") == "copilot":
                        print("---")
                        path = comment.get("path")
                        # Fallback to original_line if line is null
                        line = comment.get("line") or comment.get("original_line")
                        body = comment.get("body")
                        print(f"{path}:{line}\n{body}")
            sys.exit(0)

        time.sleep(INTERVAL_SECONDS)
        elapsed += INTERVAL_SECONDS

    print("Timed out.")
    sys.exit(2)

if __name__ == "__main__":
    main()
```

### 3. Address suggestions

| Situation | Action |
|-----------|--------|
| Agree with the suggestion | Fix and push |
| Disagree | Reply to the comment with reasoning; note in final report |
| Architectural change requested | Do **not** implement — flag to user and continue with other fixes |
| Unresolvable back-and-forth | Skip after 2 rounds; note in final report |

After pushing fixes, restart from step 1.

### 4. Final report

Summarise: CI status · what was fixed · disagreements (with reasoning) · deferred architectural items · skipped back-and-forth items.

---

## YouTrack issues

### Basic CLI usage (reading)

```bash
yt issues show RavenDB-XXXXX
yt issues list --project RavenDB
yt projects fields RavenDB
```

### Creating / updating issues — use the REST API

The `yt` CLI (`youtrack-cli` v0.22.2) has a bug: `--custom-field` breaks on version-type fields (`Affected Release`, `Target Release`) — it sends them as `EnumBundleElement` instead of `VersionBundleElement`, causing a 400.

**Workaround: use curl directly.**

#### Get credentials

The token is stored in the plaintext keyring at `~/.local/share/python_keyring/keyring_pass.cfg`. Keys:
- `encryption_2dkey` → Fernet key
- `youtrack_5ftoken` → YouTrack API token (Fernet-encrypted)
- `youtrack_5fbase_5furl` → base URL

Or set `YT_URL` and `YT_TOKEN` environment variables manually.

#### Create an issue

```bash
curl -X POST "$YT_URL/api/issues" \
  -H "Authorization: Bearer $YT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "project": {"id": "51-0"},
    "summary": "Short description",
    "description": "Full description",
    "customFields": [
      {
        "$type": "SingleEnumIssueCustomField",
        "name": "Type",
        "value": {"$type": "EnumBundleElement", "name": "Bug"}
      },
      {
        "$type": "SingleEnumIssueCustomField",
        "name": "Priority",
        "value": {"$type": "EnumBundleElement", "name": "Normal"}
      },
      {
        "$type": "SingleVersionIssueCustomField",
        "name": "Affected Release",
        "value": {"$type": "VersionBundleElement", "name": "6.2.x"}
      },
      {
        "$type": "SingleVersionIssueCustomField",
        "name": "Target Release",
        "value": {"$type": "VersionBundleElement", "name": "6.2.x"}
      }
    ]
  }'
```

Notes:
- RavenDB project internal ID: `51-0`
- `Type` valid values: `Bug`, `Feature`, `Task`, `Exception`, etc.
- `Priority` valid values: `Normal`, `Critical`, `Major`, `Minor`

#### Get the readable issue ID after creation

```bash
curl "$YT_URL/api/issues/{internal-id}?fields=idReadable,summary"
```

The `id` in the create response is the internal ID; `idReadable` is `RavenDB-XXXXX`.
