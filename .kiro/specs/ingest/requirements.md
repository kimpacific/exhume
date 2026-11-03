# Requirements Document

## Introduction

Ingest is the first pipeline stage in Exhume. It accepts a user-submitted repository (as a zip file or Git URL), validates and stores it, inventories every file, scrubs secrets, and creates an analysis run. The stage finishes by enqueuing the next pipeline job and returning a run identifier to the caller.

Validation is split into two phases. **Synchronous validation** runs in the API handler: if it fails, the request is rejected immediately and no Analysis_Run is created. **Asynchronous processing** runs in a River worker after the Analysis_Run is created in the Queued state: if it fails, the Analysis_Run transitions to Failed.

Synchronous checks: upload size, zip magic bytes, Git URL scheme / userinfo / DNS / IP address validation, commit SHA resolution (Git), Content_Hash computation (zip), abuse limits.
Asynchronous checks: zip central-directory parse, zip extraction safety, Git clone of the pinned commit, file classification, secret scanning, inventory, Scrubbed_Copy creation.

Parsing, graph building, and LLM analysis are out of scope for this stage.

## Glossary

- **Ingest_Service**: The Go backend component (internal/ingest) responsible for receiving, validating, storing, and inventorying a submitted repository.
- **Analysis_Run**: A persistent record in the database representing one execution of the full Exhume pipeline for a given repository submission. Lifecycle: Queued → Ingesting → Parsing | Failed.
- **Run**: Short form for Analysis_Run, used in identifiers such as run_id.
- **Project**: The top-level owner entity that groups one or more Analysis_Runs. Created automatically on first submission if it does not exist. Every database query is scoped by project_id.
- **Submission**: A user-initiated request containing a repository source (zip upload or Git URL) and optional supplementary files (database schema dump, documentation files). Supplementary files are subject to the same size limits, secret scanning, classification, and Scrubbed_Copy rules as repository files.
- **Zip_Upload**: A repository submitted as a single zip archive file.
- **Git_URL**: An HTTPS URL pointing to a remote Git repository used as a repository source.
- **Extraction_Root**: The temporary directory into which zip archive entries are written during extraction. The `.git` directory is never extracted.
- **Scrubbed_Copy**: A per-run copy of the extracted repository stored in Object_Storage with all secret values replaced by `[REDACTED:<TYPE>]` placeholders. Binary-classified files are represented as metadata rows only and are not copied into the Scrubbed_Copy. Files that are secret-bearing by name (`.env`, `.env.*` excluding `.env.example`, `.env.sample`, and `.env.template`, `id_rsa`, `*.pem`, `*.key`, `*.pfx`, `*.p12`, `*.jks`) are replaced entirely with a single redaction line; `.env.example`, `.env.sample`, and `.env.template` are not treated as secret-bearing by name and go through normal pattern scanning. The `.git` directory is excluded. All downstream pipeline stages read only the Scrubbed_Copy.
- **File_Classifier**: The Ingest_Service component that assigns each extracted file a classification label.
- **File_Classification**: One of: source, config, SQL, batch_script, vendor, generated, binary, docs, or unclassified. Applied in priority order: binary, vendor, generated, SQL, batch_script, config, docs, source.
- **Source_File**: A database row in the source_file table recording a single file's path, classification, size in bytes, and lines of code (non-blank lines; binary files record zero).
- **Secret_Scanner**: The Ingest_Service component that applies best-effort pattern detection to mask secrets in non-binary file content before any content can reach an LLM. Pattern matching may not catch all secrets.
- **Masked_Item**: A single detected secret occurrence replaced with a `[REDACTED:<TYPE>]` placeholder token. The original value is never stored or logged.
- **Ingest_Summary**: A per-run record of the count and type of Masked_Items found during secret scanning, stored without secret values, and readable via the Ingest_Summary endpoint.
- **River**: The background job queue used by the Go backend. Every job is idempotent, keyed by (run_id, stage, unit_id).
- **Object_Storage**: The remote blob store (S3-compatible, Cloudflare R2) where raw uploads and Scrubbed_Copies are persisted under per-project keys.
- **Zip_Bomb**: A zip archive whose extraction would produce an output many times larger than the compressed size, used to exhaust disk or memory.
- **Zip_Slip**: A zip entry whose resolved path escapes the Extraction_Root, used to overwrite arbitrary files on the host.
- **SSRF**: Server-Side Request Forgery — an attack where a crafted URL causes the server to make requests to internal network addresses.
- **Compression_Ratio**: The ratio of uncompressed size to compressed size for a zip entry (uncompressed ÷ compressed). Checked only for entries whose compressed size is at least 1 KB.
- **Shallow_Clone**: A git clone of a specific pinned commit SHA at depth=1, limiting data transfer and exposure.
- **Sanitized_URL**: A Git URL with the userinfo (credential) component stripped, retaining scheme, host, port (if present), path, and query string.
- **Content_Hash**: A SHA-256 digest used as the idempotency key for a Submission, scoped to project_id. Computed as SHA-256 over a length-prefixed list of per-part hashes: for zip submissions, the hash of the archive bytes followed by the hash of each supplementary file prefixed with its filename length and name; for Git submissions, the hash of the resolved commit SHA followed by the hash of the Sanitized_URL bytes, using the same length-prefix scheme. This construction avoids ambiguity from naive concatenation.
- **Commit_SHA**: The full 40-character hexadecimal SHA-1 resolved from a Git_URL synchronously via a hardened `git ls-remote` call before the Analysis_Run is created.
- **Synchronous_Validation**: Checks performed in the API handler before any Analysis_Run is created; failures are returned directly to the caller with no side effects.
- **Asynchronous_Processing**: Work performed in a River worker after the Analysis_Run exists in Queued state; failures transition the run to Failed.
- **Partial_Index**: A PostgreSQL partial unique index that enforces a constraint only over rows matching a WHERE clause.

---

## Requirements

### Requirement 1: Repository Submission

**User Story:** As a developer, I want to submit a repository as a zip file or as a Git URL so that Exhume can begin analyzing my codebase.

#### Acceptance Criteria

1. WHEN a user submits a Zip_Upload, THE Ingest_Service SHALL stream the archive to Object_Storage under the project's per-project key while simultaneously computing its Content_Hash, without fully buffering the archive in memory, provided the archive passes Synchronous_Validation checks.
2. WHEN a user submits a Git_URL, THE Ingest_Service SHALL perform Synchronous_Validation of the URL (scheme, userinfo, DNS, IP checks) and resolve the Commit_SHA via a hardened `git ls-remote` call (same DNS and IP validation, connection pinned to a validated address, 10-second timeout); on success THE Ingest_Service SHALL compute the Content_Hash from the Commit_SHA and Sanitized_URL, create an Analysis_Run in the Queued state, and return a run_id; the Shallow_Clone of that exact Commit_SHA SHALL be performed asynchronously in the River worker.
3. IF the Zip_Upload fails any Synchronous_Validation check (upload size limit, zip magic bytes), THEN THE Ingest_Service SHALL reject the submission and return an error indicating the reason without creating an Analysis_Run.
4. IF the Git_URL fails any Synchronous_Validation check (scheme, embedded credentials, DNS resolution, IP address) or the `git ls-remote` call fails or times out, THEN THE Ingest_Service SHALL reject the submission and return an error indicating the reason without creating an Analysis_Run.
5. IF Object_Storage is unavailable or the archive write fails during streaming, THEN THE Ingest_Service SHALL return an error indicating a storage failure without creating an Analysis_Run.
6. IF supplementary files (database schema dump or documentation files) are included in the Submission, THEN THE Ingest_Service SHALL store them alongside the archive and include them in the asynchronous inventory, applying the same size limits, secret scanning, classification, and Scrubbed_Copy rules as repository files.
7. THE Ingest_Service SHALL return a run_id to the caller upon successful creation of the Analysis_Run.

---

### Requirement 2: Upload and Extraction Limits

**User Story:** As a platform operator, I want configurable hard limits on upload and extraction size so that the system rejects oversized or malicious submissions before they can exhaust resources.

#### Acceptance Criteria

1. THE Ingest_Service SHALL enforce a configurable maximum upload size (in bytes), with a default of 104,857,600 bytes (100 MB) and a minimum configurable value of 1 byte, and reject any Zip_Upload whose raw stream size exceeds that limit during Synchronous_Validation with an error message indicating the configured limit and the actual size received; this limit also applies to the total bytes received from a Git clone and to each supplementary file.
2. THE Ingest_Service SHALL enforce a configurable maximum extracted size (in total bytes across all entries), with a default of 524,288,000 bytes (500 MB) and a minimum configurable value of 1 byte; violations are detected during streaming extraction in the worker and treated as Asynchronous_Processing failures; this limit applies to both zip working trees and Git working trees.
3. THE Ingest_Service SHALL enforce a configurable maximum file count per Submission, with a default of 5,000 files and a minimum configurable value of 1; violations are detected during extraction in the worker and treated as Asynchronous_Processing failures; this limit applies to both zip and Git working trees.
4. THE Ingest_Service SHALL enforce a configurable maximum single-file size (in bytes), with a default of 26,214,400 bytes (25 MB) and a minimum configurable value of 1 byte; violations are detected per-entry during extraction in the worker and treated as Asynchronous_Processing failures; this limit applies to both zip entries and individual files in a Git working tree.
5. WHEN a Git_URL submission is being cloned in the worker, THE Ingest_Service SHALL monitor bytes received during the clone and abort the clone if the total exceeds the configured maximum upload size, transitioning the Analysis_Run to Failed with a size-limit-exceeded reason.
6. IF any Asynchronous_Processing size limit is exceeded, THEN THE Ingest_Service SHALL abort extraction, delete any partially extracted content, and transition the Analysis_Run to Failed recording the violation reason and the name of the first offending entry.
7. IF a configured limit value is set below its minimum configurable value at startup, THEN THE Ingest_Service SHALL refuse to start and emit an error message identifying the misconfigured limit and its invalid value.

---

### Requirement 3: Zip Security Validation

**User Story:** As a security engineer, I want zip archives validated against path traversal, symlink escape, and zip bomb attacks so that Exhume cannot be used to compromise the host or exhaust its resources.

#### Acceptance Criteria

1. WHEN a Zip_Upload passes Synchronous_Validation (valid zip magic bytes confirmed, within upload size limit), THE Ingest_Service SHALL proceed to asynchronous extraction; the central directory is parsed and all remaining zip security checks are performed in the worker during streaming extraction.
2. WHEN extracting a zip entry in the worker, THE Ingest_Service SHALL resolve the entry's path relative to the Extraction_Root; IF any resolved path escapes the Extraction_Root (Zip_Slip) or is an absolute path, THEN THE Ingest_Service SHALL abort extraction, delete all partially extracted content, and transition the Analysis_Run to Failed.
3. THE Ingest_Service SHALL skip any zip entry whose path begins with `.git/` or equals `.git`; such entries SHALL NOT be extracted or inventoried.
4. WHEN a zip entry is a symlink, THE Ingest_Service SHALL record the symlink target as metadata in the Source_File row and SHALL NOT create the symlink on disk.
5. WHEN extracting a zip entry, THE Ingest_Service SHALL enforce the configured maximum single-file size against the entry's declared uncompressed size before writing any bytes; actual decompressed bytes are tracked during streaming and extraction is aborted if the actual decompressed bytes exceed either the single-file limit or the entry's declared uncompressed size.
6. WHEN extracting a zip entry whose compressed size is at least 1 KB, THE Ingest_Service SHALL compute the Compression_Ratio and reject the archive if the ratio exceeds the configured maximum, where the configured maximum defaults to 100 if unset.
7. WHEN extracting a Zip_Upload, THE Ingest_Service SHALL track cumulative decompressed bytes across all entries and abort extraction if the cumulative total exceeds the configured maximum extracted size.
8. WHEN a zip entry is itself a zip archive (detected by file signature), THE Ingest_Service SHALL treat it as an opaque binary file and SHALL NOT extract it recursively.
9. IF any zip security check in the worker fails, THEN THE Ingest_Service SHALL abort extraction, delete all partially extracted content from the Extraction_Root, and transition the Analysis_Run to Failed with an error message containing the name of the violated constraint and the name of the offending entry, and containing no file content, secrets, or credentials.

---

### Requirement 4: Git URL Security Validation

**User Story:** As a security engineer, I want Git URL submissions validated against SSRF, credential-leakage, and DNS-rebinding attacks so that the server cannot be redirected to internal infrastructure.

#### Acceptance Criteria

1. WHEN a Git_URL is submitted (Synchronous_Validation), THE Ingest_Service SHALL reject any URL whose scheme is not `https` and return an error response indicating an invalid scheme without creating an Analysis_Run.
2. WHEN a Git_URL is submitted (Synchronous_Validation), THE Ingest_Service SHALL reject any URL that contains embedded credentials (userinfo component) and return an error response indicating credential embedding is not permitted without creating an Analysis_Run.
3. WHEN a Git_URL is submitted (Synchronous_Validation), THE Ingest_Service SHALL resolve the URL's hostname to its IP addresses; IF DNS resolution fails or returns no addresses, THEN THE Ingest_Service SHALL reject the request and return an error response indicating the hostname could not be resolved without creating an Analysis_Run.
4. WHEN a Git_URL is submitted (Synchronous_Validation), THE Ingest_Service SHALL reject the request if any resolved IP address falls within the following blocked ranges and return an error response indicating a disallowed address without creating an Analysis_Run: IPv4 loopback (127.0.0.0/8), IPv4 link-local (169.254.0.0/16), IPv4 private (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16), IPv4 unspecified (0.0.0.0/8), IPv4 shared address space (100.64.0.0/10), IPv4 multicast (224.0.0.0/4), IPv4-mapped IPv6 (::ffff:0:0/96), IPv6 loopback (::1/128), IPv6 link-local (fe80::/10), IPv6 unique-local (fc00::/7).
5. WHEN the worker initiates the Shallow_Clone, THE Ingest_Service SHALL re-resolve the hostname, validate every resolved address against the full blocked-range list in criterion 4, and pin the TCP connection to a validated address; a different but valid (non-blocked) address than the one resolved during Synchronous_Validation is acceptable; IF all re-resolved addresses are blocked, THEN THE Ingest_Service SHALL abort the clone and transition the Analysis_Run to Failed.
6. WHEN the worker initiates the Shallow_Clone, THE Ingest_Service SHALL clone the exact Commit_SHA resolved in Synchronous_Validation, and SHALL disable git hooks, submodule initialization, and LFS; IF the repository requires LFS objects, the clone SHALL proceed without fetching LFS content.
7. THE Ingest_Service SHALL never store or log any representation of a Git_URL that retains the userinfo component; only the Sanitized_URL SHALL be persisted or emitted.
8. WHEN the `git ls-remote` call or the Shallow_Clone encounters an HTTP redirect, THE Ingest_Service SHALL re-validate the redirect target URL against all criteria in this requirement before following it; IF the redirect target fails any check, THE Ingest_Service SHALL abort and (if in the worker) transition the Analysis_Run to Failed.
9. IF a Shallow_Clone is in progress in the worker and the elapsed time reaches the clone timeout, where the clone timeout is a configurable value between 30 and 300 seconds inclusive, THEN THE Ingest_Service SHALL cancel the clone, delete any partial clone directory, and transition the Analysis_Run to Failed with a timeout reason.
10. Private repositories requiring credentials are not supported in the MVP; IF a remote returns an authentication challenge (HTTP 401/403), THEN THE Ingest_Service SHALL transition the Analysis_Run to Failed with an error indicating the user must upload a zip archive instead.

---

### Requirement 5: File Classification and Inventory

**User Story:** As a developer, I want every file in my repository classified and inventoried so that downstream pipeline stages know what to parse and what to skip.

#### Acceptance Criteria

1. WHEN extraction completes in the worker, THE File_Classifier SHALL assign each file exactly one File_Classification from the ordered set — evaluated in priority order: binary, vendor, generated, SQL, batch_script, config, docs, source — and IF a file matches no classification rule, THEN THE File_Classifier SHALL assign it the classification "unclassified" and record it in the Source_File row without halting the pipeline; the `.git` directory and its contents are excluded from classification and inventory.
2. WHEN extraction completes, THE Ingest_Service SHALL record a Source_File row for each file containing: path relative to the repository root, File_Classification, size in bytes, and lines of code, where lines of code is defined as the count of non-blank lines in the file; binary-classified files SHALL have their lines-of-code recorded as zero.
3. Binary-classified files SHALL be excluded from secret scanning and from lines-of-code aggregation on the Analysis_Run.
4. WHEN extraction completes, THE Ingest_Service SHALL detect the primary programming language, secondary languages, and frameworks; the primary and secondary languages are computed from non-blank line counts per language using a file-extension mapping over source-classified files only; frameworks are detected from manifest and build files only; the primary language is the language with the highest non-blank line count across source-classified files; IF no programming language is detected, THEN THE Ingest_Service SHALL record the primary language field as null.
5. WHEN inserting Source_File rows, THE Ingest_Service SHALL write all rows belonging to a single Analysis_Run in a single database transaction so that the inventory is either fully committed or fully rolled back.
6. WHILE the file inventory is being written, THE Ingest_Service SHALL scope all database queries to the project_id of the owning Project.

---

### Requirement 6: Secret Detection and Masking

**User Story:** As a developer, I want secrets in my repository detected and masked on a best-effort basis before any content can reach an LLM, and I want to be able to review what was masked.

#### Acceptance Criteria

1. WHEN the Secret_Scanner receives a non-binary file for processing, THE Secret_Scanner SHALL apply best-effort pattern detection to identify secrets of the following types: API_KEY, PASSWORD, TOKEN, PRIVATE_KEY, CONNECTION_STRING, and JWT; the system does not guarantee all secrets are detected.
2. WHEN a secret is detected, THE Secret_Scanner SHALL replace the matched secret value within its original line with a placeholder of the form `[REDACTED:<TYPE>]`, where `<TYPE>` is the detected secret type; line numbers SHALL be preserved and no lines SHALL be deleted or merged.
3. WHEN a file is identified as secret-bearing by name (`.env`, `.env.*` excluding `.env.example`, `.env.sample`, and `.env.template`, `id_rsa`, `*.pem`, `*.key`, `*.pfx`, `*.p12`, `*.jks`), THE Ingest_Service SHALL replace the entire file content in the Scrubbed_Copy with a single line reading `[FILE REDACTED: secret-bearing filename]`, regardless of whether any secret patterns were matched.
4. THE Ingest_Service SHALL store the Scrubbed_Copy of each non-binary file in Object_Storage separately from the raw archive, under a scrubbed-specific key; binary files SHALL appear in the Scrubbed_Copy as metadata rows only and SHALL NOT have their content copied; all downstream pipeline stages SHALL read only from the Scrubbed_Copy and never from the raw archive.
5. WHEN the Secret_Scanner completes processing all files in an Analysis_Run, THE Ingest_Service SHALL record an Ingest_Summary on the Analysis_Run containing the total count of Masked_Items and the type of each Masked_Item using the type labels defined in criterion 1, without including any secret values.
6. THE Ingest_Service SHALL expose a read endpoint for the Ingest_Summary of a given run_id so that users can review the count and types of items that were masked; the endpoint SHALL NOT return secret values, file content, or file paths.
7. THE Ingest_Service SHALL never include source code content in log output; log entries SHALL contain only the file path hash, file size in bytes, and the count of Masked_Items for that file.
8. IF no secrets are detected in a file and the file is not secret-bearing by name, THEN THE Secret_Scanner SHALL record that file in the Ingest_Summary with a Masked_Item count of zero and no Masked_Item entries.

---

### Requirement 7: Analysis Run Lifecycle

**User Story:** As a developer, I want to track the progress of my ingest job through a well-defined lifecycle so that I can observe what the system is doing and be notified of failures.

#### Acceptance Criteria

1. WHEN a Submission passes all Synchronous_Validation checks, THE Ingest_Service SHALL create an Analysis_Run in the Queued state and immediately return the run_id to the caller.
2. WHEN the River worker picks up a Queued run, THE Ingest_Service SHALL transition the Analysis_Run to the Ingesting state and record the transition timestamp.
3. WHEN Asynchronous_Processing completes successfully, THE Ingest_Service SHALL transition the Analysis_Run from Ingesting to Parsing and enqueue the next pipeline River job within a single database transaction so that the state transition and job enqueue are atomic.
4. IF Asynchronous_Processing fails at any step, THEN THE Ingest_Service SHALL transition the Analysis_Run to the Failed state, record a failure reason of up to 1000 characters describing the step and error, and stop processing without altering previously persisted data; failure reasons SHALL NOT contain file content, secrets, or credentials.
5. THE Ingest_Service SHALL record the timestamp of every Analysis_Run state transition in the database.
6. WHILE an Analysis_Run is in the Ingesting state, THE Ingest_Service SHALL accept context cancellation and transition the Analysis_Run to the Failed state with a cancellation reason within 5 seconds of receiving the cancellation signal.
7. IF an Analysis_Run transitions to the Failed state, THEN THE Ingest_Service SHALL emit an observable failure event containing the Analysis_Run identifier and failure reason.
8. IF an Analysis_Run has been in the Ingesting state beyond a configurable stuck-run timeout, THEN THE Ingest_Service SHALL retry the run by re-enqueuing it, up to a configurable maximum attempt count; IF the maximum attempt count is reached, THEN THE Ingest_Service SHALL transition the Analysis_Run to Failed with a stuck-run reason.

---

### Requirement 8: Idempotency

**User Story:** As a developer, I want re-submitting an identical repository to be safe so that accidental duplicate submissions do not create duplicate pipeline runs or corrupt data.

#### Acceptance Criteria

1. THE Ingest_Service SHALL compute the Content_Hash as a SHA-256 digest over a length-prefixed list of per-part hashes: for zip submissions, the SHA-256 of the archive bytes followed by the SHA-256 of each supplementary file prefixed with a 4-byte big-endian length and the file's name; for Git submissions, the SHA-256 of the Commit_SHA bytes followed by the SHA-256 of the Sanitized_URL bytes, using the same length-prefix scheme; this construction prevents ambiguity from differing part boundaries.
2. THE database SHALL enforce a Partial_Index with a unique constraint on (project_id, content_hash) WHERE status NOT IN ('failed') in the analysis_run table; this ensures that two concurrent identical submissions cannot create more than one non-failed Analysis_Run, while allowing a new run to be created after a failure.
3. WHEN a Submission is received whose Content_Hash matches an existing Analysis_Run for the same Project whose status is not Failed (i.e., Queued, Ingesting, Parsing, or any later non-failed state), THE Ingest_Service SHALL return the existing run_id accompanied by an indicator that the submission is a duplicate without creating a new Analysis_Run.
4. WHEN a Submission is received whose Content_Hash matches an existing Analysis_Run for the same Project that is in the Failed state, THE Ingest_Service SHALL create a new Analysis_Run rather than returning the failed run, allowing the user to retry.
5. IF a River job for a given (run_id, stage, unit_id) already exists in a pending, running, or completed state, THEN THE Ingest_Service SHALL not enqueue a duplicate job; a job in a failed or cancelled terminal state SHALL NOT block re-enqueue.

---

### Requirement 9: Performance

**User Story:** As a developer, I want large repositories to be ingested quickly so that I do not wait unreasonably before pipeline analysis begins.

#### Acceptance Criteria

1. THE Ingest_Service SHALL complete extraction, classification, secret scanning, and database inventory of a 5,000-file repository in under 30 seconds, measured from the moment the worker starts the job until the Analysis_Run transitions to Parsing, on a reference machine with a 4-core CPU, 8 GB RAM, and SSD storage with no other workloads running, where each file averages no more than 50 KB; upload and clone time are excluded from this measurement.
2. THE Ingest_Service SHALL process files in parallel using errgroup with a bounded worker pool whose size is configurable, defaults to 8 workers, and accepts values in the range 1 to 256 inclusive; IF a configured value is outside that range, THEN THE Ingest_Service SHALL reject the configuration with an error message indicating the valid range before processing begins.
3. THE Ingest_Service SHALL batch Source_File database inserts; each batch SHALL contain up to a configurable maximum number of rows, defaulting to 500, ensuring the number of database round trips grows sub-linearly with file count.
4. WHEN context cancellation is received during parallel file processing, THE Ingest_Service SHALL propagate cancellation through the errgroup and stop all worker goroutines within 2 seconds of receiving the cancellation signal.

---

### Requirement 10: Project Provisioning

**User Story:** As a developer, I want a Project to be created automatically on my first submission so that I do not have to provision one manually before I can run an analysis.

#### Acceptance Criteria

1. WHEN a Submission is received and no Project with the provided project identifier exists, THE Ingest_Service SHALL create a Project record before creating the Analysis_Run, within the same database transaction.
2. WHEN a Submission is received and a Project with the provided project identifier already exists, THE Ingest_Service SHALL use the existing Project without creating a duplicate.
3. WHEN a Submission is received without a project identifier, THE Ingest_Service SHALL generate a UUID as the project identifier; all project identifiers are unguessable UUIDs.
4. THE Ingest_Service SHALL scope all subsequent Analysis_Run and Source_File records to the project_id of the resolved or newly created Project.
5. IF Project creation fails, THEN THE Ingest_Service SHALL reject the Submission and return an error without creating an Analysis_Run.
6. Ownership and authorization for Projects and Analysis_Runs are out of scope for this spec and will be addressed in a later authentication specification.

---

### Requirement 11: Abuse Limits

**User Story:** As a platform operator, I want configurable limits on concurrent ingest runs so that a single project or a burst of submissions cannot exhaust shared processing capacity.

#### Acceptance Criteria

1. THE Ingest_Service SHALL enforce a configurable maximum number of concurrent runs per project, with a default of 2; WHEN a Submission is received and the project already has at least that many runs in a non-terminal state (Queued or Ingesting), THE Ingest_Service SHALL reject the submission synchronously with a retryable error indicating the per-project limit is reached.
2. THE Ingest_Service SHALL enforce a configurable maximum number of concurrent runs across all projects globally, with a default of 10; WHEN a Submission is received and the global count of runs in a non-terminal state meets or exceeds that limit, THE Ingest_Service SHALL reject the submission synchronously with a retryable error indicating the global limit is reached.
3. Per-project and global limit checks are performed during Synchronous_Validation; no Analysis_Run is created when a limit is exceeded.
4. IF a configured abuse limit is set below 1 at startup, THEN THE Ingest_Service SHALL refuse to start and emit an error message identifying the misconfigured limit.

---

### Requirement 12: Retention and Cleanup

**User Story:** As a platform operator, I want temporary files cleaned up promptly and stored objects pruned after a retention period so that the system does not accumulate unbounded storage.

#### Acceptance Criteria

1. THE Ingest_Service SHALL delete the Extraction_Root and any partial clone directory on every exit path from the worker: successful completion, failure, and context cancellation.
2. WHEN a Zip_Upload is rejected during Synchronous_Validation after the object has already been written to Object_Storage, or when a duplicate is detected at run-creation time, THE Ingest_Service SHALL delete the uploaded object from Object_Storage; raw archives belonging to runs that end in the Failed state are retained and SHALL NOT be deleted until the retention period in criterion 3 expires, so that retries can reuse them.
3. THE Ingest_Service SHALL delete raw archive objects and their corresponding Scrubbed_Copy objects from Object_Storage after a configurable retention period, with a default of 30 days.
4. WHEN a worker begins an ingest job for an Analysis_Run that has prior partial output (Source_File rows, a partial Scrubbed_Copy in Object_Storage) from a previous failed attempt, THE Ingest_Service SHALL delete those partial outputs before beginning the new attempt; the raw archive in Object_Storage SHALL NOT be deleted and SHALL be reused by the retry, so that every retry starts from a clean extraction state without re-uploading.
5. IF the retention-period deletion of a stored object fails, THE Ingest_Service SHALL log the failure including the object key (but not its content) and retry the deletion; persistent deletion failures SHALL be surfaced as an observable error, not silently ignored.
