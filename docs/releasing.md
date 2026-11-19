# Releasing

The first release, **0.1.0**, shipped nine gems with alpha maturity;
`connectrpc-health` and `connectrpc-sentry` joined the train later, and
`connectrpc-hyper` joins it in 0.3.0. Every release now includes all twelve gems
at one version, as twenty archives: one `.gem` per gem, plus eight prebuilt
platform gems for `connectrpc-hyper` (see [connectrpc-hyper archives](#connectrpc-hyper-archives)).
Versions are numeric, such as `0.1.0`, not RubyGems prereleases such as `0.1.0.alpha`.
It remains experimental and does not adopt the stronger
[supported profile](support.md) or declare production readiness.

The pipeline has three stages:

1. Every push to main runs Release Please, which opens or updates one release
   PR for the whole train. For a minor release, the same run then commits the
   prepared installation guides to that PR.
2. Merging that PR makes Release Please push a `vX.Y.Z` tag and create a draft
   GitHub release with the changelog.
3. The tag starts the [release workflow](../.github/workflows/release.yml). It
   builds and tests the nine connectrpc-hyper archives on native runners,
   then builds the canonical candidate, checks the tested archives against it,
   uploads the twenty `.gem` files and `candidate.json` to the draft, pushes
   each archive to RubyGems.org with a Sigstore attestation using trusted
   publishing, and publishes the GitHub release last.

Merging a release PR is the release decision. Every gemspec pins
`allowed_push_host` to `https://rubygems.org` and sets `rubygems_mfa_required`,
so a manual push needs the owner's MFA. Trusted-publisher keys are exempt from
that requirement. The strict package checks enforce both. A successful build
or verify command grants no release authority.

## One version train

One root Release Please Ruby component covers the whole repository, including
changes confined to one optional gem. It updates the core version and uses
explicit annotated extra files for the eleven optional `version.rb` files,
including connectrpc-hyper's. Each
gemspec reads its owning static version without requiring runtime code. OTel uses
its same owning constant for instrumentation. The root `CHANGELOG.md` describes the entire train.
The tag convention is one `v0.x.y` tag, not twelve component tags.
`ReleaseTrain::NAMES` lists the twelve gems in publication order and drives the
version files, version and dependency checks, the Release Please `extra-files`
inventory, the candidate and the publisher. `ReleaseTrain::PLATFORMS` names the
gems that also publish platform archives: `connectrpc-hyper`, with
`HyperPlatforms::NATIVE`.

`.github/.release-please-manifest.json` records the last released version, and
each release PR updates it. The config's `initial-version` of `0.1.0` only
seeded the first release, when the manifest was empty. There is no permanent
`release-as` override or history cutoff.

Before 1.0, `fix:` and compatible `feat:` commits increment the patch. A breaking
commit (`!` or a `BREAKING CHANGE:` footer) increments the minor. Breaking
minors require migration notes explaining affected calls, options, defaults,
wire behavior, tracing and generated code. Keep migration notes on main in
[upgrading.md](upgrading.md), with a summary in the breaking commit footer.
Bot-generated release branches are not durable homes for hand-edited migration
notes.

The train uses matching package versions. Internal gem dependencies keep the
existing pessimistic minor-series range, `~> M.m.0`; a patch does not narrow
that floor. External dependency constraints and tool pins are unchanged by
release preparation. Development locks are not committed, so a release carries
no lock changes. Matching versions do not add a runtime enforcement mechanism
or adopt the support profile.

## Prepare a release PR

The [Release Please workflow](../.github/workflows/release-please.yml) runs on
every push to main, and on manual dispatch from main. It opens or updates the
release PR, ready for review rather than as a draft PR. After a release PR
merges, the next run pushes its tag and creates a draft GitHub release. The
configuration sets `draft` and `force-tag-creation`: GitHub creates no tag for a
draft release until it is published, and without the tag Release Please cannot
find the previous release.
This workflow has no publishing job or OIDC permission.

After Release Please, the same job prepares the open release PR:

1. It looks up the open PR from the
   `release-please--branches--main--components--connectrpc` branch of this
   repository into main. Release Please reports no PR when the body is
   unchanged, so the lookup does not depend on its output. Forks are excluded.
2. It checks out main for tooling, then the PR head SHA into `release-pr` as
   data. Nothing from the release branch runs: `mise run release:prepare:pr`
   reads only the release tree's core version file and installation guides as
   text, and applies the same transformation as `release:prepare`.
3. If any guide changes, it commits exactly those guides to the release branch
   through the GraphQL `createCommitOnBranch` mutation with the App token, so
   GitHub signs the commit as the App. `expectedHeadOid` is the checked-out
   SHA, so a branch that moved since the lookup fails the run instead of being
   overwritten.

Patch releases and runs without an open release PR change nothing. A failed
preparation is retried by the next push to main, or by manually dispatching the
workflow from main. The App-token push starts the PR's normal CI.

The job runs in the `release-please` environment, whose deployment branch
policy allows only `main`. The existing release GitHub App needs these
references, as in the pinned
[Airplan workflow](https://github.com/jimeh/airplan/blob/f70df0f43404f07808e56613a4511b53160c2bbd/.github/workflows/release-please.yml):

| Reference | Required setup |
| --- | --- |
| `vars.RELEASE_BOT_CLIENT_ID` | The existing App's client ID, as a `release-please` environment variable. |
| `secrets.RELEASE_BOT_PRIVATE_KEY` | The existing App's private key, as a `release-please` environment secret. |
| App installation | Installation access to `jimeh/connectrpc-ruby`, with contents, issues and pull requests write permissions. |

The workflow requests an installation token scoped to the current repository
and those three permissions. Contents write also covers the guide preparation
commit, so it needs no new permission. Its `GITHUB_TOKEN` has no permissions.
App-created PRs can trigger normal downstream CI. Missing references fail the
run with an actionable error; there is no fallback token. Do not print, copy
into the checkout, or ask for private-key values in chat.

Both reference names were listed in the `release-please` environment on
2026-11-14. App installation and token permissions are verified only by the
first run on main. Publishing approvals belong to the separate `release`
environment, so its protection never gates PR preparation. Tags pushed with the
App token start the release workflow; tags pushed with `GITHUB_TOKEN` would not.

Both action pins meet the seven-day age policy: release-please-action v5.0.0,
published 2026-04-22, and create-github-app-token v3.3.0, published 2026-10-12.
Their tag commits were checked against GitHub. The pinned release action embeds
release-please 17.6.0. Review new pins and their embedded behavior before updates.

For each release PR:

1. Inspect the generated release PR. For the first release, verify that the
   proposed version is exactly `0.1.0`; every release includes all twelve gems.
2. A patch release needs no preparation: the bot's version files, manifest and
   changelog are the whole change, and `release:check` accepts them. A minor
   (breaking) release changes the internal `~> M.m.0` requirement that the
   installation guides quote. The workflow commits that guide update
   automatically; review its diff. If the workflow cannot, dispatch it again or
   use the local fallback: check out the release branch, run
   `mise run release:prepare`, review the guide diff and commit it with the
   existing signing configuration. Preparation does not resolve dependencies or
   download anything.
3. Run `mise run release:check`, focused tests and the candidate procedure below.
   Capture the exact head and CI/review evidence after the final bot update.
4. Merge the PR once the release decision is made. The release workflow then
   waits for any `release` environment approval.

**Validate the final head after every bot regeneration of a minor release.**
Release Please rebuilds its branch from main when its generated body changes,
which drops the preparation commit; the same workflow run then commits it
again. Hand edits in the release PR are still discarded. Wait for the run to
finish, then validate the final head; a quiet main during that window helps. Do
not present earlier CI/review evidence as covering a later regenerated head.

## Validate a local candidate

Use `mise tasks` for the task index:

| Task | Evidence and writes |
| --- | --- |
| `release:check` | Offline version, configuration, guard and installation-guide checks. No writes or setup dependencies. |
| `release:prepare` | Deterministic installation-guide requirement updates, needed only for a minor release. No registry resolution. Local fallback for the workflow. |
| `release:prepare:pr` | CI only. Refuses to run outside `release-please.yml` on main. Commits the prepared guides of the checked-out release PR as the App. |
| `release:build` | Twelve canonical archives with a two-build hash comparison, the eight tested connectrpc-hyper platform archives and their smoke evidence (`RELEASE_HYPER_ARTIFACTS`, `RELEASE_HYPER_EVIDENCE`), and `candidate.json`. No installation or publication. |
| `release:hyper:collect` | Copies connectrpc-hyper archives and smoke evidence into the flat `vendor/hyper/release-artifacts` and `vendor/hyper/release-evidence` directories that `release:build` reads. |
| `release:candidate` | Existing installed-package/generation/RBS checks followed by the canonical archive build. Uses normal scoped setup prerequisites. |
| `release:tools` | Installs the frozen Sigstore signing bundle (`release/Gemfile`) into `vendor/sigstore`. |
| `release:publish` | CI only. Refuses to run outside `release.yml` on a `v*` tag. |

Run the focused release specs and workflow checks first:

```sh
mise run release:check
mise run test -- spec/release_train_spec.rb spec/release_preparation_spec.rb spec/release_publisher_spec.rb spec/release_candidate_spec.rb spec/hyper_cross_spec.rb spec/package_build_spec.rb spec/ci_runtime_spec.rb
mise run lint:workflows
```

Package/tooling handoff requires the normal graph **once per pinned Ruby**,
with `rack:verify` following `verify`:

```sh
mise run verify
mise run rack:verify
MISE_RUBY_VERSION=4.0.9 mise run verify
MISE_RUBY_VERSION=4.0.9 mise run rack:verify
```

These cover core generation/installation, selected Rails (8.1 by default),
Async and Falcon. Add the distinct missing leaves, rather than re-running all
of `release:candidate` after them:

```sh
RAILS_LANE=8.0 mise run generation:check:rails
RAILS_LANE=8.0 mise run check:package:rails
mise run testing:package
mise run otel:package
mise run prometheus:package
mise run yabeda:package
mise run health:package
mise run sentry:package
mise run rbs:check
mise run rbs:package
mise run grpc:status:check
mise run floors
MISE_RUBY_VERSION=4.0.9 mise run testing:package
MISE_RUBY_VERSION=4.0.9 mise run otel:package
MISE_RUBY_VERSION=4.0.9 mise run prometheus:package
MISE_RUBY_VERSION=4.0.9 mise run yabeda:package
MISE_RUBY_VERSION=4.0.9 mise run health:package
MISE_RUBY_VERSION=4.0.9 mise run sentry:package
MISE_RUBY_VERSION=4.0.9 mise run rbs:check
MISE_RUBY_VERSION=4.0.9 mise run rbs:package
MISE_RUBY_VERSION=4.0.9 mise run grpc:status:check
```

`release:build` (and so `release:candidate`) also needs the tested
connectrpc-hyper archives and one smoke evidence record per archive and Ruby
minor, in two flat directories named by `RELEASE_HYPER_ARTIFACTS` and
`RELEASE_HYPER_EVIDENCE`. Without them it stops and names both variables. The
complete set comes from a hyper-gems run, and the local checkout must be the
exact commit that run checked out. The source gem and the canonical build both
take `SOURCE_DATE_EPOCH` from the checked-out commit, so any other commit fails
with "Tested source gem differs from the canonical build". A pull request run
checks out GitHub's synthetic merge commit (`refs/pull/<number>/merge`,
`GITHUB_SHA` for the `pull_request` event), not the PR head. Read the commit
from the checkout step's log of the run that produced the artifacts, which
prints `HEAD is now at <sha>` and then the full SHA, and check it out detached
(`git fetch origin <sha>`, or `refs/pull/<number>/merge` while it still points
there). Runs on a push to main check out the pushed commit, and tag-triggered
release runs check out the tag's commit in every job. Download the run's
`connectrpc-hyper-*` and `hyper-smoke-*` artifacts, flatten them with
`release:hyper:collect` and build:

```sh
git switch --detach <checked-out-sha>
gh run download <run-id> --pattern 'connectrpc-hyper-*' --dir vendor/hyper/downloaded/archives
gh run download <run-id> --pattern 'hyper-smoke-*' --dir vendor/hyper/downloaded/evidence
mise run release:hyper:collect -- vendor/hyper/downloaded/archives vendor/hyper/downloaded/evidence
RELEASE_HYPER_ARTIFACTS=vendor/hyper/release-artifacts RELEASE_HYPER_EVIDENCE=vendor/hyper/release-evidence \
  mise run release:build
```

The `Hyper gems / Check the release candidate` job runs the same
`release:build` in every hyper-gems run, at that run's checkout, so a green job
is the hosted equivalent of this procedure.

Locally, `HYPER_CROSS_TWICE=1 mise run hyper:cross -- <platform>` builds one
archive into `vendor/hyper/cross/<platform>/`, and
`ruby gems/connectrpc-hyper/scripts/install_smoke.rb vendor/hyper/cross/<platform> vendor/hyper/evidence/<platform>-<minor>.json`
smokes it and records evidence (use `mise run hyper:smoke:source -- vendor/hyper/cross/ruby vendor/hyper/evidence/ruby-<minor>.json`
for the source gem). Run `mise run release:hyper:collect` with no arguments to
collect those outputs. A host can only smoke its own platform, so a complete
local candidate still needs the other platforms' archives and evidence from CI.

The supported Rails 8.0 evidence lane is Ruby 3.4.12. Hosted CI owns its clean
matrix evidence; it is separate from local results. Preserve unexplained
failures and resolve them before broadening validation. Run `release:candidate`
instead when starting a bounded package-only qualification, not as an extra
duplicate graph after full verification.

The existing package checks validate independently reviewed exact manifests,
MIT notices, third-party notices and descriptor bytes, signatures and push hosts.
They install archives offline into fresh `GEM_HOME`/`GEM_PATH` outside the
checkout. Their generated consumers cover core alone, Rack, Rails with Rack,
Async, Falcon with Async, testing, API-only OTel, Prometheus, Yabeda, Sentry and
the health service with its Apache-2.0 schema notice.
Installed RBS checks cover all eleven gem-discovered signature libraries,
composed framework opt-ins and
real generated runtime consumers. Here "signatures" means RBS, not cryptographic
gem signing; published gems carry Sigstore attestations instead. Generator checks reproduce fixtures; never hand-edit generated
Ruby or RBS files.

The canonical build requires a clean Git checkout and Ruby 3.4.12. It derives
`SOURCE_DATE_EPOCH` from the source commit timestamp, records the Ruby/RubyGems
versions, and uses the existing private staging builder. Executables have mode
0755 and ordinary files 0644; checkout modes remain untouched. Every archive's
manifest, bytes, notices and modes are checked against its owning source, and
a second temporary build must produce all twelve identical SHA256 hashes.
This proves repeatability in the recorded lane, not cross-Ruby bit identity.

Output is `vendor/release/<version>/<commit>/`: exactly twenty `.gem` files and
`candidate.json`, which records source revision, epoch, tool versions, and each
archive's file, platform, member inventory and hash. The directory must be
missing or empty. The candidate is assembled in a private sibling directory and
moved into place only after every check passes, so a failed build leaves the
output untouched. Smoke evidence is attempt-specific, so `candidate.json` leaves
it out and stays deterministic for a revision. Preserve an
existing candidate; use a new reviewed commit for a changed candidate instead
of mixing or overwriting artifacts. Local lane builds retain their established
isolated package directories. Stale older archives are not selected by glob.

## Publish a release

The release workflow runs only on `v*` tags, in the `release` environment, with
job-scoped `contents: write` and `id-token: write`. It uses no dependency
caches. RubyGems trusted publishers match its filename and environment, so
renaming either breaks publication. `release:publish` runs these steps:

1. It checks that the tag is `v<version>` for the train and that the checkout
   is the tagged commit. It then verifies that `candidate.json` records each of
   the twenty expected archives exactly once with its platform and hash, and
   that each gem's internal dependencies precede it in publication order.
2. It finds the GitHub release for the tag, polling for up to five minutes
   because Release Please creates the draft just after the tag.
3. It checks all twenty archives on RubyGems.org before uploading or pushing
   anything. A gem name that already exists must be owned by `jimeh`, even
   when its bytes match (checked once per name), and an archive that already
   exists must have the candidate's SHA256. One versions snapshot per gem name
   is matched by version and platform; more than one matching row, or one
   without a SHA256, stops the run. Pending publishers do not reserve names.
   RubyGems.org does not list yanked versions, and pushing a yanked version
   fails, which stops the run for a maintainer decision.
4. It uploads any missing assets to the draft. An existing asset must be fully
   uploaded and match the candidate's SHA256, and an unexpected asset stops
   the run.
5. It pushes the archives in dependency order:
   1. `connectrpc`.
   2. `connectrpc-rack`, `connectrpc-async`, `connectrpc-testing` and
      `connectrpc-otel`.
   3. `connectrpc-on-rails` after Rack, and `connectrpc-falcon` after Async.
   4. `connectrpc-prometheus`, `connectrpc-yabeda`, `connectrpc-health` and
      `connectrpc-sentry`, which depend only on core.
   5. `connectrpc-hyper`, which depends only on core: its eight platform
      archives in `HyperPlatforms::NATIVE` order, then its source gem last, so
      Bundler never resolves the source gem while a platform gem is missing.

   For each archive missing from RubyGems.org, it signs the archive with
   `sigstore-cli` using the job's GitHub OIDC identity and pushes it with
   `gem push --attestation`. Each push exchanges a fresh OIDC token for a
   15-minute RubyGems key. The exchange asserts owner `jimeh`, which RubyGems
   requires before a pending publisher can create a new gem. Each archive gets
   its own Sigstore bundle and push. Archives found in step 3 are skipped.
6. It publishes the draft and marks it latest. With immutable releases enabled,
   GitHub then locks the tag and assets and adds its release attestation.

RubyGems' own action, `configure-rubygems-credentials` v2.1.0, cannot assert an
owner, so the script performs the exchange itself. `sigstore-cli` is pinned to
0.2.3, the signer that `rubygems/release-gem` uses against RubyGems.org;
0.3.0 adds Rekor v2 signing and needs separate review. The bundled RubyGems
3.6.9 accepts `--attestation` but does not sign on its own. Archives that are
already published are matched by SHA256 only; their attestations are not
re-verified, for connectrpc-hyper as for the other gems.

### connectrpc-hyper archives

`connectrpc-hyper` publishes nine archives under one gem name: a platform gem
for each of `x86_64-linux`, `aarch64-linux`, `x86_64-linux-musl`,
`aarch64-linux-musl`, `arm64-darwin`, `x86_64-darwin`, `x64-mingw-ucrt` and
`aarch64-mingw-ucrt`, each carrying the extension for Ruby 3.4 and 4.0, plus the
source gem. The release workflow's `hyper_gems` job calls the reusable
[hyper-gems workflow](../.github/workflows/hyper-gems.yml), which PR CI also
runs, so a release builds and tests exactly what CI does:

- Its jobs have only `contents: read`: no `id-token`, no environment and no
  caches. They run before the `release` approval prompt.
- `build` runs `HYPER_CROSS_TWICE=1 mise run hyper:cross` for each platform and
  the source gem. Platform gems build in rb-sys-dock images pinned by OCI index
  digest (`HyperPlatforms::IMAGES`, whose tag must equal the bundled rb_sys
  version). A host `cargo metadata --locked` preflight rejects a stale
  `Cargo.lock`, and the build fails if the copy's `Cargo.toml`, `Cargo.lock` or
  extension `Cargo.toml` changed. The MinGW builds link without PE timestamps or
  COFF symbol tables (`HyperPlatforms::MINGW_RUSTFLAGS`). The job then moves the
  archive out, deletes the work copy, Cargo home and rb-sys-dock cache, rebuilds
  from empty caches at the same absolute paths, and fails unless both SHA256
  match. The paths must stay the same: rb-sys-dock mounts the work copy at its
  host path with the cargo target directory under it, and darwin linkers embed
  those absolute object paths (and an LC_UUID derived from them) in the binary.
  Every CI run builds at the same workspace path, so a release rebuild
  reproduces the bytes CI tested. The source gem builds on Ruby 3.4.12 with the
  commit time as `SOURCE_DATE_EPOCH`, as the candidate does.
- The smoke jobs install every platform gem offline into a fresh `GEM_HOME` on
  a native runner for its OS and architecture (Ubuntu x86_64 and arm64, Alpine
  images for musl, macOS 15 arm64 and Intel, Windows Server 2025 x64 and
  Windows 11 arm64), with Ruby 3.4 and 4.0, check that the installed platform
  and the loaded extension's Ruby minor are right, and make real cleartext and
  TLS calls. The source gem compiles at install time with Rust 1.98.0 on
  Ubuntu. Each leg uploads a `hyper-smoke-<platform>-<minor>` record of the
  archive's file name, SHA256 and platform with the Ruby that exercised it.
- `candidate` ("Check the release candidate") needs the build and every smoke
  job. With no secrets or `id-token`, it downloads the archives and evidence
  into the same flat directories as `publish` and runs `release:build` (below)
  at the run's checkout. In pull request CI this proves the publish inputs
  before a release; in a release run it checks them before the approval prompt.
- `verified` runs after the others with `if: always()` and sets its output to
  `true` only when every other job succeeded. No other job is conditional or
  `continue-on-error`. Every smoke job needs the whole build matrix, so one
  failing build leg skips every smoke leg and `candidate`, and `verified`
  fails.

`publish` needs `hyper_gems` and runs only when its result is `success` and its
`verified` output is `true`. A failed or skipped job skips every job that needs
it unless that job sets a conditional such as `always()`
([`jobs.<job_id>.needs`](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#jobsjob_idneeds));
`publish` sets none that bypasses the result. It never runs Docker. It downloads
the nine archives and eighteen evidence records into separate flat directories,
and `release:build`:

1. requires exactly the tested source gem and one archive per platform, and
   requires the tested source gem to equal the canonical source build byte for
   byte;
2. checks each platform archive: name, version and platform, no extension
   build, exactly the `connectrpc ~> M.m.0` runtime dependency (no `rb_sys`),
   Ruby `>= 3.4, < 4.1.dev`, RubyGems `>= 3.3.22` for musl and `>= 0`
   otherwise, `lib` as the only require path, the source gem's metadata, push
   host, MFA flag, licenses and project URIs, a gemspec file list equal to the
   archive members, and members equal to the source gem's files without
   `Cargo.toml`, `Cargo.lock` and `ext/`, plus one non-empty 0755
   `lib/connectrpc/hyper/<minor>/connectrpc_hyper.<so|bundle>` per Ruby minor,
   with every other member's bytes equal to the source and mode 0644;
3. requires one evidence record per archive and Ruby minor whose file name,
   SHA256 and platform match, rejecting missing, extra, duplicate or
   mismatched records.

Any failure stops before `candidate.json` is written. The `verified` gate stops
`publish` from starting; the evidence check ties the published bytes to the
tested ones even if a gate is misconfigured.

### One-time account setup

These settings live outside the repository. They are not verified by CI.

| Where | Setting |
| --- | --- |
| RubyGems.org, `jimeh` account | One pending trusted publisher per gem name (one covers all of a gem's platform archives): repository owner `jimeh`, repository `connectrpc-ruby`, workflow `release.yml`, environment `release`. |
| GitHub `release` environment | Deployment policy allowing only `v*` tags; add `jimeh` as a required reviewer. |
| GitHub repository settings | Enable immutable releases. Optionally add a tag ruleset that lets only the release App create `v*` tags. It restricts tag creation only: anyone with write access can still dispatch the workflow on an existing tag, so the environment reviewer remains the approval checkpoint. |

**Pending publishers expire 12 hours after creation**, and a pending publisher
does not reserve a gem name. Create each one shortly before merging the release
PR that first ships its gem, or before approving its waiting release job. After the first
publication each one becomes a normal trusted publisher on its gem. On
2026-11-14 the original seven gem names returned 404 from the RubyGems.org API,
on 2026-11-15 so did `connectrpc-prometheus` and `connectrpc-yabeda`, and on
2026-11-16 so did `connectrpc-health`, and on 2026-11-19 so did
`connectrpc-hyper`. `connectrpc-sentry` is already published. A release that
first ships a gem needs its pending publisher created within 12 hours before
that release.

### Before 0.3.0

0.3.0 is the first release with `connectrpc-hyper`. Before merging its release
PR:

1. Confirm a green hyper-gems run (`Hyper gems / ...` checks) on the release
   PR's final head.
2. Within 12 hours before the release, create the pending trusted publisher for
   `connectrpc-hyper` on RubyGems.org (owner `jimeh`, repository
   `connectrpc-ruby`, workflow `release.yml`, environment `release`), after
   confirming the name still returns 404 from
   `https://rubygems.org/api/v1/gems/connectrpc-hyper.json`.
3. Recommended: make `Hyper gems / Verified` a required status check on main,
   so a PR cannot merge with a failing or skipped archive leg.
4. The `release` approval prompt appears only after every `hyper_gems` job,
   including `Check the release candidate`, succeeded. Review that run's build,
   smoke and candidate logs before approving.


Without a required reviewer, merging a release PR publishes without a further
checkpoint. On 2026-11-14 the `release` environment allowed only `v*` tags and
had no required reviewers, variables or secrets.

## Partial failure recovery

RubyGems publication is not atomic across archives, and the GitHub release
stays a draft until every archive is on RubyGems.org. If the job fails, fix the
cause, then resume in one of two ways:

- Re-run the failed `publish` job. GitHub keeps the successful `hyper_gems`
  jobs and their outputs from the earlier attempt
  ([re-running jobs](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs)),
  so `publish` reuses the run's tested archives and evidence and rebuilds only
  the canonical source archives. A re-run of the whole workflow, or a failed
  `hyper_gems` job, rebuilds and retests the hyper archives first.
- Dispatch the release workflow on the same tag. That is a new run: it rebuilds
  and retests all nine hyper archives before `publish`.

The hyper-gems artifacts are kept for 14 days. A publish-only re-run more than
14 days after the build, for example after a long approval wait, fails at the
download step before it uploads or pushes anything. If a publish-only re-run is
skipped or cannot download the run's artifacts, dispatch the workflow on the
same tag to rebuild and retest them.

Either way the candidate is rebuilt from the tagged commit, and its bytes must
match every asset already on the draft and every archive already on
RubyGems.org, so a retained artifact is never replaced or mixed with a
different build. For connectrpc-hyper this relies on reproducible platform
builds: a rebuilt platform archive that differs from a retained draft asset or
registry archive stops the run. Already-published archives are skipped, and
missing ones are pushed in publication order.

A mismatch stops the run before it uploads or pushes anything more. This
covers a conflicting registry version or gem owner, a conflicting, unexpected
or incomplete draft asset, and a rebuild that no longer matches retained bytes.
It requires a maintainer decision. Do not automatically yank, retag or replace
versions.

The draft's assets may be deleted only while none of the twenty archives exists
on RubyGems.org at the tagged version, for example when a later runner image
rebuilds different bytes before any push. A draft release is not public, so
that version has not been released yet. Once any archive at that version is
published, including a single platform gem with the source gem still absent,
the retained bytes are the only valid candidate. An interrupted upload leaves an incomplete asset, which is always
safe to delete. When repair requires a new version, release a new whole-train
version and document the partial train and migration implications.

Release Please creates its commits and tags, and the workflow creates the
guide preparation commit, through the GitHub API as the App, not with a local
GPG key. GitHub signs App commits made without a custom author or committer.
Normal source commits, including a local `release:prepare` fallback, stay
signed.

## Primary references

- [Release Please manifest/bootstrap behavior](https://github.com/googleapis/release-please/blob/v17.6.0/docs/manifest-releaser.md).
- [Generic version annotations](https://github.com/googleapis/release-please/blob/v17.6.0/docs/customizing.md#updating-arbitrary-files).
- [Pinned action inputs](https://github.com/googleapis/release-please-action/blob/45996ed1f6d02564a971a2fa1b5860e934307cf7/action.yml).
- [Draft releases and `force-tag-creation`](https://github.com/googleapis/release-please/blob/v17.6.0/docs/manifest-releaser.md).
- [Pre-1.0 bump implementation](https://github.com/googleapis/release-please/blob/v17.6.0/src/versioning-strategies/default.ts).
- [RubyGems trusted and pending publishers](https://guides.rubygems.org/trusted-publishing/).
- [RubyGems owner assertion for new gems](https://github.com/rubygems/rubygems.org/blob/master/app/models/pusher.rb) (`notify_missing_pending_owner`).
- [Sigstore Ruby CLI](https://github.com/sigstore/sigstore-ruby/tree/v0.2.3).
- [GitHub immutable releases](https://docs.github.com/en/code-security/supply-chain-security/understanding-your-software-supply-chain/immutable-releases).
- [GraphQL `createCommitOnBranch`](https://docs.github.com/en/graphql/reference/mutations#createcommitonbranch).
- [Signature verification for bots](https://docs.github.com/en/authentication/managing-commit-signature-verification/about-commit-signature-verification#signature-verification-for-bots).
- [Triggering a workflow from a workflow](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow#triggering-a-workflow-from-a-workflow).
- [`jobs.<job_id>.needs` and skipped dependents](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#jobsjob_idneeds).
- [Re-running workflows and jobs](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs).
