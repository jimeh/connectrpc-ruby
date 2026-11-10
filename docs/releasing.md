# Release preparation

The first candidate is **0.1.0 across all seven gems**, with alpha maturity.
The version is numeric `0.1.0`, not a RubyGems prerelease such as `0.1.0.alpha`.
It remains experimental. This preparation does not adopt the stronger
[supported profile](support.md) or declare production readiness.

This repository prepares draft release PRs and local gem archives. It does not
publish, push release tags or create GitHub releases. Every gem retains
`allowed_push_host = https://gems.invalid`, and the strict package checks retain
that guard. A successful candidate build grants no publication authority.

## One version train

One root Release Please Ruby component covers the whole repository, including
changes confined to one optional gem. It updates the core version and uses
explicit annotated extra files for the six optional `version.rb` files. Each
gemspec reads its owning static version without requiring runtime code. OTel uses
its same owning constant for instrumentation. The root `CHANGELOG.md` describes the entire train.
The future tag convention is one `v0.x.y` tag, not seven component tags.

The initial manifest is empty and `initial-version` is `0.1.0`. A manifest entry
of `0.1.0` would claim a previous version and is not the bootstrap mechanism.
There is no permanent `release-as` override or history cutoff. Because source
already has version `0.1.0`, the first generated release PR normally changes
only the changelog and manifest. It does not exercise future version updates.

Before 1.0, `fix:` and compatible `feat:` commits increment the patch. A breaking
commit (`!` or a `BREAKING CHANGE:` footer) increments the minor. Breaking
minors require migration notes explaining affected calls, options, defaults,
wire behavior, tracing and generated code. Keep migration notes on main in a
committed document or the breaking commit footer. Bot-generated release branches
are not durable homes for hand-edited migration notes.

The train uses matching package versions. Internal gem dependencies keep the
existing pessimistic minor-series range, `~> M.m.0`; a patch does not narrow
that floor. External dependency constraints, resolved registry versions,
checksums and tool pins are unchanged by release preparation. Matching versions
do not add a runtime enforcement mechanism or adopt the support profile.

## Prepare a draft release PR

The [workflow](../.github/workflows/release-please.yml) runs only on manual
dispatch from main. It uses `skip-github-release: true`, opens draft PRs and has
no publishing job or OIDC permission. Merging the setup PR does not run it.

The existing release GitHub App needs these references, as in the pinned
[Airplan workflow](https://github.com/jimeh/airplan/blob/f70df0f43404f07808e56613a4511b53160c2bbd/.github/workflows/release-please.yml):

| Reference | Required setup |
| --- | --- |
| `vars.RELEASE_BOT_CLIENT_ID` | The existing App's client ID, available to this repository. |
| `secrets.RELEASE_BOT_PRIVATE_KEY` | The existing App's private key, available through a repository or authorized shared secret. |
| App installation | Installation access to `jimeh/connectrpc-ruby`, with contents, issues and pull requests write permissions. |

The workflow requests an installation token scoped to the current repository
and those three permissions. Its `GITHUB_TOKEN` has no permissions. App-created
PRs can trigger normal downstream CI. Missing references fail dispatch with an
actionable error; there is no fallback token. Do not print, copy into the
checkout, or ask for private-key values in chat.

At preparation time, the coordinator's repository name listings did not show
either reference. Shared-secret availability and App installation have not
been verified. Account and repository configuration require a separate explicit
authorization. Source preparation and offline checks can proceed without them.

Both action pins meet the seven-day age policy: release-please-action v5.0.0,
published 2026-04-22, and create-github-app-token v3.3.0, published 2026-10-12.
Their tag commits were checked against GitHub. The pinned release action embeds
release-please 17.6.0. Review new pins and their embedded behavior before updates.

After App setup is separately authorized and complete:

1. Dispatch **Release preparation** on main. Inspect the generated draft PR;
   verify that the initial proposed version is exactly `0.1.0` and all seven
   gems remain included in the train.
2. Check out that release branch. Run `mise run release:prepare` before frozen
   Bundler setup. This updates all eight local path locks, including digest-free
   local CHECKSUMS entries and internal dependency floors, and installation
   guides. It does not resolve dependencies or download anything. Review the
   diff and commit with the existing signing configuration.
3. Run `mise run release:check`, focused tests and the candidate procedure below.
   Capture the exact head and CI/review evidence after the final bot update.

Release Please's Ruby updater changes only the first matching core entry in the
root lock. It leaves other path records and local CHECKSUMS stale. The release
check rejects that partial state. `release:prepare` owns preparation of the root
lock and all five optional locks plus both Rails Appraisal locks. The typecheck
tool lock has no local package records. Frozen installs in each owning CI lane
are the evidence that Bundler accepts these prepared locks.

**Re-run preparation after every bot regeneration.** Release Please rebuilds
its branch from main when its generated body changes. It can discard signed
preparation commits and hand edits in the release PR. Finish the bot update,
then prepare and validate its final head; a quiet main during that window helps.
Do not present earlier CI/review evidence as covering a later regenerated head.

## Validate a local candidate

Use `mise tasks` for the task index:

| Task | Evidence and writes |
| --- | --- |
| `release:check` | Offline version, configuration, guard and prepared local-lock checks. No writes or setup dependencies. |
| `release:prepare` | Deterministic local path-lock and installation-guide updates. No registry resolution. |
| `release:build` | Seven canonical archives, two-build hash comparison and `candidate.json`. No installation or publication. |
| `release:candidate` | Existing installed-package/generation/RBS checks followed by the canonical archive build. Uses normal scoped setup prerequisites. |

Run the focused release specs and workflow checks first:

```sh
mise run release:check
mise run test -- spec/release_train_spec.rb spec/package_build_spec.rb spec/ci_runtime_spec.rb
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
mise run rbs:check
mise run rbs:package
mise run grpc:status:check
MISE_RUBY_VERSION=4.0.9 mise run testing:package
MISE_RUBY_VERSION=4.0.9 mise run otel:package
MISE_RUBY_VERSION=4.0.9 mise run rbs:check
MISE_RUBY_VERSION=4.0.9 mise run rbs:package
MISE_RUBY_VERSION=4.0.9 mise run grpc:status:check
mise run release:build
```

The supported Rails 8.0 evidence lane is Ruby 3.4.12. Hosted CI owns its clean
matrix evidence; it is separate from local results. Preserve unexplained
failures and resolve them before broadening validation. Run `release:candidate`
instead when starting a bounded package-only qualification, not as an extra
duplicate graph after full verification.

The existing package checks validate independently reviewed exact manifests,
MIT notices, third-party notices and descriptor bytes, signatures and guards.
They install archives offline into fresh `GEM_HOME`/`GEM_PATH` outside the
checkout. Their generated consumers cover core alone, Rack, Rails with Rack,
Async, Falcon with Async, testing and API-only OTel. Installed RBS checks cover
all seven gem-discovered signature libraries, composed framework opt-ins and
real generated runtime consumers. Here "signatures" means RBS, not cryptographic
gem signing. Generator checks reproduce fixtures; never hand-edit generated
Ruby or RBS files.

The canonical build requires a clean Git checkout and Ruby 3.4.12. It derives
`SOURCE_DATE_EPOCH` from the source commit timestamp, records the Ruby/RubyGems
versions, and uses the existing private staging builder. Executables have mode
0755 and ordinary files 0644; checkout modes remain untouched. Every archive's
manifest, bytes, notices and modes are checked against its owning source, and
a second temporary build must produce all seven identical SHA256 hashes.
This proves repeatability in the recorded lane, not cross-Ruby bit identity.

Output is `vendor/release/<version>/<commit>/`: exactly seven `.gem` files and
`candidate.json`, which records source revision, epoch, tool versions, member
inventories and hashes. The directory must be missing or empty. Preserve an
existing candidate; use a new reviewed commit for a changed candidate instead
of mixing or overwriting artifacts. Local lane builds retain their established
isolated package directories. Stale older archives are not selected by glob.

## Later release enablement

This section is a future procedure, not authorization or an active workflow.
Before enabling anything, Jim must separately approve the exact candidate,
release/tag operation, publication path and guard transition. Gem-name control,
registry ownership, App installation and trusted-publisher settings are
unverified. A pending publisher does not reserve a gem name.

**Do not merge the first release PR while preparation-only mode is active.**
Release Please cannot advance normally past an untagged merged release PR.
Changing `skip-github-release` to false can tag an already-merged release PR's
commit on the next run. Inspect outstanding release PRs and labels before
authorizing that transition. A draft GitHub release setting alone is not an
equivalent prohibition on tag creation.

The recommended publication design is [RubyGems trusted publishing](https://guides.rubygems.org/trusted-publishing/):
short-lived OIDC credentials, not a long-lived RubyGems API key. For first
publication, configure one pending trusted publisher per gem with owner
`jimeh`, repository `connectrpc-ruby`, the exact future publishing workflow
filename and a protected `release` environment. An existing gem instead needs
an owner-authorized trusted publisher. Protect the environment with required
reviewers before adding a workflow with job-scoped `id-token: write`.

The enablement change must deliberately replace `gems.invalid` in all seven
gemspecs and their strict validators, review the publishing action and its
multi-gem behavior, and define the tag/signing/provenance policy. Preserve normal
signed source commits. App-created Release Please commits are a separate bot
identity; do not claim local GPG signatures for them. There is no gem certificate
signing or Sigstore attestation implemented here. An eventual publishing job
should attest the retained artifact hashes and source revision using GitHub's
OIDC identity, with verified provenance published alongside the common release.
That design, permissions and pins need review before activation.

Changing publication metadata changes archive bytes. Today's guarded dry-run
hashes cannot be published as the later enabled artifacts. Build and qualify a
new clean candidate after the authorized guard transition, and preserve those
exact approved bytes for publication and recovery. Do not rebuild midway
through a publication attempt. Verify the final head contains current main,
all required local/hosted checks and independent review cover it, migration
notes are complete, and no valid blocking feedback remains before the separate
release decision.

## Publication order and partial failure recovery

The dependency order for a later authorized publisher is:

1. `connectrpc`.
2. `connectrpc-rack`, `connectrpc-async`, `connectrpc-testing`, `connectrpc-otel`.
3. `connectrpc-rails` after Rack, and `connectrpc-falcon` after Async.

All seven are released even if only one changed. RubyGems publication is not
atomic across gems. Record per-gem success and wait for dependency availability
before its dependents. Do not declare the train complete until all seven exact
versions and the normal installed combinations have been checked from the
registry under a separately authorized qualification procedure.

If publication stops partway, freeze the source revision, retained enabled
candidate, hashes and per-gem results. With explicit authorization, inspect
which versions exist and compare their downloaded bytes to the approved
inventory. Resume only missing gems, in dependency order, from those retained
bytes. A version already published with identical bytes is a recorded success;
do not try to overwrite it. A conflicting artifact, lost candidate or suspected
compromise requires a maintainer decision. Do not automatically yank, retag,
replace versions or publish a mixture of rebuilt artifacts. When repair requires
a new version, prepare a new whole-train version and document the partial train
and migration implications.

## Primary references

- [Release Please manifest/bootstrap behavior](https://github.com/googleapis/release-please/blob/v17.6.0/docs/manifest-releaser.md).
- [Generic version annotations](https://github.com/googleapis/release-please/blob/v17.6.0/docs/customizing.md#updating-arbitrary-files).
- [Pinned action inputs and release suppression](https://github.com/googleapis/release-please-action/blob/45996ed1f6d02564a971a2fa1b5860e934307cf7/action.yml).
- [Pre-1.0 bump implementation](https://github.com/googleapis/release-please/blob/v17.6.0/src/versioning-strategies/default.ts).
- [RubyGems trusted and pending publishers](https://guides.rubygems.org/trusted-publishing/).
