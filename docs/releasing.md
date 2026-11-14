# Releasing

The first release is **0.1.0 across all seven gems**, with alpha maturity.
The version is numeric `0.1.0`, not a RubyGems prerelease such as `0.1.0.alpha`.
It remains experimental and does not adopt the stronger
[supported profile](support.md) or declare production readiness.

The pipeline has three stages:

1. Every push to main runs Release Please, which opens or updates one draft
   release PR for the whole train.
2. Merging that PR makes Release Please push a `vX.Y.Z` tag and create a draft
   GitHub release with the changelog.
3. The tag starts the [release workflow](../.github/workflows/release.yml). It
   builds the canonical candidate, uploads the seven `.gem` files and
   `candidate.json` to the draft, pushes each gem to RubyGems.org with a
   Sigstore attestation using trusted publishing, and publishes the GitHub
   release last.

Merging a release PR is the release decision. Every gemspec pins
`allowed_push_host` to `https://rubygems.org` and sets `rubygems_mfa_required`,
so a manual push needs the owner's MFA. Trusted-publisher keys are exempt from
that requirement. The strict package checks enforce both. A successful build
or verify command grants no release authority.

## One version train

One root Release Please Ruby component covers the whole repository, including
changes confined to one optional gem. It updates the core version and uses
explicit annotated extra files for the six optional `version.rb` files. Each
gemspec reads its owning static version without requiring runtime code. OTel uses
its same owning constant for instrumentation. The root `CHANGELOG.md` describes the entire train.
The tag convention is one `v0.x.y` tag, not seven component tags.

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

The [Release Please workflow](../.github/workflows/release-please.yml) runs on
every push to main, and on manual dispatch from main. It opens or updates the
draft release PR. After a release PR merges, the next run pushes its tag and
creates a draft GitHub release. The configuration sets `draft` and
`force-tag-creation`: GitHub creates no tag for a draft release until it is
published, and without the tag Release Please cannot find the previous release.
This workflow has no publishing job or OIDC permission.

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
and those three permissions. Its `GITHUB_TOKEN` has no permissions. App-created
PRs can trigger normal downstream CI. Missing references fail the run with an
actionable error; there is no fallback token. Do not print, copy into the
checkout, or ask for private-key values in chat.

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

1. Inspect the generated draft PR. For the first release, verify that the
   proposed version is exactly `0.1.0`; every release includes all seven gems.
2. Check out that release branch. Run `mise run release:prepare` before frozen
   Bundler setup. This updates all eight local path locks, including digest-free
   local CHECKSUMS entries and internal dependency floors, and installation
   guides. It does not resolve dependencies or download anything. Review the
   diff and commit with the existing signing configuration.
3. Run `mise run release:check`, focused tests and the candidate procedure below.
   Capture the exact head and CI/review evidence after the final bot update.
4. Mark the PR ready and merge it once the release decision is made. The
   release workflow then waits for any `release` environment approval.

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
| `release:tools` | Installs the frozen Sigstore signing bundle (`release/Gemfile`) into `vendor/sigstore`. |
| `release:publish` | CI only. Refuses to run outside `release.yml` on a `v*` tag. |

Run the focused release specs and workflow checks first:

```sh
mise run release:check
mise run test -- spec/release_train_spec.rb spec/release_publisher_spec.rb spec/package_build_spec.rb spec/ci_runtime_spec.rb
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
MIT notices, third-party notices and descriptor bytes, signatures and push hosts.
They install archives offline into fresh `GEM_HOME`/`GEM_PATH` outside the
checkout. Their generated consumers cover core alone, Rack, Rails with Rack,
Async, Falcon with Async, testing and API-only OTel. Installed RBS checks cover
all seven gem-discovered signature libraries, composed framework opt-ins and
real generated runtime consumers. Here "signatures" means RBS, not cryptographic
gem signing; published gems carry Sigstore attestations instead. Generator checks reproduce fixtures; never hand-edit generated
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

## Publish a release

The release workflow runs only on `v*` tags, in the `release` environment, with
job-scoped `contents: write` and `id-token: write`. It uses no dependency
caches. RubyGems trusted publishers match its filename and environment, so
renaming either breaks publication. `release:publish` runs these steps:

1. It checks that the tag is `v<version>` for the train and that the checkout
   is the tagged commit. It then verifies the candidate inventory and that each
   gem's internal dependencies precede it in publication order.
2. It finds the GitHub release for the tag, polling for up to five minutes
   because Release Please creates the draft just after the tag.
3. It checks all seven gems on RubyGems.org before uploading or pushing
   anything. A gem name that already exists must be owned by `jimeh`, even
   when its bytes match, and a version that already exists must have the
   candidate's SHA256. Pending publishers do not reserve names.
4. It uploads any missing assets to the draft. An existing asset must be fully
   uploaded and match the candidate's SHA256, and an unexpected asset stops
   the run.
5. It pushes the gems in dependency order:
   1. `connectrpc`.
   2. `connectrpc-rack`, `connectrpc-async`, `connectrpc-testing` and
      `connectrpc-otel`.
   3. `connectrpc-rails` after Rack, and `connectrpc-falcon` after Async.

   For each gem missing from RubyGems.org, it signs the archive with
   `sigstore-cli` using the job's GitHub OIDC identity and pushes it with
   `gem push --attestation`. Each push exchanges a fresh OIDC token for a
   15-minute RubyGems key. The exchange asserts owner `jimeh`, which RubyGems
   requires before a pending publisher can create a new gem. Versions found
   in step 3 are skipped.
6. It publishes the draft and marks it latest. With immutable releases enabled,
   GitHub then locks the tag and assets and adds its release attestation.

RubyGems' own action, `configure-rubygems-credentials` v2.1.0, cannot assert an
owner, so the script performs the exchange itself. `sigstore-cli` is pinned to
0.2.3, the signer that `rubygems/release-gem` uses against RubyGems.org;
0.3.0 adds Rekor v2 signing and needs separate review. The bundled RubyGems
3.6.9 accepts `--attestation` but does not sign on its own.

### One-time account setup

These settings live outside the repository. They are not verified by CI.

| Where | Setting |
| --- | --- |
| RubyGems.org, `jimeh` account | One pending trusted publisher per gem: repository owner `jimeh`, repository `connectrpc-ruby`, workflow `release.yml`, environment `release`. |
| GitHub `release` environment | Deployment policy allowing only `v*` tags; add `jimeh` as a required reviewer. |
| GitHub repository settings | Enable immutable releases. Optionally add a tag ruleset that lets only the release App create `v*` tags. It restricts tag creation only: anyone with write access can still dispatch the workflow on an existing tag, so the environment reviewer remains the approval checkpoint. |

**Pending publishers expire 12 hours after creation**, and a pending publisher
does not reserve a gem name. Create all seven shortly before merging the first
release PR, or before approving its waiting release job. After the first
publication each one becomes a normal trusted publisher on its gem. On
2026-11-14 all seven gem names returned 404 from the RubyGems.org API.

Without a required reviewer, merging a release PR publishes without a further
checkpoint. On 2026-11-14 the `release` environment allowed only `v*` tags and
had no required reviewers, variables or secrets.

## Partial failure recovery

RubyGems publication is not atomic across gems, and the GitHub release stays a
draft until every gem is on RubyGems.org. If the job fails, fix the cause, then
re-run the failed job or dispatch the release workflow on the same tag. The
re-run rebuilds the candidate from the tagged commit. Its bytes must match
every asset already on the draft and every version already on RubyGems.org,
so a retained artifact is never replaced or mixed with a different build.
Already-published gems are skipped, and missing ones are pushed in dependency
order.

A mismatch stops the run before it uploads or pushes anything more. This
covers a conflicting registry version or gem owner, a conflicting, unexpected
or incomplete draft asset, and a rebuild that no longer matches retained bytes.
It requires a maintainer decision. Do not automatically yank, retag or replace
versions.

The draft's assets may be deleted only while none of the seven gems exists on
RubyGems.org at the tagged version, for example when a later runner image
rebuilds different bytes before any push. A draft release is not public, so
that version has not been released yet. Once any gem at that version is
published, the retained bytes are the only valid candidate. An interrupted upload leaves an incomplete asset, which is always
safe to delete. When repair requires a new version, release a new whole-train
version and document the partial train and migration implications.

Release Please creates its commits and tags through the GitHub API as the App,
not with a local GPG key. Normal source commits stay signed.

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
