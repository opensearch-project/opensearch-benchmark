# SOP: OpenSearch-Benchmark Release Guide

- [Overview](#overview)
- [Branches](#branches)
    - [Releasing Branches](#release-branches)
    - [Feature Branches](#feature-branches)
- [Release Labels](#release-labels)
- [Prerequisites](#prerequisites)
- [Release new version of OpenSearch Benchmark to PyPi, Docker, and ECR](#release-new-version-of-opensearch-benchmark-to-pypi-docker-and-ecr)
- [Error Handling](#error-handling)

## Overview
This document explains the release strategy for artifacts in this organization.

## Branches

### Release Branches

Given the current major release of 1.0, projects in this organization maintain the following active branches.

* **main**: The next `1.x` release. This is the branch where all merges take place and code moves fast.
* **1.0**: The _current_ release. In between minor releases, only hotfixes (e.g. security) are backported to `1.0`.

Label PRs with the next major version label (e.g. `2.0.0`) and merge changes into `main`. Label PRs that you believe need to be backported as `1.x` and `1.0`. Backport PRs by checking out the versioned branch, cherry-pick changes and open a PR against each target backport branch.

### Feature Branches

Do not creating branches in the upstream repo, use your fork, for the exception of long lasting feature branches that require active collaboration from multiple developers. Name feature branches `feature/<thing>`. Once the work is merged to `main`, please make sure to delete the feature branch.

## Release Labels

Repositories create consistent release labels, such as `v1.0.0`, `v1.1.0` and `v2.0.0`, as well as `patch` and `backport`. Use release labels to target an issue or a PR for a given release. See [MAINTAINERS](MAINTAINERS.md#triage-open-issues) for more information on triaging issues.

The release process is standard across repositories in this org and is run by a release manager volunteering from amongst [maintainers](MAINTAINERS.md).

## Prerequisites

* Since releases are only done on Thursdays, maintainers should ensure all changes are merged by Tuesday.
    * A week prior to the scheduled release, maintainers should announce a code freeze in the [#performance channel](https://opensearch.slack.com/archives/C0516H8EJ7R) within the OpenSearch Slack community. See the following example for what that might look like:
```
OpenSearch Benchmark release is scheduled for 1/25 and a code freeze will be put in place starting on 1/23.
```
* Ensure that version.txt matches the new release version before proceeding. If not, open a PR that updates the version in version.txt and merge it in before proceeding with the following steps. For example, if OSB is currently on version 0.3.0 and we want to release the next version as 0.4.0, update version.txt from 0.3.0 to 0.4.0.
* Ensure you have git cloned the official OpenSearch Benchmark repository with the ssh address on your local computer.
* Ensure that all new committed changes in OSB that are visible by users are added to documentation

## Release new version of OpenSearch Benchmark to PyPi, Docker, and ECR

NOTE: The version number below is in semantic format, for instance, 1.2.0.

1. Create a tag: `git tag <VERSION> main`
    1. Ensure that this is done in the main official opensearch-benchmark repository
    2. This should be the new version that you have in version.txt.
2. Push the tag: `git push origin <VERSION>`
    1. This starts a workflow in Jenkins and creates an automated issue in the OSB repository. The issue needs to be commented on by a maintainer of the repository for the release process to proceed.
    2. Example of automated issue opened by Jenkins Workflow
3. Maintainer needs to comment on Automated Issue: Once Maintainer has commented, the workflow uploads OSB to PyPi and OSB Dockerhub Staging account. Once the workflows are finished publishing OSB to PyPI and OSB Dockerhub staging account (verify here), person who pushed the tag should visit both PyPi and Dockerhub staging.
    1. Check progress of release here in Jenkins console:: https://build.ci.opensearch.org/job/opensearch-benchmark-release/
        1. If failed,
    2. Verify PyPI:
        1. Download the OSB distribution build from PyPI: https://pypi.org/project/opensearch-benchmark/#files.  This is a `wheel` file with the extension `.whl`.
        2. Install it with `pip install`.
        3. Run `opensearch-benchmark --version` to ensure that it is the correct version
        4. Run `opensearch-benchmark --help`
        5. Run `opensearch-benchmark list workloads`
        6. Run a basic workload on Linux and MacOS:  `opensearch-benchmark execute-test --workload pmc --test-mode`

    3. Verify Dockerhub Staging OSB Image Works:
        1. The staging images are at https://hub.docker.com/r/opensearchstaging/opensearch-benchmark/tags.
        2. Pull the latest image: `docker pull opensearchstaging/opensearch-benchmark:<VERSION>`
        3. Check the version of OSB: `docker run opensearchstaging/opensearch-benchmark:<VERSION> —version`
        4. Run any other commands listed on the Dockerhub overview tab.
4. Copy over image from Dockerhub Staging to Dockerhub Production and ECR: Once you have verified that PyPi and Dockerhub staging image works, contact Admin team member. Admin team member will help promote the “copy-over” workflow, where Jenkins copies the Docker image from Dockerhub staging account to both Dockerhub prod account and ECR.
    1. Admin will need to invoke the copy-over four times:
        1. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: opensearchproject, image: opensearch-benchmark:<VERSION>
        2. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: opensearchproject, image: opensearch-benchmark:latest
        3. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: public.ecr.aws/opensearchproject, image: opensearch-benchmark:<VERSION>
        4. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: public.ecr.aws/opensearchproject, image: opensearch-benchmark:latest
5. See if OpenSearch-Benchmark Tags is Published:
    1. Check that the version appears in GitHub (https://github.com/opensearch-project/opensearch-benchmark/releases) and is marked as the “latest” release.  There should be an associated changelog as well.  Clicking on the “Tags” tab should indicate the version number is one of the project’s tags and its timestamp should match that of the last commit.
    2. Check Dockerhub Production: https://hub.docker.com/r/opensearchproject/opensearch-benchmark.  Both “latest” and the published release should appear on the page along with the appropriate publication timestamp.
    3. Check ECR: https://gallery.ecr.aws/opensearchproject/opensearch-benchmark.  The dropdown box at the top should list both “latest” and the published release as entries.  The publication time is also indicated.
6. Notify Community: Inform everyone in the following channels that the new OpenSearch-Benchmark version is available and provide a brief summary of what the new version includes.

```
@here OpenSearch Benchmark (OSB) 1.2.0 has just been released! :hitom: :mega: :tada:

What’s changed?
  * Read here: https://github.com/opensearch-project/opensearch-benchmark/releases/tag/1.2.0
  * This version includes several enhancements and fixes contributed by OSCI participants
  * Documentation: https://opensearch.org/docs/latest/benchmark
Wow! Where can I get this?
  * PyPI: https://pypi.org/project/opensearch-benchmark
  * DockerHub: https://hub.docker.com/r/opensearchproject/opensearch-benchmark/tags
  * ECR: https://gallery.ecr.aws/opensearchproject/opensearch-benchmark
```
Notify the following channels in OpenSearch Community Slack
    * #performance


7. Ensure that we back port changes to other version branches as needed. See guide for more information.
    1. Unless you released a major version, update main branch’s version.txt to the next minor version.  For instance, it should be updated to 1.2.0 immediately after the 1.1.0 release.
    2. Update the version.txt in the branch for the version that was just released with current version but patch version incremented
    3. Previous minor version is now stale

## Error Handling

If error occurs during build process and need to retrigger the workflow, do the following:

* Delete tag locally `git tag -d <VERSION>`
* Delete tag on Github
* Delete draft-release on Github

Afterwards, remake the tag and push it.

