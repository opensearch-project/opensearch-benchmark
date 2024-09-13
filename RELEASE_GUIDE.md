# SOP: OpenSearch-Benchmark Release Guide

## Table of Contents
- [Overview](#overview)
- [Branches](#branches)
    - [Releasing Branches](#release-branches)
    - [Feature Branches](#feature-branches)
- [Release Labels](#release-labels)
- [Prerequisites](#prerequisites)
- [Release the new version of OpenSearch Benchmark to PyPI, Docker, and ECR](#release-the-new-version-of-opensearch-benchmark-to-pypi-docker-and-ecr)
- [Error Handling](#error-handling)

## Overview
This document explains the release strategy for artifacts in this project.

## Branches

### Release Branches

Given the current major release of 1.0, the OpenSearch Benchmark project maintains the following active branches.

* **main**: The next `1.x` release. This is the branch where all merges take place and code moves fast.
* **1.0**: The _current_ release. In between minor releases, only hotfixes (e.g. security) are backported to `1.0`.

Label PRs with the next major version label (e.g. `2.0.0`) and merge changes into `main`. Label PRs that you believe need to be backported as `1.x` and `1.0`. Backport PRs by checking out the versioned branch, cherry-picking changes and opening a PR against each target backport branch.

### Feature Branches

Do not creating branches in the upstream repo, use your fork, with the exception of long lasting feature branches that require active collaboration from multiple developers. Name feature branches `feature/<FEATURE>`. Once the feature branch is merged into `main`, please make sure to delete the feature branch.

## Release Labels

Releases are tagged with labels in the scheme `vN.N.N`, for instance `v1.0.0`, `v1.1.0` and `v2.0.0`, as well as `patch` and `backport`. Use release labels to target an issue or a PR for a given release. See [MAINTAINERS](MAINTAINERS.md#triage-open-issues) for more information on triaging issues.

The release process is standard across repositories in this open-source project and is run by a release manager volunteering from amongst the [maintainers](MAINTAINERS.md).

## Backport PRs
Add backport labels to  PRs and commits so that changes can be added to `main` branch and other related major and minor version branches. For example, if a PR is published as a patch fix for OSB version 1.3.0, it  should be labeled with a backport label called `backport-1.3` so that it backports to `1.3` branch.

## Prerequisites

* Since releases are only done on Thursdays, maintainers should ensure all changes are merged by Tuesday.
    * A week prior to the scheduled release, maintainers should announce a code freeze in the [#performance channel](https://opensearch.slack.com/archives/C0516H8EJ7R) within the OpenSearch Slack community. See the following example for what that might look like:
```
OpenSearch Benchmark release is scheduled for 1/25 and a code freeze will be put in place starting on 1/23.
```
* Maintainers should create a release issue before each release. These issues should contain important dates related to announcement for code freeze and release dates on the public slack channel, freeze date, and release date. It should also include a list of features issues and bug issues that are labeled with the same release version.
* Ensure that version.txt matches the new release version before proceeding. If not, open a PR that updates the version in version.txt and merge it in before proceeding with the following steps. For example, if OSB is currently at version `0.3.0` and we want to release the next version as `0.4.0`, update `version.txt` from `0.3.0` to `0.4.0`.
* Ensure you have cloned the official OpenSearch Benchmark git repository with the ssh address.
* Ensure that all new committed changes in OSB that are visible by users are added to the documentation

## Release the new version of OpenSearch Benchmark to PyPI, Docker, and ECR

NOTE: The version number below is in semantic format, for instance, `1.2.0`.

1. Create a tag: `git tag <VERSION> main`
    1. Ensure that this is done in the main official opensearch-benchmark repository
    2. This should be the `<VERSION>` tag that matches the entry in version.txt.
    3. For patch releases: Change `main` to the major and minor version branch name

2. Push the tag: `git push origin <VERSION>`
    1. This starts a workflow in Jenkins and creates an automated issue in the OSB repository. The issue needs to be commented on by a maintainer of the repository for the release process to proceed.
    2. Example of automated issue opened by the Jenkins Workflow

3. Maintainer needs to comment on Automated Issue: Once the maintainer has commented, the workflow uploads OSB to PyPI and OSB Docker Hub Staging account. Once the workflows are finished publishing OSB to PyPI and OSB Docker Hub staging account, the maintainer who pushed the tag should visit both PyPI and Docker Hub staging to perform the following steps to verify that the artifacts have been properly uploaded.
    1. Check the progress of release here in the Jenkins console:: https://build.ci.opensearch.org/job/opensearch-benchmark-release/
        1. For a more detailed look at what's happening, you can take the Build ID (which is the number highlighted in blue beneath "Stage View") and substitute it into this URL: https://build.ci.opensearch.org/blue/organizations/jenkins/opensearch-benchmark-release/detail/opensearch-benchmark-release/<Build ID>/pipeline/
 	 2. If failed, inspect the logs.

    2. Verify PyPI:
        1. Download the OSB distribution build from PyPI: https://pypi.org/project/opensearch-benchmark/#files.  This is a `wheel` file with the extension `.whl`.
        2. Install it with `pip install`.
        3. Run `opensearch-benchmark --version` to ensure that it is the correct version
        4. Run `opensearch-benchmark --help`
        5. Run `opensearch-benchmark list workloads`
        6. Run a basic workload on Linux and MacOS:  `opensearch-benchmark execute-test --workload pmc --test-mode`
        7. If you are aware of a change going into the version being released, you can run `python3 -m site` (assuming you installed the latest version that was just released) and get the path for Python. Visit the `osbenchmark` directory and verify that the change exists in the associated OSB files.

    3. Verify Docker Hub Staging OSB Image Works:
        1. The staging images are at https://hub.docker.com/r/opensearchstaging/opensearch-benchmark/tags.
        2. Pull the latest image: `docker pull opensearchstaging/opensearch-benchmark:<VERSION>`
        3. Check the version of OSB: `docker run opensearchstaging/opensearch-benchmark:<VERSION> —version`
        4. Run any other commands listed on the Docker Hub overview tab.

4. Copy over the image from Docker Hub Staging to Docker Hub Production and ECR: Once you have verified that PyPI and Docker Hub staging image works, contact Admin team member. Admin team member will help promote the “copy-over” workflow, where Jenkins copies the Docker image from Docker Hub staging account to both Docker Hub prod account and ECR.
    1. Admin will need to invoke the copy-over four times:
        1. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: opensearchproject, image: opensearch-benchmark:<VERSION>
        2. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: opensearchproject, image: opensearch-benchmark:latest
        3. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: public.ecr.aws/opensearchproject, image: opensearch-benchmark:<VERSION>
        4. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: public.ecr.aws/opensearchproject, image: opensearch-benchmark:latest

5. See if OpenSearch-Benchmark Tags is Published:
    1. Check that the version appears in GitHub (https://github.com/opensearch-project/opensearch-benchmark/releases) and is marked as the “latest” release.  There should be an associated changelog as well.  Clicking on the “Tags” tab should indicate the version number is one of the project’s tags and its timestamp should match that of the last commit.  If there was an error that prevented the release from being published, but this was fixed manually, click on the edit button (pencil icon) next to the release.  This will provide options to generate the release notes, publish the release and label it as the "latest" one.
    2. Check Docker Hub Production: https://hub.docker.com/r/opensearchproject/opensearch-benchmark.  Both “latest” and the published release should appear on the page along with the appropriate publication timestamp.
    3. Check ECR: https://gallery.ecr.aws/opensearchproject/opensearch-benchmark.  The dropdown box at the top should list both “latest” and the published release as entries.  The publication time is also indicated.

6. Notify the Community: Create a message that introduces the newly released OpenSearch Benchmark version and includes a brief summary of changes, enhanacements, and bug fixes in the new version. The message may look something like the following:
```
@here OpenSearch Benchmark (OSB) 1.2.0 has just been released!

What’s changed?
  * Read here: https://github.com/opensearch-project/opensearch-benchmark/releases/tag/1.2.0
  * Documentation: https://opensearch.org/docs/latest/benchmark
Wow! Where can I get this?
  * PyPI: https://pypi.org/project/opensearch-benchmark
  * Docker Hub: https://hub.docker.com/r/opensearchproject/opensearch-benchmark/tags
  * ECR: https://gallery.ecr.aws/opensearchproject/opensearch-benchmark
```

Send this message in the following channels in OpenSearch Community Slack:
* [#performance](https://opensearch.slack.com/archives/C0516H8EJ7R)


7. Ensure that we backport changes to other version branches as needed. See the guide for more information.
    1. Unless you released a major version, update main branch’s `version.txt` to the next minor version. For instance, if `1.1.0` was just released, the file in the `main` branch should be updated to `1.2.0`.
    2. Update the `version.txt` in the branch for the version that was just released with the current version but patch version incremented. For instance, if 1.1.0 was just released, the file in the `1.1` branch should be updated to `1.1.1`.
    3. Previous minor version is now stale
    4. For patch releases only: Ensure that the `main` branch will not need to have its version.txt updated since it's already pointing to the nexts major version. However, the branch with the major and minor version should have its `version.txt` file updated. For example, if `1.3.1` patch was just released, we'll need to visit the `1.3` branch and update the `version.txt` file to now point to `1.3.2`. However, we won't need to update `version.txt` in `main` branch since it's already pointing to the next minor release, i.e. `1.4.0`.

## Error Handling

### Remove a tag

If error occurs during build process and need to retrigger the workflow, do the following:

* Delete the tag locally `git tag -d <VERSION>` and `git push --delete origin <VERSION>` to remove in remote repository
* Delete the tag on Github
* Delete draft-release on Github

Then, create the tag again and push it.


### Rename a tag

If you published an incorrect tag name, then follow these steps:

1. Run `git tag new_tag_name old_tag_name` to move old tag references to new tag alias
2. Run `git tag -d old_tag_name` to delete old tag locally
3. Ensure your remote is correct with `git remote -v`. Run `git push origin :refs/tags/old_tag_name` to remove old tag references in remote repository
4. Run `git push origin --tags` to make remote tags look like local tags
5. Verify that the release draft is pointing to the new tag