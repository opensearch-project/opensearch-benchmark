# SOP: OpenSearch Benchmark Release Guide

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

OpenSearch Benchmark follows the [semantic versioning convention](https://opensearch.org/blog/what-is-semver/).  Currently, the project maintains the following active branches.

* **main**: The mainline branch where most development occurs and changes get merged in.
* **1.0**, **1.1**, **1.12**, etc.: These are branches associated with released minor versions in the OSB 1.0 major version line, such as `1.0.0`, `1.1.0` and `1.12.0`.  In between minor releases, there may be occasional patch releases such as `1.9.1`, which like version `1.9.0` would also be released off the `1.9` branch.
* **2.0**: The branch associated with the next major version line of OSB, which is yet to be released.

Label PRs that you believe need to be backported with the target branch, such as `1.9` or `1.x`. Backport PRs by checking out the versioned branch, cherry-picking changes and opening a PR against each target backport branch.

### Feature Branches

In general, branches in the authoritative repository are intended for the various versions of the project.  Do not create feature branches, the exception being long-lived branches that require active collaboration from multiple developers.  Name such branches `feature/<FEATURE>`. Once the feature branch is merged into `main`, please make sure the feature branch is deleted.

## Release Labels

Releases are tagged with labels in the scheme `vN.N.N`, for instance `v1.0.0`, `v1.1.0` and `v2.0.0`, as well as `patch` and `backport`. Use release labels to target an issue or a PR for a given release. See [MAINTAINERS](MAINTAINERS.md#triage-open-issues) for more information on triaging issues.

The release process is standard across repositories in this open-source project and is run by a release manager volunteering from amongst the [maintainers](MAINTAINERS.md).

## Backport PRs
Add backport labels to PRs and commits so that changes can be added to `main` branch and other related major and minor version branches. For example, if a PR is published as a patch fix for OSB version 1.3.0, it  should be labeled with a backport label called `backport-1.3` so that it backports to `1.3` branch.

## Prerequisites

* Since releases are generally published on Thursdays, maintainers should try to ensure all changes are merged in by Tuesday.
* A week prior to the scheduled release, maintainers should announce the fact in the [#performance channel](https://opensearch.slack.com/archives/C0516H8EJ7R) within the OpenSearch Slack community.
* Ensure that documentation is appropriately updated with respect to incoming changes prior to the release.

## Release the new version of OpenSearch Benchmark to PyPI, Docker, and ECR

1. Clone the official OpenSearch Benchmark git repository and change directory to it.  This is where the following commands will be issued.

2. Ensure that version.txt matches the new release version before proceeding. If not, open a PR that updates the version in version.txt and merge it in before proceeding with the following steps. For example, if OSB is currently at version `1.3.0` and we want to release the next version as `1.4.0`, update `version.txt` from `1.3.0` to `1.4.0`.

3. Releases are published from a version branch, not directly from `main`.  In the above example, we would create the `1.4` branch and switch to it.  For patch releases, such as `1.3.1`, the `1.3` branch will already exist and does not need to be created.

4. Create a tag for the `1.4.0` release.
```
    git tag 1.4.0 1.4
```

5. Push the tag.  This starts a workflow in Jenkins and creates an automated issue in the OSB repository. The issue needs to be commented on by a maintainer of the repository for the release process to proceed.  The workflow uploads OSB to PyPI and OSB Docker Hub Staging account. Once the workflows are finished publishing OSB to PyPI and OSB Docker Hub staging account, the maintainer who pushed the tag should visit both PyPI and Docker Hub staging to perform the following steps to verify that the artifacts have been properly uploaded.

```
    git push origin 1.4.0
```

6. Check the progress of release here in the Jenkins console:: https://build.ci.opensearch.org/job/opensearch-benchmark-release/
    1. For a more detailed look at what's happening, you can take the Build ID (which is the number highlighted in blue beneath "Stage View") and substitute it into this URL: https://build.ci.opensearch.org/blue/organizations/jenkins/opensearch-benchmark-release/detail/opensearch-benchmark-release/<Build ID>/pipeline/
     2. If failed, inspect the logs.

7. Verify PyPI:
    1. Download the OSB distribution build from PyPI: https://pypi.org/project/opensearch-benchmark/#files.  This is a `wheel` file with the extension `.whl`.
    2. Install it with `pip install`.
    3. Run `opensearch-benchmark --version` to ensure that it is the correct version
    4. Run `opensearch-benchmark --help`
    5. Run `opensearch-benchmark list workloads`
    6. Run a basic workload on Linux and MacOS:  `opensearch-benchmark run --workload pmc --test-mode`
    7. If you are fastidious, you can check the installed source files at `` `python3 -m site --user-site`/osbenchmark `` to verify that a recent change is indeed present.

8. Verify Docker Hub Staging OSB Image Works:
    1. The staging images are at https://hub.docker.com/r/opensearchstaging/opensearch-benchmark/tags.
    2. Pull the latest image: `docker pull opensearchstaging/opensearch-benchmark:<VERSION>`
    3. Check the version of OSB: `docker run opensearchstaging/opensearch-benchmark:<VERSION> —version`
    4. Run any other commands listed on the Docker Hub overview tab.

9. Copy over the image from Docker Hub Staging to Docker Hub Production and ECR: Once you have verified that PyPI and Docker Hub staging image works, contact Admin team member. Admin team member will help promote the “copy-over” workflow, where Jenkins copies the Docker image from Docker Hub staging account to both Docker Hub prod account and ECR.
    1. Admin will need to invoke the copy-over four times:
        1. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: opensearchproject, image: opensearch-benchmark:<VERSION>
        2. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: opensearchproject, image: opensearch-benchmark:latest
        3. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: public.ecr.aws/opensearchproject, image: opensearch-benchmark:<VERSION>
        4. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: public.ecr.aws/opensearchproject, image: opensearch-benchmark:latest

10. See if OpenSearch-Benchmark Tags is Published:
    1. Check that the version appears in GitHub (https://github.com/opensearch-project/opensearch-benchmark/releases) and is marked as the “latest” release.  There should be an associated changelog as well.  Clicking on the “Tags” tab should indicate the version number is one of the project’s tags and its timestamp should match that of the last commit.  If there was an error that prevented the release from being published, but this was fixed manually, click on the edit button (pencil icon) next to the release.  This will provide options to generate the release notes, publish the release and label it as the "latest" one.
    2. Check Docker Hub Production: https://hub.docker.com/r/opensearchproject/opensearch-benchmark.  Both “latest” and the published release should appear on the page along with the appropriate publication timestamp.
    3. Check ECR: https://gallery.ecr.aws/opensearchproject/opensearch-benchmark.  The dropdown box at the top should list both “latest” and the published release as entries.  The publication time is also indicated.

11. Notify the Community: Create a message that introduces the newly released OpenSearch Benchmark version and includes a brief summary of changes, enhanacements, and bug fixes in the new version. The message may look something like the following:
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


12. Ensure that we backport changes to other version branches as needed. See the guide for more information.
    1. Unless you released a major version, update main branch’s `version.txt` to the next minor version. For instance, if `1.1.0` was just released, the file in the `main` branch should be updated to `1.2.0`.
    2. Update the `version.txt` in the branch for the version that was just released with the current version but patch version incremented. For instance, if 1.1.0 was just released, the file in the `1.1` branch should be updated to `1.1.1`.
    3. Previous minor version is now stale
    4. For patch releases only: Ensure that the `main` branch will not need to have its version.txt updated since it's already pointing to the nexts major version. However, the branch with the major and minor version should have its `version.txt` file updated. For example, if `1.3.1` patch was just released, we'll need to visit the `1.3` branch and update the `version.txt` file to now point to `1.3.2`. However, we won't need to update `version.txt` in `main` branch since it's already pointing to the next minor release, i.e. `1.4.0`.

## Error Handling

### Restarting the Release Process after an Error

If an error occurs during build process and you need to retrigger the workflow, do the following:

* Delete the tag locally: `git tag -d <VERSION>`
* Delete the tag on GitHub: `git push --delete origin <VERSION>`
* Delete the draft release on GitHub
* Create the tag again and push it to re-initiate the release process.


### Rename a tag

If you published an incorrect tag name, then follow these steps:

1. Run `git tag new_tag_name old_tag_name` to move old tag references to new tag alias
2. Run `git tag -d old_tag_name` to delete old tag locally
3. Ensure your remote is correct with `git remote -v`. Run `git push origin :refs/tags/old_tag_name` to remove old tag references in remote repository
4. Run `git push origin --tags` to make remote tags look like local tags
5. Verify that the release draft is pointing to the new tag