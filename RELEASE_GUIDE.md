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

1. **Update version.txt in main:** In mainline, open up a new PR to update `version.txt` to match the newest version and merge it into mainline.
- For example, if OSB is currently at version `1.3.0` and we want to release the next version as `1.4.0`, update `version.txt` from `1.3.0` to `1.4.0`. Once it's been merged into mainline, double-check that the image tagged with `dev` in opensearchstaging/opensearch-benchmark has been updated recently. Once
- If we are doing a patch release, visit the feature branch and update the `version.txt` to reflect the new patch release.

2. **Create version branch:** Releases are published from a version branch, not directly from `main`. Maintainers should create a new branch that is based off of `main`. This can be done easily through the official repository's Github UI.
- In the above example, we would create the `1.4` branch and switch to it.  For patch releases, such as `1.3.1`, the `1.3` branch will already exist and does not need to be created.

3. **Check out version branch locally:** If you haven't already, clone the official OpenSearch Benchmark git repository and change directory to it. Fetch the latest changes and the version branch you just created.
```
git fetch origin
git checkout <VERSION BRANCH>
```

4. **Create and push tag:** From the command-line, create and push a tag to the version branch.
- This starts a Github Actions workflow and creates an automated issue in the OSB repository. The issue needs to be commented on by a maintainer of the repository for the release process to proceed.
- The Github ACtions workflow uploads OSB to PyPI.
```
    # Create tag
    git tag 1.4.0 1.4

    # Push tag
    git push origin 1.4.0
```

5. **Track PyPi Release:** Visit `Actions` tab in official OSB repositroy, click `Publish Release to GitHub` on the left side menu, and click the latest action that is running to track the progress of the release.

6. **Verify PyPi Release** Once the release has completed.
    1. Download the OSB distribution build from PyPI: https://pypi.org/project/opensearch-benchmark/#files.  This is a `wheel` file with the extension `.whl`.
    2. Install it with `pip install`.
    3. Run `opensearch-benchmark --version` to ensure that it is the correct version
    4. Run `opensearch-benchmark --help`
    5. Run `opensearch-benchmark list workloads`
    6. Run a basic workload on Linux and MacOS:  `opensearch-benchmark execute-test --workload pmc --test-mode`
    7. If you are fastidious, you can check the installed source files at `` `python3 -m site --user-site`/osbenchmark `` to verify that a recent change is indeed present.

7. **Verify Docker Hub staging OSB dev image works:** Revisit and test out the latest `dev` image.
    1. The staging images are at https://hub.docker.com/r/opensearchstaging/opensearch-benchmark/tags.
    2. Pull the latest image: `docker pull opensearchstaging/opensearch-benchmark:dev`
    3. Check the version of OSB: `docker run opensearchstaging/opensearch-benchmark:dev —version`
    4. Run any other commands listed on the Docker Hub overview tab.

8. **Trigger Docker Hub copy-over workflow:** Contact the oncall managing OpenSearch repositories and request a `copy-over` workflow.After confirming that the `dev` image in Doker Hub staging repository contains the latest changes and latest version, we can copy over this image to an image with the `<VERSION>` tag in the same repository.
    - For example, if we are releasing `1.4.0`, ask the oncall to run the following workfow: repository: opensearchstaging, image: opensearch-benchmark:dev → repository: opensearchstaging, image: opensearch-benchmark:<VERSION>
- The reason this is done because `dev` is the image with the latest changes from `main`, which should also be the latest changes from the version branch we created earlier.
- Verify that the created image with the tag `<VERSION>` shares the same compressed sizes as the `dev` image.


9. **Trigger Docker Hub Promote workflow:** Run `Docker Promote` workflow on the image created from the previous step.
- This workflow will take the image created in the previous step and promotes it to to both Docker Hub prod account and ECR.
- Running `Docker Promote` workflow will automatically trigger four `copy-over` workflows:
        1. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: opensearchproject, image: opensearch-benchmark:<VERSION>
        2. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: opensearchproject, image: opensearch-benchmark:latest
        3. repository: opensearchstaging, image: opensearch-benchmark:<VERSION> → repository: public.ecr.aws/opensearchproject, image: opensearch-benchmark:<VERSION>
        4. repository: opensearchstaging, image: opensearch-benchmark:latest → repository: public.ecr.aws/opensearchproject, image: opensearch-benchmark:latest

10. **Verify release draft and that Dockerhub Production and ECR Production have newest tags:**
    1. Check that the version appears in GitHub (https://github.com/opensearch-project/opensearch-benchmark/releases) and is marked as the “latest” release.  There should be an associated changelog as well.  Clicking on the “Tags” tab should indicate the version number is one of the project’s tags and its timestamp should match that of the last commit.  If there was an error that prevented the release from being published, but this was fixed manually, click on the edit button (pencil icon) next to the release.  This will provide options to generate the release notes, publish the release and label it as the "latest" one.
    2. Check Docker Hub Production: https://hub.docker.com/r/opensearchproject/opensearch-benchmark.  Both “latest” and the published release should appear on the page along with the appropriate publication timestamp.
    3. Check ECR: https://gallery.ecr.aws/opensearchproject/opensearch-benchmark.  The dropdown box at the top should list both “latest” and the published release as entries.  The publication time is also indicated.

11. **Notify the Community:** Create a message that introduces the newly released OpenSearch Benchmark version and includes a brief summary of changes, enhanacements, and bug fixes in the new version. The message may look something like the following:
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