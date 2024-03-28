# Maintainers Guide

## Responsibilities

### Work on Current Issues and Create New Issues

* Maintainers can select any issues they would like to work on and should add should add them to the `In Progress` column of the [roadmap board](https://github.com/opensearch-project/opensearch-benchmark/projects/1). Any issue added to the `In Progress` column should be properly labeled. For more information on how to properly label issues and PRs, see the [labels](#labels) section of this document.

Maintainers should create new issues as needed.

### Triage Issues
* Maintainers should meet biweekly to triage issues. This involves assessing new, current, and old issues and prioritizing them based on the [roadmap board](https://github.com/opensearch-project/opensearch-benchmark/projects/1).

### Review Pull Requests

* Maintainers should regularly review the backlog of pull requests. Pull requests only require one maintainer to approve. The maintainer reviewing the PRs should be a subject matter expert (understand the context and purpose of the PR) and drive best practices in clean code and architecture.
* Only use GitHub's squash-and-merge to merge into `main`.  Else, the backport workflow can fail -- it does not handle multiple commits as with rebases.  Merges lead to an undesired merge commit.
* Use the `backport 3` label for every PR, since main should be the image of `3`, at least until OpenSearch version 3 is officially released.  Add other branch labels appropriately; almost certainly `backport 2` will also be needed, since this is the current OpenSearch version.
* Close out the backport PRs in chronological order. If you close out a PR on `main`, please close out the associated branch PRs as well. They get generated in a few minutes by the automated backport workflow. Once they are merged in, the backport branches can be deleted as well.

### Drive Releases
* Maintainers drive releases. A week prior to the scheduled release, maintainers should announce a code freeze in the [#performance channel](https://opensearch.slack.com/archives/C0516H8EJ7R) within the OpenSearch Slack community. For more information on the release process, see the [release guide](<https://github.com/opensearch-project/OpenSearch-Benchmark/blob/main/RELEASE_GUIDE.md>)


## Labels

Issues, pull requests and releases may be tagged with labels to categorize them. Here are some suggestions on how to use labels.

Priorities are set by Maintainers of the repository and should be assigned to a selected subset of issues and not all issues.

* **Low Priority** - Implementations and PRs should be reviewed and completed within a sprint
* **Medium Priority** - Implementations and PRs should be reviewed and completed within a week
* **High Priority** - Implementations and PRs should be reviewed and completed within a few days and up to a week


**Release Labels:**  Releases are tagged with labels in the scheme `vN.N.N`, for instance `v1.0.0`, `v1.1.0` and `v2.0.0`, as well as `patch` and `backport`. Use release labels to target an issue or a PR for a given release. See [MAINTAINERS](MAINTAINERS.md#triage-open-issues) for more information on triaging issues.

The release process is standard across repositories in this open-source project and is run by a release manager volunteering from amongst the [maintainers](MAINTAINERS.md).

**Request For Comments (RFC)** - This should only be applied to RFCs. These are automatically applied to the RFCs when they are published.

**META** - This will serve as a tracker for a set of tasks or sub-issues for a larger project.



