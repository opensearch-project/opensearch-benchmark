# Maintainers Guide

### Responsibilities

#### Work on Current Issues and Create New Issues

* Maintainers can choose any issues to work on and should make new issues when needed. Maintainers should add issues that they are working on to the the `In Progress` column of this [roadmap board](https://github.com/opensearch-project/opensearch-benchmark/projects/1). Any issue added to the `In Progress` column should be properly labeled. For more information on how to properly label issues and PRs, see the [labels](#labels) section of this document.

#### Triage Issues
* Maintainers should meet biweekly to triage issues. This involves assessing new, current, and old issues and prioritize them based on this [roadmap board](https://github.com/opensearch-project/opensearch-benchmark/projects/1).

#### Review Pull Requests

* Maintainers should review pull requests. Pull requests only require one maintainer to approve. The maintainer reviewing the PRs should be a subject matter expert (understand the context and purpose of the PR) and drive best practices in clean code and architecture.

#### Drive Releases
* Maintainers drive releases. A week prior to the scheduled release, maintainers should announce a code freeze in the [#performance channel](https://opensearch.slack.com/archives/C0516H8EJ7R) within the OpenSearch Slack community. For more information on releases, see the [release guide](<https://github.com/opensearch-project/OpenSearch-Benchmark/blob/main/RELEASE_GUIDE.md>)


### Labels

Here are a few suggestions on how to use labels.

Priorities are set by Maintainers of the repository and should be put on specific issues and not all issues.

* **Low Priority** - Implementations and PRs should be reviewed and completed within a sprint
* **Medium Priority** - Implementations and PRs should be reviewed and completed within a week
* **High Priority** - Implementations and PRs should be reviewed and completed within a few days and up to a week


**Release Labels (vN.N.N)** - Repositories create consistent release labels, such as `v1.0.0`, `v1.1.0` and `v2.0.0`, as well as `patch` and `backport`. Use release labels to target an issue or a PR for a given release. See [MAINTAINERS](MAINTAINERS.md#triage-open-issues) for more information on triaging issues.

The release process is standard across repositories in this org and is run by a release manager volunteering from amongst [maintainers](MAINTAINERS.md).

**Request For Comments (RFC)** - This should only be applied to RFCs. These are automatically applied to the RFCs when they are published.

**META** - This will serve as a tracker for a set of tasks or sub-issues for a larger project.



