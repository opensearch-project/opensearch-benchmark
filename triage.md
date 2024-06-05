## Table of Contents
- [Scope](#scope)
- [Schedule](#schedule)
- [What Is Triaging](#what-is-triaging)
- [How to Triage](#how-to-triage)

## Scope
These guidelines serve as a primary document for triaging issues in [opensearch-benchmark](https://github.com/opensearch-project/opensearch-benchmark) and [opensearch-benchmark-workloads](https://github.com/opensearch-project/opensearch-benchmark-workloads) project. For maintainers/contributors, it is a guide to ensure that we address all customer issues and address feature requests in a timely manner.

## Schedule
Every week a maintainer will be assigned on a rotational basis to conduct triage meeting.
The meeting duration will be no more than 1 hour every Tuesday @1.30 PM PDT/3.30 PM CDT

## What is Triaging
Triage is a process of grooming issues with correct labels, assigning them owners if available, ensuring that the issues have all the required information so that they are actionable

## How to Triage
The steps listed below are not exhaustive and can be updated. Start with [opensearch-benchmark](https://github.com/opensearch-project/opensearch-benchmark) followed by [opensearch-benchmark-workloads](https://github.com/opensearch-project/opensearch-benchmark-workloads) project. For each of these projects we can follow the below steps to triage Issues from different categories. Try to cover at least 1 Issue from each of the below listed category.

### Steps to triage [opensearch-benchmark](https://github.com/opensearch-project/opensearch-benchmark)
- [Untriaged](https://github.com/opensearch-project/opensearch-benchmark/issues?q=is%3Aopen+is%3Aissue+no%3Alabel)
    - First check if any of the untriaged issue is a duplicate. If yes, then comment the duplicate Issue link and close the duplicate
    - Assign appropriate labels for these Issues such as bug, enhancements, breaking etc.
    - If possible assign owners who could be [SME](https://en.wikipedia.org/wiki/Subject-matter_expert) for the Issue or volunteers if any

- [High Priority](https://github.com/opensearch-project/opensearch-benchmark/issues?q=is%3Aopen+is%3Aissue+label%3A%22High+Priority%22)
    - Search for Issues labeled as High Priority.
    - Ensure they have a status update or an owner

- [Breaking](https://github.com/opensearch-project/opensearch-benchmark/issues?q=is%3Aopen+is%3Aissue+label%3Abreaking)
    - Search for Issues which are labeled as breaking
    - Try to assign owners if it is a high priority beaking change

- [Bugs](https://github.com/opensearch-project/opensearch-benchmark/issues?q=is%3Aissue+is%3Aopen+label%3Abug)
    - Search for Issues which are labeled as bugs
    - Comment with a status update such as In Progress, Not Planned, Needs more information

- [Enhancements](https://github.com/opensearch-project/opensearch-benchmark/issues?q=is%3Aissue+is%3Aopen+sort%3Acreated-desc+label%3Aenhancement+)
    - Search for Issues which are feature requests/enhancements
    - Comment with a status update and assign owners if it is going to be the next release candidate

- [Documentation](https://github.com/opensearch-project/opensearch-benchmark/issues?q=is%3Aopen+is%3Aissue+label%3Adocumentation)
    - These Issues although not breaking are important for better customer experience
    - Feel free to encourage the requester to raise a PR, these can be tagged as good first Issue as well

### Steps to triage [opensearch-benchmark-workloads](https://github.com/opensearch-project/opensearch-benchmark-workloads)
- [Untriaged](https://github.com/opensearch-project/opensearch-benchmark-workloads/issues?q=is%3Aopen+is%3Aissue+no%3Alabel)
    - First check if any of the untriaged issue is a duplicate. If yes, then comment the duplicate Issue link and close the duplicate
    - Assign appropriate labels for these Issues such as bug, enhancements, breaking etc.
    - If possible assign owners who could be [SME](https://en.wikipedia.org/wiki/Subject-matter_expert) for the Issue or volunteers if any

- [Breaking](https://github.com/opensearch-project/opensearch-benchmark-workloads/issues?q=is%3Aopen+is%3Aissue+label%3Abreaking)
    - Search for Issues which are labeled as breaking
    - Try to assign owners if it is a high priority beaking change

- [Bugs](https://github.com/opensearch-project/opensearch-benchmark-workloads/issues?q=is%3Aopen+is%3Aissue+label%3Abug)
    - Search for Issues which are labeled as bugs
    - Comment with a status update such as In Progress, Not Planned, Needs more information

- [Enhancement](https://github.com/opensearch-project/opensearch-benchmark-workloads/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement)
    - Search for Issues which are feature requests/enhancements
    - Comment with a status update and assign owners if it is going to be the next release candidate

- [Documentation](https://github.com/opensearch-project/opensearch-benchmark-workloads/issues?q=is%3Aopen+is%3Aissue+label%3Adocumentation)
    - These Issues although not breaking are important for better customer experience
    - Feel free to encourage the requester to raise a PR, these can be tagged as good first Issue as well

*There could be an overlap of Issues in more than 1 category such as Breaking and Bugs or Breaking and High Priority. In this case try to triage a different Issue which is not already covered*
