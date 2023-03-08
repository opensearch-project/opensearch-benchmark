The release process is standard across repositories in this org and is run by a release manager volunteering from amongst [maintainers](MAINTAINERS.md).

1. Create a tag, e.g. v2.1.0, and push it to the GitHub repo.
1. The [release-drafter.yml](.github/workflows/release-drafter.yml) will be automatically kicked off and a draft release will be created.
1. This draft release triggers the [jenkins release workflow](ADD CORRESPONDING LINK HERE) as a result of which opensearch-py client is released on [PyPi](https://pypi.org/project/opensearch-benchmark/).
1. Once the above release workflow is successful, the drafted release on GitHub is published automatically.
1. Increment "version" in [_version.py](./opensearchpy/_version.py) to the next patch release, e.g. v2.1.1. See [example](https://github.com/opensearch-project/opensearch-py/pull/167).