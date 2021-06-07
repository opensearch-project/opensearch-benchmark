### Importing the project into IntelliJ IDEA

Rally builds using virtualenv. When importing into IntelliJ you will need to define an appropriate Python SDK, which is provided by virtualenv.
For more details on defining a Python SDK in IntelliJ please refer to their documentation. We recommend using the Python SDK that `make prereq` creates.
This is typically created via `Virtualenv Environment` / `Existing Environment` and pointing to `.venv/bin/python3` within the Rally source directory.

In order to run tests within the IDE, ensure the `Python Integrated Tools` / `Testing` / `Default Test Runner` is set to `pytest`.

### Submitting your changes for a pull request

Once your changes and tests are ready to submit for review:

1. Test your changes

    Run the test suite to make sure that nothing is broken: `python3 setup.py test`.

2. Sign the Developer Certificate of Origin

    Please make sure you have signed DCO certificate.

3. Rebase your changes

    Update your local repository with the most recent code from the main Rally repository, and rebase your branch on top of the latest master branch. We prefer your initial changes to be squashed into a single commit. Later, if we ask you to make changes, add them as separate commits.  This makes them easier to review.  As a final step before merging we will either ask you to squash all commits yourself or we'll do it for you.

4. Submit a pull request

    Push your local changes to your forked copy of the repository and [submit a pull request](https://help.github.com/articles/using-pull-requests). In the pull request, choose a title which sums up the changes that you have made, and in the body provide more details about what your changes do. Also mention the number of the issue where discussion has taken place, eg "Closes #123".