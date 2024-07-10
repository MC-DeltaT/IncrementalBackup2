# How to contribute to IncrementalBackup2

## Bug reports and feature requests

Feedback is greatly appreciated!

Bugs should be reported via GitHub issues. Include the `bug` label.

Feature requests should also be done via GitHub issues. Include the `enhancement` label.

Currently this tool is a hobby project, so keep in mind that I may not implement a request if I don't have enough time or don't consider it important enough.

## Contributing code

You are welcome to submit pull requests. I would recommend creating a GitHub issue first so we can discuss, as I will probably be picky on what changes I allow into the project.

Aspects of code changes which I regard as very important:

- Feature focus. Features should be simple and serve a real need in the tool which cannot be solved elsewhere.
- API excellence. Interfaces are important, and leaky abstractions are a no-go.
- Backwards compatibility. API or CLI breaking changes incur a major version release.
- Functional code style - as functional as plausible. Spaghetti state code should be avoided at all costs.

If you do wish to contribute code, please see the section below about setting up the codebase for development.

## Development setup

### 1 - Clone the repository

Self-explanatory :)

All following steps should be done in the repository directory.

### 2 - Create Python virtual environment

It's recommended to create a Python virtual environment to encapsulate project dependencies.
Use the Python version specified in [`pyproject.toml`](./pyproject.toml).

Linux (bash):

```bash
python -m venv .venv
source .venv/bin/activate
```

Windows (PowerShell):

```powershell
py -m venv .venv
& .venv/Scripts/Activate.ps1
```

### 3 - Install dependencies

Currently there are no runtime dependencies, only development/build dependencies.

```
python -m pip install -U pip
pip install -r requirements-dev.txt
```

### 4 - Enable pre-commit code formatting

`pre-commit` enables automatic code formatting and linting when committing. That way, you don't have to worry about code style when developing.

```
pre-commit install
```

### 5 - Run tests

To check everything is set up correctly, run the test suite. See the section _Running tests_ below.

## Application structure

Important files and directories:

- `incremental_backup/` - The Python package.
  - `backup/` - Backup creation functionality.
  - `cli/` - Command line interface implementation.
  - `meta/` - Functionality related to backup metadata and structure.
  - `__main__.py` - Entrypoint for using the command line interface via the package name.
  - `prune.py` - Functionality for the backup prune command.
  - `restore.py` - Backup restoration implementation.
- `test/` - Test code. Each directory/file corresponds to the module in `incremental_backup/` it tests.

## Running tests

Unit tests use pytest. There are also linting checks with ruff. Running of both is handled by tox.

To run linting and unit tests with tox:

```
tox
```

By default, unit tests run locally with tox use Python 3.9 only, for performance reasons. On GitHub Actions, unit tests run on more Python versions to validate compatibility.
