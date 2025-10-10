# Contributing to Forward Netbox

Thank you for considering contributing to the Forward Netbox Plugin! 🎉
We welcome contributions from the community and appreciate your help to make
this project better.

---

## Table of Contents

1. [How to Contribute](#how-to-contribute)
2. [Reporting Issues](#reporting-issues)
3. [Suggesting Features](#suggesting-features)
4. [Submitting Code Changes](#submitting-code-changes)
5. [Code Style and Standards](#code-style-and-standards)
6. [Testing Your Changes](#testing-your-changes)
7. [Pull Request Guidelines](#pull-request-guidelines)
8. [Publishing a Release](#publishing-a-release)

---

## How to Contribute

There are multiple ways to contribute:

- Reporting bugs.
- Suggesting features and improvements.
- Submitting pull requests (PRs) with code or documentation changes.
- Improving the project documentation.

---

## Reporting Issues

If you find a bug, inconsistency, or have a question, please file an issue.

1. Go to the [Forward Networks Support Portal](https://forwardnetworks.com/support).
2. Search for existing issues to avoid duplication.
3. If no similar issue exists, create a new issue using the provided template.

> Forward uses an internal ticket tracking system which we also track issues
> and feature requests. If you're an Forward customer, you can also report
> issues through [support.forwardnetworks.com](https://support.forwardnetworks.com).

### Guidelines for Reporting Bugs:

- Provide clear steps to reproduce the problem.
- Include the version of the plugin and environment details.
- Share logs or screenshots, if applicable.

---

## Suggesting Features

We welcome ideas for enhancements! To suggest a feature:

1. Open a new request in the [Forward Networks Support Portal](https://forwardnetworks.com/support).
2. Clearly describe the feature or enhancement.
3. Explain the benefit and potential use cases for the feature.

---

## Submitting Code Changes

We use **Git** and the standard Git workflow for contributions:

1. Fork the repository on GitHub.
2. Clone your fork:
   ```bash
   git clone https://github.com/forward-networks/forward-netbox.git
   cd forward-netbox
   ```
3. Create a new branch:
   ```bash
   git checkout -b my-new-feature
   ```
4. Prepare Python environment and install dev dependencies.
   ```bash
   pip install poetry
   ```
5. [optional] Clone netbox and link it to you env for IDE to register it. Taken
   from [offical docs](https://netboxlabs.com/docs/netbox/en/stable/plugins/development/).
   ```bash
   echo $PATH_TO_NETBOX_REPO/netbox > $VENV/lib/python3.10/site-packages/netbox.pth
   ```
6. Install the project dependencies:
   ```bash
   poetry install --with dev
   pre-commit install
   ```
7. Start the development containers, create admin user:
    ```bash
    invoke build
    invoke start
    invoke createsuperuser
    Navigate to http://localhost:8000
    ```
8. Make your changes.
9. Run the tests:
    ```bash
    invoke test
    ```
10. Add release notes to documentation and update docs if needed.
11. Check docs are correct by navigating to `http://localhost:8001/` after running the invoke commands above or directly `invoke serve-docs`.
12. Commit your changes:
    ```bash
    git commit -m 'Add new feature'
    ```
13. Push your changes to your fork:
    ```bash
    git push origin my-new-feature
    ```

---

## Publishing a Release

To publish a new release:

1. Update the version number in the `pyproject.toml` file.
2. Ensure there are changelogs for the new version in the documentation.
   `docs/administration/Release_Notes/*`.
3. Merge changed into the `develop` branch.
4. Merge the `develop` branch into the `main` branch.
5. Build the distribution package:
    ```bash
    poetry build
    ```
6. Publish the package to PyPI:
    ```bash
    poetry publish
    ```
7. Create a new release on GitHub and add the release notes and PyPI links.
