# Contributing to Forward NetBox

Thanks for your interest in the Forward Networks NetBox plugin! This project is
maintained on a best-effort basis by Craig Johnson (Principal Solutions
Architect, Forward Networks) and is intentionally unsupported by Forward
Networks services. Contributions are welcome, and the simplest ways to get
involved are:

- Open a GitHub issue for bugs, enhancement ideas, or questions.
- Email [craigjohnson@forwardnetworks.com](mailto:craigjohnson@forwardnetworks.com)
  if you prefer a direct conversation.
- Submit a pull request when you have a fix or improvement ready to share.

---

## Reporting Issues

1. Search the [issue tracker](https://github.com/forwardnetworks/forward-netbox/issues)
   to see if your topic already exists.
2. If you do not find a match, open a new issue and include:
   - NetBox and plugin versions.
   - Reproduction steps or sample data.
   - Expected vs. actual behaviour, plus any relevant logs or screenshots.

---

## Suggesting Enhancements

Follow the same process as reporting an issue, but describe the desired outcome,
why it would help, and any implementation notes you have in mind. Feature
requests are triaged alongside bug reports.

---

## Development Workflow

1. Fork the repository and clone your fork:
   ```bash
   git clone https://github.com/<your-user>/forward-netbox.git
   cd forward-netbox
   ```
2. Create a feature branch:
   ```bash
   git checkout -b my-feature
   ```
3. Prepare a development environment:
   ```bash
   python -m venv venv && source venv/bin/activate
   pip install poetry
   poetry install --with dev
   pre-commit install
   ```
4. Use the provided invoke tasks to start the sample NetBox stack if needed:
   ```bash
   invoke build
   invoke start
   invoke createsuperuser
   ```
5. Implement your changes and update documentation as appropriate.

---

## Testing

Before opening a pull request, run the automated checks:

```bash
invoke test
```

You can also run `pytest` directly or launch `invoke serve-docs` to preview the
documentation locally.

---

## Pull Request Checklist

- [ ] Tests (or relevant invoke tasks) pass locally.
- [ ] Documentation and release notes reflect user-facing changes.
- [ ] Commits are scoped and descriptive.
- [ ] The pull request references the corresponding issue (if applicable).

Please keep pull requests focused. Separate unrelated fixes into distinct PRs to
help reviewers respond quickly.

---

## Release Management

If you are helping maintain the project:

1. Bump the version in `pyproject.toml` and `forward_netbox/__init__.py`.
2. Update release notes under `docs/02_Release_Notes/`.
3. Tag the release on GitHub once changes are merged.

That’s it—thanks for contributing!
