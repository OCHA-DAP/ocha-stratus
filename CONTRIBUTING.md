# Development

## Environment

Development is currently done using Python 3.12. We recommend using a virtual
environment such as ``venv``:

    python3.12 -m venv venv
    source venv/bin/activate

In your virtual environment, please install all packages for
development by running:

    pip install -r requirements.txt
    pip install -e ".[dev]"

## Pre-Commit

Also be sure to install `pre-commit`, which is run every time
you make a git commit:

    pre-commit install

With pre-commit, all code is formatted according to
[ruff](https://github.com/astral-sh/ruff) guidelines.

To check if your changes pass pre-commit without committing, run:

    pre-commit run --all-files

## Dependencies

[pip-tools](https://github.com/jazzband/pip-tools) is used for
package management.  If you’ve introduced a new package to the
source code (i.e.anywhere in `src/`), please add it to the
`project.dependencies` section of
`pyproject.toml` with any known version constraints.

For adding packages for testing or development, add them to
the `test` or `dev` sections under `[project.optional-dependencies]`.

Any changes to the dependencies will be automatically reflected in
`requirements.txt` with `pre-commit`, but you can re-generate
the file without committing by executing:

    pre-commit run pip-compile --all-files

## Documentation

Documentation
Documentation is built using Sphinx with MyST for Markdown support. Build the docs locally:

```bash
cd docs
sphinx-build -b html . _build/html
```

View the built documentation by opening `docs/_build/html/index.html` in your browser.
