# Design: Rename impectPy → impectPyRSCA

## Context

This repo is a fork of `impectPy` (by ImpectAPI) maintained by RSCA Intelligence. The goal is to rebrand the package so it can be installed and imported as `impectPyRSCA`, clearly distinct from the original, while preserving all existing functionality.

## Decisions

- **Import name**: `impectPyRSCA` (e.g. `import impectPyRSCA as ip`)
- **Versioning**: Continue from upstream at 2.5.7
- **Metadata**: Updated to reflect RSCA ownership
- **Approach**: Full rename (directory + imports), no aliasing tricks

## Changes

### 1. Directory rename
`impectPy/` → `impectPyRSCA/`. All source files stay the same.

### 2. Internal imports
Every `from impectPy.xxx import ...` becomes `from impectPyRSCA.xxx import ...` across all modules (~15 files).

### 3. setup.py
- `name` → `"impectPyRSCA"`
- `packages` → `["impectPyRSCA"]`
- `url` → `"https://github.com/rsca-intelligence/impectPyRSCA"`
- `author` → `"RSCA Intelligence"`
- `author_email` → removed
- `description` → `"RSCA fork of impectPy — a Python package to facilitate interaction with the Impect customer API"`
- `version` stays `"2.5.7"`

### 4. __init__.py
Keep `__version__ = "2.5.7"` and all existing exports. Update internal imports.

### 5. README.md
Update installation instructions, import examples, and add a note that this is an RSCA fork.

### 6. Unchanged
- All business logic, API endpoints, data processing
- LICENSE.md (MIT allows forking)
- NEWS.md (historical changelog)
- .github/ISSUE_TEMPLATE/
- examples/ notebook
- Upstream git remote
