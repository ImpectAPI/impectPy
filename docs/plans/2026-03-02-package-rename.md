# impectPy → impectPyRSCA Rename Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rename the forked package from `impectPy` to `impectPyRSCA` so it can be installed and imported distinctly from the original.

**Architecture:** Rename the package directory, update all internal imports via find-and-replace, update packaging metadata in setup.py, and update documentation. No logic changes.

**Tech Stack:** Python, setuptools

---

### Task 1: Rename package directory

**Files:**
- Rename: `impectPy/` → `impectPyRSCA/`

**Step 1: Rename the directory**

Run: `git mv impectPy impectPyRSCA`

**Step 2: Verify directory exists**

Run: `ls impectPyRSCA/`
Expected: All .py files listed (access_token.py, config.py, events.py, etc.)

**Step 3: Commit**

```bash
git add -A && git commit -m "rename impectPy/ directory to impectPyRSCA/"
```

---

### Task 2: Update all internal imports

**Files:**
- Modify: `impectPyRSCA/access_token.py:4`
- Modify: `impectPyRSCA/events.py:7`
- Modify: `impectPyRSCA/formations.py:5`
- Modify: `impectPyRSCA/impect.py:1`
- Modify: `impectPyRSCA/iterations.py:5`
- Modify: `impectPyRSCA/matches.py:4`
- Modify: `impectPyRSCA/player_iteration_averages.py:3`
- Modify: `impectPyRSCA/player_iteration_scores.py:5`
- Modify: `impectPyRSCA/player_match_scores.py:5`
- Modify: `impectPyRSCA/player_matchsums.py:5`
- Modify: `impectPyRSCA/player_profile_scores.py:5`
- Modify: `impectPyRSCA/set_pieces.py:5`
- Modify: `impectPyRSCA/squad_coefficients.py:4`
- Modify: `impectPyRSCA/squad_iteration_averages.py:4`
- Modify: `impectPyRSCA/squad_iteration_scores.py:5`
- Modify: `impectPyRSCA/squad_match_scores.py:5`
- Modify: `impectPyRSCA/squad_matchsums.py:5`
- Modify: `impectPyRSCA/squad_ratings.py:4`
- Modify: `impectPyRSCA/starting_positions.py:5`
- Modify: `impectPyRSCA/substitutions.py:5`

**Step 1: Replace all `from impectPy.` with `from impectPyRSCA.`**

In every file listed above, replace:
```python
from impectPy.helpers import ...
```
with:
```python
from impectPyRSCA.helpers import ...
```

And in `impect.py`, replace:
```python
from impectPy.config import Config
```
with:
```python
from impectPyRSCA.config import Config
```

**Step 2: Verify no remaining references to `from impectPy.`**

Run: `grep -r "from impectPy\." impectPyRSCA/`
Expected: No output (zero matches)

Run: `grep -r "import impectPy" impectPyRSCA/`
Expected: No output (zero matches)

**Step 3: Commit**

```bash
git add impectPyRSCA/ && git commit -m "update all internal imports from impectPy to impectPyRSCA"
```

---

### Task 3: Update setup.py

**Files:**
- Modify: `setup.py`

**Step 1: Update setup.py metadata**

Replace the full `setup()` call with:

```python
setup(
    name="impectPyRSCA",
    url="https://github.com/rsca-intelligence/impectPyRSCA",
    author="RSCA Intelligence",
    packages=["impectPyRSCA"],
    install_requires=["requests>=2.24.0",
                      "pandas>=2.2.0",
                      "numpy>=1.24.2"],
    version="2.5.7",
    license="MIT",
    description="RSCA fork of impectPy — a Python package to facilitate interaction with the Impect customer API",
    long_description=README,
    long_description_content_type="text/markdown",
)
```

**Step 2: Commit**

```bash
git add setup.py && git commit -m "update setup.py metadata for impectPyRSCA"
```

---

### Task 4: Update README.md

**Files:**
- Modify: `README.md`

**Step 1: Update README content**

Replace the header and intro to identify this as the RSCA fork. Update all `import impectPy` references to `import impectPyRSCA`. Update all `from impectPy import` references to `from impectPyRSCA import`. Update installation instructions to point to the RSCA GitHub repo.

Key changes:
- Title: `# impectPyRSCA` (remove the logo picture element referencing ImpectAPI logos)
- Add: `RSCA Intelligence fork of [impectPy](https://github.com/ImpectAPI/impectPy)`
- Installation: `pip install git+https://github.com/rsca-intelligence/impectPyRSCA.git`
- All code examples: `import impectPyRSCA as ip` instead of `import impectPy as ip`
- OOP example: `from impectPyRSCA import Impect` instead of `from impectPy import Impect`
- Remove PyPI installation option (this fork is not on PyPI)

**Step 2: Commit**

```bash
git add README.md && git commit -m "update README for impectPyRSCA fork"
```

---

### Task 5: Verify the package installs and imports correctly

**Step 1: Install in editable mode**

Run: `pip install -e .`
Expected: Successfully installed impectPyRSCA

**Step 2: Verify import works**

Run: `python -c "import impectPyRSCA as ip; print(ip.__version__)"`
Expected: `2.5.7`

**Step 3: Verify class import works**

Run: `python -c "from impectPyRSCA import Impect; print(Impect)"`
Expected: `<class 'impectPyRSCA.impect.Impect'>`

**Step 4: Verify all exports are available**

Run: `python -c "import impectPyRSCA as ip; print([x for x in dir(ip) if not x.startswith('_')])"`
Expected: List including getAccessToken, getEvents, getIterations, getMatches, Impect, Config, etc.
