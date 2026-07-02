"""PyInstaller entry stub for the desktop app (see desktop/build.spec).

A plain module script (not -m) so PyInstaller resolves the package imports from
the repo root; all real logic lives in desktop/app.py.
"""

from desktop.app import main

if __name__ == "__main__":
    raise SystemExit(main())
