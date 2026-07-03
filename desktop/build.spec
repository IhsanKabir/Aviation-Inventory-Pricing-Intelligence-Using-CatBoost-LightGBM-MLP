# PyInstaller spec — OTA Discount Report desktop app.
#
# ONE-FILE build: users download a single OTADiscountReport.exe and double-click
# — no zip, no unzip, no install (field feedback 2026-07-03). Slightly slower
# first launch (self-extracts to %TEMP%), acceptable for a daily-use tool.
#
# pywebview's Windows backend runs on .NET via pythonnet + clr_loader; those
# ship runtime DLLs/configs that PyInstaller's static analysis MISSES — the
# v0.1.0/0.1.1 CI builds crashed on user machines with "Failed to resolve
# Python.Runtime.Loader.Initialize". collect_all() on each of those packages
# bundles everything. CI must ALSO pin the exact versions verified locally
# (see release-desktop.yml).
#
# Build (from repo root):  pyinstaller desktop/build.spec
# Output: dist/OTADiscountReport.exe (+ CI publishes a .sha256 sidecar).

import os

from PyInstaller.utils.hooks import collect_all

# PyInstaller resolves relative paths against the SPEC file's directory (desktop/),
# not the invocation cwd — anchor everything to the repo root explicitly.
ROOT = os.path.dirname(SPECPATH)  # noqa: F821  (SPECPATH is injected by PyInstaller)

block_cipher = None

datas = [
    (os.path.join(ROOT, "desktop", "ui.html"), "desktop"),
    (os.path.join(ROOT, "config", "discount_manual_overrides.json"), "config"),
]
binaries = []
hiddenimports = ["keyring.backends.Windows"]

# Bundle the FULL runtime of the GUI stack — hooks alone miss the .NET pieces.
for pkg in ("webview", "clr_loader", "pythonnet", "bottle", "proxy_tools"):
    pkg_datas, pkg_binaries, pkg_hidden = collect_all(pkg)
    datas += pkg_datas
    binaries += pkg_binaries
    hiddenimports += pkg_hidden

a = Analysis(
    [os.path.join(ROOT, "launcher.py")],
    pathex=[ROOT],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[
        "pandas", "numpy", "matplotlib", "scipy", "sklearn",
        "google", "google.cloud", "sqlalchemy", "psycopg2",
        "playwright", "PIL", "tkinter.test",
    ],
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="OTADiscountReport",
    debug=False,
    strip=False,
    upx=False,              # UPX-packed exes trip AV heuristics — keep unpacked
    console=False,
)
