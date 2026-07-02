# PyInstaller spec — OTA Discount Report desktop app.
#
# ONE-FOLDER build (ship the folder as OTADiscountReport.zip): one-file builds
# self-extract to %TEMP% on every launch — slower start, more AV heuristics, and
# a pointless disk+RAM copy right before a multi-GB HAR parse.
#
# The engine needs ONLY stdlib + openpyxl + requests (+ psutil/keyring/pywebview
# for the shell) — pandas/numpy & friends are EXCLUDED to keep the bundle small
# (~30 MB vs ~400 MB) and the antivirus surface low.
#
# Build (from repo root):  pyinstaller desktop/build.spec
# CI then zips dist/OTADiscountReport -> OTADiscountReport.zip (+ .sha256) and
# publishes it as the GitHub release asset the app_release mirror serves.

from PyInstaller.utils.hooks import collect_data_files

block_cipher = None

a = Analysis(
    ["launcher.py"],
    pathex=["."],
    binaries=[],
    datas=[
        ("desktop/ui.html", "desktop"),
        ("config/discount_manual_overrides.json", "config"),
    ],
    hiddenimports=[
        "keyring.backends.Windows",
        "webview.platforms.edgechromium",
    ],
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
    [],
    exclude_binaries=True,
    name="OTADiscountReport",
    debug=False,
    strip=False,
    upx=False,              # UPX-packed exes trip AV heuristics — keep unpacked
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    name="OTADiscountReport",
)
