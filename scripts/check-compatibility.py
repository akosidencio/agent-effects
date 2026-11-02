#!/usr/bin/env python3
"""Build the same v0.2.0 consumer on the tag and current tree; replay its DB."""
import io
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = ROOT / 'target' / 'compatibility'
SCRATCH.mkdir(parents=True, exist_ok=True)
# Export trusted tracked source from the local release tag; never change main.
with tempfile.TemporaryDirectory(prefix='check-', dir=SCRATCH) as tmp:
    run = Path(tmp)
    baseline = run / 'v020'
    baseline.mkdir()
    archive = subprocess.check_output(['git', 'archive', 'v0.2.0'], cwd=ROOT)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(baseline, filter='data')
    database = run / 'effects.db'
    for mode, source in [('write', baseline), ('read', ROOT)]:
        app = run / mode
        (app / 'src').mkdir(parents=True)
        (app / 'src' / 'main.rs').write_text((ROOT / 'scripts/compatibility/main.rs').read_text())
        (app / 'src' / 'legacy_memory.rs').write_text(
            (baseline / 'crates/agent-effects-memory/src/lib.rs').read_text())
        deps = '\n'.join(f'{name} = {{ path = "{source}/crates/{name}", version = "0.2.0" }}'
                         for name in ['agent-effects', 'agent-effects-store', 'agent-effects-sqlite'])
        (app / 'Cargo.toml').write_text('''[package]
name = "agent-effects-v020-consumer"
version = "0.0.0"
edition = "2024"
rust-version = "1.94"
[workspace]
resolver = "3"
[dependencies]
''' + deps + '''
serde_json = "1.0.151"
tokio = { version = "1.53.3", features = ["rt-multi-thread", "macros"] }
''')
        # Preserve the dependency choices of the reviewed workspace, then resolve
        # the small fixture's own package entry without contacting a registry.
        (app / 'Cargo.lock').write_text((ROOT / 'Cargo.lock').read_text())
        env = os.environ.copy()
        env['CARGO_TARGET_DIR'] = str(SCRATCH / ('baseline-build' if mode == 'write' else 'candidate-build'))
        subprocess.run(['cargo', '+1.94', 'generate-lockfile', '--offline'], cwd=app, env=env, check=True)
        subprocess.run(['cargo', '+1.94', 'run', '--offline', '--locked', '--', mode, str(database)],
                       cwd=app, env=env, check=True)
print('v0.2.0 consumer and SQLite upgrade: passed')
