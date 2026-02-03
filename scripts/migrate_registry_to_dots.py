import os
import shutil
import re
from pathlib import Path

REGISTRY_ROOT = Path("/workspace/lorchestra/.canonizer/registry/transforms")
JOBS_DIR = Path("/workspace/lorchestra/lorchestra/jobs/definitions/canonize")

def migrate_registry():
    print(f"Migrating registry at {REGISTRY_ROOT}")
    # Rename directories X-Y-Z -> X.Y.Z
    # os.walk is generating lists, so modifying them in place is safe if we don't recurse into renamed dirs immediately in a confusing way
    # But renaming might confuse os.walk if we valid renaming the current dir? No, os.walk walks top down.

    # Is safer to collect all moves first
    moves = []
    for root, dirs, files in os.walk(REGISTRY_ROOT):
        for d in dirs:
            if re.match(r"^\d+-\d+-\d+$", d):
                old_path = Path(root) / d
                new_name = d.replace("-", ".")
                new_path = Path(root) / new_name
                moves.append((old_path, new_path))

    for old_path, new_path in moves:
         print(f"Renaming {old_path} -> {new_path}")
         if new_path.exists():
             print(f"Skipping {old_path.name}, target {new_path.name} exists")
         else:
             shutil.move(str(old_path), str(new_path))

def migrate_jobs():
    print(f"Migrating jobs at {JOBS_DIR}")
    for yaml_file in JOBS_DIR.glob("*.yaml"):
        content = yaml_file.read_text()
        # Replace @X-Y-Z with @X.Y.Z
        new_content = re.sub(r"@(\d+)-(\d+)-(\d+)", r"@\1.\2.\3", content)
        if content != new_content:
            print(f"Updating {yaml_file}")
            yaml_file.write_text(new_content)

if __name__ == "__main__":
    migrate_registry()
    migrate_jobs()
