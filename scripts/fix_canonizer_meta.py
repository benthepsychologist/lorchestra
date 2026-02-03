import os
import re
import hashlib
from pathlib import Path

REGISTRY_ROOT = Path("/workspace/lorchestra/.canonizer/registry/transforms")
TRANSFORMS_ROOT = Path("/workspace/lorchestra/.canonizer/transforms")

def compute_sha256(file_path):
    """Compute SHA256 hash of a file."""
    with open(file_path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def fix_meta_file(meta_path):
    """Fix version format and checksum in a spec.meta.yaml file."""
    try:
        with open(meta_path, "r") as f:
            content = f.read()

        original_content = content

        # 1. Fix version format: X-Y-Z -> X.Y.Z
        # We target "version: D-D-D" pattern specifically
        content = re.sub(r"version: (\d+)-(\d+)-(\d+)", r"version: \1.\2.\3", content)

        # 2. Handle Checksum
        # Find the spec file path
        spec_dir = meta_path.parent
        spec_file = None
        for ext in [".jsonata", ".js"]:
            candidate = spec_dir / f"spec{ext}"
            if candidate.exists():
                spec_file = candidate
                break

        if spec_file:
            checksum = compute_sha256(spec_file)

            # If checksum block is missing entirely
            if "checksum:" not in content:
                # Add checksum section before provenance if it exists, otherwise at end
                checksum_block = f'checksum:\n  jsonata_sha256: "{checksum}"\n'
                if "provenance:" in content:
                    content = content.replace("provenance:", f"{checksum_block}\nprovenance:")
                else:
                    content = content + "\n" + checksum_block

            # If checksum exists but has empty string or placeholders
            else:
                # Fix empty string
                content = re.sub(
                    r'jsonata_sha256:\s*""',
                    f'jsonata_sha256: "{checksum}"',
                    content
                )
                content = re.sub(
                    r"jsonata_sha256:\s*''",
                    f'jsonata_sha256: "{checksum}"',
                    content
                )

        if content != original_content:
            print(f"Fixing: {meta_path}")
            with open(meta_path, "w") as f:
                f.write(content)
            return True

    except Exception as e:
        print(f"Error processing {meta_path}: {e}")

    return False

# Scan both locations
count = 0
for root in [REGISTRY_ROOT, TRANSFORMS_ROOT]:
    if not root.exists():
        continue
    print(f"Scanning {root}...")
    for meta_path in root.glob("**/spec.meta.yaml"):
        if fix_meta_file(meta_path):
            count += 1

print(f"Done! Fixed {count} files.")
