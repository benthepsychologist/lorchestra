import shutil
import os
from pathlib import Path

bad_paths = [
    "/workspace/lorchestra/.canonizer/transforms/forms/google_forms_to_canonical/1.0.0/spec.meta.yaml",
    "/workspace/lorchestra/.canonizer/transforms/email/exchange_to_jmap_lite/1.0.0/spec.meta.yaml",
    "/workspace/lorchestra/.canonizer/transforms/email/gmail_to_jmap_lite/1.0.0/spec.meta.yaml"
]

for p_str in bad_paths:
    p = Path(p_str)
    if p.is_dir():
        print(f"Fixing directory: {p}")
        inner_file = p / "spec.meta.yaml"
        if inner_file.exists():
            # Read content to save it
            content = inner_file.read_text()

            # Remove the directory tree
            shutil.rmtree(p)

            # Write the file back at the path
            with open(p, "w") as f:
                f.write(content)
            print(f"  Replaced directory with file at {p}")
        else:
            print(f"  ERROR: Inner file missing in {p}")
