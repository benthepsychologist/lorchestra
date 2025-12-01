"""Test clean architecture boundaries are maintained."""

import ast
import inspect
from pathlib import Path


def test_ingestor_has_no_event_client():
    """Verify ingestor package has NO event_client imports."""
    # This is a CRITICAL boundary check
    import sys
    sys.path.insert(0, '/workspace/ingestor')

    import ingestor.extractors

    # Get source code and parse AST
    source = inspect.getsource(ingestor.extractors)
    tree = ast.parse(source)

    # Find all imports
    imports = [
        node for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]

    # Check for event_client
    import_strings = [ast.unparse(imp) for imp in imports]
    has_event_client = any('event_client' in imp for imp in import_strings)

    assert not has_event_client, \
        "CRITICAL VIOLATION: ingestor MUST NOT import event_client!"

    print("✓ Boundary check passed: ingestor has no event_client imports")
    return True


def test_ingestor_has_no_bigquery():
    """Verify ingestor package has NO BigQuery imports."""
    import sys
    sys.path.insert(0, '/workspace/ingestor')

    import ingestor.extractors

    # Get source code and parse AST
    source = inspect.getsource(ingestor.extractors)
    tree = ast.parse(source)

    # Find all imports
    imports = [
        node for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]

    # Check for BigQuery
    import_strings = [ast.unparse(imp) for imp in imports]
    has_bigquery = any('bigquery' in imp for imp in import_strings)

    assert not has_bigquery, \
        "CRITICAL VIOLATION: ingestor MUST NOT import BigQuery!"

    print("✓ Boundary check passed: ingestor has no BigQuery imports")
    return True


def test_event_client_only_in_lorc_jobs():
    """Verify event_client data functions are ONLY imported in lorc/jobs/.

    The boundary rule is:
    - log_event() and upsert_objects() should only be called from jobs/
    - cli.py may import set_run_mode() and ensure_test_tables_exist() (runtime config)
    - idem_keys.py has docstring examples but no actual imports
    """
    lorc_root = Path('/workspace/lorchestra/lorchestra')

    # Find all Python files
    python_files = list(lorc_root.rglob('*.py'))

    # Exclude test files, __pycache__, and allowed files
    allowed_files = {
        'event_client.py',  # The module itself
        'cli.py',           # CLI needs set_run_mode and ensure_test_tables_exist
    }

    python_files = [
        f for f in python_files
        if '__pycache__' not in str(f)
        and 'test_' not in f.name
        and f.name not in allowed_files
    ]

    violations = []
    valid_imports = []

    for file in python_files:
        with open(file) as f:
            content = f.read()

        # Skip files where the import is only in docstrings (triple-quoted strings)
        # Simple heuristic: check if it's an actual import statement (not indented in docstring)
        lines = content.split('\n')
        has_real_import = False
        in_docstring = False

        for line in lines:
            stripped = line.strip()
            # Track docstring state (simple heuristic)
            if '"""' in stripped or "'''" in stripped:
                # Toggle docstring state (works for single-line docstrings too)
                if stripped.count('"""') == 1 or stripped.count("'''") == 1:
                    in_docstring = not in_docstring

            if not in_docstring and 'from lorchestra.stack_clients.event_client import' in line:
                # Check for actual data function imports (not just set_run_mode etc)
                if 'log_event' in line or 'upsert_objects' in line:
                    has_real_import = True
                    break

        if has_real_import:
            rel_path = file.relative_to(lorc_root.parent)

            # Only lorc/jobs/*.py should import event_client data functions
            if 'lorchestra/jobs/' in str(rel_path):
                valid_imports.append(str(rel_path))
            else:
                violations.append(str(rel_path))

    if violations:
        print(f"❌ event_client data functions imported outside jobs/:")
        for v in violations:
            print(f"  - {v}")
        assert False, "event_client data functions (log_event, upsert_objects) should ONLY be imported in lorc/jobs/"

    print("✓ Boundary check passed: event_client data functions only imported in jobs/")
    print(f"  Valid imports in: {valid_imports}")


if __name__ == "__main__":
    print("\n" + "="*80)
    print("BOUNDARY VERIFICATION TESTS")
    print("="*80 + "\n")

    try:
        test_ingestor_has_no_event_client()
        test_ingestor_has_no_bigquery()
        test_event_client_only_in_lorc_jobs()

        print("\n" + "="*80)
        print("✓ ALL BOUNDARY CHECKS PASSED")
        print("="*80 + "\n")
    except AssertionError as e:
        print(f"\n❌ Boundary check failed: {e}\n")
        exit(1)
