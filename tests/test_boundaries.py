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
    """Verify event_client is ONLY imported in lorc/jobs/."""
    lorc_root = Path('/workspace/lorchestra/lorchestra')

    # Find all Python files
    python_files = list(lorc_root.rglob('*.py'))

    # Exclude test files and __pycache__
    python_files = [
        f for f in python_files
        if '__pycache__' not in str(f)
        and 'test_' not in f.name
        and f.name != 'event_client.py'  # Exclude event_client itself
    ]

    violations = []
    valid_imports = []

    for file in python_files:
        with open(file) as f:
            content = f.read()

        # Check for event_client imports
        if 'from lorchestra.stack_clients.event_client import' in content:
            rel_path = file.relative_to(lorc_root.parent)

            # Only lorc/jobs/*.py should import event_client
            if 'lorchestra/jobs/' in str(rel_path):
                valid_imports.append(str(rel_path))
            else:
                violations.append(str(rel_path))

    if violations:
        print(f"❌ event_client imported outside jobs/:")
        for v in violations:
            print(f"  - {v}")
        assert False, "event_client should ONLY be imported in lorc/jobs/"

    print("✓ Boundary check passed: event_client only imported in jobs/")
    print(f"  Valid imports in: {valid_imports}")
    return True


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
