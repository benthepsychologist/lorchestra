import os
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock

# Mock config content
config_data = {
    "project": "test-project",
    "dataset_raw": "raw",
    "dataset_canonical": "canonical",
    "dataset_derived": "derived",
    "sqlite_path": "db.sqlite",
    "local_views_root": "views",
    "google_application_credentials": "/tmp/fake-creds.json"
}

# Create fake creds file
with open("/tmp/fake-creds.json", "w") as f:
    f.write("{}")

def test_config_sets_env_var():
    # Make sure env var is NOT set initially
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
        yaml.dump(config_data, tmp)
        config_path = Path(tmp.name)
        
    try:
        # Patch get_config_path to return our temp file
        with patch("lorchestra.config.get_config_path", return_value=config_path):
            from lorchestra.config import load_config
            
            # Load config
            config = load_config()
            
            # Check if env var was set
            env_val = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            print(f"GOOGLE_APPLICATION_CREDENTIALS is: {env_val}")
            
            assert env_val == "/tmp/fake-creds.json"
            print("SUCCESS: Environment variable set correctly from config.")
            
    finally:
        os.unlink(config_path)
        if os.path.exists("/tmp/fake-creds.json"):
            os.unlink("/tmp/fake-creds.json")

if __name__ == "__main__":
    test_config_sets_env_var()
