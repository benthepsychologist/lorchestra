#!/usr/bin/env python3
"""Refresh Gmail OAuth using deprecated OOB flow."""

import sys
from google_auth_oauthlib.flow import InstalledAppFlow
from authctl.secrets import get_credentials, set_credentials

def main():
    if len(sys.argv) < 2:
        print("Usage: python refresh_gmail_oob.py <account> [--use-native-client <source_account>]")
        print("Example: python refresh_gmail_oob.py acct3")
        print("Example: python refresh_gmail_oob.py acct3 --use-native-client acct1")
        sys.exit(1)

    account = sys.argv[1]
    scopes = ["https://mail.google.com/"]

    # Check if we should use a different account's native client credentials
    source_account = account
    if len(sys.argv) >= 4 and sys.argv[2] == "--use-native-client":
        source_account = sys.argv[3]
        print(f"Using native client credentials from gmail:{source_account}")

    # Load credentials (possibly from a different account for the client_id/secret)
    existing = get_credentials("gmail", source_account)
    client_id = existing.get("client_id")
    client_secret = existing.get("client_secret")

    print(f"Refreshing Gmail OAuth for account: {account}")

    # Build the OAuth flow with OOB redirect
    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
        }
    }

    flow = InstalledAppFlow.from_client_config(client_config, scopes)

    # Set the OOB redirect URI
    flow.redirect_uri = "urn:ietf:wg:oauth:2.0:oob"

    # Get the authorization URL
    auth_url, _ = flow.authorization_url(prompt='consent', access_type='offline')

    print("\n" + "="*60)
    print("Please visit this URL to authorize the application:")
    print("="*60)
    print(auth_url)
    print("="*60)
    print("\nAfter authorizing, you will get a code. Enter it below:")

    code = input("Authorization code: ").strip()

    # Exchange the code for credentials
    flow.fetch_token(code=code)
    credentials = flow.credentials

    # Store the credential bundle
    bundle = {
        "type": "oauth2",
        "provider": "google",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": credentials.refresh_token,
        "scopes": list(scopes),
    }

    set_credentials("gmail", account, bundle)
    print(f"\nSuccessfully authenticated gmail:{account}")

if __name__ == "__main__":
    main()
