from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.parse
from typing import Dict, List, Optional

import requests

from .constants import TOKEN_URL


def build_auth_url(*, client_id: str, redirect_uri: str, scopes: List[str], state: Optional[str] = None) -> str:
    """
    Construct the Xero authorization URL for the Authorization Code flow.
    You must include 'offline_access' in scopes to receive a refresh_token.
    """
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": " ".join(scopes),
    }
    if state:
        params["state"] = state
    return "https://login.xero.com/identity/connect/authorize?" + urllib.parse.urlencode(params)


def parse_code_from_redirect(redirect_url: str) -> Optional[str]:
    """
    Extract the `code` query param from a redirect URL returned by Xero after consent.
    """
    try:
        parsed = urllib.parse.urlparse(redirect_url)
        qs = urllib.parse.parse_qs(parsed.query)
        code_vals = qs.get("code")
        if code_vals and isinstance(code_vals, list):
            return code_vals[0]
    except Exception:
        return None
    return None


def exchange_code_for_tokens(
    *,
    client_id: str,
    client_secret: str,
    code: str,
    redirect_uri: str,
) -> Dict[str, str]:
    """
    Exchange an authorization code for access/refresh tokens.
    """
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    resp = requests.post(
        TOKEN_URL,
        headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return {
        "access_token": data["access_token"],
        "refresh_token": data["refresh_token"],
        "expires_in": data.get("expires_in"),
    }


def _parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Xero OAuth helper (auth URL + code exchange).")
    parser.add_argument("--client-id", required=True, help="Xero app client_id")
    parser.add_argument("--client-secret", required=True, help="Xero app client_secret")
    parser.add_argument(
        "--redirect-uri",
        required=True,
        help="Redirect URI registered with your Xero app (must match exactly).",
    )
    parser.add_argument(
        "--scopes",
        default=os.getenv("XERO_SCOPES", "offline_access accounting.transactions openid profile email"),
        help="Space-separated scopes. Must include offline_access to receive refresh_token.",
    )
    parser.add_argument("--state", default=None, help="Optional state for CSRF protection.")
    parser.add_argument("--code", default=None, help="Authorization code to exchange. If omitted, prints auth URL.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(list(argv or sys.argv[1:]))
    scopes = args.scopes.split()

    if not args.code:
        url = build_auth_url(client_id=args.client_id, redirect_uri=args.redirect_uri, scopes=scopes, state=args.state)
        print("Open this URL, complete consent, and capture the `code` query param from your redirect URI:\n")
        print(url)
        print("\nThen rerun with --code <value> to exchange for access/refresh tokens.")
        return

    tokens = exchange_code_for_tokens(
        client_id=args.client_id,
        client_secret=args.client_secret,
        code=args.code,
        redirect_uri=args.redirect_uri,
    )
    print(json.dumps(tokens, indent=2))


if __name__ == "__main__":
    main()
