import os
import sqlite3
from datetime import date
from calendar import monthrange
import requests
import base64
import json
import time


class ContaAzulClient:
    """Simple client for Conta Azul API."""

    def __init__(self, token: str, base_url: str = "https://api.contaazul.com"):
        self.token = token
        self.base_url = base_url.rstrip("/")

    def search_installments(self, vencimento_de: str, vencimento_ate: str):
        """Fetch installments to receive using date filter."""
        url = f"{self.base_url}/financeiro/searchinstallmentstoreceivebyfilter"
        params = {
            "data_vencimento_de": vencimento_de,
            "data_vencimento_ate": vencimento_ate,
        }
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        # the API may wrap results inside 'data'
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        return data


AUTH_URL = "https://auth.contaazul.com/oauth2/authorize"
TOKEN_URL = "https://auth.contaazul.com/oauth2/token"


class OAuthHandler:
    """Manage OAuth2 flow and token persistence."""

    def __init__(self, client_id: str, client_secret: str, redirect_uri: str, token_file: str = "tokens.json"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.token_file = token_file

    def _basic_auth_header(self) -> dict:
        cred = f"{self.client_id}:{self.client_secret}".encode()
        b64 = base64.b64encode(cred).decode()
        return {"Authorization": f"Basic {b64}"}

    def load_tokens(self) -> dict | None:
        if os.path.exists(self.token_file):
            with open(self.token_file, "r", encoding="utf-8") as fh:
                return json.load(fh)
        return None

    def save_tokens(self, tokens: dict) -> None:
        with open(self.token_file, "w", encoding="utf-8") as fh:
            json.dump(tokens, fh)

    def exchange_code(self, code: str) -> dict:
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        headers.update(self._basic_auth_header())
        resp = requests.post(TOKEN_URL, data=data, headers=headers, timeout=30)
        resp.raise_for_status()
        tokens = resp.json()
        tokens["expires_at"] = time.time() + tokens.get("expires_in", 0)
        self.save_tokens(tokens)
        return tokens

    def refresh(self, refresh_token: str) -> dict:
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        headers.update(self._basic_auth_header())
        resp = requests.post(TOKEN_URL, data=data, headers=headers, timeout=30)
        resp.raise_for_status()
        tokens = resp.json()
        tokens["expires_at"] = time.time() + tokens.get("expires_in", 0)
        self.save_tokens(tokens)
        return tokens

    def authorization_url(self, state: str = "state") -> str:
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "state": state,
            "scope": "openid profile aws.cognito.signin.user.admin",
        }
        query = "&".join(f"{k}={requests.utils.quote(v)}" for k, v in params.items())
        return f"{AUTH_URL}?{query}"


def month_range(year: int, month: int):
    """Return first and last date of the given month as ISO strings."""
    start = date(year, month, 1)
    last_day = monthrange(year, month)[1]
    end = date(year, month, last_day)
    return start.isoformat(), end.isoformat()


def ensure_table(cursor: sqlite3.Cursor, sample_record: dict):
    """Create table CR with columns based on sample record if it does not exist."""
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='CR'"
    )
    if cursor.fetchone():
        return

    columns = ",".join(f"{key} TEXT" for key in sample_record.keys())
    cursor.execute(f"CREATE TABLE IF NOT EXISTS CR ({columns})")


def insert_records(conn: sqlite3.Connection, records: list):
    if not records:
        return
    cursor = conn.cursor()
    ensure_table(cursor, records[0])

    existing_cols = [info[1] for info in cursor.execute("PRAGMA table_info(CR)")]
    placeholders = ",".join("?" for _ in existing_cols)
    insert_sql = f"INSERT INTO CR ({','.join(existing_cols)}) VALUES ({placeholders})"
    for record in records:
        values = [record.get(col) for col in existing_cols]
        cursor.execute(insert_sql, values)
    conn.commit()


def get_access_token(auth: OAuthHandler) -> str:
    tokens = auth.load_tokens()
    if tokens:
        if tokens.get("expires_at", 0) > time.time():
            return tokens["access_token"]
        if tokens.get("refresh_token"):
            tokens = auth.refresh(tokens["refresh_token"])
            return tokens["access_token"]

    code = os.environ.get("CONTA_AZUL_AUTH_CODE")
    if not code:
        url = auth.authorization_url()
        raise SystemExit(f"Defina CONTA_AZUL_AUTH_CODE com o codigo obtido em: {url}")

    tokens = auth.exchange_code(code)
    os.environ.pop("CONTA_AZUL_AUTH_CODE", None)
    return tokens["access_token"]


def main(year: int):
    client_id = os.environ.get("CONTA_AZUL_CLIENT_ID")
    client_secret = os.environ.get("CONTA_AZUL_CLIENT_SECRET")
    redirect_uri = os.environ.get("CONTA_AZUL_REDIRECT_URI")
    if not all([client_id, client_secret, redirect_uri]):
        raise SystemExit("Configure CONTA_AZUL_CLIENT_ID, CONTA_AZUL_CLIENT_SECRET e CONTA_AZUL_REDIRECT_URI")

    auth = OAuthHandler(client_id, client_secret, redirect_uri)
    token = get_access_token(auth)

    client = ContaAzulClient(token)
    db_path = "conta_azul.db"
    conn = sqlite3.connect(db_path)

    try:
        for month in range(1, 13):
            inicio, fim = month_range(year, month)
            data = client.search_installments(inicio, fim)
            insert_records(conn, data)
    finally:
        conn.close()


if __name__ == "__main__":
    main(date.today().year)
