"""Fetch Conta Azul financial events and store them in Google Sheets.

This script fetches both "contas a receber" (CR) and "contas a pagar" (CP)
financial events from the Conta Azul API. It accepts a start and end due date
in ISO format (YYYY-MM-DD) and persists the resulting records in two Google
Sheets tabs, one for each type of event. Each execution clears the target tabs
before inserting the fresh results.

Usage::

    python financial_events_sync.py 2025-08-15 2027-09-20 \
        --token YOUR_TOKEN \
        --spreadsheet-id YOUR_SHEET_ID \
        --service-account-file caminho/para/credenciais.json

An OAuth access token is typically required by the Conta Azul API. Provide it
via ``--token`` or the ``CONTA_AZUL_TOKEN`` environment variable. Access to the
Google Sheets must be granted to the service account credentials referenced via
``--service-account-file`` or ``GOOGLE_APPLICATION_CREDENTIALS``.
"""
from __future__ import annotations

import argparse
import json
import os
from typing import Any

import requests

import gspread
from google.oauth2.service_account import Credentials


CR_ENDPOINT = (
    "https://api-v2.contaazul.com/v1/financeiro/eventos-financeiros/"
    "contas-a-receber/buscar"
)
CP_ENDPOINT = (
    "https://api-v2.contaazul.com/v1/financeiro/eventos-financeiros/"
    "contas-a-pagar/buscar"
)
PAGE_SIZE = 100
SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"


class ApiError(RuntimeError):
    """Raised when the API response format does not match expectations."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Conta Azul financial events")
    parser.add_argument("start", help="Start due date (ISO format YYYY-MM-DD)")
    parser.add_argument("end", help="End due date (ISO format YYYY-MM-DD)")
    parser.add_argument(
        "--token",
        default=os.environ.get("CONTA_AZUL_TOKEN"),
        help="OAuth access token for Conta Azul API",
    )
    parser.add_argument(
        "--spreadsheet-id",
        default=os.environ.get("GOOGLE_SHEETS_ID"),
        help="Google Sheets spreadsheet ID (ou variável GOOGLE_SHEETS_ID)",
    )
    parser.add_argument(
        "--service-account-file",
        default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        help=(
            "Caminho para o arquivo JSON do service account do Google (ou"
            " variável GOOGLE_APPLICATION_CREDENTIALS)"
        ),
    )
    parser.add_argument(
        "--worksheet-cr",
        default="CR",
        help="Nome da aba do Google Sheets para Contas a Receber (default: CR)",
    )
    parser.add_argument(
        "--worksheet-cp",
        default="CP",
        help="Nome da aba do Google Sheets para Contas a Pagar (default: CP)",
    )
    parser.add_argument(
        "--verify-ssl",
        dest="verify_ssl",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Verify SSL certificates on requests (default: True)",
    )
    return parser.parse_args()


def build_session(token: str | None, verify_ssl: bool) -> requests.Session:
    session = requests.Session()
    session.verify = verify_ssl
    if token:
        session.headers.update({"Authorization": f"Bearer {token}"})
    session.headers.setdefault("Accept", "application/json")
    return session


def build_sheets_client(service_account_file: str | None) -> gspread.Client:
    if not service_account_file:
        raise SystemExit(
            "É necessário fornecer o caminho do arquivo JSON do service account"
            " via --service-account-file ou variável GOOGLE_APPLICATION_CREDENTIALS"
        )

    credentials = Credentials.from_service_account_file(
        service_account_file, scopes=[SHEETS_SCOPE]
    )
    return gspread.authorize(credentials)


def fetch_all(
    session: requests.Session,
    endpoint: str,
    start: str,
    end: str,
) -> list[dict[str, Any]]:
    """Fetch all pages of financial events from the given endpoint."""

    collected: list[dict[str, Any]] = []
    total_expected: int | None = None
    page = 1

    while True:
        params = {
            "pagina": page,
            "tamanho_pagina": PAGE_SIZE,
            "data_vencimento_de": start,
            "data_vencimento_ate": end,
        }
        response = session.get(endpoint, params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()

        if not isinstance(payload, dict):
            raise ApiError("Resposta inesperada: objeto JSON deve ser um dicionário")

        if "itens" not in payload:
            raise ApiError("Resposta inesperada: campo 'itens' ausente")
        if "itens_totais" not in payload:
            raise ApiError("Resposta inesperada: campo 'itens_totais' ausente")

        items = payload["itens"]
        if not isinstance(items, list):
            raise ApiError("Campo 'itens' deve ser uma lista")

        total = int(payload["itens_totais"])
        if total_expected is None:
            total_expected = total

        for item in items:
            if not isinstance(item, dict):
                raise ApiError("Cada item retornado deve ser um objeto JSON")
            collected.append(item)

        if len(collected) >= total_expected or not items:
            break

        page += 1

    return collected


def normalise_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def ensure_worksheet(spreadsheet: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    try:
        worksheet = spreadsheet.worksheet(name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=name, rows=1, cols=1)
    worksheet.clear()
    return worksheet


def records_to_rows(records: list[dict[str, Any]]) -> list[list[Any]]:
    columns: list[str] = []
    for record in records:
        for key in record.keys():
            if key not in columns:
                columns.append(key)

    if not columns:
        return [["raw_json"], *([[json.dumps(record, ensure_ascii=False)] for record in records])]

    rows = [[normalise_value(record.get(column)) for column in columns] for record in records]
    return [columns, *rows]


def write_to_sheet(worksheet: gspread.Worksheet, records: list[dict[str, Any]]) -> None:
    if not records:
        # Aba deve permanecer vazia após clear
        return

    values = records_to_rows(records)
    worksheet.update("A1", values)


def main() -> None:
    args = parse_args()
    if not args.token:
        raise SystemExit(
            "É necessário fornecer um token OAuth via --token ou variável CONTA_AZUL_TOKEN"
        )
    if not args.spreadsheet_id:
        raise SystemExit(
            "É necessário fornecer o ID da planilha do Google via --spreadsheet-id"
            " ou variável GOOGLE_SHEETS_ID"
        )

    session = build_session(args.token, args.verify_ssl)
    sheets_client = build_sheets_client(args.service_account_file)

    spreadsheet = sheets_client.open_by_key(args.spreadsheet_id)
    worksheet_cr = ensure_worksheet(spreadsheet, args.worksheet_cr)
    worksheet_cp = ensure_worksheet(spreadsheet, args.worksheet_cp)

    contas_receber = fetch_all(session, CR_ENDPOINT, args.start, args.end)
    contas_pagar = fetch_all(session, CP_ENDPOINT, args.start, args.end)

    write_to_sheet(worksheet_cr, contas_receber)
    write_to_sheet(worksheet_cp, contas_pagar)


if __name__ == "__main__":
    main()
