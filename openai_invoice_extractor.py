"""Utility script to query OpenAI's chat API for invoice extraction.

This module exposes a small set of helpers that mirror the code snippet
from the user request, but with the fixes required to interact with the
latest OpenAI Chat Completions endpoint.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Optional

import requests
import tiktoken


class OpenAIAPIError(RuntimeError):
    """Raised when the OpenAI API returns an error response."""


def num_tokens_from_string(string: str, encoding_name: str) -> int:
    """Return the number of tokens in a text string for a given encoding."""
    encoding = tiktoken.get_encoding(encoding_name)
    return len(encoding.encode(string))


def clean_text(raw_text: str) -> str:
    """Collapse whitespace so the prompt stays compact."""
    return re.sub(r"\s+", " ", raw_text).strip()


def build_prompt(clean_text_value: str) -> str:
    """Compose the prompt required by the automation."""
    return (
        "*** "
        + clean_text_value
        + ' *** Extraía apenas dos arquivos PDF enviados agora nessa mensagem, '
        + "os itens da tabela que contem as informações Item, Quantity, Rate e Amount. "
        + "Extraía cada linha e formate os dados da seguinte forma: 0 - A coluna \"Item\" "
        + 'tem duas linhas extraia a primeira linha é referente ao conteúdo do JSON "Item" '
        + 'e a segunda linha possui duas datas a primeira data deve ser extraída e atribuida '
        + 'ao conteúdo do JSON "dataStart" e a segunda data deve ser extraída e atribuida '
        + 'ao conteúdo do JSON "dataEnd" 1 - Deve ser removido o item da tabela que contenha '
        + 'o texto Usage na coluna "Item" 2 - A coluna Item possui duas linhas, sendo assim quero '
        + 'extraia apenas a primeira linha 3 - A coluna Rate precisa ser convertida em numérica '
        + 'com duas casas decimais, usando o carácter . como separador decimal 4 - A coluna Amount, '
        + 'deve apenas conte números e convertida em numérica com duas casas decimais, usando o '
        + 'carácter . como separador decimal. O caracter vírgula deve ser removido Na coluna '
        + 'Quantity, Rate e Amount, se possuirem casa decimal maior que 2, deve ser mantido o '
        + 'total das casas decimais extraídas. Além disso, deve ser extraída a informação que vem '
        + 'após o texto “Invoice #“ que é o número da invoice e também a informação "Amount Due" '
        + 'que fica no fim do arquivo e que deve apenas conte números e convertida em numérica '
        + 'com duas casas decimais, usando o carácter . como separador decimal. O caracter vírgula '
        + 'deve ser removido. Você deve gerar uma saída com esses dados extraídos em um formato '
        + 'json, onde os itens extraídos são ramificações filha de uma propriedade chamada '
        + '“invoice” e o conteúdo é o número da invoice e também a informação "Amount Due" deve '
        + 'vir depois da informação do número da invoice, alem disso o número da invoice possui '
        + 'um separador de traço -, você deve extrair a primeira parte do traço que atribuida a '
        + 'propriedade o JSON datadogId. Use o JSON a seguir como modelo: '
        + '{ "invoices": [ { "invoice": "1200082703-10112023",  "datadogId": "1200082703",  "amountDue": 10.20, '
        + '"items": [ { "Item": "On-Demand Analyzed Logs (Security)", "dataStart": 2024-01-01, "dataEnd": 2024-01-31, '
        + '"Quantity": 305, "Rate": 0.29, "Amount": 88.76 } ] }, ] } Quero que sua resposta apenas '
        + 'contenha o JSON e mais nenhum outro tipo de informação, caso o texto enviado não tenha '
        + 'dados a serem extraidos retorne o JSON com items vazio.'
    )


def query_custom_gpt(
    api_key: str,
    model_id: str,
    prompt: str,
    tokens: int,
    *,
    timeout: int = 60,
) -> str:
    """Send a request to the Chat Completions endpoint and return raw JSON text."""

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload: Dict[str, Any] = {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 1,
        # For the chat/completions endpoint the correct field is `max_tokens`.
        "max_tokens": tokens,
        "response_format": {"type": "json_object"},
    }

    response = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if response.status_code >= 500:
        raise OpenAIAPIError(
            f"Erro interno do servidor da OpenAI: {response.status_code} -> {response.text}"
        )

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise OpenAIAPIError(f"Erro HTTP {response.status_code}: {response.text}") from exc

    content = response.json()
    try:
        return content["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError) as exc:
        raise OpenAIAPIError(f"Resposta em formato inesperado: {content}") from exc


def parse_json_response(raw_content: str) -> Dict[str, Any]:
    """Normalise the string returned by the model into a Python dictionary."""
    compact_json = "".join(line.strip() for line in raw_content.splitlines())
    return json.loads(compact_json)


def run_extraction(
    raw_text: str,
    *,
    api_key: Optional[str] = None,
    model_id: str = "gpt-5",
    encoding: str = "cl100k_base",
) -> Dict[str, Any]:
    """High level helper that orchestrates the full workflow."""
    api_key = api_key or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("API key não fornecida. Defina OPENAI_API_KEY ou passe api_key explicitamente.")

    prompt = build_prompt(clean_text(raw_text))
    tokens = num_tokens_from_string(prompt, encoding)
    raw_content = query_custom_gpt(api_key, model_id, prompt, tokens)
    return parse_json_response(raw_content)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extrai dados de invoice via OpenAI Chat API.")
    parser.add_argument(
        "texto",
        help="Texto bruto extraído dos PDFs.",
    )
    parser.add_argument(
        "--model",
        default="gpt-5",
        help="Modelo a ser utilizado (padrão: gpt-5).",
    )
    parser.add_argument(
        "--encoding",
        default="cl100k_base",
        help="Encoding do tiktoken usado para contar tokens (padrão: cl100k_base).",
    )
    parser.add_argument(
        "--api-key",
        dest="api_key",
        default=None,
        help="API key da OpenAI. Se omitido, a variável de ambiente OPENAI_API_KEY será usada.",
    )
    args = parser.parse_args()

    try:
        result = run_extraction(
            args.texto,
            api_key=args.api_key,
            model_id=args.model,
            encoding=args.encoding,
        )
    except (OpenAIAPIError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Falha na extração: {exc}") from exc

    print(json.dumps(result, ensure_ascii=False, indent=2))
