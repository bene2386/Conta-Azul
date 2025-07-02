# Conta Azul Scripts

Este repositório contém um script para extrair informações de contas a receber da API do Conta Azul e armazená-las em um banco SQLite.

## Uso

1. Configure as variáveis de ambiente `CONTA_AZUL_CLIENT_ID`, `CONTA_AZUL_CLIENT_SECRET` e `CONTA_AZUL_REDIRECT_URI` com os dados da aplicação cadastrada no Conta Azul.
2. Na primeira execução, defina também `CONTA_AZUL_AUTH_CODE` com o código obtido após acessar a URL gerada pelo script.
3. Execute o script `conta_azul_cr.py`. O script obtém um token de acesso via OAuth2 e armazena o `refresh_token` em `tokens.json`. Em seguida ele faz chamadas mensais à API para o ano atual e grava os resultados na base `conta_azul.db`, criando a tabela `CR` caso ela não exista.

```bash
python conta_azul_cr.py
```

O banco de dados será criado no diretório atual.
