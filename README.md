# BAC BO Coletor — versão leve (requests + BeautifulSoup)

Scraper do Bac Bo via TipMiner. **Sem Playwright/Chromium** — usa `requests` + `BeautifulSoup`.  
Imagem Docker ~80 MB (vs ~800 MB com Playwright).

## Como funciona

1. A cada 30s faz GET em `https://www.tipminer.com/br/historico/blaze/bac-bo-ao-vivo`
2. Analisa o HTML com 3 estratégias em cascata:
   - **Estratégia 1**: `<div title="BANKER - 13:09">` → captura resultado + horário
   - **Estratégia 2**: classes CSS `bg-cell-banker / bg-cell-player / bg-cell-tie`
   - **Estratégia 3**: texto com `PLAYER / BANKER / TIE` em qualquer tag
3. Mapeia:
   - `PLAYER` → `azul`
   - `BANKER` → `vermelho`
   - `TIE` → `branco`
4. Salva no PostgreSQL se for resultado novo (compara resultado + horário)

## Banco de dados — tabela `resultados`

| coluna | tipo | descrição |
|---|---|---|
| id | int | PK auto-incremento |
| resultado | varchar(50) | `azul`, `vermelho` ou `branco` |
| horario | varchar(10) | `HH:MM` extraído do site |
| fonte | varchar(100) | `scraping-requests` |
| timestamp | timestamptz | quando foi coletado (UTC) |
| criado_em | timestamptz | inserção no banco (UTC) |

## Deploy no Railway

1. Suba para GitHub
2. Railway → New Project → Deploy from GitHub → seleciona o repo
3. Adicione variável de ambiente:
   - `DATABASE_URL` → sua string PostgreSQL (ex: `postgresql://user:pass@host/db`)
4. (Opcional) `COLLECT_INTERVAL_SECONDS` → padrão `30`

## Deploy no Render

1. Suba para GitHub
2. Render → New → Background Worker → seleciona o repo
3. Render detecta o `render.yaml` automaticamente
4. Configure `DATABASE_URL` em Environment → Secret Files

## Teste local

```bash
pip install -r requirements.txt
export DATABASE_URL="postgresql://user:pass@localhost/bacbo"
python -m app.coletor
```

## Variáveis de ambiente

| variável | obrigatório | padrão | descrição |
|---|---|---|---|
| `DATABASE_URL` | ✅ | — | PostgreSQL connection string |
| `COLLECT_INTERVAL_SECONDS` | ❌ | `30` | Intervalo entre coletas |
