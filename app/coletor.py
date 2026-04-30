#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAC BO — Serviço de Coleta (versão leve com requests+BS4)
=========================================================
Raspa o resultado mais recente do Bac Bo no TipMiner via requests/BeautifulSoup.
Salva no banco PostgreSQL. Roda a cada 30s via APScheduler.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import create_engine, text, String, DateTime, Integer, func
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column

# ============================================================
# CONFIGURAÇÃO
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
INTERVALO_SEGUNDOS = int(os.environ.get("COLLECT_INTERVAL_SECONDS", "30"))
URL_TIPMINER = "https://www.tipminer.com/br/historico/blaze/bac-bo-ao-vivo"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

if not DATABASE_URL:
    raise RuntimeError("❌ Variável DATABASE_URL não configurada!")

# ============================================================
# BANCO DE DADOS
# ============================================================

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=3,
    max_overflow=5,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


class Resultado(Base):
    __tablename__ = "resultados"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    resultado: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    horario: Mapped[str] = mapped_column(String(10), nullable=True)   # ex: "13:09"
    fonte: Mapped[str] = mapped_column(String(100), nullable=False, default="scraping")
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


def init_db():
    Base.metadata.create_all(bind=engine)
    logger.info("✅ Banco de dados pronto.")


# ============================================================
# SCRAPER — requests + BeautifulSoup (sem Playwright)
# ============================================================

def _mapear_resultado(title: str) -> str | None:
    """Converte PLAYER/BANKER/TIE -> azul/vermelho/branco."""
    title_up = title.upper()
    if "PLAYER" in title_up:
        return "azul"
    if "BANKER" in title_up:
        return "vermelho"
    if "TIE" in title_up:
        return "branco"
    return None


def _extrair_horario(title: str) -> str | None:
    """Extrai 'HH:MM' do atributo title, ex: 'BANKER - 13:09'."""
    m = re.search(r"(\d{1,2}:\d{2})", title)
    return m.group(1) if m else None


def scrape() -> dict | None:
    """
    Tenta 3 fontes em cascata:
      1. API JSON interna do TipMiner (mais confiável)
      2. HTML com atributo title="BANKER - 13:09"
      3. HTML com padrão title="PLAYER - N - HH:MM" em qualquer tag
    Retorna dict {"resultado": ..., "horario": ...} ou None.
    """

    # ------------------------------------------------------------------
    # Fonte 1 — API JSON do TipMiner
    # ------------------------------------------------------------------
    API_URLS = [
        "https://www.tipminer.com/api/historico/blaze/bac-bo-ao-vivo",
        "https://www.tipminer.com/api/bac-bo/resultados",
        "https://www.tipminer.com/api/blaze/bac-bo",
    ]
    for api_url in API_URLS:
        try:
            r = requests.get(api_url, headers=HEADERS, timeout=10)
            if r.status_code == 200 and "application/json" in r.headers.get("Content-Type", ""):
                data = r.json()
                logger.info(f"[SCRAPER] API JSON OK: {api_url}")
                # Tenta pegar o último item da lista
                items = data if isinstance(data, list) else data.get("data", data.get("results", []))
                if items:
                    ultimo = items[-1] if isinstance(items, list) else items
                    # Campos possíveis: result, resultado, winner, outcome
                    raw = (
                        ultimo.get("result") or ultimo.get("resultado") or
                        ultimo.get("winner") or ultimo.get("outcome") or ""
                    ).upper()
                    resultado = _mapear_resultado(raw)
                    # Horário: created_at, hora, time, timestamp
                    hora_raw = (
                        ultimo.get("hora") or ultimo.get("time") or
                        ultimo.get("created_at") or ultimo.get("timestamp") or ""
                    )
                    horario = _extrair_horario(str(hora_raw))
                    if resultado:
                        logger.info(f"[SCRAPER] ✅ API JSON → {resultado} @ {horario}")
                        return {"resultado": resultado, "horario": horario, "title": raw}
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Fonte 2 — HTML scraping (página renderizada pelo servidor)
    # ------------------------------------------------------------------
    try:
        logger.info("[SCRAPER] Acessando TipMiner via requests...")
        resp = requests.get(URL_TIPMINER, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        logger.info(f"[SCRAPER] HTTP {resp.status_code} — {len(resp.text)} bytes recebidos")
    except requests.RequestException as e:
        logger.error(f"[SCRAPER] ❌ Falha no request: {e}")
        return None

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    # Estratégia 2a: qualquer tag com atributo title contendo PLAYER/BANKER/TIE + horário
    candidatos = []
    for tag in soup.find_all(title=True):
        title = tag.get("title", "")
        resultado = _mapear_resultado(title)
        horario = _extrair_horario(title)
        if resultado and horario:
            candidatos.append({"resultado": resultado, "horario": horario, "title": title})

    if candidatos:
        mais_recente = candidatos[-1]
        logger.info(
            f"[SCRAPER] ✅ {len(candidatos)} resultados (title+horario). "
            f"Mais recente: {mais_recente['resultado']} @ {mais_recente['horario']}"
        )
        return mais_recente

    # Estratégia 2b: title sem horário (aceita mesmo sem hora)
    candidatos_sem_hora = []
    for tag in soup.find_all(title=True):
        title = tag.get("title", "")
        resultado = _mapear_resultado(title)
        if resultado:
            candidatos_sem_hora.append({"resultado": resultado, "horario": None, "title": title})

    if candidatos_sem_hora:
        mais_recente = candidatos_sem_hora[-1]
        logger.info(
            f"[SCRAPER] ✅ {len(candidatos_sem_hora)} resultados (title sem horario). "
            f"Mais recente: {mais_recente['resultado']}"
        )
        return mais_recente

    # Estratégia 2c: dados embutidos no HTML como JSON (Next.js __NEXT_DATA__)
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if match:
        try:
            next_data = json.loads(match.group(1))
            # Navega pela estrutura tentando achar lista de resultados
            raw_str = json.dumps(next_data)
            titulos = re.findall(r'"(?:result|resultado|winner)["\s]*:\s*"([^"]+)"', raw_str, re.I)
            horas = re.findall(r'"(?:hora|time|created_at)["\s]*:\s*"([^"]+)"', raw_str, re.I)
            if titulos:
                resultado = _mapear_resultado(titulos[-1])
                horario = _extrair_horario(horas[-1]) if horas else None
                if resultado:
                    logger.info(f"[SCRAPER] ✅ __NEXT_DATA__ → {resultado} @ {horario}")
                    return {"resultado": resultado, "horario": horario, "title": titulos[-1]}
        except Exception:
            pass

    # Estratégia 2d: regex direto no HTML cru
    # Padrão: "BANKER - 7 - 13:09" ou "PLAYER - 5 - 14:22"
    matches = re.findall(
        r'(PLAYER|BANKER|TIE)\s*[-–]\s*\d+\s*[-–]\s*(\d{1,2}:\d{2})',
        html, re.I
    )
    if matches:
        tipo, hora = matches[-1]
        resultado = _mapear_resultado(tipo)
        logger.info(f"[SCRAPER] ✅ regex HTML → {resultado} @ {hora}")
        return {"resultado": resultado, "horario": hora, "title": f"{tipo} - {hora}"}

    # Último recurso: qualquer menção a PLAYER/BANKER/TIE no HTML
    matches2 = re.findall(r'\b(PLAYER|BANKER|TIE)\b', html, re.I)
    if matches2:
        resultado = _mapear_resultado(matches2[-1])
        logger.warning(f"[SCRAPER] ⚠️  Horário não encontrado — usando resultado mais recente: {resultado}")
        return {"resultado": resultado, "horario": None, "title": matches2[-1]}

    logger.warning("[SCRAPER] ❌ Nenhum resultado encontrado.")
    logger.debug("[SCRAPER] Primeiros 3000 chars do HTML:\n" + html[:3000])
    return None


# ============================================================
# COLETA + SAVE
# ============================================================

def coletar_e_salvar():
    logger.info("🔄 Iniciando ciclo de coleta...")

    dados = scrape()

    if dados is None:
        logger.warning("⚠️  Scraper retornou None — pulando ciclo.")
        return

    valor = dados["resultado"]
    horario = dados.get("horario")

    db = SessionLocal()
    try:
        db.execute(text("SELECT 1"))  # testa conexão

        ultimo = (
            db.query(Resultado)
            .order_by(Resultado.timestamp.desc())
            .first()
        )

        # Deduplicação:
        # - Se tem horário: só salva se resultado+horário for diferente do último
        # - Se não tem horário: salva sempre que o resultado mudar (ignora repetição do mesmo tipo)
        if ultimo:
            if horario and ultimo.horario == horario and ultimo.resultado == valor:
                logger.info(f"🔁 Resultado repetido ({valor} @ {horario}) — ignorando.")
                return
            if not horario and ultimo.resultado == valor:
                logger.info(f"🔁 Resultado repetido ({valor} sem horário) — ignorando.")
                return

        novo = Resultado(
            resultado=valor,
            horario=horario,
            fonte="scraping-requests",
            timestamp=datetime.now(timezone.utc),
        )
        db.add(novo)
        db.commit()
        if horario:
            logger.info(f"💾 Salvo: {valor} @ {horario}")
        else:
            logger.info(f"💾 Salvo: {valor} (sem horário — resultado mais recente da página)")

    except Exception as e:
        db.rollback()
        logger.error(f"❌ Erro ao salvar no banco: {e}")
    finally:
        db.close()


# ============================================================
# PING — evita hibernação no Render/Railway free tier
# ============================================================

def ping():
    logger.debug("💓 Ping — serviço ativo")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    logger.info("🚀 BAC BO Coletor iniciando...")
    init_db()

    scheduler = BlockingScheduler(timezone="UTC")

    scheduler.add_job(
        coletar_e_salvar,
        "interval",
        seconds=INTERVALO_SEGUNDOS,
        id="coleta",
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        ping,
        "interval",
        minutes=10,
        id="ping",
    )

    logger.info(f"⏱️  Coleta configurada a cada {INTERVALO_SEGUNDOS}s")
    logger.info("✅ Scheduler iniciado — aguardando ciclos...")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Coletor encerrado.")
