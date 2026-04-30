#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAC BO — Serviço de Coleta (Playwright)
========================================
Raspa o resultado mais recente do Bac Bo no TipMiner via Playwright (browser headless).
Necessário porque o site renderiza os dados via JavaScript no cliente.
Salva no banco PostgreSQL. Roda a cada 30s via APScheduler.
"""

import logging
import os
import re
from datetime import datetime, timezone

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
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
    horario: Mapped[str] = mapped_column(String(10), nullable=True)
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
# SCRAPER — Playwright (browser headless real)
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
    """Extrai HH:MM do atributo title, ex: 'BANKER - 5 - 13:09'."""
    m = re.search(r"(\d{1,2}:\d{2})", title)
    return m.group(1) if m else None


def scrape() -> dict | None:
    """
    Abre o TipMiner com Playwright, aguarda as bolinhas renderizarem
    (classes bg-cell-player / bg-cell-banker / bg-cell-tie) e retorna
    o resultado mais recente.
    """
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--no-zygote",
                    "--single-process",
                ],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="pt-BR",
                viewport={"width": 1280, "height": 800},
            )
            page = context.new_page()

            logger.info("[SCRAPER] Abrindo TipMiner com Playwright...")
            page.goto(URL_TIPMINER, wait_until="domcontentloaded", timeout=30000)

            # Aguarda aparecer pelo menos uma bolinha de resultado
            seletor = "div.bg-cell-player, div.bg-cell-banker, div.bg-cell-tie"
            try:
                page.wait_for_selector(seletor, timeout=20000)
                logger.info("[SCRAPER] Bolinhas de resultado encontradas ✅")
            except PlaywrightTimeout:
                logger.warning("[SCRAPER] ⚠️  Timeout aguardando bolinhas — tentando mesmo assim...")

            # Coleta todos os títulos das bolinhas
            titulos = page.eval_on_selector_all(
                seletor,
                "elements => elements.map(e => e.getAttribute('title')).filter(Boolean)"
            )

            browser.close()

            if not titulos:
                logger.warning("[SCRAPER] ❌ Nenhuma bolinha encontrada no HTML renderizado.")
                return None

            logger.info(f"[SCRAPER] {len(titulos)} bolinhas encontradas.")

            candidatos_com_hora = []
            candidatos_sem_hora = []
            for title in titulos:
                resultado = _mapear_resultado(title)
                horario = _extrair_horario(title)
                if resultado:
                    if horario:
                        candidatos_com_hora.append({"resultado": resultado, "horario": horario, "title": title})
                    else:
                        candidatos_sem_hora.append({"resultado": resultado, "horario": None, "title": title})

            if candidatos_com_hora:
                mais_recente = sorted(candidatos_com_hora, key=lambda x: x["horario"])[-1]
                logger.info(f"[SCRAPER] ✅ Mais recente: {mais_recente['resultado']} @ {mais_recente['horario']}")
                return mais_recente

            if candidatos_sem_hora:
                mais_recente = candidatos_sem_hora[-1]
                logger.info(f"[SCRAPER] ✅ Mais recente (sem horário): {mais_recente['resultado']}")
                return mais_recente

            logger.warning("[SCRAPER] ❌ Bolinhas encontradas mas sem dados válidos.")
            return None

    except Exception as e:
        logger.error(f"[SCRAPER] ❌ Erro no Playwright: {e}")
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
        db.execute(text("SELECT 1"))

        ultimo = (
            db.query(Resultado)
            .order_by(Resultado.timestamp.desc())
            .first()
        )

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
            fonte="scraping-playwright",
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
# PING
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
        id="coletar_e_salvar",
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
