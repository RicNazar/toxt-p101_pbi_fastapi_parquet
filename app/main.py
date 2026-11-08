from __future__ import annotations

import io
import os
import random
import secrets
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import polars as pl
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials


load_dotenv(Path(__file__).resolve().parent.parent / ".env")

app = FastAPI(
    title="Power BI Parquet API",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
security = HTTPBasic()

PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
PARQUET_HEADERS = {"Cache-Control": "private, no-store"}

CLIENTES_SCHEMA = {
    "id": pl.Int64,
    "codigo": pl.String,
    "nome": pl.String,
    "nome_fantasia": pl.String,
    "email": pl.String,
    "telefone": pl.String,
    "ativo": pl.Boolean,
    "idade": pl.Int8,
    "quantidade_dependentes": pl.Int16,
    "quantidade_pedidos": pl.Int32,
    "pontos": pl.UInt32,
    "codigo_externo": pl.UInt64,
    "limite_credito": pl.Decimal(precision=14, scale=2),
    "saldo": pl.Float32,
    "faturamento_total": pl.Float64,
    "data_nascimento": pl.Date,
    "data_cadastro": pl.Datetime("us"),
    "ultima_atualizacao": pl.Datetime("us"),
    "horario_preferencial": pl.Time,
    "observacao": pl.String,
}

CLIENTES_DATA = [
    (1, "CLI-0001", "Ana Beatriz Lima", "Emporio Aurora", "ana@aurora.com.br", "11987651001", True, 34, 1, 18, 2450, 10_000_000_001, Decimal("18000.00"), 1250.50, 78540.92, date(1992, 3, 14), datetime(2021, 1, 8, 9, 15), datetime(2026, 10, 28, 14, 30), time(9, 0), "Prefere contato por e-mail"),
    (2, "CLI-0002", "Bruno Cesar Rocha", "Rocha Ferragens", "bruno@rochaferragens.com.br", "1133224402", True, 47, 2, 31, 5180, 10_000_000_002, Decimal("35000.00"), -430.75, 156230.41, date(1979, 8, 22), datetime(2020, 6, 17, 11, 40), datetime(2026, 11, 2, 10, 5), time(14, 30), None),
    (3, "CLI-0003", "Carla Mendes Souza", "Atelie Horizonte", "carla@ateliehorizonte.com.br", None, True, 29, 0, 12, 1320, 10_000_000_003, Decimal("12000.00"), 0.0, 42310.18, date(1997, 11, 5), datetime(2022, 2, 10, 16, 20), datetime(2026, 9, 19, 8, 45), time(10, 15), "Telefone indisponivel"),
    (4, "CLI-0004", "Daniel Alves Pereira", "Mercado Boa Praca", None, "2133557704", True, 52, 3, 44, 8040, 10_000_000_004, Decimal("50000.00"), 8920.25, 318904.77, date(1974, 1, 30), datetime(2019, 9, 3, 8, 5), datetime(2026, 10, 30, 17, 10), time(8, 30), "Cliente desde a inauguracao"),
    (5, "CLI-0005", "Elisa Ramos Martins", "Clinica Viver Bem", "compras@viverbem.med.br", "3132448805", True, 41, 2, 27, 4315, 10_000_000_005, Decimal("28000.00"), 3410.0, 119870.36, date(1985, 6, 18), datetime(2020, 11, 23, 13, 55), datetime(2026, 11, 1, 12, 0), time(13, 0), None),
    (6, "CLI-0006", "Fabio Nunes Castro", "Nunes Autopecas", "fabio@nunesautopecas.com.br", "4133669906", False, 38, 1, 9, 640, 10_000_000_006, Decimal("9000.00"), -2150.75, 28740.0, date(1988, 9, 9), datetime(2023, 1, 12, 10, 10), datetime(2026, 6, 15, 15, 25), time(16, 0), "Cadastro inativo por solicitacao"),
    (7, "CLI-0007", "Gabriela Oliveira Reis", "Papelaria Estacao", "gabriela@papelariaestacao.com.br", "5133771107", True, 31, 0, 22, 2975, 10_000_000_007, Decimal("22000.00"), 1875.25, 93654.82, date(1995, 4, 27), datetime(2021, 7, 29, 9, 45), datetime(2026, 10, 21, 11, 35), time(11, 0), None),
    (8, "CLI-0008", "Henrique Costa Melo", "Cafe Serra Alta", None, "6133882208", True, 56, 4, 36, 6220, 10_000_000_008, Decimal("40000.00"), 7240.0, 207115.63, date(1970, 12, 2), datetime(2018, 5, 14, 7, 50), datetime(2026, 9, 30, 9, 20), time(7, 30), "Recebimento somente pela manha"),
    (9, "CLI-0009", "Isabela Freitas Dias", "Studio Movimento", "isabela@studiomovimento.com.br", "7133993309", True, 27, 0, 7, 510, 10_000_000_009, Decimal("7500.00"), 250.0, 19480.29, date(1999, 2, 16), datetime(2024, 3, 5, 14, 30), datetime(2026, 10, 14, 16, 40), time(15, 0), None),
    (10, "CLI-0010", "Joao Paulo Azevedo", "Construtora Base Forte", "suprimentos@baseforte.com.br", "8133114410", True, 45, 2, 51, 9530, 10_000_000_010, Decimal("75000.00"), 14250.75, 486920.14, date(1981, 7, 11), datetime(2017, 10, 20, 10, 0), datetime(2026, 11, 3, 13, 15), time(10, 30), "Pedidos exigem numero da obra"),
    (11, "CLI-0011", "Karen Silva Moraes", "Boutique Amora", "karen@boutiqueamora.com.br", None, True, 36, 1, 16, 1880, 10_000_000_011, Decimal("15000.00"), 980.5, 68220.75, date(1990, 5, 3), datetime(2022, 8, 18, 12, 25), datetime(2026, 8, 25, 10, 55), time(12, 0), None),
    (12, "CLI-0012", "Lucas Teixeira Barros", "Tech Norte Solucoes", "lucas@technorte.com.br", "9133005512", True, 33, 1, 24, 3640, 10_000_000_012, Decimal("30000.00"), 5125.0, 134508.66, date(1993, 10, 25), datetime(2020, 4, 6, 15, 35), datetime(2026, 10, 7, 14, 5), time(16, 30), "Enviar nota fiscal em PDF"),
    (13, "CLI-0013", "Marina Campos Leal", "Livraria Pagina Um", "marina@paginaum.com.br", "8533226613", False, 49, 2, 14, 990, 10_000_000_013, Decimal("11000.00"), -780.25, 51034.98, date(1977, 3, 8), datetime(2019, 2, 27, 11, 15), datetime(2026, 5, 9, 8, 50), time(9, 30), "Loja em processo de encerramento"),
    (14, "CLI-0014", "Nelson Ribeiro Gomes", "Hotel Mar Azul", "compras@hotelmarazul.com.br", "4833337714", True, 61, 3, 63, 11750, 10_000_000_014, Decimal("90000.00"), 22480.0, 625340.55, date(1965, 8, 19), datetime(2016, 1, 11, 8, 40), datetime(2026, 11, 4, 9, 10), time(8, 0), "Entregas pela doca lateral"),
    (15, "CLI-0015", "Olivia Martins Prado", "Flor de Cacau", "olivia@flordecacau.com.br", "1933448815", True, 30, 0, 11, 1210, 10_000_000_015, Decimal("10000.00"), 1430.75, 37680.44, date(1996, 1, 21), datetime(2023, 6, 9, 13, 10), datetime(2026, 10, 18, 15, 45), time(14, 0), None),
]

tabClientes = pl.DataFrame(CLIENTES_DATA, schema=CLIENTES_SCHEMA, orient="row")

PEDIDOS_SCHEMA = {
    "id": pl.Int64,
    "cliente_id": pl.Int64,
    "numero": pl.String,
    "descricao": pl.String,
    "quantidade": pl.Int32,
    "quantidade_embalada": pl.UInt16,
    "valor_unitario": pl.Decimal(precision=12, scale=2),
    "desconto": pl.Float32,
    "valor_total": pl.Float64,
    "aprovado": pl.Boolean,
    "data_pedido": pl.Date,
    "data_hora_pedido": pl.Datetime("us"),
    "updated_at": pl.Datetime("us"),
    "hora_separacao": pl.Time,
    "status": pl.String,
    "observacao": pl.String,
}

PRODUTOS = [
    ("Papel sulfite A4 premium", Decimal("34.90")),
    ("Cadeira ergonomica executiva", Decimal("1280.00")),
    ("Monitor LED 27 polegadas", Decimal("1649.50")),
    ("Kit cafe gourmet corporativo", Decimal("189.90")),
    ("Armario de aco duas portas", Decimal("2175.00")),
    ("Impressora termica de etiquetas", Decimal("980.40")),
    ("Luminaria de mesa articulada", Decimal("215.75")),
    ("Caixa organizadora reforcada", Decimal("72.30")),
]
STATUS_PEDIDOS = ("aprovado", "pendente", "cancelado", "finalizado")
DESCONTOS = tuple(
    (Decimal(indice) * Decimal("0.025")).quantize(Decimal("0.001"))
    for indice in range(4)
)


def obter_inteiro_positivo(nome: str, padrao: int) -> int:
    valor = os.getenv(nome)
    if valor is None:
        return padrao

    try:
        resultado = int(valor)
    except ValueError as erro:
        raise RuntimeError(f"{nome} deve ser um numero inteiro.") from erro

    if resultado <= 0:
        raise RuntimeError(f"{nome} deve ser maior que zero.")
    return resultado


def obter_data_hora(nome: str, padrao: datetime) -> datetime:
    valor = os.getenv(nome)
    if valor is None:
        return padrao

    try:
        resultado = datetime.fromisoformat(valor)
    except ValueError as erro:
        raise RuntimeError(
            f"{nome} deve usar o formato ISO 8601, por exemplo 2024-01-01."
        ) from erro

    if resultado.tzinfo is not None:
        raise RuntimeError(f"{nome} nao deve conter fuso horario.")
    return resultado


QUANTIDADE_PEDIDOS = obter_inteiro_positivo("QUANTIDADE_PEDIDOS", 20_000_000)
INICIO_PEDIDOS = obter_data_hora("INICIO_PEDIDOS", datetime(2024, 1, 1))
FIM_PEDIDOS = obter_data_hora("FIM_PEDIDOS", datetime(2027, 1, 1))

if INICIO_PEDIDOS >= FIM_PEDIDOS:
    raise RuntimeError("INICIO_PEDIDOS deve ser anterior a FIM_PEDIDOS.")


def criar_pedidos() -> list[tuple[object, ...]]:
    pedidos: list[tuple[object, ...]] = []
    gerador = random.Random()
    intervalo_segundos = int((FIM_PEDIDOS - INICIO_PEDIDOS).total_seconds())

    for pedido_id in range(1, QUANTIDADE_PEDIDOS + 1):
        data_hora = INICIO_PEDIDOS + timedelta(
            seconds=gerador.randrange(intervalo_segundos)
        )
        descricao, valor_unitario = gerador.choice(PRODUTOS)
        quantidade = gerador.randint(1, 24)
        status_pedido = gerador.choice(STATUS_PEDIDOS)
        desconto = gerador.choice(DESCONTOS)
        valor_total = (
            valor_unitario * quantidade * (Decimal("1") - desconto)
        ).quantize(Decimal("0.01"))
        observacao = None
        if gerador.randrange(9) == 0:
            observacao = "Entrega agendada com o cliente"
        elif status_pedido == "cancelado":
            observacao = "Cancelado antes da separacao"

        if status_pedido == "cancelado":
            quantidade_embalada = 0
        elif status_pedido == "pendente":
            quantidade_embalada = gerador.randint(0, quantidade)
        else:
            quantidade_embalada = quantidade

        updated_at = min(
            data_hora + timedelta(
                seconds=gerador.randint(2 * 60 * 60, 18 * 24 * 60 * 60)
            ),
            FIM_PEDIDOS - timedelta(microseconds=1),
        )

        pedidos.append(
            (
                pedido_id,
                gerador.randint(1, len(CLIENTES_DATA)),
                f"PED-{data_hora.year}-{pedido_id:06d}",
                descricao,
                quantidade,
                quantidade_embalada,
                valor_unitario,
                float(desconto),
                float(valor_total),
                status_pedido in {"aprovado", "finalizado"},
                data_hora.date(),
                data_hora,
                updated_at,
                time(gerador.randint(8, 16), gerador.randint(0, 59)),
                status_pedido,
                observacao,
            )
        )

    return pedidos


tabPedidos = pl.DataFrame(
    criar_pedidos(),
    schema=PEDIDOS_SCHEMA,
    orient="row",
)


def autenticar(
    credenciais: Annotated[HTTPBasicCredentials, Depends(security)],
) -> None:
    usuario_esperado = os.getenv("BI_USERNAME")
    senha_esperada = os.getenv("BI_PASSWORD")
    if not usuario_esperado or not senha_esperada:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="BI_USERNAME e BI_PASSWORD devem estar configurados no ambiente.",
        )

    usuario_valido = secrets.compare_digest(
        credenciais.username.encode("utf-8"),
        usuario_esperado.encode("utf-8"),
    )
    senha_valida = secrets.compare_digest(
        credenciais.password.encode("utf-8"),
        senha_esperada.encode("utf-8"),
    )
    if not (usuario_valido and senha_valida):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciais invalidas.",
            headers={"WWW-Authenticate": "Basic"},
        )


def resposta_parquet(df: pl.DataFrame, nome_arquivo: str) -> Response:
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="zstd")
    return Response(
        content=buffer.getvalue(),
        media_type=PARQUET_MEDIA_TYPE,
        headers={
            **PARQUET_HEADERS,
            "Content-Disposition": f'attachment; filename="{nome_arquivo}"',
        },
    )


def normalizar_data_hora(valor: datetime) -> datetime:
    if valor.tzinfo is None:
        return valor
    return valor.astimezone(timezone.utc).replace(tzinfo=None)


@app.get("/bi/clientes.parquet")
def obter_clientes(_: Annotated[None, Depends(autenticar)]) -> Response:
    return resposta_parquet(tabClientes, "clientes.parquet")


@app.get("/bi/pedidos.parquet")
def obter_pedidos(
    inicio: Annotated[datetime, Query(description="Inicio inclusivo em ISO 8601")],
    fim: Annotated[datetime, Query(description="Fim exclusivo em ISO 8601")],
    _: Annotated[None, Depends(autenticar)],
) -> Response:
    inicio_normalizado = normalizar_data_hora(inicio)
    fim_normalizado = normalizar_data_hora(fim)
    if inicio_normalizado >= fim_normalizado:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="O parametro inicio deve ser anterior ao parametro fim.",
        )

    pedidos_filtrados = tabPedidos.filter(
        (pl.col("data_hora_pedido") >= inicio_normalizado)
        & (pl.col("data_hora_pedido") < fim_normalizado)
    )
    return resposta_parquet(pedidos_filtrados, "pedidos.parquet")


@app.get("/bi/health")
def health() -> dict[str, str]:
    return {"status": "ok"}