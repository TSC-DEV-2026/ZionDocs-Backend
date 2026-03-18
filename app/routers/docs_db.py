import base64
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

from babel.dates import format_date
from fastapi import APIRouter, HTTPException, Depends, Request, Response, Body, Query
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import text
from sqlalchemy.orm import Session
from fpdf import FPDF  # type: ignore

from app.database.connection import get_db

router = APIRouter()


class BuscarCompetenciasInformeRendimentos(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    cpf: str = Field(..., min_length=1)


class BuscarHolerite(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    cpf: str = Field(..., min_length=1)
    matricula: str = Field(..., min_length=1)
    competencia: str = Field(..., min_length=1)
    empresa: str = Field(..., min_length=1)


class MontarHolerite(BaseModel):
    matricula: str
    competencia: str
    lote: str
    cpf: str = Field(
        ...,
        pattern=r"^\d{11}$",
        description="CPF sem formatação, 11 dígitos",
    )


class BuscarInformeRendimentos(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    cpf: str = Field(..., min_length=1)
    competencia: str = Field(..., min_length=1)


class MontarInformeRendimentos(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    cpf: str = Field(..., min_length=1)
    competencia: str = Field(..., min_length=1)


class BuscarCompetenciasFerias(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    cpf: str = Field(..., min_length=1)
    matricula: str = Field(..., min_length=1)
    empresa: str = Field(..., min_length=1)


class BuscarFerias(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    cpf: str = Field(..., min_length=1)
    matricula: str = Field(..., min_length=1)
    competencia: str = Field(..., min_length=1)
    empresa: str = Field(..., min_length=1)


class MontarFerias(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    cpf: str = Field(..., min_length=1)
    matricula: str = Field(..., min_length=1)
    competencia: str = Field(..., min_length=1)
    empresa: str = Field(..., min_length=1)

def _normalizar_cpf(cpf: Any) -> str:
    digits = re.sub(r"\D", "", _as_str(cpf))
    return digits.zfill(11)

def _numero_ate_999(n: int) -> str:
    unidades = [
        "", "um", "dois", "três", "quatro", "cinco", "seis", "sete", "oito", "nove"
    ]
    especiais = [
        "dez", "onze", "doze", "treze", "quatorze", "quinze",
        "dezesseis", "dezessete", "dezoito", "dezenove"
    ]
    dezenas = [
        "", "", "vinte", "trinta", "quarenta", "cinquenta",
        "sessenta", "setenta", "oitenta", "noventa"
    ]
    centenas = [
        "", "cento", "duzentos", "trezentos", "quatrocentos",
        "quinhentos", "seiscentos", "setecentos", "oitocentos", "novecentos"
    ]

    if n == 0:
        return ""
    if n == 100:
        return "cem"
    if n < 10:
        return unidades[n]
    if n < 20:
        return especiais[n - 10]
    if n < 100:
        d, u = divmod(n, 10)
        return dezenas[d] if u == 0 else f"{dezenas[d]} e {unidades[u]}"

    c, r = divmod(n, 100)
    if r == 0:
        return centenas[c]
    return f"{centenas[c]} e {_numero_ate_999(r)}"


def _numero_por_extenso_br(n: int) -> str:
    if n == 0:
        return "zero"

    if n < 1000:
        return _numero_ate_999(n)

    milhares, resto = divmod(n, 1000)

    if milhares == 1:
        prefixo = "mil"
    else:
        prefixo = f"{_numero_ate_999(milhares)} mil"

    if resto == 0:
        return prefixo

    if resto < 100:
        return f"{prefixo} e {_numero_ate_999(resto)}"

    return f"{prefixo}, {_numero_ate_999(resto)}"


def _valor_monetario_por_extenso(v: Any) -> str:
    valor = _as_decimal(v)

    inteiro = int(valor)
    centavos = int((valor - Decimal(inteiro)) * 100)

    parte_reais = _numero_por_extenso_br(inteiro)
    parte_centavos = _numero_por_extenso_br(centavos)

    if inteiro == 1:
        reais_txt = "real"
    else:
        reais_txt = "reais"

    if centavos == 1:
        centavos_txt = "centavo"
    else:
        centavos_txt = "centavos"

    if centavos == 0:
        return f"{parte_reais} {reais_txt}".upper()

    if inteiro == 0:
        return f"{parte_centavos} {centavos_txt}".upper()

    return f"{parte_reais} {reais_txt} e {parte_centavos} {centavos_txt}".upper()

def _only_yyyymm(s: str) -> str:
    return re.sub(r"\D", "", (s or ""))[:6]


def _normaliza_anomes(valor: str) -> Optional[str]:
    v = (valor or "").strip()
    if not v:
        return None
    try:
        datetime.strptime(v, "%Y-%m")
        return v
    except ValueError:
        pass
    if len(v) == 6 and v.isdigit():
        return f"{v[:4]}-{v[4:]}"
    if "/" in v:
        a, b = v.split("/", 1)
        if len(a) == 4 and b.isdigit():
            return f"{a}-{b.zfill(2)}"
        if len(b) == 4 and a.isdigit():
            return f"{b}-{a.zfill(2)}"
    if "-" in v:
        a, b = v.split("-", 1)
        if len(a) == 4 and b.isdigit():
            return f"{a}-{b.zfill(2)}"
    return None


def pad_left(valor: str, width: int) -> str:
    return str(valor).strip().zfill(width)


def fmt_num(valor: float) -> str:
    s = f"{valor:,.2f}"
    s = s.replace(",", "X").replace(".", ",")
    return s.replace("X", ".")


def truncate(text: str, max_len: int) -> str:
    text = text or ""
    return text if len(text) <= max_len else text[: max_len - 3] + "..."


def _as_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _only_digits(v: Any) -> str:
    return "".join(ch for ch in _as_str(v) if ch.isdigit())


def _as_decimal(v: Any) -> Decimal:
    if v is None or v == "":
        return Decimal("0")
    try:
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v).replace(",", "."))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _fmt_money(v: Any) -> str:
    d = _as_decimal(v)
    s = f"{d:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def _draw_box(pdf: FPDF, x: float, y: float, w: float, h: float):
    pdf.rect(x, y, w, h)


def _cell_text(
    pdf: FPDF,
    x: float,
    y: float,
    w: float,
    h: float,
    text: str,
    font_size: int = 8,
    style: str = "",
    align: str = "L",
):
    pdf.set_xy(x, y)
    pdf.set_font("Arial", style, font_size)
    pdf.multi_cell(w, h, text, border=0, align=align)


def _fmt_date_br(v: Any) -> str:
    s = _as_str(v)
    if not s:
        return ""

    s = s.split(" ")[0]

    for fmt_in in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt_in).strftime("%d/%m/%Y")
        except ValueError:
            pass

    return s


def _fmt_competencia_mm_yyyy(valor: Any) -> str:
    comp = _only_yyyymm(_as_str(valor))
    if len(comp) == 6:
        return f"{comp[4:6]}/{comp[:4]}"
    return _as_str(valor)


def _split_periodo(valor: Any) -> tuple[str, str]:
    s = _as_str(valor)
    if not s:
        return "", ""

    s = re.sub(r"\s+", " ", s).strip()

    padroes = [
        r"^(.*?)\s+a\s+(.*?)$",
        r"^(.*?)\s+à\s+(.*?)$",
        r"^(.*?)\s+ate\s+(.*?)$",
        r"^(.*?)\s+até\s+(.*?)$",
        r"^(.*?)\s*-\s*(.*?)$",
    ]

    for padrao in padroes:
        m = re.match(padrao, s, flags=re.IGNORECASE)
        if m:
            return _fmt_date_extenso_br(m.group(1)), _fmt_date_extenso_br(m.group(2))

    return _fmt_date_extenso_br(s), ""

def _parse_date_any(v: Any) -> Optional[datetime]:
    s = _as_str(v)
    if not s:
        return None

    s = s.split(" ")[0].strip()

    formatos = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%Y%m%d",
    )

    for fmt in formatos:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    return None


def _fmt_date_extenso_br(v: Any) -> str:
    dt = _parse_date_any(v)
    if not dt:
        return _as_str(v)

    meses = {
        1: "janeiro",
        2: "fevereiro",
        3: "março",
        4: "abril",
        5: "maio",
        6: "junho",
        7: "julho",
        8: "agosto",
        9: "setembro",
        10: "outubro",
        11: "novembro",
        12: "dezembro",
    }

    return f"{dt.day:02d} de {meses[dt.month]} de {dt.year}"

def _fmt_date_curta_br(v: Any) -> str:
    dt = _parse_date_any(v)
    if not dt:
        return _as_str(v)
    return dt.strftime("%d/%m/%Y")

def _tipo_evento_ferias(tipo: Any) -> str:
    t = _as_str(tipo).upper()

    if t in {"D", "DESC", "DESCONTO", "DESCONTOS"}:
        return "D"

    if t in {"P", "PROVENTO", "PROVENTOS", "V", "VENTO", "VENCIMENTO", "VENCIMENTOS"}:
        return "P"

    return "P"


def _empresa_match_sql(alias: str = "c") -> str:
    return f"""
        (
            TRIM(COALESCE({alias}.cod_empresa::text, '')) = TRIM(:empresa)
            OR TRIM(COALESCE({alias}.codigo::text, '')) = TRIM(:empresa)
            OR TRIM(COALESCE({alias}.numero::text, '')) = TRIM(:empresa)
            OR TRIM(COALESCE({alias}.cliente::text, '')) = TRIM(:empresa)
            OR UPPER(TRIM(COALESCE({alias}.empresa::text, ''))) = UPPER(TRIM(:empresa))
        )
    """


def gerar_recibo_ferias_pdf(dados: dict) -> bytes:
    def as_str(v: Any) -> str:
        if v is None:
            return ""
        return str(v).strip()

    def as_decimal(v: Any) -> Decimal:
        if v is None or v == "":
            return Decimal("0")
        try:
            if isinstance(v, Decimal):
                return v
            return Decimal(str(v).replace(",", "."))
        except (InvalidOperation, ValueError, TypeError):
            return Decimal("0")

    def fmt_money(v: Any) -> str:
        d = as_decimal(v)
        s = f"{d:,.2f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")

    def only_digits(v: Any) -> str:
        return "".join(ch for ch in as_str(v) if ch.isdigit())

    def fmt_cpf(v: Any) -> str:
        s = only_digits(v)
        if len(s) == 11:
            return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"
        return as_str(v)

    def fmt_date_br(v: Any) -> str:
        s = as_str(v)
        if not s:
            return ""
        s = s.split(" ")[0]
        formatos = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d")
        for fmt in formatos:
            try:
                return datetime.strptime(s, fmt).strftime("%d/%m/%Y")
            except ValueError:
                pass
        return s

    def truncate_local(text: Any, max_len: int) -> str:
        s = as_str(text)
        return s if len(s) <= max_len else s[: max_len - 3] + "..."

    def money_extenso_placeholder(valor_liquido: Any, valor_extenso: Any) -> str:
        if as_str(valor_extenso):
            return as_str(valor_extenso).upper()
        return _valor_monetario_por_extenso(valor_liquido)

    def draw_box(pdf: FPDF, x: float, y: float, w: float, h: float, line_width: float = 0.2):
        pdf.set_line_width(line_width)
        pdf.rect(x, y, w, h)

    def hline(pdf: FPDF, x1: float, y: float, x2: float, line_width: float = 0.2):
        pdf.set_line_width(line_width)
        pdf.line(x1, y, x2, y)

    def preencher_com_asteriscos_duas_linhas(
        texto: str,
        total_linha_1: int = 86,
        total_linha_2: int = 86,
    ) -> tuple[str, str]:
        texto = as_str(texto).upper().strip()

        if not texto:
            return "*" * total_linha_1, "*" * total_linha_2

        linha_1 = texto
        if len(linha_1) < total_linha_1:
            linha_1 += "*" * (total_linha_1 - len(linha_1))

        linha_2 = "*" * total_linha_2

        return linha_1, linha_2

    def cell_text(
        pdf: FPDF,
        x: float,
        y: float,
        w: float,
        h: float,
        text: str,
        font_size: float = 8,
        style: str = "",
        align: str = "L",
        border: int = 0,
    ):
        pdf.set_xy(x, y)
        pdf.set_font("Arial", style, font_size)
        pdf.cell(w, h, text, border=border, align=align)

    def multi_text(
        pdf: FPDF,
        x: float,
        y: float,
        w: float,
        h: float,
        text: str,
        font_size: float = 8,
        style: str = "",
        align: str = "L",
        border: int = 0,
    ):
        pdf.set_xy(x, y)
        pdf.set_font("Arial", style, font_size)
        pdf.multi_cell(w, h, text, border=border, align=align)

    def linha_periodo(
        pdf: FPDF,
        y: float,
        rotulo: str,
        data_inicio: str = "",
        data_fim: str = "",
        quantidade: str = "",
    ):
        x_rotulo = 12
        x_inicio = 67
        x_a = 137
        x_fim = 143
        x_qtd = 177

        cell_text(pdf, x_rotulo, y, 52, 4, rotulo, 6.7, "B", "L")
        cell_text(pdf, x_inicio, y, 66, 4, data_inicio, 6.2, "", "L")

        if data_fim:
            cell_text(pdf, x_a, y, 4, 4, "À", 6.7, "B", "C")
            cell_text(pdf, x_fim, y, 32, 4, data_fim, 6.2, "", "L")

        if quantidade:
            cell_text(pdf, x_qtd, y, 14, 4, quantidade, 6.2, "", "R")

    def render_tabela_eventos(
        pdf: FPDF,
        x: float,
        y: float,
        w: float,
        h: float,
        titulo: str,
        eventos: list[dict],
        total_label: str,
        total_valor: Any,
    ):
        draw_box(pdf, x, y, w, h)

        cell_text(pdf, x, y + 0.8, w, 4.2, titulo, 8.7, "B", "C")
        hline(pdf, x, y + 5.5, x + w)

        col_cod = 12
        col_desc = 40
        col_qtde = 11
        col_mes = 15
        col_valor = w - (col_cod + col_desc + col_qtde + col_mes)

        x_cod = x
        x_desc = x_cod + col_cod
        x_qtde = x_desc + col_desc
        x_mes = x_qtde + col_qtde
        x_val = x_mes + col_mes

        cell_text(pdf, x_cod + 1, y + 6.0, col_cod - 2, 3.0, "COD.", 6.2, "B", "L")
        cell_text(pdf, x_desc + 1, y + 6.0, col_desc - 2, 3.0, "DESCRIÇÃO", 6.2, "B", "L")
        cell_text(pdf, x_qtde + 1, y + 6.0, col_qtde - 2, 3.0, "QTDE", 6.2, "B", "R")
        cell_text(pdf, x_mes + 1, y + 6.0, col_mes - 2, 3.0, "MÊS/ANO", 6.2, "B", "R")
        cell_text(pdf, x_val + 1, y + 6.0, col_valor - 2, 3.0, "VALOR", 6.2, "B", "R")

        row_y = y + 9.8
        row_h = 3.45
        area_util = h - 15.2
        max_rows = int(area_util / row_h)

        eventos_render = eventos[:max_rows]

        for evt in eventos_render:
            codigo = truncate_local(evt.get("codigo") or evt.get("evento") or evt.get("cod"), 8)
            descricao = truncate_local(evt.get("descricao") or evt.get("evento_nome") or "", 34)
            qtde = as_str(evt.get("qtde") or evt.get("quantidade") or evt.get("referencia") or "")
            mes_ano = as_str(evt.get("mes_ano") or evt.get("competencia") or "")
            valor = fmt_money(evt.get("valor") or evt.get("valor_total") or 0)

            cell_text(pdf, x_cod + 1, row_y, col_cod - 2, 3.0, codigo, 6.4, "", "L")
            cell_text(pdf, x_desc + 1, row_y, col_desc - 2, 3.0, descricao, 6.4, "", "L")
            cell_text(pdf, x_qtde + 1, row_y, col_qtde - 2, 3.0, qtde, 6.4, "", "R")
            cell_text(pdf, x_mes + 1, row_y, col_mes - 2, 3.0, mes_ano, 6.4, "", "R")
            cell_text(pdf, x_val + 1, row_y, col_valor - 2, 3.0, valor, 6.4, "", "R")

            row_y += row_h

        total_y = y + h - 4.5
        hline(pdf, x, total_y, x + w)
        cell_text(pdf, x + 1, total_y + 0.4, w - 40, 3.6, total_label, 6.7, "B", "L")
        cell_text(pdf, x + w - 38, total_y + 0.4, 37, 3.6, fmt_money(total_valor), 6.7, "B", "R")

    cab = dados.get("cabecalho", {}) or {}
    periodo = dados.get("periodo", {}) or {}
    base_calc = dados.get("base_calculo", {}) or {}
    totais = dados.get("totais", {}) or {}
    aviso = dados.get("aviso", {}) or {}
    recibo = dados.get("recibo", {}) or {}
    rodape = dados.get("rodape", {}) or {}

    vencimentos = dados.get("vencimentos", []) or []
    descontos = dados.get("descontos", []) or []

    empregado_linha = as_str(cab.get("empregado_linha"))
    if not empregado_linha:
        cod_empresa = as_str(cab.get("empresa"))
        cod_filial = as_str(cab.get("cod_filial"))
        matricula = as_str(cab.get("codigo_empregado"))
        codigolote = as_str(cab.get("codigolote"))
        cod_cliente = as_str(cab.get("cliente_nome"))
        nome_empregado_notif = truncate_local(cab.get("nome"), 45)

        partes_empregado = [
            cod_empresa,
            cod_filial,
            matricula,
            codigolote,
            cod_cliente,
            nome_empregado_notif,
        ]

        empregado_linha = " - ".join([p for p in partes_empregado if p])
        empregado_linha = f"EMPREGADO: {empregado_linha}"

    funcao_admissao = as_str(cab.get("funcao_admissao"))
    if not funcao_admissao:
        funcao = truncate_local(cab.get("funcao"), 35)
        admissao = fmt_date_br(cab.get("admissao"))
        funcao_admissao = f"{funcao} ADMISSÃO:{admissao}".strip()

    de_aquisicao = as_str(periodo.get("de_aquisicao"))
    ate_aquisicao = as_str(periodo.get("ate_aquisicao"))
    de_abono = as_str(periodo.get("de_abono"))
    ate_abono = as_str(periodo.get("ate_abono"))
    de_gozo = as_str(periodo.get("de_gozo"))
    ate_gozo = as_str(periodo.get("ate_gozo"))
    retorno = as_str(periodo.get("retorno"))

    faltas_nao_just = as_str(base_calc.get("faltas_nao_justificadas") or "00")
    salario_base_1 = fmt_money(base_calc.get("salario_base_1") or 0)
    salario_base_2 = fmt_money(base_calc.get("salario_base_2") or 0)
    dep_irf = as_str(base_calc.get("dep_irf") or "00")

    total_vencimentos = totais.get("total_vencimentos") or sum(
        (as_decimal(v.get("valor")) for v in vencimentos),
        Decimal("0"),
    )
    total_descontos = totais.get("total_descontos") or sum(
        (as_decimal(v.get("valor")) for v in descontos),
        Decimal("0"),
    )
    valor_liquido = totais.get("valor_liquido") or (
        as_decimal(total_vencimentos) - as_decimal(total_descontos)
    )
    valor_extenso = money_extenso_placeholder(valor_liquido, totais.get("valor_extenso"))

    cidade_aviso = as_str(aviso.get("cidade") or rodape.get("cidade") or "")
    data_aviso = as_str(aviso.get("data"))
    cidade_recibo = as_str(recibo.get("cidade") or rodape.get("cidade") or "")
    data_recibo = as_str(recibo.get("data"))

    empresa_assinatura = truncate_local(
        cab.get("empresa_nome") or cab.get("cliente_nome") or cab.get("empresa") or "",
        55,
    )
    nome_empregado = truncate_local(cab.get("nome"), 40)

    banco = as_str(rodape.get("banco"))
    agencia = as_str(rodape.get("agencia"))
    conta = as_str(rodape.get("conta"))
    tipo_conta = as_str(rodape.get("tipo_conta"))
    folha = as_str(rodape.get("folha"))
    cpf_rodape = fmt_cpf(rodape.get("cpf") or cab.get("cpf"))
    codigo_empresa = as_str(rodape.get("codigo_empresa") or cab.get("empresa"))
    endereco_empresa = as_str(rodape.get("endereco_empresa"))
    cidade_empresa = as_str(rodape.get("cidade_empresa") or rodape.get("cidade"))
    uf_empresa = as_str(rodape.get("uf"))
    nome_banco = as_str(rodape.get("nome_banco"))
    cod_filial = as_str(rodape.get("cod_filial"))
    cod_cliente = as_str(rodape.get("cod_cliente"))
    empresa_nome_rodape = truncate_local(
        rodape.get("empresa_nome") or cab.get("empresa_nome") or cab.get("empresa") or "",
        70,
    )
    codigo_cidade = as_str(rodape.get("codigo_cidade"))

    pdf = FPDF(format="A4", unit="mm")
    pdf.set_auto_page_break(auto=False)
    pdf.add_page()
    pdf.set_draw_color(0, 0, 0)
    pdf.set_text_color(0, 0, 0)
    pdf.set_line_width(0.2)

    main_x = 10
    main_y = 10
    main_w = 190
    main_h = 126
    draw_box(pdf, main_x, main_y, main_w, main_h)

    cell_text(pdf, 10, 11.5, 190, 6, "AVISO E RECIBO DE FÉRIAS", 14, "B", "C")
    cell_text(pdf, 10, 17.3, 190, 5, "CAPÍTULO VI - TÍTULO II DA C.L.T.", 11, "", "C")
    cell_text(
        pdf,
        10,
        21.6,
        190,
        3.5,
        "LEI N° 5452 DE 01/05/1943, COM AS ALTERAÇÕES DO DEC. - LEI N° 1.535 DE 13/04/1977",
        6,
        "",
        "C",
    )
    cell_text(
        pdf,
        10,
        25.0,
        190,
        3.5,
        "AVISO PRÉVIO DE FÉRIAS - DE ACORDO COM O ART. 135 DA C.L.T., PARTICIPANDO DO MÍNIMO COM 30 DIAS DE ANTECEDÊNCIA",
        6,
        "",
        "C",
    )

    hline(pdf, 10, 29.5, 200)
    cell_text(pdf, 10, 30.0, 190, 4.5, "NOTIFICAÇÃO", 9, "B", "C")
    hline(pdf, 10, 34.0, 200)

    cell_text(pdf, 12, 35.0, 115, 4, empregado_linha, 5.8, "", "L")
    cell_text(pdf, 129, 35.0, 68, 4, funcao_admissao, 5.8, "", "L")
    hline(pdf, 10, 40.0, 200)

    cell_text(pdf, 10, 40.3, 190, 4.5, "PERÍODO", 9, "B", "C")
    hline(pdf, 10, 44.5, 200)

    linha_periodo(pdf, 47.5, "DE AQUISIÇÃO:.............:", de_aquisicao, ate_aquisicao)
    linha_periodo(
        pdf,
        52.0,
        "DE 1/3 ABONO PECUNIÁRIO...:",
        de_abono,
        ate_abono,
        f"({as_str(periodo.get('dias_abono') or '')})",
    )
    linha_periodo(
        pdf,
        56.5,
        "DE GOZO DAS FÉRIAS........:",
        de_gozo,
        ate_gozo,
        f"({as_str(periodo.get('dias_gozo') or '')})",
    )
    linha_periodo(pdf, 61.0, "DE RETORNO................:", retorno)

    hline(pdf, 10, 66.8, 200)

    cell_text(
        pdf,
        10,
        67.2,
        190,
        4.2,
        "BASE DE CÁLCULO DA REMUNERAÇÃO DAS FÉRIAS",
        8.6,
        "B",
        "C",
    )

    hline(pdf, 10, 71.5, 200)

    linha_base_y = 73.5

    cell_text(pdf, 12, linha_base_y, 42, 4.2, f"FALTAS NÃO JUSTIFICADAS: {faltas_nao_just}", 6.9, "B", "L")
    cell_text(pdf, 61, linha_base_y, 44, 4.2, f"1° SALÁRIO BASE: {salario_base_1}", 6.9, "B", "L")
    cell_text(pdf, 114, linha_base_y, 38, 4.2, f"2° SALÁRIO BASE: {salario_base_2}", 6.9, "B", "L")
    cell_text(pdf, 167, linha_base_y, 28, 4.2, f"DEP. IRF: {dep_irf}", 6.9, "B", "L")

    tabela_y = 80.5
    tabela_h = 50.0
    tabela_gap = 2
    tabela_w = (190 - tabela_gap) / 2

    render_tabela_eventos(
        pdf=pdf,
        x=10,
        y=tabela_y,
        w=tabela_w,
        h=tabela_h,
        titulo="VENCIMENTOS",
        eventos=vencimentos,
        total_label="TOTAL DE VENCIMENTO ----> R$",
        total_valor=total_vencimentos,
    )

    render_tabela_eventos(
        pdf=pdf,
        x=10 + tabela_w + tabela_gap,
        y=tabela_y,
        w=tabela_w,
        h=tabela_h,
        titulo="DESCONTOS",
        eventos=descontos,
        total_label="TOTAL DE DESCONTOS ----> R$",
        total_valor=total_descontos,
    )

    offset_pos_tabelas = 6.0

    cell_text(
        pdf,
        10,
        125.2 + offset_pos_tabelas,
        190,
        4.5,
        f"VALOR TOTAL LÍQUIDO ----> R$ {fmt_money(valor_liquido)}",
        9,
        "B",
        "C",
    )
    hline(pdf, 10, 129.4 + offset_pos_tabelas, 200)

    texto_aviso = (
        f"Pelo presente comunicamos-lhes que, de acordo com a lei, ser-lhe-ão concedidas férias relativas ao período "
        f"acima descrito e a sua disposição fica a importância líquida de R$ {fmt_money(valor_liquido)}"
    )
    multi_text(pdf, 16, 130.6 + offset_pos_tabelas, 178, 3.8, texto_aviso, 6.8, "", "C")

    draw_box(pdf, 18, 140.2 + offset_pos_tabelas, 174, 8.5)
    cell_text(pdf, 20, 141.4 + offset_pos_tabelas, 18, 3.5, "VALOR POR", 7.2, "B", "L")
    cell_text(pdf, 20, 144.8 + offset_pos_tabelas, 18, 3.5, "EXTENSO", 7.2, "B", "L")

    linha1_extenso, linha2_extenso = preencher_com_asteriscos_duas_linhas(
        valor_extenso,
        total_linha_1=86,
        total_linha_2=86,
    )

    pdf.set_font("Arial", "", 7.0)
    pdf.set_xy(40, 142.2 + offset_pos_tabelas)
    pdf.cell(147, 2.8, linha1_extenso, border=0, align="L")

    pdf.set_xy(40, 145.6 + offset_pos_tabelas)
    pdf.cell(147, 2.8, linha2_extenso, border=0, align="L")

    cell_text(pdf, 10, 150.0 + offset_pos_tabelas, 70, 3.5, "a ser paga adiantadamente", 7, "", "L")

    texto_data_aviso = ""
    if cidade_aviso and data_aviso:
        texto_data_aviso = f"{cidade_aviso}, {data_aviso}"
    elif data_aviso:
        texto_data_aviso = data_aviso
    elif cidade_aviso:
        texto_data_aviso = cidade_aviso

    cell_text(pdf, 10, 153.7 + offset_pos_tabelas, 190, 3.5, texto_data_aviso, 7.2, "", "C")

    y_ass_top = 164.8 + offset_pos_tabelas
    hline(pdf, 20, y_ass_top, 100, 0.2)
    hline(pdf, 108, y_ass_top, 190, 0.2)
    cell_text(pdf, 20, y_ass_top + 0.8, 80, 3.5, nome_empregado.upper(), 6.8, "B", "L")
    cell_text(pdf, 108, y_ass_top + 0.8, 82, 3.5, empresa_assinatura.upper(), 6.8, "B", "L")

    rec_y = 173.0 + offset_pos_tabelas
    rec_h = 67.0
    draw_box(pdf, 10, rec_y, 190, rec_h)

    cell_text(pdf, 10, rec_y + 1.0, 190, 5.5, "RECIBO DE FÉRIAS", 14, "B", "C")
    cell_text(pdf, 10, rec_y + 6.2, 190, 4, "DE ACORDO COM O PARÁGRAFO ÚNICO DO ARTIGO 145 DA C.L.T.", 9, "B", "C")
    hline(pdf, 10, rec_y + 10.5, 200)

    texto_recibo_base = as_str(recibo.get("texto") or cab.get("recibo_txt")).strip()

    if texto_recibo_base:
        texto_recibo_1 = f"{texto_recibo_base} {fmt_money(valor_liquido)}"
    else:
        if endereco_empresa:
            texto_recibo_1 = (
                f"Recebi da empresa {empresa_assinatura.upper()}, estabelecida a {endereco_empresa}, "
                f"a importância de R$: {fmt_money(valor_liquido)}"
            )
        else:
            texto_recibo_1 = (
                f"Recebi da empresa {empresa_assinatura.upper()}, "
                f"a importância de R$: {fmt_money(valor_liquido)}"
            )

    multi_text(pdf, 18, rec_y + 11.5, 174, 3.7, texto_recibo_1, 6.8, "", "C")

    draw_box(pdf, 18, rec_y + 20.0, 174, 8.5)
    cell_text(pdf, 20, rec_y + 21.2, 18, 3.5, "VALOR POR", 7.2, "B", "L")
    cell_text(pdf, 20, rec_y + 24.6, 18, 3.5, "EXTENSO", 7.2, "B", "L")

    linha1_extenso_rec, linha2_extenso_rec = preencher_com_asteriscos_duas_linhas(
        valor_extenso,
        total_linha_1=86,
        total_linha_2=86,
    )

    pdf.set_font("Arial", "", 7.0)
    pdf.set_xy(40, rec_y + 22.0)
    pdf.cell(147, 2.8, linha1_extenso_rec, border=0, align="L")

    pdf.set_xy(40, rec_y + 25.4)
    pdf.cell(147, 2.8, linha2_extenso_rec, border=0, align="L")

    texto_recibo_2 = (
        'que me é paga antecipadamente por motivo das minhas férias regulares, ora concedidas e que vou gozar de acordo '
        'com a descrição acima, tudo conforme o aviso que recebi em tempo, no qual dei o meu "CIENTE".\n'
        "Para clareza e documento, firmo o presente recibo, dando a empresa plena e legal quitação."
    )
    multi_text(pdf, 10, rec_y + 29.8, 190, 3.5, texto_recibo_2, 6.7, "", "L")

    texto_data_recibo = ""
    if cidade_recibo and data_recibo:
        texto_data_recibo = f"{cidade_recibo}, {data_recibo}"
    elif data_recibo:
        texto_data_recibo = data_recibo
    elif cidade_recibo:
        texto_data_recibo = cidade_recibo

    cell_text(pdf, 10, rec_y + 46.5, 190, 3.5, texto_data_recibo, 7.2, "", "C")

    hline(pdf, 112, rec_y + 58.5, 190, 0.2)
    cell_text(pdf, 112, rec_y + 59.0, 70, 3.5, nome_empregado.upper(), 6.8, "B", "L")

    hline(pdf, 10, 240.8, 200)
    cell_text(pdf, 10, 241.8, 190, 3.5, "Boas Férias!", 6.8, "", "L")

    rod_y = 249.0

    linha_banco_partes = []
    if banco:
        linha_banco_partes.append(f"Banco:{banco}")
    if agencia:
        linha_banco_partes.append(f"Agencia:{agencia}")
    if conta:
        linha_banco_partes.append(f"Conta:{conta}")
    if tipo_conta:
        linha_banco_partes.append(f"Tipo:{tipo_conta}")

    sufixo_folha = ""
    if folha and nome_banco:
        sufixo_folha = f"(Folha:{folha} {nome_banco.upper()})"
    elif folha:
        sufixo_folha = f"(Folha:{folha})"
    elif nome_banco:
        sufixo_folha = f"({nome_banco.upper()})"

    linha_banco = " ".join(linha_banco_partes)
    if sufixo_folha:
        linha_banco = f"{linha_banco}    {sufixo_folha}".strip()

    if linha_banco:
        cell_text(pdf, 10, rod_y, 190, 3.3, linha_banco, 6.5, "", "L")

    partes_empresa = [
        codigo_empresa.zfill(5) if codigo_empresa else "",
        cod_filial.zfill(3) if cod_filial else "",
        cod_cliente.zfill(2) if cod_cliente else "",
    ]

    prefixo_empresa = " - ".join([p for p in partes_empresa if p])
    linha_empresa = f"{prefixo_empresa} - {empresa_nome_rodape.upper()}".strip(" -")
    cell_text(pdf, 10, rod_y + 4, 190, 3.3, linha_empresa, 6.5, "", "L")

    if cpf_rodape:
        cell_text(pdf, 10, rod_y + 8, 190, 3.3, f"CPF: {cpf_rodape}", 6.5, "", "L")

    cidade_uf = ""
    if cidade_empresa and uf_empresa:
        cidade_uf = f"{cidade_empresa.upper()} - {uf_empresa.upper()}"
    elif cidade_empresa:
        cidade_uf = cidade_empresa.upper()
    elif uf_empresa:
        cidade_uf = uf_empresa.upper()

    linha_cidade = cidade_uf
    if codigo_cidade and linha_cidade:
        linha_cidade = f"{codigo_cidade} - {linha_cidade}"
    elif codigo_cidade:
        linha_cidade = codigo_cidade

    if linha_cidade:
        cell_text(pdf, 10, rod_y + 12, 190, 3.3, linha_cidade, 6.5, "", "L")

    return pdf.output(dest="S").encode("latin-1")

def montar_payload_ferias(cabecalho_row: dict, detalhe_rows: list[dict]) -> dict:
    vencimentos = []
    descontos = []
    det0 = detalhe_rows[0] if detalhe_rows else {}

    for det in detalhe_rows:
        evento = {
            "codigo": _as_str(det.get("codigoevento")),
            "descricao": _as_str(det.get("descricao")),
            "qtde": _as_str(det.get("qtde")),
            "mes_ano": _fmt_competencia_mm_yyyy(det.get("competencia")),
            "valor": det.get("valor") or 0,
            "tipo": _as_str(det.get("tipo")).upper(),
        }

        tipo_evt = _tipo_evento_ferias(det.get("tipo"))

        if tipo_evt == "D":
            descontos.append(evento)
        else:
            vencimentos.append(evento)

    total_vencimentos = sum(
        (_as_decimal(v.get("valor")) for v in vencimentos),
        Decimal("0"),
    )
    total_descontos = sum(
        (_as_decimal(v.get("valor")) for v in descontos),
        Decimal("0"),
    )
    valor_liquido = total_vencimentos - total_descontos

    recibo_txt = _as_str(cabecalho_row.get("recibo_txt"))
    cidade = _as_str(cabecalho_row.get("cidade"))
    uf = _as_str(cabecalho_row.get("uf"))
    cidade_exibicao = cidade.upper() if cidade else ""

    data_aviso_fmt = _fmt_date_extenso_br(cabecalho_row.get("dataaviso"))
    data_pagto_fmt = _fmt_date_extenso_br(cabecalho_row.get("datapagto"))

    dados = {
        "cabecalho": {
            "codigo_empregado": _as_str(cabecalho_row.get("matricula")),
            "pis": "",
            "cpf": _only_digits(cabecalho_row.get("cpf")),
            "nome": _as_str(cabecalho_row.get("nome")),
            "funcao": _as_str(cabecalho_row.get("cargo")),
            "admissao": _fmt_date_curta_br(cabecalho_row.get("dt_admissao")),
            "empresa": _as_str(cabecalho_row.get("cod_empresa")),
            "empresa_nome": _as_str(cabecalho_row.get("empresa")),
            "cliente_nome": _as_str(cabecalho_row.get("cod_cliente")),
            "cnpj": _as_str(cabecalho_row.get("cnpj")),
            "cod_filial": _as_str(cabecalho_row.get("cod_filial")),
            "cod_cliente": _as_str(cabecalho_row.get("cod_cliente")),
            "codigolote": _as_str(cabecalho_row.get("codigolote")),
            "recibo_txt": recibo_txt,
        },
        "periodo": {
            "de_aquisicao": _fmt_date_extenso_br(cabecalho_row.get("dataaquisini")),
            "ate_aquisicao": _fmt_date_extenso_br(cabecalho_row.get("dataaquisfin")),
            "de_abono": _fmt_date_extenso_br(cabecalho_row.get("dataabonoini")),
            "ate_abono": _fmt_date_extenso_br(cabecalho_row.get("dataabonofin")),
            "dias_abono": _as_str(cabecalho_row.get("diasabono")),
            "de_gozo": _fmt_date_extenso_br(cabecalho_row.get("datagozoini")),
            "ate_gozo": _fmt_date_extenso_br(cabecalho_row.get("datagozofin")),
            "dias_gozo": _as_str(cabecalho_row.get("duracao")),
            "retorno": _fmt_date_extenso_br(cabecalho_row.get("dataretorno")),
        },
        "base_calculo": {
            "faltas_nao_justificadas": _as_str(cabecalho_row.get("qtdfaltas")),
            "salario_base_1": cabecalho_row.get("salprimeiroperiodo"),
            "salario_base_2": cabecalho_row.get("salsegundoperiodo"),
            "dep_irf": _as_str(cabecalho_row.get("depirf")),
            "dependentes": _as_str(cabecalho_row.get("dependentes")),
        },
        "vencimentos": vencimentos,
        "descontos": descontos,
        "totais": {
            "total_vencimentos": total_vencimentos,
            "total_descontos": total_descontos,
            "valor_liquido": valor_liquido,
            "valor_extenso": _valor_monetario_por_extenso(valor_liquido),
        },
        "aviso": {
            "cidade": cidade_exibicao,
            "data": data_aviso_fmt,
        },
        "recibo": {
            "cidade": cidade_exibicao,
            "data": data_pagto_fmt,
            "texto": recibo_txt,
        },
        "rodape": {
            "banco": _as_str(det0.get("codigobanco") or det0.get("nomebanco")),
            "agencia": _as_str(det0.get("agenciatitulo")),
            "conta": _as_str(det0.get("contatitulo")),
            "tipo_conta": _as_str(det0.get("tipoctabanco")),
            "folha": _as_str(det0.get("codigocontrato")),
            "nome_banco": _as_str(det0.get("nomebanco")),
            "lote": _as_str(det0.get("lote")),
            "tipofat": _as_str(det0.get("tipofat")),
            "cpf": _only_digits(cabecalho_row.get("cpf")),
            "codigo_empresa": _as_str(cabecalho_row.get("cod_empresa")),
            "cod_filial": _as_str(cabecalho_row.get("cod_filial")),
            "cod_cliente": _as_str(cabecalho_row.get("cod_cliente")),
            "empresa_nome": _as_str(cabecalho_row.get("empresa")),
            "cidade_empresa": _as_str(cabecalho_row.get("cidade")),
            "uf": uf,
            "cnpj": _as_str(cabecalho_row.get("cnpj")),
            "endereco_empresa": _as_str(cabecalho_row.get("endereco_empresa")),
            "cidade": _as_str(cabecalho_row.get("cidade")),
            "codigo_cidade": _as_str(cabecalho_row.get("codigo_cidade")),
        },
    }

    return dados

def gerar_informe_rendimentos_pdf(
    registros: list[dict],
    complementos: list[dict],
    pensoes: list[dict],
) -> bytes:
    if not registros:
        raise ValueError("Nenhum registro encontrado para montar o PDF.")

    GOVERNO_LOGO_PATH = "app/assets/images.jpg"

    pdf = FPDF(format="A4", unit="mm")
    pdf.set_auto_page_break(auto=False)

    def draw_q3_row(
        pdf: FPDF,
        y_row: float,
        desc: str,
        valor: str,
        x_desc: float = 12,
        w_desc: float = 145,
        x_val: float = 160,
        w_val: float = 35,
        line_h: float = 4,
        font_size: int = 8,
    ) -> float:
        pdf.set_xy(x_desc, y_row)
        pdf.set_font("Arial", "", font_size)
        pdf.multi_cell(w_desc, line_h, desc, border=0, align="L")
        y_end = pdf.get_y()
        row_h = y_end - y_row

        if row_h < line_h:
            row_h = line_h

        pdf.set_xy(x_val, y_row)
        pdf.set_font("Arial", "", font_size)
        pdf.cell(w_val, row_h, valor, border=0, align="R")

        return row_h

    for registro in registros:
        pdf.add_page()
        pdf.set_draw_color(0, 0, 0)
        pdf.set_line_width(0.2)

        cpf_cnpj_cliente = _as_str(registro.get("cpf_cnpj_cliente"))
        nome_cliente = truncate(_as_str(registro.get("nome_cliente")), 90)
        matricula = _as_str(registro.get("matricula"))
        cpf = _as_str(registro.get("cpf"))
        nome = truncate(_as_str(registro.get("nome")), 90)
        competencia = _as_str(registro.get("competencia"))

        rendimento_ferias_01 = _fmt_money(registro.get("rendimento_ferias_01"))
        inss_02 = _fmt_money(registro.get("inss_02"))
        prevprivada_03 = _fmt_money(registro.get("prevprivada_03"))
        pensao_04 = _fmt_money(registro.get("pensao_04"))
        irrf_irrfferias_05 = _fmt_money(registro.get("irrf_irrfferias_05"))

        q4_01 = "0,00"
        ajucusto_02 = _fmt_money(registro.get("ajucusto_02"))
        q4_03 = "0,00"
        q4_04 = "0,00"
        q4_05 = "0,00"
        avisoprevio_06 = _fmt_money(registro.get("avisoprevio_06"))
        feriasabono_07 = _fmt_money(registro.get("feriasabono_07"))

        rendimento_irrf_inss_dependente_01 = _fmt_money(
            registro.get("rendimento_irrf_inss_dependente_01")
        )
        irrf_02 = _fmt_money(registro.get("irrf_02"))
        plucro_03 = _fmt_money(registro.get("plucro_03"))

        q6_01 = "0,00"
        q6_02 = "0,00"
        q6_03 = "0,00"
        q6_04 = "0,00"
        q6_05 = "0,00"
        q6_06 = "0,00"
        q6_numero_processo = "0"
        q6_quantidade_meses = "0"
        q6_natureza_rendimento = "0,00"

        abono_pecuniario = _fmt_money(registro.get("abono_pecuniario"))
        rendimentos_isentos = _fmt_money(registro.get("rendimentos_isentos"))

        try:
            ano_calendario = str(int(str(competencia).strip()))
            exercicio = str(int(ano_calendario) + 1)
        except Exception:
            ano_calendario = _as_str(competencia) or "2025"
            try:
                exercicio = str(int(ano_calendario) + 1)
            except Exception:
                exercicio = "2026"

        try:
            pdf.image(GOVERNO_LOGO_PATH, x=10, y=7, w=24)
        except Exception:
            pass

        pdf.set_xy(36, 8)
        pdf.set_font("Arial", "", 8)
        pdf.multi_cell(
            82,
            4,
            "Ministério da Fazenda\n"
            "Secretaria Especial da Receita Federal do Brasil\n"
            "Imposto sobre a Renda da Pessoa Física\n"
            f"Exercício de {exercicio}",
            border=0,
            align="C",
        )

        pdf.set_xy(123, 10)
        pdf.set_font("Arial", "B", 9)
        pdf.multi_cell(
            76,
            4.5,
            "COMPROVANTE DE RENDIMENTOS PAGOS E DE\n"
            "IMPOSTO SOBRE A RENDA RETIDO NA FONTE\n"
            f"ANO-CALENDÁRIO {ano_calendario}",
            border=0,
            align="C",
        )

        pdf.set_xy(10, 29)
        pdf.set_font("Arial", "", 6.5)
        pdf.multi_cell(
            190,
            3.5,
            "Verifique as condições e o prazo para a apresentação da Declaração do Imposto sobre a Renda da Pessoa Física para este ano-calendário no sítio da Secretaria Especial da Receita Federal do Brasil na internet.",
            border=0,
            align="L",
        )

        y = 39
        _draw_box(pdf, 10, y, 190, 18)
        _cell_text(pdf, 11, y + 1, 188, 4, "1. FONTE PAGADORA PESSOA JURÍDICA", 8, "B")
        pdf.line(10, y + 6, 200, y + 6)
        pdf.line(55, y + 6, 55, y + 18)
        _cell_text(pdf, 12, y + 7, 40, 4, "CNPJ", 7, "B")
        _cell_text(pdf, 57, y + 7, 140, 4, "Nome Empresarial", 7, "B")
        _cell_text(pdf, 12, y + 11, 40, 4, cpf_cnpj_cliente, 8, "")
        _cell_text(pdf, 57, y + 11, 140, 4, nome_cliente, 8, "")

        y = 59
        _draw_box(pdf, 10, y, 190, 28)
        _cell_text(pdf, 11, y + 1, 188, 4, "2. PESSOA FÍSICA BENEFICIÁRIA DOS RENDIMENTOS", 8, "B")
        pdf.line(10, y + 6, 200, y + 6)
        pdf.line(55, y + 6, 55, y + 16)
        _cell_text(pdf, 12, y + 7, 40, 4, "CPF", 7, "B")
        _cell_text(pdf, 57, y + 7, 140, 4, "Nome Completo", 7, "B")
        _cell_text(pdf, 12, y + 11, 40, 4, cpf, 8, "")
        _cell_text(pdf, 57, y + 11, 140, 4, nome, 8, "")
        pdf.line(10, y + 16, 200, y + 16)
        _cell_text(pdf, 12, y + 17, 100, 4, "Natureza do Rendimento", 7, "B")
        _cell_text(pdf, 12, y + 21, 120, 4, "Rendimento do Trabalho Assalariado (0561)", 8, "")
        _cell_text(pdf, 150, y + 21, 45, 4, f"Matrícula: {matricula}", 8, "", "R")

        y = 89
        _draw_box(pdf, 10, y, 190, 30)
        _cell_text(
            pdf,
            11,
            y + 1,
            140,
            4,
            "3. RENDIMENTOS TRIBUTÁVEIS, DEDUÇÕES E IMPOSTO SOBRE A RENDA RETIDO NA FONTE",
            8,
            "B",
        )
        _cell_text(pdf, 160, y + 1, 35, 4, "VALORES EM REAIS", 7, "B", "R")
        pdf.line(10, y + 6, 200, y + 6)

        linhas_q3 = [
            ("01. Total dos rendimentos (inclusive férias)", rendimento_ferias_01),
            ("02. Contribuição previdenciária oficial", inss_02),
            (
                "03. Contribuições a entidades de previdência complementar, pública ou privada, e a fundos de aposentadoria programada individual (FAPI)",
                prevprivada_03,
            ),
            ("04. Pensão alimentícia", pensao_04),
            ("05. Imposto sobre a renda retido na fonte", irrf_irrfferias_05),
        ]

        y_line = y + 7
        for desc, valor in linhas_q3:
            row_h = draw_q3_row(pdf, y_line, desc, valor, line_h=3.2, font_size=7)
            y_line += row_h + 0.2

        y = 122
        _draw_box(pdf, 10, y, 190, 30)
        _cell_text(pdf, 11, y + 1, 135, 4, "4. RENDIMENTOS ISENTOS E NÃO TRIBUTÁVEIS", 8, "B")
        _cell_text(pdf, 160, y + 1, 35, 4, "VALORES EM REAIS", 7, "B", "R")
        pdf.line(10, y + 6, 200, y + 6)

        linhas_q4 = [
            ("01. Parcela isenta dos proventos de aposentadoria, reserva remunerada, reforma e pensão (65 anos ou mais)", q4_01),
            ("02. Diárias e ajudas de custo", ajucusto_02),
            ("03. Pensão e proventos de aposentadoria ou reforma por moléstia grave e aposentadoria ou reforma por acidente em serviço", q4_03),
            ("04. Lucros e dividendos apurados a partir de 1996 pagos por pessoa jurídica", q4_04),
            ("05. Valores pagos ao titular ou sócio da microempresa ou empresa de pequeno porte, exceto pró-labore, aluguéis ou serviços prestados", q4_05),
            ("06. Indenizações por rescisão de contrato de trabalho, inclusive a título de PDV, e por acidente de trabalho", avisoprevio_06),
            ("07. Outros", feriasabono_07),
        ]

        y_line = y + 7
        for desc, valor in linhas_q4:
            row_h = draw_q3_row(pdf, y_line, desc, valor, line_h=3.0, font_size=6)
            y_line += row_h + 0.15

        y = 155
        _draw_box(pdf, 10, y, 190, 22)
        _cell_text(
            pdf,
            11,
            y + 1,
            140,
            4,
            "5. RENDIMENTOS SUJEITOS À TRIBUTAÇÃO EXCLUSIVA (RENDIMENTO LÍQUIDO)",
            8,
            "B",
        )
        _cell_text(pdf, 160, y + 1, 35, 4, "VALORES EM REAIS", 7, "B", "R")
        pdf.line(10, y + 6, 200, y + 6)

        linhas_q5 = [
            ("01. Décimo terceiro salário", rendimento_irrf_inss_dependente_01),
            ("02. Imposto sobre a renda retido na fonte sobre décimo terceiro salário", irrf_02),
            ("03. Outros", plucro_03),
        ]

        y_line = y + 7
        for desc, valor in linhas_q5:
            _cell_text(pdf, 12, y_line, 145, 5, desc, 7, "")
            _cell_text(pdf, 160, y_line, 35, 5, valor, 7, "", "R")
            y_line += 3.5

        y = 180
        _draw_box(pdf, 10, y, 190, 28)
        _cell_text(
            pdf,
            11,
            y + 1,
            150,
            4,
            "6. RENDIMENTOS RECEBIDOS ACUMULADAMENTE",
            8,
            "B",
        )
        _cell_text(pdf, 160, y + 1, 35, 4, "VALORES EM REAIS", 7, "B", "R")
        pdf.line(10, y + 6, 200, y + 6)

        linhas_q6 = [
            ("01. Total dos rendimentos tributáveis", q6_01),
            ("02. Exclusão: despesas com a ação judicial", q6_02),
            ("03. Dedução: contribuição previdenciária oficial", q6_03),
            ("04. Dedução: pensão alimentícia", q6_04),
            ("05. Dedução: imposto sobre a renda retido na fonte", q6_05),
            ("06. Rendimentos isentos de pensão, aposentadoria ou reforma por moléstia grave ou acidente em serviço", q6_06),
        ]

        y_line = y + 7
        for desc, valor in linhas_q6:
            row_h = draw_q3_row(pdf, y_line, desc, valor, line_h=2.8, font_size=6)
            y_line += row_h + 0.1

        y = 210
        _draw_box(pdf, 10, y, 190, 16)
        _cell_text(pdf, 11, y + 1, 188, 4, "6.1 INFORMAÇÕES COMPLEMENTARES - RENDIMENTOS RECEBIDOS ACUMULADAMENTE", 7, "B")
        pdf.line(10, y + 6, 200, y + 6)
        pdf.line(85, y + 6, 85, y + 16)
        pdf.line(130, y + 6, 130, y + 16)
        _cell_text(pdf, 12, y + 7, 70, 4, "Número do processo", 6, "B")
        _cell_text(pdf, 87, y + 7, 40, 4, "Quantidade de meses", 6, "B")
        _cell_text(pdf, 132, y + 7, 63, 4, "Natureza do rendimento", 6, "B")
        _cell_text(pdf, 12, y + 11, 70, 4, q6_numero_processo, 7, "")
        _cell_text(pdf, 87, y + 11, 40, 4, q6_quantidade_meses, 7, "")
        _cell_text(pdf, 132, y + 11, 63, 4, q6_natureza_rendimento, 7, "")

        y = 228
        _draw_box(pdf, 10, y, 190, 45)
        _cell_text(pdf, 11, y + 1, 188, 4, "7. INFORMAÇÕES COMPLEMENTARES", 8, "B")
        pdf.line(10, y + 6, 200, y + 6)

        linhas_info = [
            f"Rendimentos Isentos: {rendimentos_isentos}",
            f"Abono Pecuniário: {abono_pecuniario}",
        ]

        if complementos:
            linhas_info.append("Pagamentos a plano privado de assistência à saúde:")
            for comp in complementos:
                cnpj_operadora = _as_str(comp.get("cnpj_operadora"))
                nome_operadora = _as_str(comp.get("nome_operadora"))
                cpf_comp = _as_str(comp.get("cpf"))
                data_nascimento = _fmt_date_br(comp.get("data_nascimento"))
                nome_titular = _as_str(comp.get("nome_titular"))
                valor = _fmt_money(comp.get("valor"))

                linhas_info.append(
                    f"CNPJ da oper: {cnpj_operadora} | Operador: {nome_operadora} | "
                    f"CPF: {cpf_comp} | Dt nasc.: {data_nascimento} | "
                    f"Titular: {nome_titular} | Vlr: {valor}"
                )

        if pensoes:
            for pens in pensoes:
                cpf_pens = _as_str(pens.get("cpf_beneficiario"))
                data_nascimento = _fmt_date_br(pens.get("data_nascimento"))
                nome_pens = _as_str(pens.get("nome"))
                parentesco = _as_str(pens.get("parentesco"))
                valor = _fmt_money(pens.get("valor"))
                e13 = pens.get("e13")

                prefixo = "(Pensionista 13º CPF:" if bool(e13) else "(Pensionista CPF:"

                linhas_info.append(
                    f"{prefixo} {cpf_pens} | Dt nasc.: {data_nascimento} | "
                    f"Nome: {nome_pens} | Parentesco: {parentesco} | Vlr: {valor})"
                )

        info_complementar = "\n".join(linhas_info)
        _cell_text(pdf, 12, y + 7, 184, 2.8, info_complementar, 6, "")

        y = 277
        _draw_box(pdf, 10, y, 190, 14)
        _cell_text(pdf, 11, y + 1, 188, 4, "8. RESPONSÁVEL PELAS INFORMAÇÕES", 8, "B")
        pdf.line(10, y + 6, 200, y + 6)
        pdf.line(120, y + 6, 120, y + 14)
        _cell_text(pdf, 12, y + 7, 105, 4, "Nome", 7, "B")
        _cell_text(pdf, 122, y + 7, 30, 4, "Data", 7, "B")
        _cell_text(pdf, 155, y + 7, 40, 4, "Assinatura", 7, "B")
        _cell_text(pdf, 12, y + 10, 105, 4, "RESPONSÁVEL PELAS INFORMAÇÕES", 7, "")
        _cell_text(pdf, 122, y + 10, 30, 4, datetime.now().strftime("%d/%m/%Y"), 7, "")
        _cell_text(pdf, 155, y + 10, 40, 4, "", 7, "")

        pdf.set_xy(10, 292)
        pdf.set_font("Arial", "", 5.5)
        pdf.multi_cell(
            190,
            2.8,
            "Aprovado pela IN RFB nº 1.682, de 28 de dezembro de 2016.",
            border=0,
            align="L",
        )

    return pdf.output(dest="S").encode("latin-1")


def gerar_recibo(cabecalho: dict, eventos: list[dict], rodape: dict, page_number: int = 1) -> bytes:
    cabecalho["matricula"] = pad_left(cabecalho["matricula"], 6)
    cabecalho["cliente"] = pad_left(cabecalho["cliente"], 5)
    cabecalho["empresa"] = pad_left(cabecalho["empresa"], 3)
    cabecalho["filial"] = pad_left(cabecalho["filial"], 3)

    adm = datetime.fromisoformat(cabecalho["admissao"])
    cabecalho["admissao"] = format_date(adm, "dd/MM/yyyy", locale="pt_BR")
    comp = datetime.strptime(cabecalho["competencia"], "%Y%m")
    cabecalho["competencia"] = format_date(comp, "LLLL/yyyy", locale="pt_BR").capitalize()

    empresa_nome = truncate(cabecalho.get("empresa_nome", ""), 50)
    cliente_nome = truncate(cabecalho.get("cliente_nome", ""), 50)
    funcionario = truncate(cabecalho.get("nome", ""), 30)
    funcao = truncate(cabecalho.get("funcao_nome", ""), 16)

    pdf = FPDF(format="A4", unit="mm")
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 6, "Recibo de Pagamento de Salário", ln=0)
    pdf.ln(6)

    pdf.set_font("Arial", "", 9)
    pdf.cell(120, 5, f"Empresa: {cabecalho['empresa']} - {cabecalho['filial']} {empresa_nome}", ln=0)
    pdf.cell(0, 5, f"Nº Inscrição: {cabecalho['empresa_cnpj']}", ln=1, align="R")
    pdf.cell(120, 5, f"Cliente: {cabecalho['cliente']} {cliente_nome}", ln=0)
    pdf.cell(0, 5, f"Nº Inscrição: {cabecalho['cliente_cnpj']}", ln=1, align="R")
    pdf.ln(3)

    col_widths = [20, 60, 40, 30, 30]
    headers = ["Código", "Nome do Funcionário", "Função", "Admissão", "Competência"]
    pdf.set_font("Arial", "B", 9)
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 6, h)
    pdf.ln(6)

    pdf.set_font("Arial", "", 7)
    vals = [
        cabecalho["matricula"],
        funcionario,
        funcao,
        cabecalho["admissao"],
        cabecalho["competencia"],
    ]
    for w, v in zip(col_widths, vals):
        pdf.cell(w, 6, v)
    pdf.ln(6)

    y_sep = pdf.get_y()
    pdf.set_draw_color(0, 0, 0)
    pdf.set_line_width(0.2)
    pdf.line(pdf.l_margin, y_sep, pdf.w - pdf.r_margin, y_sep)
    pdf.ln(3)

    evt_headers = ["Cód.", "Descrição", "Referência", "Vencimentos", "Descontos"]
    pdf.set_font("Arial", "B", 9)
    for i, (w, h) in enumerate(zip(col_widths, evt_headers)):
        align = "C" if i >= 2 else ""
        pdf.cell(w, 6, h, align=align)
    pdf.ln(6)

    y_start = pdf.get_y()
    pdf.set_font("Arial", "", 9)
    for evt in eventos:
        nome_evt = truncate(evt.get("evento_nome", ""), 30).upper()
        row = [
            str(evt["evento"]),
            nome_evt,
            fmt_num(evt["referencia"]),
            fmt_num(evt["valor"]) if evt["tipo"] == "V" else "",
            fmt_num(evt["valor"]) if evt["tipo"] == "D" else "",
        ]
        for i, (w, v) in enumerate(zip(col_widths, row)):
            align = "R" if i >= 2 else ""
            pdf.cell(w, 6, v, align=align)
        pdf.ln(6)
    y_end = pdf.get_y()

    x0 = pdf.l_margin + col_widths[0] + col_widths[1]
    x1 = x0 + col_widths[2]
    x2 = x1 + col_widths[3]
    pdf.set_line_width(0.2)
    for x in (x0, x1, x2):
        pdf.line(x, y_start, x, y_end)
    pdf.ln(2)

    y = pdf.get_y()
    pdf.line(pdf.l_margin, y, pdf.w - pdf.r_margin, y)
    pdf.ln(3)

    usable = pdf.w - pdf.l_margin - pdf.r_margin
    half = (usable - 10) / 2
    pdf.set_font("Arial", "B", 9)
    pdf.cell(half, 6, "Total Vencimentos", ln=0, align="R")
    pdf.cell(10, 6, "", ln=0)
    pdf.cell(half, 6, "Total Descontos", ln=1, align="R")
    pdf.set_font("Arial", "", 9)
    pdf.cell(half, 6, fmt_num(rodape["total_vencimentos"]), ln=0, align="R")
    pdf.cell(10, 6, "", ln=0)
    pdf.cell(half, 6, fmt_num(rodape["total_descontos"]), ln=1, align="R")
    pdf.ln(3)

    pdf.set_font("Arial", "B", 9)
    pdf.cell(0, 6, f"Valor Líquido »» {fmt_num(rodape['valor_liquido'])}", ln=1, align="R")
    pdf.ln(4)

    y = pdf.get_y()
    pdf.line(pdf.l_margin, y, pdf.w - pdf.r_margin, y)
    pdf.ln(3)

    detalhes = [
        "Salário Base",
        "Sal. Contr. INSS",
        "Base Cálc FGTS",
        "F.G.T.S. do Mês",
        "Base Cálc IRRF",
        "DEP SF",
        "DEP IRF",
    ]
    pdf.set_font("Arial", "B", 8)
    for d in detalhes:
        pdf.cell(28, 5, d)
    pdf.ln(5)

    pdf.set_font("Arial", "", 8)
    footer_vals = [
        f"{fmt_num(rodape['salario_base'])}/M",
        fmt_num(rodape["sal_contr_inss"]),
        fmt_num(rodape["base_calc_fgts"]),
        fmt_num(rodape["fgts_mes"]),
        fmt_num(rodape["base_calc_irrf"]),
        pad_left(rodape["dep_sf"], 2),
        pad_left(rodape["dep_irf"], 2),
    ]
    for v in footer_vals:
        pdf.cell(28, 6, v)
    pdf.ln(10)

    pdf.ln(10)
    y_sig = pdf.get_y()
    pdf.set_line_width(0.2)
    pdf.line(pdf.l_margin, y_sig, pdf.l_margin + 80, y_sig)
    pdf.ln(2)
    pdf.set_font("Arial", "", 9)
    pdf.cell(80, 6, funcionario, ln=0)
    pdf.cell(0, 6, "Data: ____/____/____", ln=1, align="R")

    return pdf.output(dest="S").encode("latin-1")


@router.post("/documents/holerite/competencias")
async def listar_competencias_holerite(
    request: Request,
    cpf: Optional[str] = Query(None, description="CPF (com ou sem máscara)"),
    matricula: Optional[str] = Query(None, description="Matrícula exata"),
    empresa: Optional[str] = Query(None, description="Código da empresa/cliente"),
    cliente: Optional[str] = Query(None, description="Alias antigo do código do cliente"),
    db: Session = Depends(get_db),
):
    empresa = empresa or cliente

    if not cpf or not matricula or not empresa:
        try:
            body = await request.json()
            if isinstance(body, dict):
                cpf = cpf or body.get("cpf")
                matricula = matricula or body.get("matricula")
                empresa = empresa or body.get("empresa") or body.get("cliente")
        except Exception:
            pass

    if not cpf or not matricula or not empresa:
        raise HTTPException(
            status_code=422,
            detail="Informe 'cpf', 'matricula' e 'empresa' (na querystring ou no body JSON).",
        )

    params: Dict[str, Any] = {
        "cpf": str(cpf).strip(),
        "matricula": str(matricula).strip(),
        "empresa": str(empresa).strip(),
    }

    sql_lista_comp = text("""
        WITH norm_evt AS (
            SELECT DISTINCT
                   regexp_replace(TRIM(e.competencia), '[^0-9]', '', 'g') AS comp,
                   e.lote
            FROM tb_holerite_eventos e
            WHERE TRIM(e.cpf::text) = TRIM(:cpf)
              AND TRIM(e.matricula::text) = TRIM(:matricula)
              AND TRIM(e.cliente::text)   = TRIM(:empresa)
              AND e.competencia IS NOT NULL
        ),
        norm_cab AS (
            SELECT DISTINCT
                   regexp_replace(TRIM(c.competencia), '[^0-9]', '', 'g') AS comp,
                   c.lote
            FROM tb_holerite_cabecalhos c
            WHERE TRIM(c.cpf::text) = TRIM(:cpf)
              AND TRIM(c.matricula::text) = TRIM(:matricula)
              AND TRIM(c.cliente::text)   = TRIM(:empresa)
              AND coalesce(c.pagamento, '2999-12-31')::date < current_date - 1
              AND c.competencia IS NOT NULL
        ),
        norm_rod AS (
            SELECT DISTINCT
                   regexp_replace(TRIM(r.competencia), '[^0-9]', '', 'g') AS comp,
                   r.lote
            FROM tb_holerite_rodapes r
            WHERE TRIM(r.cpf::text) = TRIM(:cpf)
              AND TRIM(r.matricula::text) = TRIM(:matricula)
              AND TRIM(r.cliente::text)   = TRIM(:empresa)
              AND r.competencia IS NOT NULL
        ),
        valid AS (
            SELECT e.comp
            FROM norm_evt e
            JOIN norm_cab c ON c.comp = e.comp AND c.lote = e.lote
            JOIN norm_rod r ON r.comp = e.comp AND r.lote = e.lote
            GROUP BY e.comp
        )
        SELECT
          CAST(SUBSTRING(comp, 1, 4) AS int) AS ano,
          CAST(SUBSTRING(comp, 5, 2) AS int) AS mes
        FROM valid
        WHERE comp ~ '^[0-9]{6}$'
        ORDER BY ano DESC, mes DESC
    """)

    rows = db.execute(sql_lista_comp, params).fetchall()
    competencias = [{"ano": r[0], "mes": r[1]} for r in rows if r[0] is not None and r[1] is not None]

    if not competencias:
        raise HTTPException(status_code=404, detail="Nenhuma competência encontrada para os parâmetros informados.")

    return {"competencias": competencias}


@router.post("/documents/holerite/buscar")
def buscar_holerite(payload: BuscarHolerite = Body(...), db: Session = Depends(get_db)):
    cpf = (payload.cpf or "").strip()
    matricula = (payload.matricula or "").strip()
    competencia = (payload.competencia or "").strip()
    empresa = (payload.empresa or "").strip()

    if not cpf or not matricula or not competencia or not empresa:
        raise HTTPException(status_code=422, detail="Informe cpf, matricula, competencia e empresa.")

    filtro_comp_evt = """
      regexp_replace(TRIM(e.competencia), '[^0-9]', '', 'g') =
      regexp_replace(TRIM(:competencia),  '[^0-9]', '', 'g')
    """
    filtro_comp_cab = filtro_comp_evt.replace("e.", "c.")
    filtro_comp_rod = filtro_comp_evt.replace("e.", "r.")

    params_base = {
        "cpf": cpf,
        "matricula": matricula,
        "competencia": competencia,
        "empresa": empresa,
    }

    sql_uuids = text(f"""
        WITH cab AS (
            SELECT DISTINCT c.uuid::text AS uuid
              FROM tb_holerite_cabecalhos c
             WHERE TRIM(c.cpf::text)       = TRIM(:cpf)
               AND TRIM(c.matricula::text) = TRIM(:matricula)
               AND TRIM(c.cliente::text)   = TRIM(:empresa)
               AND coalesce(c.pagamento, '2999-12-31')::date < current_date - 1
               AND {filtro_comp_cab}
        ),
        rod AS (
            SELECT DISTINCT r.uuid::text AS uuid
              FROM tb_holerite_rodapes r
             WHERE TRIM(r.cpf::text)       = TRIM(:cpf)
               AND TRIM(r.matricula::text) = TRIM(:matricula)
               AND TRIM(r.cliente::text)   = TRIM(:empresa)
               AND {filtro_comp_rod}
        ),
        evt AS (
            SELECT DISTINCT e.uuid::text AS uuid
              FROM tb_holerite_eventos e
             WHERE TRIM(e.cpf::text)       = TRIM(:cpf)
               AND TRIM(e.matricula::text) = TRIM(:matricula)
               AND TRIM(e.cliente::text)   = TRIM(:empresa)
               AND {filtro_comp_evt}
        )
        SELECT cab.uuid
          FROM cab
          JOIN rod USING (uuid)
          JOIN evt USING (uuid)
         ORDER BY cab.uuid DESC
    """)

    uuid_rows = db.execute(sql_uuids, params_base).fetchall()
    uuids = [r[0] for r in uuid_rows if r and r[0]]

    if not uuids:
        raise HTTPException(
            status_code=404,
            detail="Nenhum holerite completo encontrado (cabecalho+rodape+eventos) para os critérios informados."
        )

    comp_norm_input = _only_yyyymm(_normaliza_anomes(competencia) or competencia)

    def _table_exists(schema: str, table: str) -> bool:
        q = text("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema=:schema AND table_name=:table
            LIMIT 1
        """)
        return db.execute(q, {"schema": schema, "table": table}).first() is not None

    def _column_exists(schema: str, table: str, column: str) -> bool:
        q = text("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema=:schema AND table_name=:table AND column_name=:column
            LIMIT 1
        """)
        return db.execute(q, {"schema": schema, "table": table, "column": column}).first() is not None

    schema_status = "public"
    table_try = "tb_satus_doc"
    table_fbk = "tb_status_doc"

    table_name = None
    if _table_exists(schema_status, table_try):
        table_name = f"{schema_status}.{table_try}"
    elif _table_exists(schema_status, table_fbk):
        table_name = f"{schema_status}.{table_fbk}"

    aceito_bool = False
    if table_name:
        raw_table = table_name.split(".")[1]
        has_comp = _column_exists(schema_status, raw_table, "competencia")
        has_data = _column_exists(schema_status, raw_table, "data")
        has_hora = _column_exists(schema_status, raw_table, "hora")

        if has_comp:
            comp_norm_expr = "regexp_replace(TRIM(sd.competencia), '[^0-9]', '', 'g')"
        elif has_data:
            comp_norm_expr = """
                COALESCE(
                    to_char(
                        CASE
                            WHEN sd.data ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                            THEN to_date(sd.data, 'YYYY-MM-DD')
                            ELSE NULL
                        END,
                        'YYYYMM'
                    ),
                    substr(regexp_replace(TRIM(sd.data), '[^0-9]', '', 'g'), 1, 6)
                )
            """
        else:
            comp_norm_expr = "NULL"

        order_parts = ["sd.id DESC NULLS LAST"]
        if has_data:
            order_parts.append("sd.data DESC NULLS LAST")
        if has_hora:
            order_parts.append("sd.hora DESC NULLS LAST")
        order_by_sql = ", ".join(order_parts)

        sql_aceite = text(f"""
            SELECT (ARRAY_AGG(sd.aceito ORDER BY {order_by_sql}))[1] AS aceito
              FROM {table_name} sd
             WHERE TRIM(sd.cpf::text)       = TRIM(:cpf)
               AND TRIM(sd.matricula::text) = TRIM(:matricula)
               AND {comp_norm_expr}         = :comp_norm
        """)
        try:
            val = db.execute(sql_aceite, {"cpf": cpf, "matricula": matricula, "comp_norm": comp_norm_input}).scalar()
            aceito_bool = bool(val) if val is not None else False
        except Exception:
            db.rollback()
            aceito_bool = False

    holerites = []

    for uuid in uuids:
        sql_cab = text("""
            SELECT
                c.*,
                c.uuid::text AS uuid,
                UPPER(TRIM(c.tipo_calculo::text)) AS tipo_calculo
            FROM tb_holerite_cabecalhos c
            WHERE c.uuid::text = :uuid
            LIMIT 1
        """)
        cab_res = db.execute(sql_cab, {"uuid": uuid})
        cab_row = cab_res.first()
        if not cab_row:
            continue
        cabecalho = dict(zip(cab_res.keys(), cab_row))

        tc = (cabecalho.get("tipo_calculo") or "").strip().upper()
        cabecalho["tipo_calculo"] = tc if tc in ("A", "P") else tc

        sql_rod = text("""
            SELECT *
              FROM tb_holerite_rodapes r
             WHERE r.uuid::text = :uuid
             LIMIT 1
        """)
        rod_res = db.execute(sql_rod, {"uuid": uuid})
        rod_row = rod_res.first()
        if not rod_row:
            continue
        rodape = dict(zip(rod_res.keys(), rod_row))

        sql_evt = text("""
            SELECT *
              FROM tb_holerite_eventos e
             WHERE e.uuid::text = :uuid
             ORDER BY tipo_calculo, evento
        """)
        evt_res = db.execute(sql_evt, {"uuid": uuid})
        eventos = [dict(zip(evt_res.keys(), row)) for row in evt_res.fetchall()]
        if not eventos:
            continue

        def _ord_tc(tc: str) -> int:
            tc = (tc or "").upper()
            return 1 if tc == "A" else (2 if tc == "P" else 99)

        try:
            eventos_sorted = sorted(eventos, key=lambda e: (_ord_tc(e.get("tipo_calculo")), e.get("evento")))
        except Exception:
            eventos_sorted = eventos

        grupos = {"A": [], "P": []}
        for e in eventos_sorted:
            tc = (e.get("tipo_calculo") or "").upper()
            if tc in grupos:
                grupos[tc].append(e)

        documentos = []
        if grupos["A"]:
            documentos.append({"tipo_calculo": "A", "descricao": "Adiantamento", "eventos": grupos["A"]})
        if grupos["P"]:
            documentos.append({"tipo_calculo": "P", "descricao": "Pagamento", "eventos": grupos["P"]})

        tc = (cabecalho.get("tipo_calculo") or "").strip().upper()

        holerites.append({
            "uuid": uuid,
            "aceito": aceito_bool,
            "tipo_calculo": tc,
            "descricao": "Adiantamento" if tc == "A" else ("Pagamento" if tc == "P" else None),
            "cabecalho": cabecalho,
            "rodape": rodape,
            "documentos": documentos,
        })

    if not holerites:
        raise HTTPException(status_code=404, detail="UUIDs encontrados, mas não foi possível montar holerites completos.")

    return {
        "tipo": "holerite",
        "competencia_utilizada": competencia,
        "empresa_utilizada": empresa,
        "cpf": cpf,
        "matricula": matricula,
        "total": len(holerites),
        "holerites": holerites,
    }


@router.post("/documents/holerite/montar")
def montar_holerite(payload: MontarHolerite, db: Session = Depends(get_db)):
    params = {
        "matricula": payload.matricula,
        "competencia": payload.competencia,
        "lote": payload.lote,
        "cpf": payload.cpf,
    }

    sql_cabecalho = text("""
        SELECT empresa, filial, empresa_nome, empresa_cnpj,
               cliente, cliente_nome, cliente_cnpj,
               matricula, nome, funcao_nome, admissao,
               competencia, lote,
               uuid::text AS uuid
        FROM tb_holerite_cabecalhos
        WHERE matricula   = :matricula
          AND competencia = :competencia
          AND lote        = :lote
          AND cpf         = :cpf
    """)
    cab_res = db.execute(sql_cabecalho, params)
    cab_row = cab_res.first()
    if not cab_row:
        raise HTTPException(status_code=404, detail="Cabeçalho não encontrado")
    cabecalho = dict(zip(cab_res.keys(), cab_row))

    sql_eventos = text("""
        SELECT evento, evento_nome, referencia, valor, tipo
        FROM tb_holerite_eventos
        WHERE matricula   = :matricula
          AND competencia = :competencia
          AND lote        = :lote
          AND cpf         = :cpf
        ORDER BY evento
    """)
    evt_res = db.execute(sql_eventos, params)
    eventos = [dict(zip(evt_res.keys(), row)) for row in evt_res.fetchall()]

    if not eventos:
        return Response(status_code=204)

    for evt in eventos:
        tipo = evt.get("tipo", "").upper()
        if tipo not in ("V", "D"):
            raise HTTPException(status_code=400, detail=f"Tipo de evento inválido: {tipo}")
        evt["tipo"] = tipo

    sql_rodape = text("""
        SELECT total_vencimentos, total_descontos,
               valor_liquido, salario_base,
               sal_contr_inss, base_calc_fgts,
               fgts_mes, base_calc_irrf,
               dep_sf, dep_irf
        FROM tb_holerite_rodapes
        WHERE matricula   = :matricula
          AND competencia = :competencia
          AND lote        = :lote
          AND cpf         = :cpf
    """)
    rod_res = db.execute(sql_rodape, params)
    rod_row = rod_res.first()
    if not rod_row:
        raise HTTPException(status_code=404, detail="Rodapé não encontrado")
    rodape = dict(zip(rod_res.keys(), rod_row))

    raw_pdf = gerar_recibo(cabecalho, eventos, rodape)
    pdf_base64 = base64.b64encode(raw_pdf).decode("utf-8")

    return {
        "uuid": cabecalho.get("uuid"),
        "cabecalho": cabecalho,
        "eventos": eventos,
        "rodape": rodape,
        "pdf_base64": pdf_base64,
    }

@router.post("/documents/informe-rendimentos/competencias")
def listar_competencias_informe_rendimentos(
    payload: BuscarCompetenciasInformeRendimentos = Body(...),
    db: Session = Depends(get_db),
):
    cpf = _normalizar_cpf(payload.cpf)

    if not cpf:
        raise HTTPException(status_code=422, detail="Informe cpf.")

    sql = text("""
        SELECT DISTINCT
            regexp_replace(TRIM(competencia::text), '[^0-9]', '', 'g') AS comp
        FROM public.tb_informe_rendimentos
        WHERE LPAD(regexp_replace(TRIM(cpf::text), '[^0-9]', '', 'g'), 11, '0') = :cpf
          AND competencia IS NOT NULL
          AND regexp_replace(TRIM(competencia::text), '[^0-9]', '', 'g') ~ '^[0-9]{4}$'
        ORDER BY comp DESC
    """)

    rows = db.execute(sql, {"cpf": cpf}).fetchall()

    competencias = [
        {"ano": int(r[0])}
        for r in rows
        if r[0] and len(r[0]) == 4
    ]

    if not competencias:
        raise HTTPException(
            status_code=404,
            detail="Nenhuma competência encontrada para os parâmetros informados."
        )

    return {"competencias": competencias}


@router.post("/documents/informe-rendimentos/buscar")
def buscar_informe_rendimentos(
    payload: BuscarInformeRendimentos = Body(...),
    db: Session = Depends(get_db),
):
    cpf = _normalizar_cpf(payload.cpf)
    competencia = _as_str(payload.competencia)

    if not cpf or not competencia:
        raise HTTPException(status_code=422, detail="Informe cpf e competencia.")

    sql = text("""
        SELECT
            MAX(codigo_empresa) AS codigo_empresa,
            MAX(codigo_cliente) AS codigo_cliente,
            CASE
                WHEN COUNT(DISTINCT cpf_cnpj_cliente) = 1 THEN MAX(cpf_cnpj_cliente)
                ELSE 'MÚLTIPLOS CNPJS'
            END AS cpf_cnpj_cliente,
            MAX(nome_cliente) AS nome_cliente,
            MAX(matricula) AS matricula,
            MAX(cpf) AS cpf,
            MAX(nome) AS nome,
            MAX(competencia) AS competencia,
            SUM(COALESCE(rendimento_ferias_01, 0)) AS rendimento_ferias_01,
            SUM(COALESCE(inss_02, 0)) AS inss_02,
            SUM(COALESCE(prevprivada_03, 0)) AS prevprivada_03,
            SUM(COALESCE(pensao_04, 0)) AS pensao_04,
            SUM(COALESCE(irrf_irrfferias_05, 0)) AS irrf_irrfferias_05,
            SUM(COALESCE(ajucusto_02, 0)) AS ajucusto_02,
            SUM(COALESCE(avisoprevio_06, 0)) AS avisoprevio_06,
            SUM(COALESCE(feriasabono_07, 0)) AS feriasabono_07,
            SUM(COALESCE(rendimento_irrf_inss_dependente_01, 0)) AS rendimento_irrf_inss_dependente_01,
            SUM(COALESCE(irrf_02, 0)) AS irrf_02,
            SUM(COALESCE(plucro_03, 0)) AS plucro_03,
            SUM(COALESCE(abono_pecuniario, 0)) AS abono_pecuniario,
            SUM(COALESCE(rendimentos_isentos, 0)) AS rendimentos_isentos
        FROM public.tb_informe_rendimentos
        WHERE LPAD(regexp_replace(TRIM(cpf::text), '[^0-9]', '', 'g'), 11, '0') = :cpf
          AND regexp_replace(TRIM(competencia::text), '[^0-9]', '', 'g') =
              regexp_replace(TRIM(:competencia), '[^0-9]', '', 'g')
    """)

    row = db.execute(sql, {"cpf": cpf, "competencia": competencia}).first()

    if not row:
        raise HTTPException(
            status_code=404,
            detail="Nenhum informe de rendimentos encontrado para os critérios informados."
        )

    informe = dict(row._mapping)

    if not informe.get("cpf"):
        raise HTTPException(
            status_code=404,
            detail="Nenhum informe de rendimentos encontrado para os critérios informados."
        )

    sql_complementos = text("""
        SELECT
            codigo_empresa,
            cnpj_operadora,
            nome_operadora,
            cpf,
            data_nascimento,
            nome_titular,
            valor,
            competencia
        FROM public.tb_informe_rendimentos_complementos_beneficios
        WHERE LPAD(regexp_replace(TRIM(cpf::text), '[^0-9]', '', 'g'), 11, '0') = :cpf
          AND regexp_replace(TRIM(competencia::text), '[^0-9]', '', 'g') =
              regexp_replace(TRIM(:competencia), '[^0-9]', '', 'g')
        ORDER BY nome_titular, data_nascimento
    """)

    comp_rows = db.execute(
        sql_complementos,
        {"cpf": cpf, "competencia": competencia},
    ).fetchall()

    complementos = [dict(r._mapping) for r in comp_rows]

    return {
        "tipo": "informe_rendimentos",
        "cpf": cpf,
        "competencia": competencia,
        "total": 1,
        "informes": [informe],
        "complementos": complementos,
    }


@router.post("/documents/informe-rendimentos/montar")
def montar_informe_rendimentos(
    payload: MontarInformeRendimentos = Body(...),
    db: Session = Depends(get_db),
):
    cpf = _normalizar_cpf(payload.cpf)
    competencia = _as_str(payload.competencia)

    if not cpf or not competencia:
        raise HTTPException(status_code=422, detail="Informe cpf e competencia.")

    sql = text("""
        SELECT
            MAX(codigo_empresa) AS codigo_empresa,
            MAX(codigo_cliente) AS codigo_cliente,
            CASE
                WHEN COUNT(DISTINCT cpf_cnpj_cliente) = 1 THEN MAX(cpf_cnpj_cliente)
                ELSE 'MÚLTIPLOS CNPJS'
            END AS cpf_cnpj_cliente,
            MAX(nome_cliente) AS nome_cliente,
            MAX(matricula) AS matricula,
            MAX(cpf) AS cpf,
            MAX(nome) AS nome,
            MAX(competencia) AS competencia,
            SUM(COALESCE(rendimento_ferias_01, 0)) AS rendimento_ferias_01,
            SUM(COALESCE(inss_02, 0)) AS inss_02,
            SUM(COALESCE(prevprivada_03, 0)) AS prevprivada_03,
            SUM(COALESCE(pensao_04, 0)) AS pensao_04,
            SUM(COALESCE(irrf_irrfferias_05, 0)) AS irrf_irrfferias_05,
            SUM(COALESCE(ajucusto_02, 0)) AS ajucusto_02,
            SUM(COALESCE(avisoprevio_06, 0)) AS avisoprevio_06,
            SUM(COALESCE(feriasabono_07, 0)) AS feriasabono_07,
            SUM(COALESCE(rendimento_irrf_inss_dependente_01, 0)) AS rendimento_irrf_inss_dependente_01,
            SUM(COALESCE(irrf_02, 0)) AS irrf_02,
            SUM(COALESCE(plucro_03, 0)) AS plucro_03,
            SUM(COALESCE(abono_pecuniario, 0)) AS abono_pecuniario,
            SUM(COALESCE(rendimentos_isentos, 0)) AS rendimentos_isentos
        FROM public.tb_informe_rendimentos
        WHERE LPAD(regexp_replace(TRIM(cpf::text), '[^0-9]', '', 'g'), 11, '0') = :cpf
          AND regexp_replace(TRIM(competencia::text), '[^0-9]', '', 'g') =
              regexp_replace(TRIM(:competencia), '[^0-9]', '', 'g')
    """)

    row = db.execute(sql, {"cpf": cpf, "competencia": competencia}).first()

    if not row:
        raise HTTPException(
            status_code=404,
            detail="Nenhum informe de rendimentos encontrado para os critérios informados."
        )

    informe = dict(row._mapping)

    if not informe.get("cpf"):
        raise HTTPException(
            status_code=404,
            detail="Nenhum informe de rendimentos encontrado para os critérios informados."
        )

    sql_complementos = text("""
        SELECT
            codigo_empresa,
            cnpj_operadora,
            nome_operadora,
            cpf,
            data_nascimento,
            nome_titular,
            valor,
            competencia
        FROM public.tb_informe_rendimentos_complementos_beneficios
        WHERE LPAD(regexp_replace(TRIM(cpf::text), '[^0-9]', '', 'g'), 11, '0') = :cpf
          AND regexp_replace(TRIM(competencia::text), '[^0-9]', '', 'g') =
              regexp_replace(TRIM(:competencia), '[^0-9]', '', 'g')
        ORDER BY nome_titular, data_nascimento
    """)

    comp_rows = db.execute(
        sql_complementos,
        {"cpf": cpf, "competencia": competencia},
    ).fetchall()

    complementos = [dict(r._mapping) for r in comp_rows]

    sql_pensoes = text("""
        SELECT
            cpf_beneficiario,
            data_nascimento,
            nome,
            parentesco,
            e13,
            competencia,
            valor
        FROM public.tb_informe_rendimentos_complementos_pensoes
        WHERE LPAD(regexp_replace(TRIM(cpf_titular::text), '[^0-9]', '', 'g'), 11, '0') = :cpf
          AND regexp_replace(TRIM(competencia::text), '[^0-9]', '', 'g') =
              regexp_replace(TRIM(:competencia), '[^0-9]', '', 'g')
        ORDER BY nome, data_nascimento
    """)

    pens_rows = db.execute(
        sql_pensoes,
        {"cpf": cpf, "competencia": competencia},
    ).fetchall()

    pensionistas = [dict(r._mapping) for r in pens_rows]

    raw_pdf = gerar_informe_rendimentos_pdf([informe], complementos, pensionistas)
    pdf_base64 = base64.b64encode(raw_pdf).decode("utf-8")

    return {
        "tipo": "informe_rendimentos",
        "cpf": cpf,
        "competencia": competencia,
        "total": 1,
        "informes": [informe],
        "complementos": complementos,
        "pensionistas": pensionistas,
        "pdf_base64": pdf_base64,
    }

@router.post("/documents/beneficios/buscar")
def buscar_beneficios(payload: dict = Body(...), db: Session = Depends(get_db)):
    cpf = (payload.get("cpf") or "").strip()
    matricula = (payload.get("matricula") or "").strip()
    competencia = (payload.get("competencia") or "").strip()
    empresa = (payload.get("empresa") or payload.get("cliente") or "").strip()

    if not cpf or not matricula or not competencia or not empresa:
        raise HTTPException(status_code=422, detail="Informe cpf, matricula, competencia e empresa.")

    sql_benef = text("""
        SELECT
            uuid::text AS uuid,
            empresa,
            filial,
            cliente,
            matricula,
            cpf,
            competencia,
            lote,
            codigo_beneficio,
            descricao_beneficio,
            tipo_beneficio,
            valor_unitario,
            dia,
            mes,
            valor_total
        FROM public.tb_beneficio_detalhes
        WHERE TRIM(cpf::text)       = TRIM(:cpf)
        AND TRIM(matricula::text) = TRIM(:matricula)
        AND TRIM(cliente::text)   = TRIM(:empresa)
        AND regexp_replace(TRIM(competencia), '[^0-9]', '', 'g') =
            regexp_replace(TRIM(:competencia),  '[^0-9]', '', 'g')
        ORDER BY tipo_beneficio, codigo_beneficio
    """)
    benef_rows = db.execute(
        sql_benef,
        {"cpf": cpf, "matricula": matricula, "competencia": competencia, "empresa": empresa},
    ).fetchall()

    if not benef_rows:
        raise HTTPException(status_code=404, detail="Nenhum benefício encontrado para os critérios informados.")

    beneficios = [dict(r._mapping) for r in benef_rows]
    uuid = beneficios[0].get("uuid") if beneficios else None

    return {
        "uuid": uuid,
        "cpf": cpf,
        "matricula": matricula,
        "competencia": competencia,
        "empresa": empresa,
        "beneficios": beneficios,
    }


@router.post("/documents/beneficios/competencias")
async def listar_competencias_beneficios(
    request: Request,
    cpf: Optional[str] = Query(None),
    matricula: Optional[str] = Query(None),
    empresa: Optional[str] = Query(None),
    cliente: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    empresa = empresa or cliente

    if not cpf or not matricula or not empresa:
        try:
            body = await request.json()
            if isinstance(body, dict):
                cpf = cpf or body.get("cpf")
                matricula = matricula or body.get("matricula")
                empresa = empresa or body.get("empresa") or body.get("cliente")
        except Exception:
            pass

    if not cpf or not matricula or not empresa:
        raise HTTPException(
            status_code=422,
            detail="Informe 'cpf', 'matricula' e 'empresa' (na querystring ou no body JSON)."
        )

    params: Dict[str, Any] = {
        "cpf": str(cpf).strip(),
        "matricula": str(matricula).strip(),
        "empresa": str(empresa).strip(),
    }

    sql_lista_comp = text("""
        SELECT DISTINCT
            regexp_replace(TRIM(competencia), '[^0-9]', '', 'g') AS comp
        FROM public.tb_beneficio_detalhes
        WHERE TRIM(cpf::text)       = TRIM(:cpf)
        AND TRIM(matricula::text) = TRIM(:matricula)
        AND TRIM(cliente::text)   = TRIM(:empresa)
        AND competencia IS NOT NULL
        AND regexp_replace(TRIM(competencia), '[^0-9]', '', 'g') ~ '^[0-9]{6}$'
        ORDER BY comp DESC
    """)

    rows = db.execute(sql_lista_comp, params).fetchall()
    competencias = [
        {"ano": int(r[0][:4]), "mes": int(r[0][4:6])}
        for r in rows if r[0] and len(r[0]) == 6
    ]

    if not competencias:
        raise HTTPException(status_code=404, detail="Nenhuma competência encontrada para os parâmetros informados.")

    return {"competencias": competencias}


@router.post("/documents/beneficios/montar")
def montar_beneficio(payload: dict = Body(...), db: Session = Depends(get_db)):
    def as_str(v) -> str:
        if v is None:
            return ""
        return str(v).strip()

    def as_int(v, default: int = 0) -> int:
        try:
            if v is None or v == "":
                return default
            return int(float(v))
        except Exception:
            return default

    def as_decimal(v) -> Decimal:
        if v is None or v == "":
            return Decimal("0")
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError):
            return Decimal("0")

    def fmt_money(v) -> str:
        d = as_decimal(v)
        s = f"{d:,.2f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")

    cpf = as_str(payload.get("cpf"))
    matricula = as_str(payload.get("matricula"))
    competencia = as_str(payload.get("competencia"))

    if not cpf or not matricula or not competencia:
        raise HTTPException(status_code=422, detail="Informe cpf, matricula e competencia.")

    sql_benef = text("""
        SELECT
            uuid::text AS uuid,
            empresa,
            filial,
            cliente,
            cpf,
            matricula,
            competencia,
            lote,
            codigo_beneficio,
            descricao_beneficio,
            tipo_beneficio,
            valor_unitario,
            dia,
            mes,
            valor_total
        FROM public.tb_beneficio_detalhes
        WHERE TRIM(cpf::text)       = TRIM(:cpf)
        AND TRIM(matricula::text)  = TRIM(:matricula)
        AND regexp_replace(TRIM(competencia), '[^0-9]', '', 'g') =
            regexp_replace(TRIM(:competencia),  '[^0-9]', '', 'g')
        ORDER BY tipo_beneficio, codigo_beneficio
    """)

    rows = db.execute(
        sql_benef,
        {"cpf": cpf, "matricula": matricula, "competencia": competencia}
    ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="Nenhum benefício encontrado para os critérios informados.")

    beneficios = [dict(r._mapping) for r in rows]

    info = beneficios[0]
    empresa = info.get("empresa", "")
    filial = info.get("filial", "")
    cliente = info.get("cliente", "")
    lote = info.get("lote", "")
    competencia = info.get("competencia", "")
    cpf = info.get("cpf", "")
    matricula = info.get("matricula", "")

    total_geral = sum((as_decimal(b.get("valor_total")) for b in beneficios), Decimal("0"))

    pdf = FPDF(format="A4", unit="mm")
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 6, "Recibo de Benefícios", ln=1)
    pdf.ln(2)

    pdf.set_font("Arial", "", 9)
    pdf.cell(100, 5, f"Empresa: {empresa} - Filial: {filial}", ln=0)
    pdf.cell(0, 5, f"Cliente: {cliente}", ln=1)
    pdf.cell(0, 5, f"Competência: {competencia}   Lote: {lote}", ln=1)
    pdf.cell(0, 5, f"CPF: {cpf}   Matrícula: {matricula}", ln=1)
    pdf.ln(3)

    pdf.set_font("Arial", "B", 9)
    pdf.cell(15, 6, "Cód.", border=1)
    pdf.cell(80, 6, "Descrição do Benefício", border=1)
    pdf.cell(35, 6, "Tipo de Benefício", border=1)
    pdf.cell(18, 6, "Unitário", border=1, align="R")
    pdf.cell(12, 6, "Dia", border=1, align="R")
    pdf.cell(12, 6, "Mês", border=1, align="R")
    pdf.cell(18, 6, "Total", border=1, align="R")
    pdf.ln(6)

    pdf.set_font("Arial", "", 9)

    for b in beneficios:
        codigo = as_str(b.get("codigo_beneficio"))
        desc = as_str(b.get("descricao_beneficio"))
        tipo_b = as_str(b.get("tipo_beneficio"))

        vu = b.get("valor_unitario")
        dia = as_int(b.get("dia"))
        mes = as_int(b.get("mes"))
        vt = b.get("valor_total")

        desc_cell = desc[:60]
        tipo_cell = tipo_b[:25]

        pdf.cell(15, 6, codigo, border=1)
        pdf.cell(80, 6, desc_cell, border=1)
        pdf.cell(35, 6, tipo_cell, border=1)
        pdf.cell(18, 6, fmt_money(vu), border=1, align="R")
        pdf.cell(12, 6, str(dia), border=1, align="R")
        pdf.cell(12, 6, str(mes), border=1, align="R")
        pdf.cell(18, 6, fmt_money(vt), border=1, align="R")
        pdf.ln(6)

    pdf.ln(2)
    pdf.set_font("Arial", "B", 9)
    pdf.cell(172, 6, "Total Geral", border=1, align="R")
    pdf.cell(18, 6, fmt_money(total_geral), border=1, align="R")
    pdf.ln(10)

    pdf.set_font("Arial", "", 9)
    pdf.cell(0, 6, "Assinatura: _________________________________________", ln=1)
    pdf.ln(6)
    pdf.cell(0, 6, "Data: ____/____/____", ln=1, align="R")

    raw_pdf = pdf.output(dest="S").encode("latin-1")
    pdf_base64 = base64.b64encode(raw_pdf).decode("utf-8")

    return {
        "cpf": cpf,
        "matricula": matricula,
        "competencia": competencia,
        "empresa": empresa,
        "filial": filial,
        "cliente": cliente,
        "lote": lote,
        "total_geral": float(total_geral),
        "beneficios": beneficios,
        "pdf_base64": pdf_base64,
    }


@router.post("/documents/ferias/competencias")
async def listar_competencias_ferias(
    request: Request,
    cpf: Optional[str] = Query(None),
    matricula: Optional[str] = Query(None),
    empresa: Optional[str] = Query(None),
    cliente: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    empresa = empresa or cliente

    if not matricula or not empresa:
        try:
            body = await request.json()
            if isinstance(body, dict):
                matricula = matricula or body.get("matricula")
                empresa = empresa or body.get("empresa") or body.get("cliente")
        except Exception:
            pass

    if not matricula or not empresa:
        raise HTTPException(
            status_code=422,
            detail="Informe 'matricula' e 'empresa' (na querystring ou no body JSON).",
        )

    params = {
        "matricula": _as_str(matricula),
        "empresa": _as_str(empresa),
    }

    sql = text("""
        SELECT DISTINCT
            regexp_replace(TRIM(c.cpt1per::text), '[^0-9]', '', 'g') AS comp
        FROM public.tb_ferias_cabecalho c
        WHERE TRIM(c.matricula::text) = TRIM(:matricula)
          AND TRIM(c.cod_empresa::text) = TRIM(:empresa)
          AND c.cpt1per IS NOT NULL
          AND regexp_replace(TRIM(c.cpt1per::text), '[^0-9]', '', 'g') ~ '^[0-9]{6}$'
        ORDER BY comp DESC
    """)

    rows = db.execute(sql, params).fetchall()

    competencias = [
        {"ano": int(r[0][:4]), "mes": int(r[0][4:6])}
        for r in rows
        if r[0] and len(r[0]) == 6
    ]

    if not competencias:
        raise HTTPException(
            status_code=404,
            detail="Nenhuma competência de férias encontrada para os parâmetros informados.",
        )

    return {"competencias": competencias}


@router.post("/documents/ferias/buscar")
def buscar_ferias(payload: BuscarFerias = Body(...), db: Session = Depends(get_db)):
    cpf = _only_digits(payload.cpf)
    matricula = _as_str(payload.matricula)
    competencia = _as_str(payload.competencia)
    empresa = _as_str(payload.empresa)

    if not cpf or not matricula or not competencia or not empresa:
        raise HTTPException(
            status_code=422,
            detail="Informe cpf, matricula, competencia e empresa.",
        )

    comp_norm = _only_yyyymm(_normaliza_anomes(competencia) or competencia)

    sql_cab = text("""
        SELECT
            c.cod_empresa,
            c.cod_filial,
            c.matricula,
            c.cod_cliente,
            c.nome,
            c.cargo,
            c.dt_admissao,
            c.empresa,
            c.cnpj,
            c.codigolote,
            c.cpf,
            c.recibo_txt,
            c.dataaquisini,
            c.dataaquisfin,
            c.dataabonoini,
            c.dataabonofin,
            c.diasabono,
            c.datagozoini,
            c.datagozofin,
            c.duracao,
            c.cpt1per,
            c.dataretorno,
            c.qtdfaltas,
            c.salprimeiroperiodo,
            c.salsegundoperiodo,
            c.dependentes,
            c.depirf,
            c.dataaviso,
            c.cidade,
            c.uf,
            c.datapagto
        FROM public.tb_ferias_cabecalho c
        WHERE regexp_replace(TRIM(c.cpf::text), '[^0-9]', '', 'g') =
            regexp_replace(TRIM(:cpf), '[^0-9]', '', 'g')
        AND TRIM(c.matricula::text) = TRIM(:matricula)
        AND TRIM(c.cod_empresa::text) = TRIM(:empresa)
        AND regexp_replace(TRIM(c.cpt1per::text), '[^0-9]', '', 'g') = :competencia
        LIMIT 1
    """)

    cab_row = db.execute(
        sql_cab,
        {
            "cpf": cpf,
            "matricula": matricula,
            "empresa": empresa,
            "competencia": comp_norm,
        },
    ).first()

    if not cab_row:
        raise HTTPException(
            status_code=404,
            detail="Nenhum recibo de férias encontrado para os critérios informados.",
        )

    cabecalho = dict(cab_row._mapping)
    sql_det = text("""
        SELECT
            d.cod_empresa,
            d.cod_filial,
            d.matricula,
            d.codigoevento,
            d.descricao,
            d.qtde,
            d.competencia,
            d.valor,
            d.tipo,
            d.lote,
            d.numerobcotitulo,
            d.agenciatitulo,
            d.contatitulo,
            d.tipoctabanco,
            d.codigocontrato,
            d.tipofat,
            d.codigobanco,
            d.nomebanco
        FROM public.tb_ferias_detalhe d
        WHERE TRIM(d.matricula::text) = TRIM(:matricula)
        AND TRIM(d.cod_empresa::text) = TRIM(:empresa)
        AND regexp_replace(TRIM(d.competencia::text), '[^0-9]', '', 'g') = :competencia
        ORDER BY d.tipo, d.codigoevento
    """)

    det_rows = db.execute(
        sql_det,
        {
            "matricula": matricula,
            "empresa": empresa,
            "competencia": comp_norm,
        },
    ).fetchall()

    detalhes = [dict(r._mapping) for r in det_rows]

    payload_pdf = montar_payload_ferias(cabecalho, detalhes)

    return {
        "tipo": "ferias",
        "cpf": cpf,
        "matricula": matricula,
        "competencia": comp_norm,
        "empresa": empresa,
        "total": 1,
        "ferias": [
            {
                "cabecalho": cabecalho,
                "detalhes": detalhes,
                "dados_pdf": payload_pdf,
            }
        ],
    }


@router.post("/documents/ferias/montar")
def montar_ferias(payload: MontarFerias = Body(...), db: Session = Depends(get_db)):
    cpf = _only_digits(payload.cpf)
    matricula = _as_str(payload.matricula)
    competencia = _as_str(payload.competencia)
    empresa = _as_str(payload.empresa)

    if not cpf or not matricula or not competencia or not empresa:
        raise HTTPException(
            status_code=422,
            detail="Informe cpf, matricula, competencia e empresa.",
        )

    comp_norm = _only_yyyymm(_normaliza_anomes(competencia) or competencia)

    sql_cab = text("""
        SELECT
            c.cod_empresa,
            c.cod_filial,
            c.matricula,
            c.cod_cliente,
            c.nome,
            c.cargo,
            c.dt_admissao,
            c.empresa,
            c.cnpj,
            c.codigolote,
            c.cpf,
            c.recibo_txt,
            c.dataaquisini,
            c.dataaquisfin,
            c.dataabonoini,
            c.dataabonofin,
            c.diasabono,
            c.datagozoini,
            c.datagozofin,
            c.duracao,
            c.cpt1per,
            c.dataretorno,
            c.qtdfaltas,
            c.salprimeiroperiodo,
            c.salsegundoperiodo,
            c.dependentes,
            c.depirf,
            c.dataaviso,
            c.cidade,
            c.uf,
            c.datapagto
        FROM public.tb_ferias_cabecalho c
        WHERE regexp_replace(TRIM(c.cpf::text), '[^0-9]', '', 'g') =
            regexp_replace(TRIM(:cpf), '[^0-9]', '', 'g')
        AND TRIM(c.matricula::text) = TRIM(:matricula)
        AND TRIM(c.cod_empresa::text) = TRIM(:empresa)
        AND regexp_replace(TRIM(c.cpt1per::text), '[^0-9]', '', 'g') = :competencia
        LIMIT 1
    """)

    cab_row = db.execute(
        sql_cab,
        {
            "cpf": cpf,
            "matricula": matricula,
            "empresa": empresa,
            "competencia": comp_norm,
        },
    ).first()

    if not cab_row:
        raise HTTPException(
            status_code=404,
            detail="Nenhum recibo de férias encontrado para os critérios informados.",
        )

    cabecalho = dict(cab_row._mapping)

    sql_det = text("""
        SELECT
            d.cod_empresa,
            d.cod_filial,
            d.matricula,
            d.codigoevento,
            d.descricao,
            d.qtde,
            d.competencia,
            d.valor,
            d.tipo,
            d.lote,
            d.numerobcotitulo,
            d.agenciatitulo,
            d.contatitulo,
            d.tipoctabanco,
            d.codigocontrato,
            d.tipofat,
            d.codigobanco,
            d.nomebanco
        FROM public.tb_ferias_detalhe d
        WHERE TRIM(d.matricula::text) = TRIM(:matricula)
        AND TRIM(d.cod_empresa::text) = TRIM(:empresa)
        AND regexp_replace(TRIM(d.competencia::text), '[^0-9]', '', 'g') = :competencia
        ORDER BY d.tipo, d.codigoevento
    """)
    det_rows = db.execute(
        sql_det,
        {
            "matricula": matricula,
            "empresa": empresa,
            "competencia": comp_norm,
        },
    ).fetchall()

    detalhes = [dict(r._mapping) for r in det_rows]

    dados_pdf = montar_payload_ferias(cabecalho, detalhes)

    raw_pdf = gerar_recibo_ferias_pdf(dados_pdf)
    pdf_base64 = base64.b64encode(raw_pdf).decode("utf-8")

    return {
        "tipo": "ferias",
        "cpf": cpf,
        "matricula": matricula,
        "competencia": comp_norm,
        "empresa": empresa,
        "cabecalho": cabecalho,
        "detalhes": detalhes,
        "dados_pdf": dados_pdf,
        "pdf_base64": pdf_base64,
    }