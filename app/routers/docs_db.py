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

router = APIRouter(tags=["Documentos DB"])


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
    matricula: Optional[str] = None
    empresa: Optional[str] = None


class MontarInformeRendimentos(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    cpf: str = Field(..., min_length=1)
    matricula: Optional[str] = None
    empresa: Optional[str] = None


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


def _as_decimal(v: Any) -> Decimal:
    if v is None or v == "":
        return Decimal("0")
    try:
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
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


def gerar_informe_rendimentos_pdf(registros: list[dict]) -> bytes:
    if not registros:
        raise ValueError("Nenhum registro encontrado para montar o PDF.")

    GOVERNO_LOGO_PATH = "app/assets/images.jpg"

    pdf = FPDF(format="A4", unit="mm")
    pdf.set_auto_page_break(auto=False)

    for registro in registros:
        pdf.add_page()
        pdf.set_draw_color(0, 0, 0)
        pdf.set_line_width(0.2)

        codigo_empresa = _as_str(registro.get("codigo_empresa"))
        cpf_cnpj = _as_str(registro.get("cpf_cnpj"))
        nome_empresa = truncate(_as_str(registro.get("nome_empresa")), 90)
        matricula = _as_str(registro.get("matricula"))
        cpf = _as_str(registro.get("cpf"))
        nome = truncate(_as_str(registro.get("nome")), 90)

        rendimento_ferias_01 = _fmt_money(registro.get("rendimento_ferias_01"))
        inss_02 = _fmt_money(registro.get("inss_02"))
        prevprivada_03 = _fmt_money(registro.get("prevprivada_03"))
        pensao_04 = _fmt_money(registro.get("pensao_04"))
        irrf_irrfferias_05 = _fmt_money(registro.get("irrf_irrfferias_05"))

        ajucusto_02 = _fmt_money(registro.get("ajucusto_02"))
        avisoprevio_06 = _fmt_money(registro.get("avisoprevio_06"))
        feriasabono_07 = _fmt_money(registro.get("feriasabono_07"))

        rendimento_irrf_inss_dependente_01 = _fmt_money(
            registro.get("rendimento_irrf_inss_dependente_01")
        )
        irrf_02 = _fmt_money(registro.get("irrf_02"))
        plucro_03 = _fmt_money(registro.get("plucro_03"))

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
            "Exercício de 2026",
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
            "ANO-CALENDÁRIO 2025",
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
        _cell_text(pdf, 12, y + 11, 40, 4, cpf_cnpj, 8, "")
        _cell_text(pdf, 57, y + 11, 140, 4, nome_empresa, 8, "")

        y = 59
        _draw_box(pdf, 10, y, 190, 24)
        _cell_text(pdf, 11, y + 1, 188, 4, "2. PESSOA FÍSICA BENEFICIÁRIA DOS RENDIMENTOS", 8, "B")
        pdf.line(10, y + 6, 200, y + 6)
        pdf.line(55, y + 6, 55, y + 16)
        _cell_text(pdf, 12, y + 7, 40, 4, "CPF", 7, "B")
        _cell_text(pdf, 57, y + 7, 140, 4, "Nome Completo", 7, "B")
        _cell_text(pdf, 12, y + 11, 40, 4, cpf, 8, "")
        _cell_text(pdf, 57, y + 11, 140, 4, nome, 8, "")
        pdf.line(10, y + 16, 200, y + 16)
        _cell_text(pdf, 12, y + 17, 180, 4, f"Matrícula: {matricula}", 8, "")

        y = 85
        _draw_box(pdf, 10, y, 190, 34)
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
            ("01. Total dos rendimentos de férias", rendimento_ferias_01),
            ("02. Contribuição previdenciária oficial", inss_02),
            ("03. Contribuição à previdência privada", prevprivada_03),
            ("04. Pensão alimentícia", pensao_04),
            ("05. Imposto sobre a renda retido na fonte", irrf_irrfferias_05),
        ]

        y_line = y + 7
        for desc, valor in linhas_q3:
            _cell_text(pdf, 12, y_line, 145, 4, desc, 8, "")
            _cell_text(pdf, 160, y_line, 35, 4, valor, 8, "", "R")
            y_line += 5.2

        y = 121
        _draw_box(pdf, 10, y, 190, 30)
        _cell_text(pdf, 11, y + 1, 135, 4, "4. RENDIMENTOS ISENTOS E NÃO TRIBUTÁVEIS", 8, "B")
        _cell_text(pdf, 160, y + 1, 35, 4, "VALORES EM REAIS", 7, "B", "R")
        pdf.line(10, y + 6, 200, y + 6)

        linhas_q4 = [
            ("02. Diárias e ajuda de custo", ajucusto_02),
            ("06. Aviso prévio indenizado", avisoprevio_06),
            ("07. Abono pecuniário de férias", feriasabono_07),
        ]

        y_line = y + 7
        for desc, valor in linhas_q4:
            _cell_text(pdf, 12, y_line, 145, 5, desc, 8, "")
            _cell_text(pdf, 160, y_line, 35, 5, valor, 8, "", "R")
            y_line += 6.7

        y = 153
        _draw_box(pdf, 10, y, 190, 26)
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
            ("01. Rendimentos tributação exclusiva", rendimento_irrf_inss_dependente_01),
            ("02. Imposto sobre a renda na fonte", irrf_02),
            ("03. Participação nos lucros ou resultados", plucro_03),
        ]

        y_line = y + 7
        for desc, valor in linhas_q5:
            _cell_text(pdf, 12, y_line, 145, 5, desc, 8, "")
            _cell_text(pdf, 160, y_line, 35, 5, valor, 8, "", "R")
            y_line += 6

        y = 181
        _draw_box(pdf, 10, y, 190, 48)
        _cell_text(pdf, 11, y + 1, 188, 4, "7. INFORMAÇÕES COMPLEMENTARES", 8, "B")
        pdf.line(10, y + 6, 200, y + 6)

        info_complementar = (
            f"Código da empresa: {codigo_empresa}\n"
            f"Fonte pagadora: {nome_empresa}\n"
            f"CPF do beneficiário: {cpf}\n"
            f"Matrícula: {matricula}\n"
            f"Comprovante gerado a partir das informações registradas no banco de dados."
        )
        _cell_text(pdf, 12, y + 8, 184, 4, info_complementar, 8, "")

        y = 231
        _draw_box(pdf, 10, y, 190, 26)
        _cell_text(pdf, 11, y + 1, 188, 4, "8. RESPONSÁVEL PELAS INFORMAÇÕES", 8, "B")
        pdf.line(10, y + 6, 200, y + 6)
        pdf.line(120, y + 6, 120, y + 26)
        _cell_text(pdf, 12, y + 7, 105, 4, "Nome", 7, "B")
        _cell_text(pdf, 122, y + 7, 30, 4, "Data", 7, "B")
        _cell_text(pdf, 155, y + 7, 40, 4, "Assinatura", 7, "B")
        _cell_text(pdf, 12, y + 12, 105, 4, "RESPONSÁVEL PELAS INFORMAÇÕES", 8, "")
        _cell_text(pdf, 122, y + 12, 30, 4, datetime.now().strftime("%d/%m/%Y"), 8, "")
        _cell_text(pdf, 155, y + 12, 40, 4, "", 8, "")

        pdf.set_xy(10, 262)
        pdf.set_font("Arial", "", 6)
        pdf.multi_cell(
            190,
            3.5,
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
        q = text("""SELECT 1 FROM information_schema.tables
                    WHERE table_schema=:schema AND table_name=:table LIMIT 1""")
        return db.execute(q, {"schema": schema, "table": table}).first() is not None

    def _column_exists(schema: str, table: str, column: str) -> bool:
        q = text("""SELECT 1 FROM information_schema.columns
                    WHERE table_schema=:schema AND table_name=:table AND column_name=:column LIMIT 1""")
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


@router.post("/documents/informe-rendimentos/buscar")
def buscar_informe_rendimentos(
    payload: BuscarInformeRendimentos = Body(...),
    db: Session = Depends(get_db),
):
    cpf = _as_str(payload.cpf)
    matricula = _as_str(payload.matricula)
    empresa = _as_str(payload.empresa)

    if not cpf:
        raise HTTPException(status_code=422, detail="Informe cpf.")

    filtros = [
        "regexp_replace(TRIM(cpf::text), '[^0-9]', '', 'g') = regexp_replace(TRIM(:cpf), '[^0-9]', '', 'g')"
    ]
    params: Dict[str, Any] = {"cpf": cpf}

    if matricula:
        filtros.append("TRIM(matricula::text) = TRIM(:matricula)")
        params["matricula"] = matricula

    if empresa:
        filtros.append("TRIM(nome_empresa::text) = TRIM(:empresa)")
        params["empresa"] = empresa

    sql = text(f"""
        SELECT
            codigo_empresa,
            cpf_cnpj,
            nome_empresa,
            matricula,
            cpf,
            nome,
            rendimento_ferias_01,
            inss_02,
            prevprivada_03,
            pensao_04,
            irrf_irrfferias_05,
            ajucusto_02,
            avisoprevio_06,
            feriasabono_07,
            rendimento_irrf_inss_dependente_01,
            irrf_02,
            plucro_03
        FROM public.tb_informe_rendimentos
        WHERE {" AND ".join(filtros)}
        ORDER BY nome, matricula
    """)

    rows = db.execute(sql, params).fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="Nenhum informe de rendimentos encontrado para os critérios informados."
        )

    informes = [dict(r._mapping) for r in rows]

    return {
        "tipo": "informe_rendimentos",
        "cpf": cpf,
        "matricula": matricula,
        "empresa": empresa,
        "total": len(informes),
        "informes": informes,
    }


@router.post("/documents/informe-rendimentos/montar")
def montar_informe_rendimentos(
    payload: MontarInformeRendimentos = Body(...),
    db: Session = Depends(get_db),
):
    cpf = _as_str(payload.cpf)
    matricula = _as_str(payload.matricula)
    empresa = _as_str(payload.empresa)

    if not cpf:
        raise HTTPException(status_code=422, detail="Informe cpf.")

    filtros = [
        "regexp_replace(TRIM(cpf::text), '[^0-9]', '', 'g') = regexp_replace(TRIM(:cpf), '[^0-9]', '', 'g')"
    ]
    params: Dict[str, Any] = {"cpf": cpf}

    if matricula:
        filtros.append("TRIM(matricula::text) = TRIM(:matricula)")
        params["matricula"] = matricula

    if empresa:
        filtros.append("TRIM(nome_empresa::text) = TRIM(:empresa)")
        params["empresa"] = empresa

    sql = text(f"""
        SELECT
            codigo_empresa,
            cpf_cnpj,
            nome_empresa,
            matricula,
            cpf,
            nome,
            rendimento_ferias_01,
            inss_02,
            prevprivada_03,
            pensao_04,
            irrf_irrfferias_05,
            ajucusto_02,
            avisoprevio_06,
            feriasabono_07,
            rendimento_irrf_inss_dependente_01,
            irrf_02,
            plucro_03
        FROM public.tb_informe_rendimentos
        WHERE {" AND ".join(filtros)}
        ORDER BY nome, matricula
    """)

    rows = db.execute(sql, params).fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="Nenhum informe de rendimentos encontrado para os critérios informados."
        )

    informes = [dict(r._mapping) for r in rows]

    raw_pdf = gerar_informe_rendimentos_pdf(informes)
    pdf_base64 = base64.b64encode(raw_pdf).decode("utf-8")

    return {
        "tipo": "informe_rendimentos",
        "cpf": cpf,
        "matricula": matricula,
        "empresa": empresa,
        "total": len(informes),
        "informes": informes,
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