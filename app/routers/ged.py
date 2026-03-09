import unicodedata
import re
from fastapi import APIRouter, HTTPException, Form, Depends, Request, Response, Body, Query
from typing import Any, Dict, Optional, Set
import requests
from pydantic import BaseModel, Field, field_validator
from pydantic import ConfigDict
from datetime import datetime
from babel.dates import format_date
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.database.connection import get_db
from config.settings import settings
from typing import List
import base64
from fpdf import FPDF # type: ignore
from decimal import Decimal, InvalidOperation


router = APIRouter()

class MontarBeneficio(BaseModel):
    matricula: str
    competencia: str
    cpf: str
    lote_holerite: str
    uuid: str

class TemplateFieldsRequest(BaseModel):
    id_template: int

class DocumentoGED(BaseModel):
    id_documento: str
    nomearquivo: str
    datacriacao: str
    cpf: str = ""
    datadevencimento: str = ""
    nossonumero: str = ""

class CampoConsulta(BaseModel):
    nome: str
    valor: str

class BuscaDocumentoCampos(BaseModel):
    id_template: int
    cp: List[CampoConsulta]

class DownloadDocumentoPayload(BaseModel):
    id_tipo: int
    id_documento: int

class CampoValor(BaseModel):
    nome: str
    valor: str

class SearchDocumentosRequest(BaseModel):
    id_template: int | str
    cp: List[CampoValor] = Field(default_factory=list)
    campo_anomes: str
    anomes: Optional[str] = None
    anomes_in: Optional[List[str]] = None

    @field_validator("campo_anomes")
    @classmethod
    def _valida_campo_anomes(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("campo_anomes é obrigatório")
        return v

    @field_validator("anomes", mode="before")
    @classmethod
    def _blank_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and v.strip() == "":
            return None
        return v

    @field_validator("anomes_in", mode="before")
    @classmethod
    def _normalize_anomes_in(cls, v):
        if v is None or v == "":
            return None
        if isinstance(v, list):
            cleaned = [str(x).strip() for x in v if str(x).strip()]
            return cleaned or None
        s = str(v).strip()
        return [s] if s else None

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
        pattern=r'^\d{11}$',
        description="CPF sem formatação, 11 dígitos (ex: 06485294015)"
    )

class UploadBase64Payload(BaseModel):
    id_tipo: int
    formato: str
    documento_nome: str
    documento_base64: str
    campos: List[CampoConsulta]

def _only_yyyymm(s: str) -> str:
    """Mantém apenas dígitos e retorna os 6 primeiros (YYYYMM)."""
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

def _flatten_attributes(document: Dict[str, Any]) -> Dict[str, Any]:
    d = dict(document)
    for a in (d.pop("attributes", []) or []):
        n, val = a.get("name"), a.get("value")
        if n:
            d[n] = val
    return d

def _to_ano_mes(yyyymm: str) -> Dict[str, int]:
    ano_str, mes_str = yyyymm.split("-", 1)
    return {"ano": int(ano_str), "mes": int(mes_str)}

def _coleta_anomes_via_search(
    headers: Dict[str, str],
    id_template: int | str,
    nomes_campos: List[str],
    lista_cp: List[str],
    campo_anomes: str,
    max_pages: int = 10
) -> List[str]:
    meses: Set[str] = set()
    pagina = 1
    total_paginas = 1
    BASE_URL = "http://ged.byebyepaper.com.br:9090/idocs_bbpaper/api/v1"
    while pagina <= total_paginas and pagina <= max_pages:
        form = [("id_tipo", str(id_template))]
        form += [("cp[]", v) for v in lista_cp]
        form += [
            ("ordem", "no_ordem"),
            ("dt_criacao", ""),
            ("pagina", str(pagina)),
            ("colecao", "S"),
        ]
        r = requests.post(f"{BASE_URL}/documents/search", data=form, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json() or {}
        docs = [_flatten_attributes(doc) for doc in (data.get("documents") or [])]
        for d in docs:
            bruto = str(d.get(campo_anomes, "")).strip()
            n = _normaliza_anomes(bruto)
            if n:
                meses.add(n)
        vars_ = data.get("variables") or {}
        try:
            total_paginas = int(vars_.get("totalpaginas", total_paginas))
        except Exception:
            total_paginas = 1
        pagina += 1
    return sorted(meses, reverse=True)

def _norm(s: str) -> str:
    s = s or ""
    s = s.strip().lower().replace(" ", "").replace("-", "").replace(".", "").replace("__", "_").replace("_", "")
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def _only_digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

def _cpf_from_any(value: str) -> Optional[str]:
    digits = _only_digits(value or "")
    if len(digits) < 11:
        return None
    return digits[-11:]

def _headers(auth_key: str) -> Dict[str, str]:
    return {
        "Authorization": auth_key,
        "Content-Type": "application/x-www-form-urlencoded; charset=ISO-8859-1",
    }

BASE_URL = "http://ged.byebyepaper.com.br:9090/idocs_bbpaper/api/v1"

def login(conta: str, usuario: str, senha: str) -> str:
    payload = {
        "conta": conta,
        "usuario": usuario,
        "senha": senha,
        "id_interface": "CLIENT_WEB"
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=ISO-8859-1"
    }
    response = requests.post(f"{BASE_URL}/login", data=payload, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Erro ao autenticar no GED")
    data = response.json()
    if data.get("error"):
        raise HTTPException(status_code=401, detail="Login falhou")
    return data["authorization_key"]

@router.get("/searchdocuments/templates")
def listar_templates() -> Any:
    auth_key = login(
        conta=settings.GED_CONTA,
        usuario=settings.GED_USUARIO,
        senha=settings.GED_SENHA
    )
    headers = {"Authorization": auth_key}
    response = requests.get(f"{BASE_URL}/templates/getall", headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Erro ao buscar templates")
    data = response.json()
    if data.get("error"):
        raise HTTPException(status_code=400, detail="Erro na resposta da API GED")
    return data.get("templates", [])

@router.post("/searchdocuments/templateFields")
def get_template_fields(id_template: int = Form(...)):
    auth_key = login(
        conta=settings.GED_CONTA,
        usuario=settings.GED_USUARIO,
        senha=settings.GED_SENHA
    )
    headers = {
        "Authorization": auth_key,
        "Content-Type": "application/x-www-form-urlencoded; charset=ISO-8859-1"
    }
    payload = f"id_template={id_template}"
    response = requests.post(
        f"{BASE_URL}/templates/getfields",
        headers=headers,
        data=payload
    )
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Erro ao buscar campos do template")
    return response.json()

@router.post("/documents/upload_base64")
def upload_documento_base64(payload: UploadBase64Payload):
    auth_key = login(
        conta=settings.GED_CONTA,
        usuario=settings.GED_USUARIO,
        senha=settings.GED_SENHA
    )
    headers = {
        "Authorization": auth_key,
        "Content-Type": "application/x-www-form-urlencoded; charset=ISO-8859-1"
    }
    response_fields = requests.post(
        f"{BASE_URL}/templates/getfields",
        data={"id_template": payload.id_tipo},
        headers=headers
    )
    if response_fields.status_code != 200:
        raise HTTPException(status_code=500, detail="Erro ao buscar campos do template")
    campos_template = response_fields.json().get("fields", [])
    nomes_campos = [campo["nomecampo"] for campo in campos_template]
    lista_cp = ["" for _ in nomes_campos]
    for campo in payload.campos:
        if campo.nome not in nomes_campos:
            raise HTTPException(status_code=400, detail=f"Campo '{campo.nome}' não encontrado no template")
        idx = nomes_campos.index(campo.nome)
        lista_cp[idx] = campo.valor
    data = {
        "id_tipo": str(payload.id_tipo),
        "formato": payload.formato,
        "documento_nome": payload.documento_nome,
        "documento": payload.documento_base64
    }
    for valor in lista_cp:
        data.setdefault("cp[]", []).append(valor)
    response = requests.post(
        f"{BASE_URL}/documents/uploadbase64",
        headers=headers,
        data=data
    )
    try:
        return response.json()
    except Exception:
        raise HTTPException(status_code=500, detail=f"Erro no upload: {response.text}")

# ============================
# NOVA ROTA: listar competências
# ============================

@router.post("/documents/holerite/competencias")
async def listar_competencias_holerite(
    request: Request,
    cpf: Optional[str] = Query(None, description="CPF (com ou sem máscara)"),
    matricula: Optional[str] = Query(None, description="Matrícula exata"),
    empresa: Optional[str] = Query(None, description="Código da empresa/cliente"),
    cliente: Optional[str] = Query(None, description="Alias antigo do código do cliente"),
    db: Session = Depends(get_db),
):
    """
    Lista competências (ano, mes) onde há coerência entre eventos, cabeçalho e rodapé
    para a chave (cpf, matricula, empresa).
    """

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

    sql_lista_comp = text(f"""
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
        WHERE comp ~ '^[0-9]{{6}}$'
        ORDER BY ano DESC, mes DESC
    """)

    rows = db.execute(sql_lista_comp, params).fetchall()
    competencias = [{"ano": r[0], "mes": r[1]} for r in rows if r[0] is not None and r[1] is not None]

    if not competencias:
        raise HTTPException(status_code=404, detail="Nenhuma competência encontrada para os parâmetros informados.")

    return {"competencias": competencias}

# ==========================================
# ROTA SIMPLIFICADA: buscar holerite direto
# ==========================================

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

    # 1) UUIDs válidos (interseção cab+rod+evt)
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

    # 2) Aceite (por competência). Se quiser por UUID, precisa armazenar UUID na tabela status.
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

    # 3) Monta holerites completos por UUID
    holerites = []

    for uuid in uuids:
        # cabecalho
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

        # rodape
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

        # eventos
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

        # agrupar A/P como antes
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
            "aceito": aceito_bool,  # por competência
            "tipo_calculo": tc,      # <<< NOVO (root)
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

def pad_left(valor: str, width: int) -> str:
    return str(valor).strip().zfill(width)

def fmt_num(valor: float) -> str:
    s = f"{valor:,.2f}"
    s = s.replace(",", "X").replace(".", ",")
    return s.replace("X", ".")

def truncate(text: str, max_len: int) -> str:
    text = text or ""
    return text if len(text) <= max_len else text[: max_len - 3] + "..."

def gerar_recibo(cabecalho: dict, eventos: list[dict], rodape: dict, page_number: int = 1) -> bytes:
    cabecalho["matricula"] = pad_left(cabecalho["matricula"], 6)
    cabecalho["cliente"]   = pad_left(cabecalho["cliente"],   5)
    cabecalho["empresa"]   = pad_left(cabecalho["empresa"],   3)
    cabecalho["filial"]    = pad_left(cabecalho["filial"],    3)

    adm = datetime.fromisoformat(cabecalho["admissao"])
    cabecalho["admissao"]   = format_date(adm, "dd/MM/yyyy", locale="pt_BR")
    comp = datetime.strptime(cabecalho["competencia"], "%Y%m")
    cabecalho["competencia"] = format_date(comp, "LLLL/yyyy", locale="pt_BR").capitalize()

    empresa_nome = truncate(cabecalho.get("empresa_nome", ""), 50)
    cliente_nome = truncate(cabecalho.get("cliente_nome", ""), 50)
    funcionario  = truncate(cabecalho.get("nome", ""), 30)
    funcao       = truncate(cabecalho.get("funcao_nome", ""), 16)

    pdf = FPDF(format='A4', unit='mm')
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    pdf.set_font("Arial", 'B', 12)
    pdf.cell(0, 6, "Recibo de Pagamento de Salário", ln=0)
    pdf.ln(6)

    pdf.set_font("Arial", '', 9)
    pdf.cell(120, 5, f"Empresa: {cabecalho['empresa']} - {cabecalho['filial']} {empresa_nome}", ln=0)
    pdf.cell(0,   5, f"Nº Inscrição: {cabecalho['empresa_cnpj']}", ln=1, align='R')
    pdf.cell(120, 5, f"Cliente: {cabecalho['cliente']} {cliente_nome}",       ln=0)
    pdf.cell(0,   5, f"Nº Inscrição: {cabecalho['cliente_cnpj']}", ln=1, align='R')
    pdf.ln(3)

    col_widths = [20, 60, 40, 30, 30]
    headers    = ["Código", "Nome do Funcionário", "Função", "Admissão", "Competência"]
    pdf.set_font("Arial", 'B', 9)
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 6, h)
    pdf.ln(6)

    pdf.set_font("Arial", '', 7)
    vals = [cabecalho["matricula"], funcionario, funcao,
            cabecalho["admissao"], cabecalho["competencia"]]
    for w, v in zip(col_widths, vals):
        pdf.cell(w, 6, v)
    pdf.ln(6)

    y_sep = pdf.get_y()
    pdf.set_draw_color(0, 0, 0)
    pdf.set_line_width(0.2)
    pdf.line(pdf.l_margin, y_sep, pdf.w - pdf.r_margin, y_sep)
    pdf.ln(3)

    evt_headers = ["Cód.", "Descrição", "Referência", "Vencimentos", "Descontos"]
    pdf.set_font("Arial", 'B', 9)
    for i, (w, h) in enumerate(zip(col_widths, evt_headers)):
        align = 'C' if i >= 2 else ''
        pdf.cell(w, 6, h, align=align)
    pdf.ln(6)

    y_start = pdf.get_y()
    pdf.set_font("Arial", '', 9)
    for evt in eventos:
        nome_evt = truncate(evt.get("evento_nome", ""), 30).upper()
        row = [
            str(evt['evento']),
            nome_evt,
            fmt_num(evt['referencia']),
            fmt_num(evt['valor']) if evt['tipo'] == 'V' else "",
            fmt_num(evt['valor']) if evt['tipo'] == 'D' else ""
        ]
        for i, (w, v) in enumerate(zip(col_widths, row)):
            align = 'R' if i >= 2 else ''
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
    half   = (usable - 10) / 2
    pdf.set_font("Arial", 'B', 9)
    pdf.cell(half, 6, "Total Vencimentos", ln=0, align='R')
    pdf.cell(10,   6, "", ln=0)
    pdf.cell(half, 6, "Total Descontos",    ln=1, align='R')
    pdf.set_font("Arial", '', 9)
    pdf.cell(half, 6, fmt_num(rodape['total_vencimentos']), ln=0, align='R')
    pdf.cell(10,   6, "", ln=0)
    pdf.cell(half, 6, fmt_num(rodape['total_descontos']),    ln=1, align='R')
    pdf.ln(3)

    pdf.set_font("Arial", 'B', 9)
    pdf.cell(0, 6, f"Valor Líquido »» {fmt_num(rodape['valor_liquido'])}", ln=1, align='R')
    pdf.ln(4)

    y = pdf.get_y()
    pdf.line(pdf.l_margin, y, pdf.w - pdf.r_margin, y)
    pdf.ln(3)

    detalhes = ["Salário Base", "Sal. Contr. INSS", "Base Cálc FGTS",
               "F.G.T.S. do Mês", "Base Cálc IRRF", "DEP SF", "DEP IRF"]
    pdf.set_font("Arial", 'B', 8)
    for d in detalhes:
        pdf.cell(28, 5, d)
    pdf.ln(5)

    pdf.set_font("Arial", '', 8)
    footer_vals = [
        f"{fmt_num(rodape['salario_base'])}/M",
        fmt_num(rodape['sal_contr_inss']),
        fmt_num(rodape['base_calc_fgts']),
        fmt_num(rodape['fgts_mes']),
        fmt_num(rodape['base_calc_irrf']),
        pad_left(rodape['dep_sf'], 2),
        pad_left(rodape['dep_irf'], 2),
    ]
    for v in footer_vals:
        pdf.cell(28, 6, v)
    pdf.ln(10)

    pdf.ln(10)
    y_sig = pdf.get_y()
    pdf.set_line_width(0.2)
    pdf.line(pdf.l_margin, y_sig, pdf.l_margin + 80, y_sig)
    pdf.ln(2)
    pdf.set_font("Arial", '', 9)
    pdf.cell(80, 6, funcionario, ln=0)
    pdf.cell(0, 6, "Data: ____/____/____", ln=1, align='R')

    return pdf.output(dest='S').encode('latin-1')


@router.post("/documents/holerite/montar")
def montar_holerite(
    payload: MontarHolerite,
    db: Session = Depends(get_db)
):
    params = {
        "matricula": payload.matricula,
        "competencia": payload.competencia,
        "lote": payload.lote,
        "cpf": payload.cpf
    }

    # >>> ALTERAÇÃO: inclui uuid como texto <<<
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
        tipo = evt.get('tipo', '').upper()
        if tipo not in ('V', 'D'):
            raise HTTPException(status_code=400, detail=f"Tipo de evento inválido: {tipo}")
        evt['tipo'] = tipo

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
        "uuid": cabecalho.get("uuid"),  # >>> AGORA VEM NO ROOT <<<
        "cabecalho": cabecalho,         # (inclui uuid também aqui)
        "eventos": eventos,
        "rodape": rodape,
        "pdf_base64": pdf_base64
    }

@router.post("/searchdocuments/download")
def baixar_documento(payload: DownloadDocumentoPayload):
    auth_key = login(
        conta=settings.GED_CONTA,
        usuario=settings.GED_USUARIO,
        senha=settings.GED_SENHA
    )
    headers = {
        "Authorization": auth_key,
        "Content-Type": "application/x-www-form-urlencoded; charset=ISO-8859-1"
    }
    data = {
        "id_tipo": payload.id_tipo,
        "id_documento": payload.id_documento
    }
    response = requests.post(
        f"{BASE_URL}/documents/download",
        headers=headers,
        data=data
    )
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"Erro {response.status_code}: {response.text}")
    try:
        return response.json()
    except ValueError:
        return {
            "erro": False,
            "base64_raw": response.text
        }

@router.post("/documents/search/informetrct")
def buscar_search_documentos_ano(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    """
    Clonagem da /documents/search, mas usando chave:
      - tipodedoc (exato)
      - cpf (11 dígitos, com ou sem máscara)
      - ano (extraído do campo_anomes)

    Body igual ao da /documents/search:

    {
      "id_template": 6,
      "cp": [
        { "nome": "tipodedoc", "valor": "trtc" },
        { "nome": "cpf", "valor": "01547656000" }
      ],
      "campo_anomes": "ano",
      "anomes": "2025"
    }

    Regras:
      - Se NÃO vier 'anomes'/'anomes_in' -> retorna somente os ANOS disponíveis
      - Se vier 'anomes'/'anomes_in' -> retorna documentos daquele ano
    """

    def _normaliza_ano_local(valor: str) -> Optional[str]:
        v = (valor or "").strip()
        if not v:
            return None
        m = re.search(r"\d{4}", v)
        return m.group(0) if m else None

    def _coleta_anos_via_search(
        headers: Dict[str, str],
        id_template: int | str,
        nomes_campos: List[str],
        lista_cp: List[str],
        campo_ano: str,
        max_pages: int = 10,
    ) -> List[str]:
        anos: Set[str] = set()
        pagina = 1
        total_paginas = 1
        while pagina <= total_paginas and pagina <= max_pages:
            form = [("id_tipo", str(id_template))]
            form += [("cp[]", v) for v in lista_cp]
            form += [
                ("ordem", "no_ordem"),
                ("dt_criacao", ""),
                ("pagina", str(pagina)),
                ("colecao", "S"),
            ]
            r = requests.post(
                f"{BASE_URL}/documents/search",
                data=form,
                headers=headers,
                timeout=60,
            )
            r.raise_for_status()
            data = r.json() or {}
            docs = [_flatten_attributes(doc) for doc in (data.get("documents") or [])]
            for d in docs:
                bruto = str(d.get(campo_ano, "")).strip()
                n = _normaliza_ano_local(bruto)
                if n:
                    anos.add(n)

            vars_ = data.get("variables") or {}
            try:
                total_paginas = int(vars_.get("totalpaginas", total_paginas))
            except Exception:
                total_paginas = 1
            pagina += 1

        return sorted(anos, reverse=True)

    # ---- 0) lê payload no MESMO formato da /documents/search ----
    id_template = payload.get("id_template")
    cp_items = payload.get("cp") or []
    campo_anomes = (payload.get("campo_anomes") or "ano").strip()
    anomes_raw = (payload.get("anomes") or "").strip()
    anomes_in_raw = payload.get("anomes_in")

    if not id_template:
        raise HTTPException(422, detail="Informe 'id_template' no payload.")

    # ---- 1) autenticação GED ----
    try:
        auth_key = login(
            conta=settings.GED_CONTA,
            usuario=settings.GED_USUARIO,
            senha=settings.GED_SENHA,
        )
    except Exception as e:
        raise HTTPException(502, f"Falha na autenticação no GED: {e}")
    headers = _headers(auth_key)

    # ---- 2) campos do template ----
    r_fields = requests.post(
        f"{BASE_URL}/templates/getfields",
        data={"id_template": id_template},
        headers=headers,
        timeout=30,
    )
    r_fields.raise_for_status()
    fields_json = r_fields.json() or {}
    nomes_campos = [f.get("nomecampo") for f in fields_json.get("fields", []) if f.get("nomecampo")]

    if not nomes_campos:
        raise HTTPException(400, "Template sem campos ou inválido")
    if campo_anomes not in nomes_campos:
        raise HTTPException(400, f"Campo '{campo_anomes}' não existe no template")

    def _campos_template_txt() -> str:
        return ", ".join(nomes_campos) if nomes_campos else "(vazio)"

    # ---- 3) monta cp[] na ordem do template (igual /documents/search) ----
    lista_cp = ["" for _ in nomes_campos]
    for item in cp_items:
        nome = (item.get("nome") or "").strip()
        valor = (item.get("valor") or "").strip()
        if not nome:
            continue
        if nome not in nomes_campos:
            raise HTTPException(400, f"Campo '{nome}' não existe no template")
        lista_cp[nomes_campos.index(nome)] = valor

    # ---- 4) chave composta: tipodedoc + cpf ----
    if "tipodedoc" not in nomes_campos:
        raise HTTPException(400, f"Template precisa ter 'tipodedoc'. Campos: [{_campos_template_txt()}]")
    if "cpf" not in nomes_campos:
        raise HTTPException(400, f"Template precisa ter 'cpf'. Campos: [{_campos_template_txt()}]")

    idx_tipodedoc = nomes_campos.index("tipodedoc")
    idx_cpf = nomes_campos.index("cpf")

    tipodedoc_val = (lista_cp[idx_tipodedoc] or "").strip()
    if not tipodedoc_val:
        raise HTTPException(422, "Informe 'tipodedoc' em cp[] para a chave composta.")

    cpf_raw = (lista_cp[idx_cpf] or "").strip()
    cpf_digits = _only_digits(cpf_raw)
    if len(cpf_digits) != 11:
        raise HTTPException(422, "Informe 'cpf' em cp[] com 11 dígitos (com ou sem máscara).")

    # força valores normalizados em cp[]
    lista_cp[idx_tipodedoc] = tipodedoc_val
    lista_cp[idx_cpf] = f"%{cpf_digits}%"

    # ------------------------------------------------------------------
    # 5) Se NÃO informar anomes/anomes_in → lista ANOS (tipodedoc + cpf)
    # ------------------------------------------------------------------
    alvo: Set[str] = set()
    if not anomes_raw and not anomes_in_raw:
        form_filter = [
            ("id_tipo", str(id_template)),
            ("filtro", campo_anomes),
            ("filtro1", "tipodedoc"),
            ("filtro1_valor", tipodedoc_val),
            ("filtro2", "cpf"),
            ("filtro2_valor", f"%{cpf_digits}%"),
        ]
        try:
            rf = requests.post(
                f"{BASE_URL}/documents/filter",
                data=form_filter,
                headers=headers,
                timeout=60,
            )
            rf.raise_for_status()
            fdata = rf.json() or {}
            if fdata.get("error"):
                raise RuntimeError(f"GED error: {fdata.get('message')}")
            grupos = fdata.get("groups") or []

            anos_set: Set[str] = set()
            for g in grupos:
                bruto = str(g.get(campo_anomes, "")).strip()
                n = _normaliza_ano_local(bruto)
                if n:
                    anos_set.add(n)

            if anos_set:
                anos_sorted = sorted({int(a) for a in anos_set}, reverse=True)
                return {"anos": [{"ano": a} for a in anos_sorted]}

        except (requests.HTTPError, requests.RequestException, RuntimeError):
            # === Fallback padrão igual /documents/search → via /documents/search paginado ===
            anos_norm = _coleta_anos_via_search(
                headers=headers,
                id_template=id_template,
                nomes_campos=nomes_campos,
                lista_cp=lista_cp,
                campo_ano=campo_anomes,
            )
            if anos_norm:
                anos_sorted = sorted({int(a) for a in anos_norm}, reverse=True)
                return {"anos": [{"ano": a} for a in anos_sorted]}
            raise HTTPException(404, "Nenhum ano disponível para os parâmetros enviados.")

    # ------------------------------------------------------------------
    # 6) Temos anomes/anomes_in → tratar como lista de ANOS (YYYY)
    # ------------------------------------------------------------------
    if anomes_raw:
        n = _normaliza_ano_local(anomes_raw)
        if not n:
            raise HTTPException(
                400,
                "anomes inválido. Informe algo que contenha um ano, ex: '2024' ou '01/2024'.",
            )
        alvo.add(n)

    if anomes_in_raw:
        if isinstance(anomes_in_raw, list):
            for val in anomes_in_raw:
                n = _normaliza_ano_local(str(val))
                if not n:
                    raise HTTPException(400, f"Valor inválido em anomes_in: '{val}'")
                alvo.add(n)
        else:
            n = _normaliza_ano_local(str(anomes_in_raw))
            if not n:
                raise HTTPException(400, f"Valor inválido em anomes_in: '{anomes_in_raw}'")
            alvo.add(n)

    # ------------------------------------------------------------------
    # 7) executa a busca (idêntico à /documents/search, mas filtra por ANO)
    # ------------------------------------------------------------------
    def _do_search(cp_override: Optional[List[str]] = None):
        form = [("id_tipo", str(id_template))]
        form += [("cp[]", v) for v in (cp_override if cp_override is not None else lista_cp)]
        form += [
            ("ordem", "no_ordem"),
            ("dt_criacao", ""),
            ("pagina", "1"),
            ("colecao", "S"),
        ]
        r = requests.post(f"{BASE_URL}/documents/search", data=form, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json() or {}
        if data.get("error"):
            raise HTTPException(400, f"Documento não encontrardo para crietérios informados")
        return [_flatten_attributes(doc) for doc in (data.get("documents") or [])]

    try:
        documentos_total = _do_search()
    except requests.HTTPError as err:
        try:
            raise HTTPException(err.response.status_code, f"GED erro: {err.response.json()}")
        except Exception:
            raise HTTPException(
                getattr(err.response, "status_code", 502),
                f"GED erro: {getattr(err.response, 'text', err)}",
            )
    except requests.RequestException as e:
        raise HTTPException(502, f"Falha ao consultar GED (search): {e}")

    # ------------------------------------------------------------------
    # 8) pós-processa e aplica filtro por ANO
    # ------------------------------------------------------------------
    filtrados: List[Dict[str, Any]] = []
    for d in documentos_total:
        bruto = str(d.get(campo_anomes, "")).strip()
        n = _normaliza_ano_local(bruto)
        if n and (not alvo or n in alvo):
            d["_norm_ano"] = n
            filtrados.append(d)

    if not filtrados:
        raise HTTPException(404, "Nenhum documento encontrado para os parâmetros informados.")

    filtrados.sort(key=lambda x: x["_norm_ano"], reverse=True)

    return {
        "total_bruto": len(documentos_total),
        "anos_solicitados": sorted(alvo, reverse=True) if alvo else [],
        "total_encontrado": len(filtrados),
        "documentos": filtrados,
    }

@router.post("/documents/search/recibos")
def buscar_search_documentos(payload: SearchDocumentosRequest, db: Session = Depends(get_db)):
    try:
        auth_key = login(
            conta=settings.GED_CONTA, usuario=settings.GED_USUARIO, senha=settings.GED_SENHA
        )
    except Exception as e:
        raise HTTPException(502, f"Falha na autenticação no GED: {e}")
    headers = _headers(auth_key)

    r_fields = requests.post(
        f"{BASE_URL}/templates/getfields",
        data={"id_template": payload.id_template},
        headers=headers,
        timeout=30,
    )
    r_fields.raise_for_status()
    fields_json = r_fields.json() or {}
    nomes_campos = [f.get("nomecampo") for f in fields_json.get("fields", []) if f.get("nomecampo")]

    if not nomes_campos:
        raise HTTPException(400, "Template sem campos ou inválido")
    if payload.campo_anomes not in nomes_campos:
        raise HTTPException(400, f"Campo '{payload.campo_anomes}' não existe no template")

    lista_cp = ["" for _ in nomes_campos]
    for item in payload.cp:
        if item.nome not in nomes_campos:
            raise HTTPException(400, f"Campo '{item.nome}' não existe no template")
        lista_cp[nomes_campos.index(item.nome)] = (item.valor or "").strip()

    def _campos_template_txt() -> str:
        return ", ".join(nomes_campos) if nomes_campos else "(vazio)"

    if "matricula" not in nomes_campos:
        raise HTTPException(400, f"Template precisa ter 'matricula'. Campos: [{_campos_template_txt()}]")
    idx_matricula = nomes_campos.index("matricula")
    matricula_val = (lista_cp[idx_matricula] or "").strip()
    if not matricula_val:
        raise HTTPException(422, "Informe 'matricula' em cp[] para a chave composta.")

    norm_map = {_norm(n): i for i, n in enumerate(nomes_campos)}
    idx_colaborador = norm_map.get(_norm("colaborador"))
    if idx_colaborador is None:
        raise HTTPException(
            422,
            f"O template não possui o campo 'colaborador' necessário para busca aproximada por CPF. "
            f"Campos do template: [{_campos_template_txt()}]."
        )

    colaborador_original = (lista_cp[idx_colaborador] or "").strip()
    cpf_extraido = _cpf_from_any(colaborador_original)
    if not cpf_extraido:
        raise HTTPException(
            422,
            "Não foi possível extrair um CPF válido (11 dígitos) do campo 'colaborador'. "
            "Envie 'colaborador' como 'NOME_99999999999' ou apenas os 11 dígitos do CPF."
        )
    lista_cp[idx_colaborador] = f"%{cpf_extraido}%"

    # <<< NOVO: empresa como busca aproximada >>>
    idx_empresa = None
    for key in ("empresa", "cliente"):
        idx_empresa = norm_map.get(_norm(key))
        if idx_empresa is not None:
            break

    if idx_empresa is not None:
        empresa_val = (lista_cp[idx_empresa] or "").strip()
        if empresa_val:
            lista_cp[idx_empresa] = f"%{empresa_val}%"

    if not payload.anomes and not payload.anomes_in:
        if "tipodedoc" not in nomes_campos:
            raise HTTPException(400, "Campo 'tipodedoc' não existe no template")
        tipodedoc_val = (lista_cp[nomes_campos.index("tipodedoc")] or "").strip()
        if not tipodedoc_val:
            raise HTTPException(400, "Para listar anomes, informe 'tipodedoc' em cp[].")

        form_filter = [
            ("id_tipo", str(payload.id_template)),
            ("filtro", payload.campo_anomes),
            ("filtro1", "tipodedoc"),
            ("filtro1_valor", tipodedoc_val),
            ("filtro2", "matricula"),
            ("filtro2_valor", matricula_val),
        ]
        try:
            rf = requests.post(f"{BASE_URL}/documents/filter", data=form_filter, headers=headers, timeout=60)
            rf.raise_for_status()
            fdata = rf.json() or {}
            if fdata.get("error"):
                raise RuntimeError(f"GED error: {fdata.get('message')}")
            grupos = fdata.get("groups") or []

            meses_set: Set[str] = set()
            for g in grupos:
                bruto = str(g.get(payload.campo_anomes, "")).strip()
                n = _normaliza_anomes(bruto)
                if n:
                    meses_set.add(n)

            if meses_set:
                meses_sorted_objs = sorted(
                    (_to_ano_mes(m) for m in meses_set),
                    key=lambda x: (x["ano"], x["mes"]),
                    reverse=True
                )
                return {"anomes": meses_sorted_objs}

        except (requests.HTTPError, requests.RequestException, RuntimeError):
            meses_norm = _coleta_anomes_via_search(
                headers=headers,
                id_template=payload.id_template,
                nomes_campos=nomes_campos,
                lista_cp=lista_cp,
                campo_anomes=payload.campo_anomes,
            )
            if meses_norm:
                meses_sorted_objs = sorted(
                    (_to_ano_mes(m) for m in meses_norm),
                    key=lambda x: (x["ano"], x["mes"]),
                    reverse=True
                )
                return {"anomes": meses_sorted_objs}
            raise HTTPException(404, "Nenhum mês disponível para os parâmetros enviados.")

    alvo: Set[str] = set()
    if payload.anomes:
        n = _normaliza_anomes(payload.anomes)
        if not n:
            raise HTTPException(400, "anomes inválido. Use 'YYYY-MM', 'YYYYMM', 'YYYY/MM' ou 'MM/YYYY'.")
        alvo.add(n)
    if payload.anomes_in:
        for val in payload.anomes_in:
            n = _normaliza_anomes(val)
            if not n:
                raise HTTPException(400, f"Valor inválido em anomes_in: '{val}'")
            alvo.add(n)

    def _do_search(cp_override: Optional[List[str]] = None):
        form = [("id_tipo", str(payload.id_template))]
        form += [("cp[]", v) for v in (cp_override if cp_override is not None else lista_cp)]
        form += [
            ("ordem", "no_ordem"),
            ("dt_criacao", ""),
            ("pagina", "1"),
            ("colecao", "S"),
        ]
        r = requests.post(f"{BASE_URL}/documents/search", data=form, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json() or {}
        if data.get("error"):
            raise HTTPException(500, f"GED erro (search): {data.get('message')}")
        return [_flatten_attributes(doc) for doc in (data.get("documents") or [])]

    try:
        documentos_total = _do_search()
    except requests.HTTPError as err:
        try:
            raise HTTPException(err.response.status_code, f"GED erro: {err.response.json()}")
        except Exception:
            raise HTTPException(getattr(err.response, "status_code", 502), f"GED erro: {getattr(err.response, 'text', err)}")
    except requests.RequestException as e:
        raise HTTPException(502, f"Falha ao consultar GED (search): {e}")

    filtrados: List[Dict[str, Any]] = []
    for d in documentos_total:
        bruto = str(d.get(payload.campo_anomes, "")).strip()
        n = _normaliza_anomes(bruto)
        if n and (not alvo or n in alvo):
            d["_norm_anomes"] = n
            filtrados.append(d)

    if not filtrados:
        raise HTTPException(404, "Nenhum documento encontrado para os parâmetros informados.")

    filtrados.sort(key=lambda x: x["_norm_anomes"], reverse=True)

    def __table_exists(schema: str, table: str) -> bool:
        q = text("""
            SELECT 1 FROM information_schema.tables
             WHERE table_schema = :schema AND table_name = :table
            LIMIT 1
        """)
        return db.execute(q, {"schema": schema, "table": table}).first() is not None

    def __column_exists(schema: str, table: str, column: str) -> bool:
        q = text("""
            SELECT 1 FROM information_schema.columns
             WHERE table_schema = :schema AND table_name = :table AND column_name = :column
            LIMIT 1
        """)
        return db.execute(q, {"schema": schema, "table": table, "column": column}).first() is not None

    schema_status = "public"
    table_try = "tb_satus_doc"
    table_fbk = "tb_status_doc"
    table_name: Optional[str] = None
    if __table_exists(schema_status, table_try):
        table_name = f"{schema_status}.{table_try}"
    elif __table_exists(schema_status, table_fbk):
        table_name = f"{schema_status}.{table_fbk}"

    aceito_cache: Dict[str, bool] = {}

    def _aceito_for_comp(comp_norm_input: str) -> bool:
        if not table_name:
            return False
        raw_table = table_name.split(".")[1]
        has_comp = __column_exists(schema_status, raw_table, "competencia")
        has_data = __column_exists(schema_status, raw_table, "data")
        has_hora = __column_exists(schema_status, raw_table, "hora")

        if has_comp:
            comp_norm_expr = "regexp_replace(TRIM(sd.competencia), '[^0-9]', '', 'g')"
        elif has_data:
            comp_norm_expr = """
                COALESCE(
                    to_char(CASE
                                WHEN sd.data ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                                THEN to_date(sd.data, 'YYYY-MM-DD')
                                ELSE NULL
                            END, 'YYYYMM'),
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
             WHERE TRIM(sd.cpf::text) = TRIM(:cpf)
               AND TRIM(sd.matricula::text) = TRIM(:matricula)
               AND {comp_norm_expr} = :comp_norm
        """)

        try:
            val = db.execute(sql_aceite, {
                "cpf": cpf_extraido,
                "matricula": matricula_val,
                "comp_norm": comp_norm_input,
            }).scalar()
            return bool(val) if val is not None else False
        except Exception:
            db.rollback()
            return False

    meses_unicos = {d["_norm_anomes"] for d in filtrados}
    for m in meses_unicos:
        aceito_cache[m] = _aceito_for_comp(m)

    for d in filtrados:
        d["aceito"] = bool(aceito_cache.get(d["_norm_anomes"], False))

    return {
        "total_bruto": len(documentos_total),
        "meses_solicitados": sorted(alvo, reverse=True) if alvo else [],
        "total_encontrado": len(filtrados),
        "documentos": filtrados,
    }

@router.post("/documents/beneficios/buscar")
def buscar_beneficios(payload: dict = Body(...), db: Session = Depends(get_db)):
    cpf = (payload.get("cpf") or "").strip()
    matricula = (payload.get("matricula") or "").strip()
    competencia = (payload.get("competencia") or "").strip()
    empresa = (payload.get("empresa") or payload.get("cliente") or "").strip()

    if not cpf or not matricula or not competencia or not empresa:
        raise HTTPException(status_code=422, detail="Informe cpf, matricula, competencia e empresa.")

    filtro_comp = """
        regexp_replace(TRIM(competencia), '[^0-9]', '', 'g') =
        regexp_replace(TRIM(:competencia), '[^0-9]', '', 'g')
    """

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
        "beneficios": beneficios
    }

@router.post("/documents/beneficios/competencias")
async def listar_competencias_beneficios(
    request: Request,
    cpf: Optional[str] = Query(None),
    matricula: Optional[str] = Query(None),
    empresa: Optional[str] = Query(None),
    cliente: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Lista competências (ano, mes) disponíveis em tb_beneficio_eventos
    para a chave (cpf, matricula, empresa).
    """

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
def montar_beneficio(
    payload: dict = Body(...),
    db: Session = Depends(get_db)
):
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

    # Busca eventos de benefícios
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

    # Dados gerais — da primeira linha
    info = beneficios[0]
    empresa = info.get("empresa", "")
    filial = info.get("filial", "")
    cliente = info.get("cliente", "")
    lote = info.get("lote", "")
    competencia = info.get("competencia", "")
    cpf = info.get("cpf", "")
    matricula = info.get("matricula", "")

    # Totais (Decimal)
    total_geral = sum((as_decimal(b.get("valor_total")) for b in beneficios), Decimal("0"))

    # PDF
    pdf = FPDF(format="A4", unit="mm")
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    # Título
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 6, "Recibo de Benefícios", ln=1)
    pdf.ln(2)

    # Cabeçalho
    pdf.set_font("Arial", "", 9)
    pdf.cell(100, 5, f"Empresa: {empresa} - Filial: {filial}", ln=0)
    pdf.cell(0, 5, f"Cliente: {cliente}", ln=1)
    pdf.cell(0, 5, f"Competência: {competencia}   Lote: {lote}", ln=1)
    pdf.cell(0, 5, f"CPF: {cpf}   Matrícula: {matricula}", ln=1)
    pdf.ln(3)

    # Tabela - cabeçalho
    pdf.set_font("Arial", "B", 9)
    pdf.cell(15, 6, "Cód.", border=1)
    pdf.cell(80, 6, "Descrição do Benefício", border=1)
    pdf.cell(35, 6, "Tipo de Benefício", border=1)
    pdf.cell(18, 6, "Unitário", border=1, align="R")
    pdf.cell(12, 6, "Dia", border=1, align="R")
    pdf.cell(12, 6, "Mês", border=1, align="R")
    pdf.cell(18, 6, "Total", border=1, align="R")
    pdf.ln(6)

    # Tabela - linhas
    pdf.set_font("Arial", "", 9)

    for b in beneficios:
        codigo = as_str(b.get("codigo_beneficio"))
        desc = as_str(b.get("descricao_beneficio"))
        tipo_b = as_str(b.get("tipo_beneficio"))

        vu = b.get("valor_unitario")
        dia = as_int(b.get("dia"))
        mes = as_int(b.get("mes"))
        vt = b.get("valor_total")

        # cortes simples para não estourar célula (FPDF cell não quebra linha)
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

    # Total geral (uma vez)
    pdf.ln(2)
    pdf.set_font("Arial", "B", 9)
    pdf.cell(172, 6, "Total Geral", border=1, align="R")
    pdf.cell(18, 6, fmt_money(total_geral), border=1, align="R")
    pdf.ln(10)

    # Assinatura
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
        "total_geral": float(total_geral),  # se você preferir Decimal->str, eu ajusto
        "beneficios": beneficios,
        "pdf_base64": pdf_base64,
    }
