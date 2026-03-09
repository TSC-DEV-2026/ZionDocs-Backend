import base64
import json
import re
from typing import Any, Dict, List, Optional, Set

import requests
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

from config.settings import settings

router = APIRouter()

ZIONGED_BASE_URL = getattr(settings, "ZIONGED_BASE_URL", "https://apiged.ziondocs.com.br").rstrip("/")
ZIONGED_TIMEOUT = int(getattr(settings, "ZIONGED_TIMEOUT", 60))
ZIONGED_TOKEN = getattr(settings, "ZIONGED_TOKEN", None)


class CampoTag(BaseModel):
    chave: str
    valor: str


class DocumentoUploadMetaIn(BaseModel):
    cliente_id: int
    tags: List[CampoTag] = Field(default_factory=list)


class UploadBase64Payload(BaseModel):
    cliente_id: int
    formato: str
    documento_nome: str
    documento_base64: str
    tags: List[CampoTag] = Field(default_factory=list)


class SearchDocumentosRequest(BaseModel):
    cliente_id: int
    tags: List[CampoTag] = Field(default_factory=list)
    campo_anomes: Optional[str] = None
    anomes: Optional[str] = None
    anomes_in: Optional[List[str]] = None

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


class UpdateDocumentoPayload(BaseModel):
    filename: Optional[str] = None
    tags: Optional[List[CampoTag]] = None


def _headers(content_type_json: bool = True) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    if ZIONGED_TOKEN:
        headers["Authorization"] = f"Bearer {ZIONGED_TOKEN}"
    if content_type_json:
        headers["Content-Type"] = "application/json"
    return headers


def _extract_tag_map(document: Dict[str, Any]) -> Dict[str, str]:
    tag_map: Dict[str, str] = {}
    for tag in (document.get("tags") or []):
        chave = str(tag.get("chave") or "").strip()
        valor = str(tag.get("valor") or "").strip()
        if chave:
            tag_map[chave] = valor
    return tag_map


def _legacy_document_shape(document: Dict[str, Any]) -> Dict[str, Any]:
    tag_map = _extract_tag_map(document)
    data = {
        "id": document.get("id"),
        "uuid": document.get("uuid"),
        "cliente_id": document.get("cliente_id"),
        "bucket_key": document.get("bucket_key"),
        "nomearquivo": document.get("filename"),
        "filename": document.get("filename"),
        "content_type": document.get("content_type"),
        "tamanho_bytes": document.get("tamanho_bytes"),
        "hash_sha256": document.get("hash_sha256"),
        "datacriacao": document.get("criado_em"),
        "criado_em": document.get("criado_em"),
        "tags": document.get("tags") or [],
    }
    data.update(tag_map)
    return data


def _matches_expected_value(actual: str, expected: str) -> bool:
    actual_norm = (actual or "").strip().lower()
    expected_norm = (expected or "").strip().lower()

    if "%" in expected_norm:
        pattern = expected_norm.replace("%", "").strip()
        return pattern in actual_norm

    return actual_norm == expected_norm


def _doc_matches_tags(document: Dict[str, Any], tags: List[CampoTag]) -> bool:
    tag_map = _extract_tag_map(document)

    for tag in tags:
        chave = (tag.chave or "").strip()
        valor = (tag.valor or "").strip()

        if not chave:
            continue

        actual = tag_map.get(chave, "")
        if not _matches_expected_value(actual, valor):
            return False

    return True


def _normaliza_anomes(valor: str) -> Optional[str]:
    v = (valor or "").strip()
    if not v:
        return None

    if re.fullmatch(r"\d{4}-\d{2}", v):
        return v

    if re.fullmatch(r"\d{6}", v):
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


def _normaliza_ano(valor: str) -> Optional[str]:
    v = (valor or "").strip()
    if not v:
        return None
    match = re.search(r"\d{4}", v)
    return match.group(0) if match else None


def _to_ano_mes(yyyymm: str) -> Dict[str, int]:
    ano_str, mes_str = yyyymm.split("-", 1)
    return {"ano": int(ano_str), "mes": int(mes_str)}


def _search_all(cliente_id: int, page_size: int = 200) -> List[Dict[str, Any]]:
    page = 1
    all_items: List[Dict[str, Any]] = []

    while True:
        response = requests.get(
            f"{ZIONGED_BASE_URL}/documents/search",
            params={
                "cliente_id": cliente_id,
                "page": page,
                "page_size": page_size,
            },
            headers=_headers(content_type_json=False),
            timeout=ZIONGED_TIMEOUT,
        )

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Erro ao buscar documentos no ZionGED: {response.text}",
            )

        payload = response.json() or {}
        items = payload.get("items") or []
        meta = payload.get("meta") or {}

        all_items.extend(items)

        if not meta.get("has_next"):
            break

        page += 1

    return all_items


def _search_filtered(cliente_id: int, tags: List[CampoTag]) -> List[Dict[str, Any]]:
    docs = _search_all(cliente_id=cliente_id)
    if not tags:
        return docs
    return [doc for doc in docs if _doc_matches_tags(doc, tags)]


@router.post("/upload")
async def upload_document(
    meta: str = Form(...),
    file: UploadFile = File(...),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Arquivo sem nome.")

    try:
        meta_obj = DocumentoUploadMetaIn.model_validate_json(meta)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Meta inválida: {e}")

    file_bytes = await file.read()

    response = requests.post(
        f"{ZIONGED_BASE_URL}/documents/upload",
        data={"meta": json.dumps(meta_obj.model_dump(), ensure_ascii=False)},
        files={
            "file": (
                file.filename,
                file_bytes,
                file.content_type or "application/octet-stream",
            )
        },
        headers=_headers(content_type_json=False),
        timeout=ZIONGED_TIMEOUT,
    )

    if response.status_code not in (200, 201):
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Erro no upload para o ZionGED: {response.text}",
        )

    return _legacy_document_shape(response.json() or {})

@router.get("/search")
def search_documents(
    cliente_id: Optional[int] = None,
    tag_chave: Optional[str] = None,
    tag_valor: Optional[str] = None,
    q: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
) -> Any:
    params: Dict[str, Any] = {
        "page": page,
        "page_size": page_size,
    }

    if cliente_id is not None:
        params["cliente_id"] = cliente_id
    if tag_chave is not None:
        params["tag_chave"] = tag_chave
    if tag_valor is not None:
        params["tag_valor"] = tag_valor
    if q is not None:
        params["q"] = q

    response = requests.get(
        f"{ZIONGED_BASE_URL}/documents/search",
        params=params,
        headers=_headers(content_type_json=False),
        timeout=ZIONGED_TIMEOUT,
    )

    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Erro ao buscar documentos no ZionGED: {response.text}",
        )

    payload = response.json() or {}
    items = payload.get("items") or []
    meta = payload.get("meta") or {}

    return {
        "items": [_legacy_document_shape(item) for item in items],
        "meta": meta,
    }

@router.get("/{uuid}/download")
def baixar_documento(uuid: str):
    response = requests.get(
        f"{ZIONGED_BASE_URL}/documents/{uuid}/download",
        headers=_headers(content_type_json=False),
        timeout=ZIONGED_TIMEOUT,
        stream=True,
    )

    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Erro ao baixar documento no ZionGED: {response.text}",
        )

    content_type = response.headers.get("Content-Type", "application/octet-stream")
    content_disposition = response.headers.get("Content-Disposition", f'attachment; filename="{uuid}"')

    return StreamingResponse(
        response.iter_content(chunk_size=8192),
        media_type=content_type,
        headers={"Content-Disposition": content_disposition},
    )


@router.get("/tags")
def listar_tags(cliente_id: Optional[int] = Query(None)):
    response = requests.get(
        f"{ZIONGED_BASE_URL}/documents/tags",
        params={"cliente_id": cliente_id} if cliente_id is not None else None,
        headers=_headers(content_type_json=False),
        timeout=ZIONGED_TIMEOUT,
    )

    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Erro ao buscar tags no ZionGED: {response.text}",
        )

    return response.json()