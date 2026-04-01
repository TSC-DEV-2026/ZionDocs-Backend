import hashlib
import os
import re
import secrets
from datetime import datetime, timedelta
import traceback
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database.connection import get_db
from app.models.blacklist import TokenBlacklist
from app.models.token_interno import TokenInterno
from app.models.password_reset_token import PasswordResetToken
from app.models.user import Pessoa, Usuario
from app.schemas.user import (
    AtualizarSenhaRequest,
    CadastroPessoa,
    DadoItem,
    PessoaResponse,
    UsuarioLogin,
    InternalSendTokenResponse,
    InternalValidateTokenRequest,
    InternalValidateTokenResponse,
    PasswordResetRequest,
    PasswordResetConfirm,
    PasswordResetResponse,
)
from app.utils.email_sender import send_email_smtp
from app.utils.jwt_handler import criar_token, decode_token, verificar_token
from app.utils.password import gerar_hash_senha, verificar_senha, senha_esta_hasheada
from dotenv import load_dotenv

router = APIRouter()

load_dotenv()
is_prod = os.getenv("ENVIRONMENT") == "prod"

cookie_domain = "ziondocs.com.br" if is_prod else None

cookie_env = {
    "secure": True if is_prod else False,
    "samesite": "Lax",
    "domain": cookie_domain,
}

ACCESS_TOKEN_MINUTES = 60 * 24 * 7
REFRESH_TOKEN_MINUTES = 60 * 24 * 30


def _norm_digits(v: str) -> str:
    return "".join(ch for ch in str(v or "") if ch.isdigit())


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _token_is_expired(data_criacao, hora_criacao, tempo_expiracao_min: int) -> bool:
    created_dt = datetime.combine(data_criacao, hora_criacao)
    exp_dt = created_dt + timedelta(minutes=int(tempo_expiracao_min or 0))
    return datetime.now() > exp_dt


def _gen_reset_code() -> str:
    return f"{secrets.randbelow(1000000):06d}"


def is_email(valor: str) -> bool:
    return re.match(r"[^@]+@[^@]+\.[^@]+", str(valor or "").strip()) is not None


def get_bearer_token(request: Request) -> Optional[str]:
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return None

    parts = auth_header.split(" ", 1)
    if len(parts) != 2:
        return None

    scheme, token = parts
    if scheme.lower() != "bearer":
        return None

    token = token.strip()
    return token or None


def get_access_token_from_request(request: Request) -> Optional[str]:
    token = request.cookies.get("access_token")
    if token:
        return token
    return get_bearer_token(request)


def get_refresh_token_from_request(request: Request) -> Optional[str]:
    token = request.cookies.get("refresh_token")
    if token:
        return token
    return get_bearer_token(request)


def get_usuario_from_login(db: Session, login_value: str) -> Optional[Usuario]:
    login_value = str(login_value or "").strip()

    if is_email(login_value):
        return db.query(Usuario).filter(Usuario.email == login_value).first()

    pessoa = db.query(Pessoa).filter(Pessoa.cpf == login_value).first()
    if not pessoa:
        return None

    return db.query(Usuario).filter(Usuario.id_pessoa == pessoa.id).first()


def build_auth_payload(usuario: Usuario):
    access_token = criar_token(
        {"id": usuario.id_pessoa, "sub": usuario.email, "tipo": "access"},
        expires_in=ACCESS_TOKEN_MINUTES,
    )
    refresh_token = criar_token(
        {"id": usuario.id_pessoa, "sub": usuario.email, "tipo": "refresh"},
        expires_in=REFRESH_TOKEN_MINUTES,
    )
    return access_token, refresh_token


def set_auth_cookies(response: Response, access_token: str, refresh_token: str):
    response.set_cookie(
        "access_token",
        access_token,
        httponly=True,
        max_age=60 * 60 * 24 * 7,
        path="/",
        **cookie_env,
    )
    response.set_cookie(
        "refresh_token",
        refresh_token,
        httponly=True,
        max_age=60 * 60 * 24 * 30,
        path="/",
        **cookie_env,
    )
    response.set_cookie(
        "logged_user",
        "true",
        httponly=False,
        max_age=60 * 60 * 24 * 7,
        path="/",
        **cookie_env,
    )


def clear_auth_cookies(response: Response):
    response.delete_cookie("access_token", path="/", domain=cookie_domain)
    response.delete_cookie("refresh_token", path="/", domain=cookie_domain)
    response.delete_cookie("logged_user", path="/", domain=cookie_domain)


def get_current_auth(request: Request, db: Session):
    access_token = get_access_token_from_request(request)
    if not access_token:
        raise HTTPException(status_code=401, detail="Token de autenticação ausente")

    payload = verificar_token(access_token)
    if not payload:
        raise HTTPException(status_code=401, detail="Token inválido")

    if payload.get("tipo") != "access":
        raise HTTPException(status_code=401, detail="Tipo de token inválido")

    jti = payload.get("jti")
    if not jti:
        raise HTTPException(status_code=401, detail="Token sem identificador único (jti)")

    if db.query(TokenBlacklist).filter_by(jti=jti).first():
        raise HTTPException(status_code=401, detail="Token expirado ou inválido")

    pessoa_id = payload.get("id")
    pessoa = db.query(Pessoa).filter(Pessoa.id == pessoa_id).first()
    if not pessoa:
        raise HTTPException(status_code=401, detail="Pessoa não encontrada")

    usuario = db.query(Usuario).filter(Usuario.id_pessoa == pessoa.id).first()
    if not usuario:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    return payload, pessoa, usuario


@router.post(
    "/user/internal/send-token",
    response_model=InternalSendTokenResponse,
    status_code=status.HTTP_200_OK,
)
def internal_send_token(
    request: Request,
    db: Session = Depends(get_db),
):
    _, pessoa, _ = get_current_auth(request, db)

    if not bool(getattr(pessoa, "interno", False)):
        raise HTTPException(status_code=403, detail="Pessoa não é interna")

    email_destino = str(getattr(pessoa, "email", "") or "").strip().lower()
    if not email_destino:
        raise HTTPException(status_code=400, detail="Pessoa interna sem email cadastrado")

    token_plain = f"{secrets.randbelow(10000):04d}"
    token_hash = _hash_token(token_plain)

    db.query(TokenInterno).filter(
        TokenInterno.id_pessoa == pessoa.id,
        TokenInterno.inativo == False
    ).update({"inativo": True}, synchronize_session=False)
    db.commit()

    now = datetime.now()

    novo = TokenInterno(
        id_pessoa=pessoa.id,
        token=token_hash,
        data_criacao=now.date(),
        hora_criacao=now.time().replace(microsecond=0),
        tempo_expiracao_min=15,
        inativo=False,
    )
    db.add(novo)
    db.commit()

    subject = "Seu token de validação (ZionDocs)"
    body_text = (
        f"Olá, {pessoa.nome}.\n\n"
        f"Seu token de validação é:\n\n"
        f"{token_plain}\n\n"
        f"Esse token expira em 15 minutos.\n"
        f"Se você não solicitou, ignore esta mensagem.\n"
    )

    try:
        send_email_smtp(email_destino, subject, body_text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao enviar e-mail: {e}")

    return InternalSendTokenResponse(ok=True, message="Token enviado para o e-mail")


@router.post(
    "/user/internal/validate-token",
    response_model=InternalValidateTokenResponse,
    status_code=status.HTTP_200_OK,
)
def internal_validate_token(
    body: InternalValidateTokenRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    _, pessoa, _ = get_current_auth(request, db)

    if not bool(getattr(pessoa, "interno", False)):
        return InternalValidateTokenResponse(valid=False, reason="not_internal")

    token_plain = str(body.token or "").strip()
    if not token_plain:
        return InternalValidateTokenResponse(valid=False, reason="token_empty")

    token_hash = _hash_token(token_plain)

    row = (
        db.query(TokenInterno)
        .filter(TokenInterno.id_pessoa == pessoa.id)
        .filter(TokenInterno.token == token_hash)
        .first()
    )

    if not row:
        return InternalValidateTokenResponse(valid=False, reason="token_not_found")

    if bool(row.inativo):
        return InternalValidateTokenResponse(valid=False, reason="token_inactive")

    if _token_is_expired(row.data_criacao, row.hora_criacao, row.tempo_expiracao_min):
        row.inativo = True
        db.commit()
        return InternalValidateTokenResponse(valid=False, reason="token_expired")

    row.inativo = True

    db.query(TokenInterno).filter(
        TokenInterno.id_pessoa == pessoa.id,
        TokenInterno.inativo == False
    ).update({"inativo": True}, synchronize_session=False)

    db.commit()

    return InternalValidateTokenResponse(valid=True, reason=None)


@router.post(
    "/user/register",
    response_model=None,
    status_code=status.HTTP_201_CREATED
)
def registrar_usuario(
    payload: CadastroPessoa,
    db: Session = Depends(get_db),
):
    cpf = str(payload.pessoa.cpf or "").strip()
    email = str(payload.usuario.email or "").strip().lower()

    if db.query(Pessoa).filter(Pessoa.cpf == cpf).first():
        raise HTTPException(400, "CPF já cadastrado")

    if db.query(Usuario).filter(Usuario.email == email).first():
        raise HTTPException(400, "Email já cadastrado")

    pessoa = Pessoa(**payload.pessoa.dict())
    db.add(pessoa)
    db.commit()
    db.refresh(pessoa)

    usuario = Usuario(
        id_pessoa=pessoa.id,
        email=email,
        senha=gerar_hash_senha(payload.usuario.senha)
    )
    db.add(usuario)
    db.commit()
    db.refresh(usuario)

    return {
        "message": "Usuário cadastrado com sucesso",
        "id_pessoa": pessoa.id,
        "email": usuario.email,
    }


@router.post(
    "/user/login",
    response_model=None,
    status_code=status.HTTP_200_OK
)
def login_user(
    payload: UsuarioLogin,
    db: Session = Depends(get_db),
):
    print("[LOGIN] Requisição recebida:", {"usuario": payload.usuario})

    try:
        usuario = get_usuario_from_login(db, payload.usuario)

        if not usuario:
            print("[LOGIN] Usuário não encontrado:", payload.usuario)
            raise HTTPException(status_code=401, detail="Usuário ou senha inválidos")

        print("[LOGIN] Usuário encontrado:", {
            "id": usuario.id,
            "id_pessoa": usuario.id_pessoa,
            "email": usuario.email,
        })

        senha_ok = verificar_senha(payload.senha, usuario.senha)

        if not senha_ok:
            print("[LOGIN] Senha inválida para usuário:", usuario.email)
            raise HTTPException(status_code=401, detail="Usuário ou senha inválidos")

        if not senha_esta_hasheada(usuario.senha):
            usuario.senha = gerar_hash_senha(payload.senha)
            db.add(usuario)
            db.commit()
            db.refresh(usuario)
            print("[LOGIN] Senha legada migrada para bcrypt:", usuario.email)

        access_token, refresh_token = build_auth_payload(usuario)

        response = JSONResponse(content={
            "message": "Login com sucesso",
            "mode": "cookie",
        })
        set_auth_cookies(response, access_token, refresh_token)

        print("[LOGIN] OK - resposta retornada.")
        return response

    except HTTPException as e:
        print("[LOGIN] HTTPException:", {"status": e.status_code, "detail": e.detail})
        raise

    except Exception as e:
        print("[LOGIN] ERRO 500:", repr(e))
        print("[LOGIN] TRACEBACK:\n", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Erro interno no login")


@router.post(
    "/user/login-mobile",
    response_model=None,
    status_code=status.HTTP_200_OK
)
def login_user_mobile(
    payload: UsuarioLogin,
    db: Session = Depends(get_db),
):
    usuario = get_usuario_from_login(db, payload.usuario)

    if not usuario:
        raise HTTPException(status_code=401, detail="Usuário ou senha inválidos")

    senha_ok = verificar_senha(payload.senha, usuario.senha)

    if not senha_ok:
        raise HTTPException(status_code=401, detail="Usuário ou senha inválidos")

    if not senha_esta_hasheada(usuario.senha):
        usuario.senha = gerar_hash_senha(payload.senha)
        db.add(usuario)
        db.commit()
        db.refresh(usuario)

    access_token, refresh_token = build_auth_payload(usuario)

    return {
        "message": "Login com sucesso",
        "mode": "bearer",
        "token_type": "bearer",
        "access_token": access_token,
        "refresh_token": refresh_token,
    }


@router.get("/user/me", response_model=PessoaResponse)
def get_me(request: Request, db: Session = Depends(get_db)):
    _, pessoa, usuario = get_current_auth(request, db)

    cpf_pessoa = str(pessoa.cpf or "").strip()
    mat_pessoa = str(getattr(pessoa, "matricula", "") or "").strip()

    sql_dados = text("""
        SELECT DISTINCT
               TRIM(c.cliente::text)   AS id,
               TRIM(c.cliente_nome)    AS nome,
               TRIM(c.matricula::text) AS mat
        FROM tb_holerite_cabecalhos c
        WHERE
            (
                regexp_replace(TRIM(c.cpf::text), '[^0-9]', '', 'g')
                = regexp_replace(TRIM(:cpf), '[^0-9]', '', 'g')
                OR
                (TRIM(:matricula) <> '' AND TRIM(c.matricula::text) = TRIM(:matricula))
            )
          AND c.matricula IS NOT NULL AND TRIM(c.matricula::text) <> ''
          AND c.cliente  IS NOT NULL AND TRIM(c.cliente::text)  <> ''
          AND c.cliente_nome IS NOT NULL AND TRIM(c.cliente_nome) <> ''
        ORDER BY nome, id, mat
    """)

    rows = db.execute(sql_dados, {"cpf": cpf_pessoa, "matricula": mat_pessoa}).fetchall()

    dados: List[DadoItem] = [
        DadoItem(id=row[0], nome=row[1], matricula=row[2])
        for row in rows
        if row[1] and str(row[1]).strip()
    ]

    nomes_por_cliente = {
        str(row[0]).strip(): str(row[1]).strip()
        for row in rows
        if row[1] and str(row[1]).strip()
    }

    if mat_pessoa and getattr(pessoa, "cliente", None):
        cli_pessoa = str(pessoa.cliente).strip()

        ja_existe_vinculo = any(
            d.id == cli_pessoa and d.matricula == mat_pessoa
            for d in dados
        )

        if not ja_existe_vinculo:
            nome_cliente = nomes_por_cliente.get(cli_pessoa)

            if not nome_cliente:
                sql_nome_cliente = text("""
                    SELECT TRIM(c.cliente_nome) AS nome
                    FROM tb_holerite_cabecalhos c
                    WHERE TRIM(c.cliente::text) = TRIM(:cliente)
                      AND c.cliente_nome IS NOT NULL
                      AND TRIM(c.cliente_nome) <> ''
                    ORDER BY c.cliente_nome
                    LIMIT 1
                """)
                nome_cliente = db.execute(sql_nome_cliente, {"cliente": cli_pessoa}).scalar()

            if nome_cliente and str(nome_cliente).strip():
                dados.insert(
                    0,
                    DadoItem(
                        id=cli_pessoa,
                        nome=str(nome_cliente).strip(),
                        matricula=mat_pessoa,
                    ),
                )

    return PessoaResponse(
        nome=pessoa.nome,
        cpf=str(pessoa.cpf),
        email=str(usuario.email),
        cliente=getattr(pessoa, "cliente", None),
        centro_de_custo=getattr(pessoa, "centro_de_custo", None),
        gestor=bool(getattr(pessoa, "gestor", False)),
        rh=bool(getattr(pessoa, "rh", False)),
        senha_trocada=bool(getattr(usuario, "senha_trocada", False)),
        interno=bool(getattr(pessoa, "interno", False)),
        email_pessoa=(pessoa.email or None),
        dados=dados,
    )


@router.post("/user/refresh")
def refresh_token(request: Request, db: Session = Depends(get_db)):
    token = request.cookies.get("refresh_token")
    if not token:
        raise HTTPException(status_code=400, detail="refreshToken não fornecido")

    payload = verificar_token(token)
    if not payload or payload.get("tipo") != "refresh":
        raise HTTPException(status_code=401, detail="refreshToken inválido ou expirado")

    email = payload.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Token inválido")

    usuario = db.query(Usuario).filter(Usuario.email == email).first()
    if not usuario:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    novo_access, novo_refresh = build_auth_payload(usuario)

    response = JSONResponse(content={
        "message": "Token renovado",
        "mode": "cookie",
    })
    set_auth_cookies(response, novo_access, novo_refresh)
    return response


@router.post("/user/refresh-mobile")
def refresh_token_mobile(request: Request, db: Session = Depends(get_db)):
    token = get_bearer_token(request)
    if not token:
        raise HTTPException(status_code=400, detail="refreshToken não fornecido")

    payload = verificar_token(token)
    if not payload or payload.get("tipo") != "refresh":
        raise HTTPException(status_code=401, detail="refreshToken inválido ou expirado")

    email = payload.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Token inválido")

    usuario = db.query(Usuario).filter(Usuario.email == email).first()
    if not usuario:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    novo_access, novo_refresh = build_auth_payload(usuario)

    return {
        "message": "Token renovado",
        "mode": "bearer",
        "token_type": "bearer",
        "access_token": novo_access,
        "refresh_token": novo_refresh,
    }


@router.post("/user/logout")
def logout(request: Request, db: Session = Depends(get_db)):
    token = get_access_token_from_request(request)

    if token:
        try:
            payload = decode_token(token)
            jti = payload.get("jti")
            exp_ts = payload.get("exp")

            if jti and exp_ts:
                exists = db.query(TokenBlacklist).filter_by(jti=jti).first()
                if not exists:
                    exp = datetime.fromtimestamp(exp_ts)
                    db.add(TokenBlacklist(jti=jti, expira_em=exp))
                    db.commit()
        except Exception as e:
            print(f"[ERRO LOGOUT] {e}")
    else:
        print("[LOGOUT] Token não enviado")

    response = JSONResponse(content={"message": "Logout realizado com sucesso"})
    clear_auth_cookies(response)
    return response


@router.post(
    "/user/request-password-reset",
    response_model=PasswordResetResponse,
    status_code=status.HTTP_200_OK,
)
def request_password_reset(
    body: PasswordResetRequest,
    db: Session = Depends(get_db),
):
    usuario_login = str(body.usuario or "").strip()
    if not usuario_login:
        raise HTTPException(status_code=400, detail="Usuário inválido")

    is_cpf = re.fullmatch(r"[\d.\-]+", usuario_login or "") is not None
    usuario_norm = _norm_digits(usuario_login) if is_cpf else usuario_login.strip().lower()

    sql_find = text("""
        SELECT
            u.id_pessoa,
            TRIM(COALESCE(p.nome::text, '')) AS nome,
            TRIM(COALESCE(p.email::text, '')) AS email_real
        FROM app_rh.tb_usuario u
        JOIN app_rh.tb_pessoa p ON p.id = u.id_pessoa
        WHERE
            (
                (:is_cpf = TRUE AND regexp_replace(TRIM(u.email::text), '[^0-9]', '', 'g') = :usuario_norm)
                OR
                (:is_cpf = FALSE AND LOWER(TRIM(u.email::text)) = LOWER(:usuario_norm))
            )
        LIMIT 1
    """)

    row = db.execute(
        sql_find,
        {"is_cpf": is_cpf, "usuario_norm": usuario_norm},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    id_pessoa = int(row[0])
    nome_pessoa = str(row[1] or "Usuário").strip() or "Usuário"
    email_destino = str(row[2] or "").strip().lower()

    if not email_destino or "@" not in email_destino:
        raise HTTPException(status_code=400, detail="Pessoa sem e-mail válido cadastrado")

    db.query(PasswordResetToken).filter(
        PasswordResetToken.id_pessoa == id_pessoa,
        PasswordResetToken.inativo == False,
        PasswordResetToken.usado == False,
    ).update({"inativo": True}, synchronize_session=False)
    db.commit()

    token_plain = _gen_reset_code()
    token_hash = _hash_token(token_plain)

    now = datetime.now()
    db.add(
        PasswordResetToken(
            id_pessoa=id_pessoa,
            token_hash=token_hash,
            data_criacao=now.date(),
            hora_criacao=now.time().replace(microsecond=0),
            tempo_expiracao_min=15,
            usado=False,
            inativo=False,
        )
    )
    db.commit()

    subject = "Recuperação de senha (ZionDocs)"
    body_text = (
        f"Olá, {nome_pessoa}.\n\n"
        f"Seu código para redefinir a senha é:\n\n"
        f"{token_plain}\n\n"
        f"Esse código expira em 15 minutos.\n"
        f"Se você não solicitou, ignore esta mensagem.\n"
    )

    try:
        send_email_smtp(email_destino, subject, body_text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao enviar e-mail: {e}")

    return PasswordResetResponse(ok=True, message="Código enviado para o e-mail")


@router.post(
    "/user/reset-password",
    response_model=PasswordResetResponse,
    status_code=status.HTTP_200_OK,
)
def reset_password(
    body: PasswordResetConfirm,
    db: Session = Depends(get_db),
):
    usuario_login = str(body.usuario or "").strip()
    token_plain = str(body.token or "").strip()
    nova_senha = str(body.nova_senha or "").strip()

    if not usuario_login or not token_plain or not nova_senha:
        raise HTTPException(status_code=400, detail="Dados inválidos")

    is_cpf = re.fullmatch(r"[\d.\-]+", usuario_login or "") is not None
    usuario_norm = _norm_digits(usuario_login) if is_cpf else usuario_login.strip().lower()

    sql_resolve = text("""
        SELECT
            u.id_pessoa,
            regexp_replace(TRIM(p.cpf::text), '[^0-9]', '', 'g') AS cpf
        FROM app_rh.tb_usuario u
        JOIN app_rh.tb_pessoa p ON p.id = u.id_pessoa
        WHERE
            (
                (:is_cpf = TRUE AND regexp_replace(TRIM(u.email::text), '[^0-9]', '', 'g') = :usuario_norm)
                OR
                (:is_cpf = FALSE AND LOWER(TRIM(u.email::text)) = LOWER(:usuario_norm))
            )
        LIMIT 1
    """)

    row = db.execute(sql_resolve, {"is_cpf": is_cpf, "usuario_norm": usuario_norm}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    id_pessoa = int(row[0])
    cpf_digits = str(row[1] or "").strip()
    if not cpf_digits:
        raise HTTPException(status_code=400, detail="CPF inválido para redefinição")

    token_hash = _hash_token(token_plain)

    token_row = (
        db.query(PasswordResetToken)
        .filter(PasswordResetToken.id_pessoa == id_pessoa)
        .filter(PasswordResetToken.token_hash == token_hash)
        .first()
    )

    if not token_row:
        raise HTTPException(status_code=400, detail="Código inválido")

    if bool(token_row.inativo) or bool(token_row.usado):
        raise HTTPException(status_code=400, detail="Código inválido ou já utilizado")

    if _token_is_expired(token_row.data_criacao, token_row.hora_criacao, token_row.tempo_expiracao_min):
        token_row.inativo = True
        db.commit()
        raise HTTPException(status_code=400, detail="Código expirado")

    token_row.usado = True
    token_row.inativo = True

    db.query(PasswordResetToken).filter(
        PasswordResetToken.id_pessoa == id_pessoa,
        PasswordResetToken.inativo == False,
        PasswordResetToken.usado == False,
    ).update({"inativo": True}, synchronize_session=False)

    db.commit()

    senha_hash = gerar_hash_senha(nova_senha)

    sql_update = text("""
        UPDATE app_rh.tb_usuario u
        SET
            senha = :senha_nova,
            senha_trocada = TRUE
        FROM app_rh.tb_pessoa p
        WHERE
            u.id_pessoa = p.id
            AND regexp_replace(TRIM(p.cpf::text), '[^0-9]', '', 'g')
                = regexp_replace(TRIM(:cpf), '[^0-9]', '', 'g')
    """)

    result = db.execute(sql_update, {"senha_nova": senha_hash, "cpf": cpf_digits})
    db.commit()

    updated_rows = result.rowcount or 0
    if updated_rows == 0:
        raise HTTPException(status_code=404, detail="Nenhum usuário encontrado para o CPF informado")

    return PasswordResetResponse(ok=True, message="Senha redefinida com sucesso")


@router.put("/user/update-password")
def update_password(
    body: AtualizarSenhaRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    _, pessoa, _ = get_current_auth(request, db)

    cpf_body = "".join(ch for ch in str(body.cpf or "") if ch.isdigit())
    cpf_pessoa = "".join(ch for ch in str(pessoa.cpf or "") if ch.isdigit())

    if not cpf_body or cpf_body != cpf_pessoa:
        raise HTTPException(status_code=403, detail="CPF não confere com o usuário autenticado")

    if body.senha_atual == body.senha_nova:
        raise HTTPException(status_code=400, detail="A nova senha não pode ser igual à senha antiga")

    usuarios_do_cpf = (
        db.query(Usuario, Pessoa)
        .join(Pessoa, Pessoa.id == Usuario.id_pessoa)
        .all()
    )

    encontrou_senha_atual = False
    for usuario_item, pessoa_item in usuarios_do_cpf:
        cpf_item = "".join(ch for ch in str(pessoa_item.cpf or "") if ch.isdigit())
        if cpf_item == cpf_body and verificar_senha(body.senha_atual, usuario_item.senha):
            encontrou_senha_atual = True
            break

    if not encontrou_senha_atual:
        raise HTTPException(status_code=400, detail="Senha antiga incorreta")

    senha_hash = gerar_hash_senha(body.senha_nova)

    sql_update = text("""
        UPDATE app_rh.tb_usuario u
        SET
            senha = :senha_nova,
            senha_trocada = TRUE
        FROM app_rh.tb_pessoa p
        WHERE
            u.id_pessoa = p.id
            AND regexp_replace(TRIM(p.cpf::text), '[^0-9]', '', 'g')
                = regexp_replace(TRIM(:cpf), '[^0-9]', '', 'g')
    """)

    result = db.execute(
        sql_update,
        {"senha_nova": senha_hash, "cpf": cpf_body},
    )
    db.commit()

    updated_rows = result.rowcount or 0
    if updated_rows == 0:
        raise HTTPException(status_code=404, detail="Nenhum usuário encontrado para o CPF informado")

    return {
        "message": "Senha atualizada com sucesso",
        "senha_trocada": True,
        "usuarios_atualizados": updated_rows,
    }