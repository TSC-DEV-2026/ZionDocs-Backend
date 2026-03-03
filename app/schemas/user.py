# app/schemas/user.py

from pydantic import BaseModel, ConfigDict, EmailStr, Field
from typing import Optional, Annotated, List
from datetime import date

#
# Schemas para Pessoa
#
class PessoaBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    nome: str
    centro_de_custo: Optional[str]
    cliente: Optional[str]
    cpf: Optional[Annotated[str, Field(min_length=11, max_length=14)]]
    matricula: Optional[str]
    data_nascimento: Optional[date]
    gestor: Optional[bool]
    rh: Optional[bool]
    interno: Optional[bool] = False
    email: Optional[EmailStr] = None

class PessoaCreate(BaseModel):
    nome: str
    cpf: str
    cliente: str
    centro_de_custo: str
    matricula: str
    gestor: bool
    rh: bool
    data_nascimento: date
    interno: bool = False
    email: Optional[EmailStr] = None

class PessoaRead(PessoaBase):
    id: int

#
# Schemas para Usuário
#
class UsuarioCreate(BaseModel):
    email: EmailStr
    senha: str

class UsuarioRead(BaseModel):
    id: int
    email: EmailStr
    id_pessoa: int
#
class CadastroPessoa(BaseModel):
    pessoa: PessoaCreate
    usuario: "UsuarioCreate"

class UsuarioLogin(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    usuario: str   # e-mail ou CPF
    senha: str

class DadoItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    nome: Optional[str] = None
    matricula: str

class PessoaResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    nome: str
    cpf: Optional[str]
    cliente: Optional[str]
    centro_de_custo: Optional[str]
    gestor: Optional[bool]
    rh: Optional[bool]
    email: str
    senha_trocada: bool = False
    dados: List[DadoItem]
    interno: bool = False
    email_pessoa: Optional[EmailStr] = None

class CadastroColaborador(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    pessoa: PessoaCreate
    usuario: UsuarioCreate

class ColabResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    nome: str
    cpf: Optional[str]
    cliente: Optional[str]
    centro_de_custo: Optional[str]
    matricula: Optional[str]
    email: str


class AtualizarSenhaRequest(BaseModel):

    cpf: str = Field(..., min_length=1)
    senha_atual: str = Field(..., min_length=1)
    senha_nova: str = Field(..., min_length=1)

class InternalSendTokenResponse(BaseModel):
    ok: bool
    message: str

class InternalValidateTokenRequest(BaseModel):
    token: str = Field(..., min_length=1)

class InternalValidateTokenResponse(BaseModel):
    valid: bool
    reason: Optional[str] = None

class PasswordResetRequest(BaseModel):
    email: str = Field(..., min_length=5)


class PasswordResetConfirm(BaseModel):
    email: str = Field(..., min_length=5)
    token: str = Field(..., min_length=4)
    nova_senha: str = Field(..., min_length=4)


class PasswordResetResponse(BaseModel):
    ok: bool
    message: str

class PasswordResetRequest(BaseModel):
    usuario: str = Field(..., min_length=3)  # CPF ou "email-login" (tb_usuario.email)

class PasswordResetConfirm(BaseModel):
    usuario: str = Field(..., min_length=3)
    token: str = Field(..., min_length=4)
    nova_senha: str = Field(..., min_length=4)

class PasswordResetResponse(BaseModel):
    ok: bool
    message: str