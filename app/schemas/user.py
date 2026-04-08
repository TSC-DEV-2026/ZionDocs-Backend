from pydantic import BaseModel, ConfigDict, EmailStr, Field
from typing import Optional, Annotated, List
from datetime import date


class PessoaBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    nome: str
    centro_de_custo: Optional[str] = None
    cliente: Optional[str] = None
    cpf: Optional[Annotated[str, Field(min_length=11, max_length=14)]] = None
    matricula: Optional[str] = None
    data_nascimento: Optional[date] = None
    gestor: Optional[bool] = None
    rh: Optional[bool] = None
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


class UsuarioCreate(BaseModel):
    email: EmailStr
    senha: str


class UsuarioRead(BaseModel):
    id: int
    email: EmailStr
    id_pessoa: int


class CadastroPessoa(BaseModel):
    pessoa: PessoaCreate
    usuario: "UsuarioCreate"


class UsuarioLogin(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    usuario: str
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
    empresa: List[EmpresaFilialItem] = []
    interno: bool = False
    email_pessoa: Optional[EmailStr] = None

class CadastroColaborador(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    pessoa: PessoaCreate
    usuario: UsuarioCreate


class ColabResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    nome: str
    cpf: Optional[str] = None
    cliente: Optional[str] = None
    centro_de_custo: Optional[str] = None
    matricula: Optional[str] = None
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
    usuario: str = Field(..., min_length=3)


class PasswordResetConfirm(BaseModel):
    usuario: str = Field(..., min_length=3)
    token: str = Field(..., min_length=4)
    nova_senha: str = Field(..., min_length=4)


class PasswordResetResponse(BaseModel):
    ok: bool
    message: str

class EmpresaFilialItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    empresa: str
    filial: str
    nome_empresa: Optional[str] = None