from sqlalchemy import Column, Integer, String, ForeignKey, Boolean, Date
from sqlalchemy.orm import relationship
from app.database.connection import Base


class Pessoa(Base):
    __tablename__ = 'tb_pessoa'
    __table_args__ = {'schema': 'app_rh'}

    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String(100), nullable=False)
    centro_de_custo = Column(String(100), nullable=True)
    cliente = Column(String(100), nullable=True)
    empresa = Column(String(100), nullable=True)
    filial = Column(String(100), nullable=True)
    nome_empresa = Column(String(200), nullable=True)
    cpf = Column(String(14), nullable=True, unique=True)
    matricula = Column(String(50), nullable=True)
    data_nascimento = Column(Date, nullable=True)
    gestor = Column(Boolean, default=False)
    rh = Column(Boolean, default=False)
    interno = Column(Boolean, nullable=False, default=False, server_default="false")
    email = Column(String(100), nullable=True)

    usuarios = relationship("Usuario", back_populates="pessoa")

class Usuario(Base):
    __tablename__ = "tb_usuario"
    __table_args__ = {"schema": "app_rh"}

    id = Column(Integer, primary_key=True, index=True)
    id_pessoa = Column(Integer, ForeignKey("app_rh.tb_pessoa.id"), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    senha = Column(String, nullable=False)
    senha_trocada = Column(Boolean, nullable=False, default=False, server_default="false")

    pessoa = relationship("Pessoa", back_populates="usuarios")