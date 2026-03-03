from __future__ import annotations

from sqlalchemy import Boolean, Column, Date, Integer, BigInteger, String, Time
from sqlalchemy.schema import ForeignKey

from app.database.connection import Base


class PasswordResetToken(Base):
    __tablename__ = "tb_password_reset_token"
    __table_args__ = {"schema": "app_rh"}  # <<< IMPORTANTÍSSIMO

    id = Column(BigInteger, primary_key=True, index=True)

    id_pessoa = Column(
        BigInteger,
        ForeignKey("app_rh.tb_pessoa.id", ondelete="CASCADE"),  # <<< schema-qualified
        nullable=False,
        index=True,
    )

    token_hash = Column(String(64), nullable=False, index=True)

    data_criacao = Column(Date, nullable=False)
    hora_criacao = Column(Time, nullable=False)

    tempo_expiracao_min = Column(Integer, nullable=False, default=15)

    usado = Column(Boolean, nullable=False, default=False)
    inativo = Column(Boolean, nullable=False, default=False)