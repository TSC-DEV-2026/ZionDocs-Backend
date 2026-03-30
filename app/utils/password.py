from passlib.context import CryptContext
from passlib.exc import UnknownHashError

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def gerar_hash_senha(senha: str) -> str:
    return pwd_context.hash(senha)


def senha_esta_hasheada(senha_salva: str) -> bool:
    senha_salva = str(senha_salva or "").strip()

    if not senha_salva:
        return False

    try:
        return pwd_context.identify(senha_salva) is not None
    except Exception:
        return False


def verificar_senha(senha_plain: str, senha_hashed: str) -> bool:
    senha_plain = str(senha_plain or "")
    senha_hashed = str(senha_hashed or "")

    if not senha_hashed:
        return False

    try:
        return pwd_context.verify(senha_plain, senha_hashed)
    except UnknownHashError:
        return senha_plain == senha_hashed