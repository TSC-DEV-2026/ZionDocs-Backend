from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database.connection import engine, Base

from app.models.user import Pessoa, Usuario
_ = (Pessoa, Usuario)

Base.metadata.create_all(bind=engine)

from app.routers import livechat as livechat_router
from app.routers import document as documents_router
from app.routers import user  as usuario_router
from app.routers import ged   as ged_router
from app.routers import gustavo as gustavo_router

app = FastAPI(title="Consulta de Documentos – WeCanBR")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "https://rh.ziondocs.com.br", "http://frontend-ziondocs.s3-website.us-east-2.amazonaws.com", "https://ziondocs-frontend.onrender.com", "https://rh2.ziondocs.com.br"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# rotas
app.include_router(documents_router.router, tags=["Documentos"])
app.include_router(usuario_router.router, tags=["Usuários"])
app.include_router(ged_router.router, tags=["GED"])
app.include_router(livechat_router.router, tags=["Live Chat"])
app.include_router(gustavo_router.router, tags=["Gustavo"])

@app.get("/")
def root():
    return {"msg": "API ok"}
