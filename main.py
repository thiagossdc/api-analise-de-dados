from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import validator
from cache import cache_manager, cached
from datetime import timedelta
from fastapi_limiter import FastAPILimiter
from redis import Redis
from typing import Dict, Any
import time
import random
from functools import wraps
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.openapi.utils import get_openapi
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
import asyncio
from messaging import message_queue, TOPICS
from typing import List
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from jose import JWTError, jwt
import bcrypt
import os
import io
import pdfkit
from pathlib import Path

# Configurações
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/analise_dados")
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-here")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

# Inicialização do FastAPI
app = FastAPI(
    title="API de Análise de Dados",
    version="1.0.0",
    description="API desenvolvida para análise estatística e visualização de dados, seguindo os princípios de Clean Code e arquitetura hexagonal.",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Eventos de inicialização
@app.on_event("startup")
async def startup_event():
    message_queue.start()

@app.on_event("shutdown")
async def shutdown_event():
    message_queue.stop()

# Configuração CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuração do banco de dados
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Configuração do rate limiting
@app.on_event("startup")
async def startup_rate_limit():
    redis_instance = Redis(host="localhost", port=6379, db=0)
    await FastAPILimiter.init(redis_instance)

# Padrão Circuit Breaker
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60, expected_exception=Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failures = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def __call__(self, func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if self.state == "OPEN":
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = "HALF_OPEN"
                else:
                    raise HTTPException(status_code=503, detail="Serviço temporariamente indisponível")

            try:
                result = await func(*args, **kwargs)
                # Se funcionou, resetar falhas
                self.failures = 0
                self.state = "CLOSED"
                return result
            except self.expected_exception:
                self.failures += 1
                self.last_failure_time = time.time()
                if self.failures >= self.failure_threshold:
                    self.state = "OPEN"
                raise
        return wrapper

# Padrão Retry (Tentativa Novamente)
def retry(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        raise
                    await asyncio.sleep(delay * (backoff ** (retries - 1)) + random.random())
            return await func(*args, **kwargs)
        return wrapper
    return decorator

# Segurança
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")

# Modelos do Banco de Dados
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

class DataUpload(Base):
    __tablename__ = "data_uploads"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    filename = Column(String)
    file_type = Column(String)
    upload_date = Column(DateTime, default=datetime.utcnow)
    data_preview = Column(Text)

# Modelos de Validação Pydantic
class UserCreate(BaseModel):
    username: str
    email: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: str | None = None

class UserOut(BaseModel):
    id: int
    username: str
    email: str
    created_at: datetime

    class Config:
        from_attributes = True

class DataUploadOut(BaseModel):
    id: int
    filename: str
    file_type: str
    upload_date: datetime
    data_preview: str

    class Config:
        from_attributes = True

class UploadFileSchema(BaseModel):
    file: UploadFile = Field(..., description="Arquivo CSV ou JSON para upload")
    chart_type: str = Field(None, description="Tipo de gráfico (bar, line, pie)")
    format: str = Field(None, description="Formato do relatório (pdf, csv, json)")

    @validator('file')
    def validate_file(cls, v):
        if v.content_type not in ["text/csv", "application/json"]:
            raise ValueError("Formato de arquivo não suportado. Apenas CSV e JSON são permitidos.")
        return v

    @validator('chart_type')
    def validate_chart_type(cls, v):
        if v and v not in ["bar", "line", "pie"]:
            raise ValueError("Tipo de gráfico inválido. Opções: bar, line, pie")
        return v

    @validator('format')
    def validate_format(cls, v):
        if v and v not in ["pdf", "csv", "json"]:
            raise ValueError("Formato inválido. Opções: pdf, csv, json")
        return v

class BIExportSchema(BaseModel):
    format: str = Field(..., description="Formato de exportação (tableau, powerbi, csv, json)")
    include_metadata: bool = Field(True, description="Incluir metadados na exportação")

    @validator('format')
    def validate_format(cls, v):
        if v not in ["tableau", "powerbi", "csv", "json"]:
            raise ValueError("Formato inválido. Opções: tableau, powerbi, csv, json")
        return v

# Funções de autenticação
def get_password_hash(password):
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())

def verify_password(plain_password, hashed_password):
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password)

# Dependência para sessão do banco de dados
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_user(db, username: str):
    return db.query(User).filter(User.username == username).first()

def authenticate_user(db, username: str, password: str):
    user = get_user(db, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme), db = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=401,
            detail="Não foi possível validar as credenciais",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user = get_user(db, username=username)
    if user is None:
        raise credentials_exception
    return user

async def get_current_active_user(current_user: UserOut = Depends(get_current_user)):
    return current_user

# Endpoints
@app.post("/register", response_model=UserOut)
@FastAPILimiter.limit("5/minute")  # Limite de 5 registros por minuto
def register(user: UserCreate, db: SessionLocal = Depends(SessionLocal)):
    # Limpar cache relacionado ao usuário
    cache_manager.delete(f"user:{user.username}")
    db_user = get_user(db, username=user.username)
    if db_user:
        raise HTTPException(status_code=400, detail="Nome de usuário já está cadastrado")
    hashed_password = get_password_hash(user.password)
    db_user = User(
        username=user.username,
        email=user.email,
        hashed_password=hashed_password
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

@app.post("/token", response_model=Token)
@FastAPILimiter.limit("10/minute")  # Limite de 10 tentativas de login por minuto
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: SessionLocal = Depends(SessionLocal)):
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=400,
            detail="Nome de usuário ou senha incorretos",
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/upload", response_model=DataUploadOut)
@FastAPILimiter.limit("20/minute")  # Limite de 20 uploads por minuto por usuário
@retry(max_retries=3, delay=1.0, backoff=2.0)
@CircuitBreaker(failure_threshold=3, recovery_timeout=60)
async def upload_file(file: UploadFile = File(...), current_user: UserOut = Depends(get_current_active_user)):
    # Validação adicional de segurança
    if not file.filename.endswith(('.csv', '.json')):
        raise HTTPException(status_code=400, detail="Nome de arquivo inválido. Deve terminar em .csv ou .json")

    # Limite de tamanho do arquivo (10MB)
    if file.size > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Tamanho do arquivo excede o limite de 10MB")
    if file.content_type not in ["text/csv", "application/json"]:
        raise HTTPException(status_code=400, detail="Formato de arquivo não suportado")

    try:
        if file.content_type == "text/csv":
            df = pd.read_csv(file.file)
            file_type = "csv"
        else:
            df = pd.read_json(file.file)
            file_type = "json"

        # Salva no banco de dados
        db = SessionLocal()
        data_preview = df.head(10).to_json()
        data_upload = DataUpload(
            user_id=current_user.id,
            filename=file.filename,
            file_type=file_type,
            data_preview=data_preview
        )
        db.add(data_upload)
        db.commit()
        db.refresh(data_upload)

        # Publicar evento de arquivo enviado
        message_queue.publish(TOPICS["FILE_UPLOADED"], {
            "upload_id": data_upload.id,
            "filename": data_upload.filename,
            "file_type": data_upload.file_type,
            "user_id": current_user.id,
            "record_count": len(df)
        })

        return data_upload
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao processar arquivo: {str(e)}")

@app.get("/statistics/{upload_id}", response_model=dict)
@cached(timedelta(minutes=5))
@FastAPILimiter.limit("30/minute")  # Limite de 30 consultas de estatísticas por minuto
@retry(max_retries=2, delay=0.5, backoff=1.5)
@CircuitBreaker(failure_threshold=2, recovery_timeout=30)
async def get_statistics(upload_id: int, current_user: UserOut = Depends(get_current_active_user)):
    db = SessionLocal()
    data_upload = db.query(DataUpload).filter(DataUpload.id == upload_id).first()
    if not data_upload:
        raise HTTPException(status_code=404, detail="Upload não encontrado")

    try:
        df = pd.read_json(data_upload.data_preview)
        statistics = {
            "count": df.shape[0],
            "columns": list(df.columns),
            "numeric_columns": df.select_dtypes(include=["number"]).columns.tolist(),
            "summary": df.describe().to_dict()
        }
        # Armazenar estatísticas no cache
        cache_manager.set(f"statistics:{upload_id}", statistics, timedelta(minutes=5))

        # Publicar evento de análise concluída
        message_queue.publish(TOPICS["ANALYSIS_COMPLETED"], {
            "upload_id": upload_id,
            "user_id": current_user.id,
            "record_count": len(df),
            "columns_count": len(df.columns)
        })

        return statistics
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao calcular estatísticas: {str(e)}")

@app.get("/charts/{upload_id}")
@cached(timedelta(minutes=10))
@FastAPILimiter.limit("20/minute")  # Limite de 20 gerações de gráficos por minuto
@retry(max_retries=2, delay=0.5, backoff=1.5)
@CircuitBreaker(failure_threshold=2, recovery_timeout=30)
async def generate_charts(upload_id: int, chart_type: str = Query("bar", description="Tipo de gráfico (bar, line, pie)"), current_user: UserOut = Depends(get_current_active_user)):
    # Validação de segurança para ID
    if upload_id <= 0:
        raise HTTPException(status_code=400, detail="ID de upload inválido")
    db = SessionLocal()
    data_upload = db.query(DataUpload).filter(DataUpload.id == upload_id).first()
    if not data_upload:
        raise HTTPException(status_code=404, detail="Upload não encontrado")

    try:
        df = pd.read_json(data_upload.data_preview)
        # Verificar se o gráfico está no cache
        cache_key = f"chart:{upload_id}:{chart_type}"
        cached_chart = cache_manager.get(cache_key)
        if cached_chart:
            return cached_chart

        if chart_type == "bar":
            fig = px.bar(df)
        elif chart_type == "line":
            fig = px.line(df)
        elif chart_type == "pie":
            fig = px.pie(df)
        else:
            raise HTTPException(status_code=400, detail="Tipo de gráfico não suportado")

        chart_path = f"/tmp/chart_{upload_id}_{chart_type}.html"
        fig.write_html(chart_path)

        # Armazenar gráfico no cache
        chart_data = {"chart_url": chart_path}
        cache_manager.set(cache_key, chart_data, timedelta(minutes=10))

        # Publicar evento de gráfico gerado
        message_queue.publish(TOPICS["CHART_GENERATED"], {
            "upload_id": upload_id,
            "user_id": current_user.id,
            "chart_type": chart_type,
            "file_path": chart_path
        })

        return {"chart_url": chart_path}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao gerar gráfico: {str(e)}")

@app.get("/reports/{upload_id}")
@cached(timedelta(minutes=30))
@FastAPILimiter.limit("15/minute")  # Limite de 15 gerações de relatórios por minuto
@retry(max_retries=2, delay=0.5, backoff=1.5)
@CircuitBreaker(failure_threshold=2, recovery_timeout=30)
async def generate_report(upload_id: int, format: str = Query("pdf", description="Formato do relatório (pdf, csv, json)"), current_user: UserOut = Depends(get_current_active_user), db = Depends(get_db)):
    # Validação de segurança para ID
    if upload_id <= 0:
        raise HTTPException(status_code=400, detail="ID de upload inválido")
    
    data_upload = db.query(DataUpload).filter(DataUpload.id == upload_id).first()
    if not data_upload:
        raise HTTPException(status_code=404, detail="Upload não encontrado")

    try:
        df = pd.read_json(data_upload.data_preview)
        report_content = f"""<html>
        <head><title>Relatório de Análise</title></head>
        <body>
            <h1>Relatório de Análise de Dados</h1>
            <h2>Estatísticas</h2>
            <pre>{df.describe().to_string()}</pre>
            <h2>Prévia dos Dados</h2>
            <pre>{df.head(10).to_string()}</pre>
        </body>
        </html>"""

        if format == "pdf":
            report_path = f"/tmp/report_{upload_id}.pdf"
            pdfkit.from_string(report_content, report_path)
        elif format == "csv":
            report_path = f"/tmp/report_{upload_id}.csv"
            df.to_csv(report_path, index=False)
        elif format == "json":
            report_path = f"/tmp/report_{upload_id}.json"
            df.to_json(report_path)
        else:
            raise HTTPException(status_code=400, detail="Formato não suportado")

        # Armazenar relatório no cache
        report_data = {"report_url": report_path}
        cache_manager.set(cache_key, report_data, timedelta(minutes=30))

        # Publicar evento de relatório gerado
        message_queue.publish(TOPICS["REPORT_GENERATED"], {
            "upload_id": upload_id,
            "user_id": current_user.id,
            "format": format,
            "file_path": report_path
        })

        return {"report_url": report_path}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao gerar relatório: {str(e)}")

# Schema OpenAPI Personalizado
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="API de Análise de Dados",
        version="1.0.0",
        description="API desenvolvida para análise estatística e visualização de dados, seguindo os princípios de Clean Code e arquitetura hexagonal.",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# Endpoint para exportação para ferramentas de BI
@app.post("/export/bi/{upload_id}", response_model=Dict[str, Any])
@FastAPILimiter.limit("10/minute")  # Limite de 10 exportações para BI por minuto
@retry(max_retries=2, delay=0.5, backoff=1.5)
@CircuitBreaker(failure_threshold=2, recovery_timeout=30)
async def export_bi(upload_id: int, export_data: BIExportSchema, current_user: UserOut = Depends(get_current_active_user)):
    """Exporta dados em formatos compatíveis com ferramentas de BI"""
    db = SessionLocal()
    data_upload = db.query(DataUpload).filter(DataUpload.id == upload_id).first()
    if not data_upload:
        raise HTTPException(status_code=404, detail="Upload não encontrado")

    try:
        df = pd.read_json(data_upload.data_preview)

        # Preparar dados para exportação
        if export_data.format == "tableau":
            # Formato específico para Tableau
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            return {
                "format": "tableau",
                "data": buffer.getvalue(),
                "metadata": {
                    "columns": list(df.columns),
                    "record_count": len(df),
                    "upload_id": upload_id
                } if export_data.include_metadata else None
            }
        elif export_data.format == "powerbi":
            # Formato específico para Power BI
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            return {
                "format": "powerbi",
                "data": buffer.getvalue(),
                "metadata": {
                    "columns": list(df.columns),
                    "record_count": len(df),
                    "upload_id": upload_id
                } if export_data.include_metadata else None
            }
        elif export_data.format == "csv":
            # Formato CSV padrão
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            return {
                "format": "csv",
                "data": buffer.getvalue(),
                "metadata": {
                    "columns": list(df.columns),
                    "record_count": len(df),
                    "upload_id": upload_id
                } if export_data.include_metadata else None
            }
        elif export_data.format == "json":
            # Formato JSON
            return {
                "format": "json",
                "data": df.to_dict(orient="records"),
                "metadata": {
                    "columns": list(df.columns),
                    "record_count": len(df),
                    "upload_id": upload_id
                } if export_data.include_metadata else None
            }
        else:
            raise HTTPException(status_code=400, detail="Formato de exportação não suportado")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao exportar dados: {str(e)}")

# Endpoint para webhooks de notificação
@app.post("/webhooks/notify")
@FastAPILimiter.limit("5/minute")  # Limite de 5 webhooks por minuto
@retry(max_retries=2, delay=0.5, backoff=1.5)
@CircuitBreaker(failure_threshold=2, recovery_timeout=30)
async def webhook_notify(data: Dict[str, Any] = Body(...)):
    """Recebe notificações de eventos de ferramentas de BI"""
    try:
        # Processar webhook
        event_type = data.get("event_type")
        payload = data.get("payload")

        if not event_type or not payload:
            raise HTTPException(status_code=400, detail="Evento ou payload inválido")

        # Publicar evento
        message_queue.publish(TOPICS["NOTIFICATION"], {
            "event_type": event_type,
            "payload": payload,
            "timestamp": datetime.utcnow().isoformat()
        })

        return {"status": "success", "message": "Webhook processado com sucesso"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao processar webhook: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)