import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, engine, SessionLocal
from main import app
from core import get_password_hash, verify_password
from models import User, DataUpload
from schemas import UserCreate, DataUploadCreate

# Configuração do banco de dados para testes
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)

# Substituir conexão com banco para testes
def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

app.dependency_overrides[SessionLocal] = override_get_db

client = TestClient(app)

# Dados para testes
test_user = {"username": "testuser", "email": "test@example.com", "password": "testpassword"}
test_csv_data = b"name,age\nJohn,25\nJane,30\n"
test_json_data = b'[{"name": "John", "age": 25}, {"name": "Jane", "age": 30}]'

@pytest.fixture
def db():
    yield TestingSessionLocal()

@pytest.fixture
def test_user_data():
    return test_user

@pytest.fixture
def test_csv_bytes():
    return test_csv_data

@pytest.fixture
def test_json_bytes():
    return test_json_data

def test_create_user(db, test_user_data):
    hashed_password = get_password_hash(test_user_data["password"])
    db_user = User(
        username=test_user_data["username"],
        email=test_user_data["email"],
        hashed_password=hashed_password
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    assert db_user.username == test_user_data["username"]
    assert verify_password(test_user_data["password"], db_user.hashed_password)

def test_read_user(db, test_user_data):
    user = db.query(User).filter(User.username == test_user_data["username"]).first()
    assert user is not None
    assert user.email == test_user_data["email"]

def test_upload_csv(test_csv_bytes):
    response = client.post("/upload", files={"file": ("test.csv", test_csv_bytes, "text/csv")})
    assert response.status_code == 200
    assert "id" in response.json()
    assert "filename" in response.json()

def test_upload_json(test_json_bytes):
    response = client.post("/upload", files={"file": ("test.json", test_json_bytes, "application/json")})
    assert response.status_code == 200
    assert "id" in response.json()
    assert "filename" in response.json()

def test_get_statistics():
    # Primeiro faz upload de um arquivo
    response = client.post("/upload", files={"file": ("test.csv", test_csv_data, "text/csv")})
    upload_id = response.json()["id"]

    # Depois obtém estatísticas
    response = client.get(f"/statistics/{upload_id}")
    assert response.status_code == 200
    assert "count" in response.json()
    assert "columns" in response.json()

def test_generate_charts():
    response = client.post("/upload", files={"file": ("test.csv", test_csv_data, "text/csv")})
    upload_id = response.json()["id"]

    response = client.get(f"/charts/{upload_id}", params={"chart_type": "bar"})
    assert response.status_code == 200
    assert "chart_url" in response.json()

def test_generate_report():
    response = client.post("/upload", files={"file": ("test.csv", test_csv_data, "text/csv")})
    upload_id = response.json()["id"]

    response = client.get(f"/reports/{upload_id}", params={"format": "pdf"})
    assert response.status_code == 200
    assert "report_url" in response.json()
