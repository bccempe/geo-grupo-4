import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Carga las variables de entorno desde el archivo .env
load_dotenv()

# Lee la URL de conexión a Postgres
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL no está definida en el archivo .env")

# Crea el motor de conexión a la base de datos
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    future=True
)

# SessionLocal queda disponible por si después quieres usar sesiones ORM
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


def get_db():
    """
    Generador de sesiones de base de datos.
    Útil si más adelante quieres usar dependencias de FastAPI por sesión.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()