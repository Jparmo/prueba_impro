# models.py - Modelos SQLAlchemy para el esquema normalizado 3NF
from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import date

Base = declarative_base()

# Tabla de Aduanas
class Aduana(Base):
    __tablename__ = "aduanas"
    
    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(100), unique=True, nullable=False, index=True)
    
    # Relación con declaraciones
    declaraciones = relationship("DeclaracionImportacion", back_populates="aduana")

# Tabla de Países
class Pais(Base):
    __tablename__ = "paises"
    
    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(100), unique=True, nullable=False, index=True)
    
    # Relación con declaraciones
    declaraciones = relationship("DeclaracionImportacion", back_populates="pais")

# Tabla de Regímenes
class TipoRegimen(Base):
    __tablename__ = "tipos_regimen"
    
    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(100), unique=True, nullable=False, index=True)
    
    # Relación con declaraciones
    declaraciones = relationship("DeclaracionImportacion", back_populates="tipo_regimen")

# Tabla de Unidades de Medida
class UnidadMedida(Base):
    __tablename__ = "unidades_medida"
    
    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(50), unique=True, nullable=False, index=True)
    
    # Relación con declaraciones
    declaraciones = relationship("DeclaracionImportacion", back_populates="unidad_medida")

# Tabla de Códigos SAC
class CodigoSAC(Base):
    __tablename__ = "codigos_sac"
    
    id = Column(Integer, primary_key=True, index=True)
    codigo = Column(String(20), unique=True, nullable=False, index=True)
    
    # Relación con declaraciones
    declaraciones = relationship("DeclaracionImportacion", back_populates="codigo_sac")

# Tabla principal de Declaraciones de Importación
class DeclaracionImportacion(Base):
    __tablename__ = "declaraciones_importacion"
    
    id = Column(Integer, primary_key=True, index=True)
    correlativo = Column(String(20), nullable=False, index=True)
    fecha_declaracion = Column(Date, nullable=False, index=True)
    tipo_cambio_dolar = Column(Float, nullable=False)
    descripcion = Column(String(500), nullable=False)
    cantidad_fraccion = Column(Float, nullable=False)
    tasa_dai = Column(Float, nullable=False)
    valor_dai = Column(Float, nullable=False)
    valor_cif_usd = Column(Float, nullable=False)
    tasa_cif_cantidad_fraccion = Column(Float, nullable=False)
    
    # Claves foráneas
    aduana_id = Column(Integer, ForeignKey("aduanas.id"), nullable=False)
    pais_id = Column(Integer, ForeignKey("paises.id"), nullable=False)
    tipo_regimen_id = Column(Integer, ForeignKey("tipos_regimen.id"), nullable=False)
    unidad_medida_id = Column(Integer, ForeignKey("unidades_medida.id"), nullable=False)
    codigo_sac_id = Column(Integer, ForeignKey("codigos_sac.id"), nullable=False)
    
    # Relaciones
    aduana = relationship("Aduana", back_populates="declaraciones")
    pais = relationship("Pais", back_populates="declaraciones")
    tipo_regimen = relationship("TipoRegimen", back_populates="declaraciones")
    unidad_medida = relationship("UnidadMedida", back_populates="declaraciones")
    codigo_sac = relationship("CodigoSAC", back_populates="declaraciones")
