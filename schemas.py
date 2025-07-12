from pydantic import BaseModel
from datetime import date

class DeclaracionResponse(BaseModel):
    id: int
    correlativo: str
    fecha_declaracion: date
    tipo_cambio_dolar: float
    descripcion: str
    cantidad_fraccion: float
    tasa_dai: float
    valor_dai: float
    valor_cif_usd: float
    tasa_cif_cantidad_fraccion: float
    aduana_nombre: str
    pais_nombre: str
    tipo_regimen_nombre: str
    unidad_medida_nombre: str
    codigo_sac: str
    
    class Config:
        from_attributes = True

class ImportacionesPorSAC(BaseModel):
    codigo_sac: str
    cantidad_importaciones: int
    valor_total: float
    
class TopPaisPorSAC(BaseModel):
    codigo_sac: str
    pais_nombre: str
    valor_total: float

class ImportacionesPorMes(BaseModel):
    año: int
    mes: int
    cantidad_importaciones: int
    valor_total: float
