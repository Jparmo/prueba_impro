
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, extract
from typing import List
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from database import get_db, create_tables
from models import *
from schemas import *
from csv_loader import CSVLoader
import os

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Iniciando aplicación...")
    create_tables()
    
    csv_file = "pr.csv"
    if os.path.exists(csv_file):
        loader = CSVLoader(csv_file)
        result = loader.load_csv_data()
        print(f"Resultado de carga: {result}")
    
    print("Aplicación iniciada correctamente")
    
    yield
    
    print("🔄 Cerrando aplicación...")
    print("✅ Aplicación cerrada correctamente")

app = FastAPI(
    title="Sistema de Importaciones",
    description="API para gestión de declaraciones de importación con base de datos normalizada",
    version="1.0.0",
    lifespan=lifespan  
)

@app.get("/")
async def root():
    return {
        "message": "Sistema de Importaciones API",
        "version": "1.0.0",
        "endpoints": [
            "/declaraciones",
            "/declaraciones/correlativo/{correlativo}",
            "/declaraciones/sac/{codigo_sac}",
            "/analytics/top-pais-por-sac",
            "/analytics/importaciones-por-mes"
        ]
    }

@app.get("/declaraciones/correlativo/{correlativo}", response_model=List[DeclaracionResponse])
async def get_importaciones_por_correlativo(correlativo: str, db: Session = Depends(get_db)):
    """Busca todas las importaciones por correlativo específico"""
    
    declaraciones = db.query(DeclaracionImportacion).filter(
        DeclaracionImportacion.correlativo == correlativo
    ).all()
    
    if not declaraciones:
        raise HTTPException(status_code=404, detail=f"No se encontraron importaciones para el correlativo {correlativo}")
    
    result = []
    for decl in declaraciones:
        result.append(DeclaracionResponse(
            id=decl.id,
            correlativo=decl.correlativo,
            fecha_declaracion=decl.fecha_declaracion,
            tipo_cambio_dolar=decl.tipo_cambio_dolar,
            descripcion=decl.descripcion,
            cantidad_fraccion=decl.cantidad_fraccion,
            tasa_dai=decl.tasa_dai,
            valor_dai=decl.valor_dai,
            valor_cif_usd=decl.valor_cif_usd,
            tasa_cif_cantidad_fraccion=decl.tasa_cif_cantidad_fraccion,
            aduana_nombre=decl.aduana.nombre,
            pais_nombre=decl.pais.nombre,
            tipo_regimen_nombre=decl.tipo_regimen.nombre,
            unidad_medida_nombre=decl.unidad_medida.nombre,
            codigo_sac=decl.codigo_sac.codigo
        ))
    
    return result

@app.get("/declaraciones/sac/{codigo_sac}", response_model=List[DeclaracionResponse])
async def get_importaciones_por_sac(
    codigo_sac: str, 
    limit: int = Query(100, description="Límite de resultados"),
    offset: int = Query(0, description="Desplazamiento para paginación"),
    db: Session = Depends(get_db)
):
    """Lista todas las importaciones de un código SAC específico"""
    
    query = db.query(DeclaracionImportacion).join(CodigoSAC).filter(
        CodigoSAC.codigo == codigo_sac
    )
    
    total = query.count()
    declaraciones = query.offset(offset).limit(limit).all()
    
    if not declaraciones:
        raise HTTPException(status_code=404, detail=f"No se encontraron importaciones para el SAC {codigo_sac}")
    
    result = []
    for decl in declaraciones:
        result.append(DeclaracionResponse(
            id=decl.id,
            correlativo=decl.correlativo,
            fecha_declaracion=decl.fecha_declaracion,
            tipo_cambio_dolar=decl.tipo_cambio_dolar,
            descripcion=decl.descripcion,
            cantidad_fraccion=decl.cantidad_fraccion,
            tasa_dai=decl.tasa_dai,
            valor_dai=decl.valor_dai,
            valor_cif_usd=decl.valor_cif_usd,
            tasa_cif_cantidad_fraccion=decl.tasa_cif_cantidad_fraccion,
            aduana_nombre=decl.aduana.nombre,
            pais_nombre=decl.pais.nombre,
            tipo_regimen_nombre=decl.tipo_regimen.nombre,
            unidad_medida_nombre=decl.unidad_medida.nombre,
            codigo_sac=decl.codigo_sac.codigo
        ))
    
    return result

@app.get("/analytics/top-pais-por-sac", response_model=List[TopPaisPorSAC])
async def get_top_pais_por_sac(
    limit: int = Query(10, description="Número de resultados por SAC"),
    db: Session = Depends(get_db)
):
    """Obtiene el top de países por valor total de importaciones agrupado por SAC"""
    
    subquery = db.query(
        CodigoSAC.codigo.label('codigo_sac'),
        Pais.nombre.label('pais_nombre'),
        func.sum(DeclaracionImportacion.valor_cif_usd).label('valor_total')
    ).select_from(DeclaracionImportacion)\
    .join(CodigoSAC, DeclaracionImportacion.codigo_sac_id == CodigoSAC.id)\
    .join(Pais, DeclaracionImportacion.pais_id == Pais.id)\
    .group_by(CodigoSAC.codigo, Pais.nombre)\
    .subquery()
    
    from sqlalchemy import func
    from sqlalchemy.sql import select
    
    ranked_query = db.query(
        subquery.c.codigo_sac,
        subquery.c.pais_nombre,
        subquery.c.valor_total,
        func.row_number().over(
            partition_by=subquery.c.codigo_sac,
            order_by=subquery.c.valor_total.desc()
        ).label('ranking')
    ).subquery()
    
    result = db.query(
        ranked_query.c.codigo_sac,
        ranked_query.c.pais_nombre,
        ranked_query.c.valor_total
    ).filter(ranked_query.c.ranking <= limit).order_by(
        ranked_query.c.codigo_sac,
        ranked_query.c.valor_total.desc()
    ).all()
    
    return [
        TopPaisPorSAC(
            codigo_sac=row.codigo_sac,
            pais_nombre=row.pais_nombre,
            valor_total=row.valor_total
        ) for row in result
    ]

@app.get("/analytics/importaciones-por-mes", response_model=List[ImportacionesPorMes])
async def get_importaciones_por_mes(db: Session = Depends(get_db)):
    """Obtiene el total de importaciones por mes en el último año"""
    
    fecha_limite = datetime.now().date() - timedelta(days=365)
    
    result = db.query(
        extract('year', DeclaracionImportacion.fecha_declaracion).label('año'),
        extract('month', DeclaracionImportacion.fecha_declaracion).label('mes'),
        func.count(DeclaracionImportacion.id).label('cantidad_importaciones'),
        func.sum(DeclaracionImportacion.valor_cif_usd).label('valor_total')
    ).filter(
        DeclaracionImportacion.fecha_declaracion >= fecha_limite
    ).group_by(
        extract('year', DeclaracionImportacion.fecha_declaracion),
        extract('month', DeclaracionImportacion.fecha_declaracion)
    ).order_by(
        extract('year', DeclaracionImportacion.fecha_declaracion),
        extract('month', DeclaracionImportacion.fecha_declaracion)
    ).all()
    
    return [
        ImportacionesPorMes(
            año=int(row.año),
            mes=int(row.mes),
            cantidad_importaciones=row.cantidad_importaciones,
            valor_total=row.valor_total or 0
        ) for row in result
    ]

@app.get("/declaraciones", response_model=List[DeclaracionResponse])
async def get_all_declaraciones(
    limit: int = Query(50, description="Límite de resultados"),
    offset: int = Query(0, description="Desplazamiento para paginación"),
    db: Session = Depends(get_db)
):
    """Obtiene todas las declaraciones de importación"""
    
    declaraciones = db.query(DeclaracionImportacion).offset(offset).limit(limit).all()
    
    result = []
    for decl in declaraciones:
        result.append(DeclaracionResponse(
            id=decl.id,
            correlativo=decl.correlativo,
            fecha_declaracion=decl.fecha_declaracion,
            tipo_cambio_dolar=decl.tipo_cambio_dolar,
            descripcion=decl.descripcion,
            cantidad_fraccion=decl.cantidad_fraccion,
            tasa_dai=decl.tasa_dai,
            valor_dai=decl.valor_dai,
            valor_cif_usd=decl.valor_cif_usd,
            tasa_cif_cantidad_fraccion=decl.tasa_cif_cantidad_fraccion,
            aduana_nombre=decl.aduana.nombre,
            pais_nombre=decl.pais.nombre,
            tipo_regimen_nombre=decl.tipo_regimen.nombre,
            unidad_medida_nombre=decl.unidad_medida.nombre,
            codigo_sac=decl.codigo_sac.codigo
        ))
    
    return result

@app.get("/stats")
async def get_stats(db: Session = Depends(get_db)):
    """Obtiene estadísticas generales del sistema"""
    
    total_declaraciones = db.query(DeclaracionImportacion).count()
    total_aduanas = db.query(Aduana).count()
    total_paises = db.query(Pais).count()
    total_sacs = db.query(CodigoSAC).count()
    valor_total = db.query(func.sum(DeclaracionImportacion.valor_cif_usd)).scalar() or 0
    
    return {
        "total_declaraciones": total_declaraciones,
        "total_aduanas": total_aduanas,
        "total_paises": total_paises,
        "total_codigos_sac": total_sacs,
        "valor_total_importaciones": valor_total
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)