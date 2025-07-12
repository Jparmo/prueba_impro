import pandas as pd
from sqlalchemy.orm import Session
from models import *
from database import engine, SessionLocal
from datetime import datetime
import os
import csv

class CSVLoader:
    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        self.db = SessionLocal()
    
    def load_csv_data(self):
        """Carga datos desde el archivo CSV al esquema normalizado"""
        try:
            # Leer el archivo CSV
            if not os.path.exists(self.csv_file_path):
                # Si no existe el archivo, usar los datos de ejemplo
                print(f"⚠️  Archivo {self.csv_file_path} no encontrado. Usando datos de ejemplo.")
                return self._load_sample_data()
            
            # Intentar leer CSV con múltiples estrategias
            df = self._read_csv_robust()
            
            if df is None:
                return {"status": "error", "message": "No se pudo leer el archivo CSV"}
            
            print(f"📊 Cargando {len(df)} registros desde {self.csv_file_path}")
            
            # Crear las tablas
            Base.metadata.create_all(bind=engine)
            
            # Limpiar datos existentes (opcional)
            self._clean_existing_data()
            
            # Cargar datos maestros
            self._load_master_data(df)
            
            # Cargar declaraciones
            self._load_declarations(df)
            
            print("✅ Datos cargados exitosamente")
            return {"status": "success", "records_loaded": len(df)}
            
        except Exception as e:
            self.db.rollback()
            print(f"❌ Error al cargar datos: {e}")
            return {"status": "error", "message": str(e)}
        finally:
            self.db.close()
    
    def _read_csv_robust(self):
        """Intenta leer el CSV con diferentes estrategias robustas"""
        
        # Estrategia 1: Lectura básica con manejo de errores
        try:
            print("🔄 Intentando lectura básica del CSV...")
            df = pd.read_csv(
                self.csv_file_path, 
                sep=';', 
                encoding='utf-8',
                on_bad_lines='skip',  # Salta las líneas problemáticas
                engine='python'      # Usa el motor de Python (más tolerante)
            )
            print(f"✅ Lectura básica exitosa: {len(df)} registros")
            return df
        except Exception as e:
            print(f"❌ Error en lectura básica: {e}")
        
        # Estrategia 2: Lectura con parámetros más robustos
        try:
            print("🔄 Intentando lectura robusta del CSV...")
            df = pd.read_csv(
                self.csv_file_path,
                sep=';',
                encoding='utf-8',
                on_bad_lines='skip',
                engine='python',
                quoting=csv.QUOTE_MINIMAL,  # Manejo de comillas
                skipinitialspace=True,      # Ignora espacios iniciales
                low_memory=False,           # Lee todo en memoria
                dtype=str                   # Lee todo como string inicialmente
            )
            print(f"✅ Lectura robusta exitosa: {len(df)} registros")
            return df
        except Exception as e:
            print(f"❌ Error en lectura robusta: {e}")
        
        # Estrategia 3: Lectura línea por línea para identificar problemas
        try:
            print("🔄 Intentando lectura línea por línea...")
            return self._read_csv_line_by_line()
        except Exception as e:
            print(f"❌ Error en lectura línea por línea: {e}")
        
        # Estrategia 4: Lectura con diferentes encodings
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        for encoding in encodings:
            try:
                print(f"🔄 Intentando lectura con encoding {encoding}...")
                df = pd.read_csv(
                    self.csv_file_path,
                    sep=';',
                    encoding=encoding,
                    on_bad_lines='skip',
                    engine='python',
                    dtype=str
                )
                print(f"✅ Lectura con {encoding} exitosa: {len(df)} registros")
                return df
            except Exception as e:
                print(f"❌ Error con encoding {encoding}: {e}")
                continue
        
        return None
    
    def _read_csv_line_by_line(self):
        """Lee el CSV línea por línea para identificar y manejar problemas"""
        
        valid_rows = []
        headers = None
        problematic_lines = []
        
        with open(self.csv_file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=';')
            
            for line_num, row in enumerate(reader, 1):
                try:
                    if line_num == 1:
                        headers = row
                        expected_cols = len(headers)
                        continue
                    
                    # Verificar si la línea tiene el número correcto de columnas
                    if len(row) == expected_cols:
                        valid_rows.append(row)
                    else:
                        problematic_lines.append(line_num)
                        print(f"⚠️  Línea {line_num}: esperaba {expected_cols} columnas, encontró {len(row)}")
                        
                        # Intentar reparar la línea
                        if len(row) > expected_cols:
                            # Demasiadas columnas, unir las extras
                            repaired_row = row[:expected_cols-1] + [';'.join(row[expected_cols-1:])]
                            valid_rows.append(repaired_row)
                        elif len(row) < expected_cols:
                            # Muy pocas columnas, rellenar con vacíos
                            repaired_row = row + [''] * (expected_cols - len(row))
                            valid_rows.append(repaired_row)
                
                except Exception as e:
                    problematic_lines.append(line_num)
                    print(f"⚠️  Error en línea {line_num}: {e}")
                    continue
        
        if problematic_lines:
            print(f"⚠️  Se encontraron {len(problematic_lines)} líneas problemáticas que fueron reparadas o saltadas")
        
        # Crear DataFrame con las filas válidas
        df = pd.DataFrame(valid_rows, columns=headers)
        return df
    
    def _diagnose_csv_issues(self):
        """Diagnostica problemas comunes en el archivo CSV"""
        
        print("🔍 Diagnosticando problemas en el archivo CSV...")
        
        try:
            # Verificar encoding
            with open(self.csv_file_path, 'rb') as file:
                raw_data = file.read(1000)
                print(f"📋 Primeros bytes del archivo: {raw_data[:100]}")
            
            # Contar líneas y verificar estructura
            line_count = 0
            col_counts = {}
            
            with open(self.csv_file_path, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    line_count += 1
                    col_count = line.count(';') + 1
                    col_counts[col_count] = col_counts.get(col_count, 0) + 1
                    
                    if line_num <= 10:
                        print(f"📋 Línea {line_num}: {col_count} columnas")
            
            print(f"📊 Total de líneas: {line_count}")
            print(f"📊 Distribución de columnas: {col_counts}")
            
            # Identificar el número de columnas más común
            most_common_cols = max(col_counts, key=col_counts.get)
            print(f"📊 Número de columnas más común: {most_common_cols}")
            
        except Exception as e:
            print(f"❌ Error en diagnóstico: {e}")
    
    
    def _load_master_data(self, df):
        """Carga datos maestros (tablas de referencia)"""
        
        # Verificar que las columnas existen
        required_columns = ['aduana', 'pais', 'tipo_regimen', 'tipo_unidad_medida', 'sac']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"⚠️  Columnas faltantes: {missing_columns}")
            print(f"📋 Columnas disponibles: {list(df.columns)}")
            return
        
        # Aduanas
        if 'aduana' in df.columns:
            aduanas_unicas = df['aduana'].dropna().unique()
            for aduana_nombre in aduanas_unicas:
                if not self.db.query(Aduana).filter(Aduana.nombre == aduana_nombre).first():
                    aduana = Aduana(nombre=aduana_nombre)
                    self.db.add(aduana)
        
        # Países
        if 'pais' in df.columns:
            paises_unicos = df['pais'].dropna().unique()
            for pais_nombre in paises_unicos:
                if not self.db.query(Pais).filter(Pais.nombre == pais_nombre).first():
                    pais = Pais(nombre=pais_nombre)
                    self.db.add(pais)
        
        # Tipos de régimen
        if 'tipo_regimen' in df.columns:
            regimenes_unicos = df['tipo_regimen'].dropna().unique()
            for regimen_nombre in regimenes_unicos:
                if not self.db.query(TipoRegimen).filter(TipoRegimen.nombre == regimen_nombre).first():
                    regimen = TipoRegimen(nombre=regimen_nombre)
                    self.db.add(regimen)
        
        # Unidades de medida
        if 'tipo_unidad_medida' in df.columns:
            unidades_unicas = df['tipo_unidad_medida'].dropna().unique()
            for unidad_nombre in unidades_unicas:
                if not self.db.query(UnidadMedida).filter(UnidadMedida.nombre == unidad_nombre).first():
                    unidad = UnidadMedida(nombre=unidad_nombre)
                    self.db.add(unidad)
        
        # Códigos SAC
        if 'sac' in df.columns:
            codigos_sac_unicos = df['sac'].dropna().unique()
            for codigo_sac in codigos_sac_unicos:
                if not self.db.query(CodigoSAC).filter(CodigoSAC.codigo == str(codigo_sac)).first():
                    sac = CodigoSAC(codigo=str(codigo_sac))
                    self.db.add(sac)
        
        self.db.commit()
    
    def _load_declarations(self, df):
        """Carga las declaraciones de importación"""
        
        success_count = 0
        error_count = 0
        
        for index, row in df.iterrows():
            try:
                if not all(col in row.index for col in ['aduana', 'pais', 'tipo_regimen', 'tipo_unidad_medida', 'sac']):
                    print(f"⚠️  Fila {index}: Faltan campos requeridos")
                    error_count += 1
                    continue
                
                # Obtener IDs de las tablas de referencia
                aduana = self.db.query(Aduana).filter(Aduana.nombre == row['aduana']).first()
                pais = self.db.query(Pais).filter(Pais.nombre == row['pais']).first()
                regimen = self.db.query(TipoRegimen).filter(TipoRegimen.nombre == row['tipo_regimen']).first()
                unidad = self.db.query(UnidadMedida).filter(UnidadMedida.nombre == row['tipo_unidad_medida']).first()
                sac = self.db.query(CodigoSAC).filter(CodigoSAC.codigo == str(row['sac'])).first()
                
                if not all([aduana, pais, regimen, unidad, sac]):
                    print(f"⚠️  Fila {index}: No se encontraron todas las referencias")
                    error_count += 1
                    continue
                
                try:
                    fecha = self._parse_date(row['fecha_declaracion'])
                except Exception as e:
                    print(f"⚠️  Fila {index}: Error parsing fecha: {e}")
                    error_count += 1
                    continue
                
                existing = self.db.query(DeclaracionImportacion).filter(
                    DeclaracionImportacion.correlativo == row['correlativo'],
                    DeclaracionImportacion.codigo_sac_id == sac.id,
                    DeclaracionImportacion.descripcion == row['descripcion']
                ).first()
                
                if not existing:
                    try:
                        declaracion = DeclaracionImportacion(
                            correlativo=row['correlativo'],
                            fecha_declaracion=fecha,
                            tipo_cambio_dolar=self._safe_float_conversion(row['tipo_cambio_dolar']),
                            descripcion=row['descripcion'],
                            cantidad_fraccion=self._safe_float_conversion(row['cantidad_fraccion']),
                            tasa_dai=self._safe_float_conversion(row['tasa_dai']),
                            valor_dai=self._safe_float_conversion(row['valor_dai']),
                            valor_cif_usd=self._safe_float_conversion(row['valor_cif_uds']),
                            tasa_cif_cantidad_fraccion=self._safe_float_conversion(row['tasa_cif_cantidad_fraccion']),
                            aduana_id=aduana.id,
                            pais_id=pais.id,
                            tipo_regimen_id=regimen.id,
                            unidad_medida_id=unidad.id,
                            codigo_sac_id=sac.id
                        )
                        self.db.add(declaracion)
                        success_count += 1
                    except Exception as e:
                        print(f"⚠️  Fila {index}: Error creando declaración: {e}")
                        error_count += 1
                
            except Exception as e:
                print(f"⚠️  Error procesando fila {index}: {e}")
                error_count += 1
                continue
        
        print(f"📊 Procesamiento completado: {success_count} exitosos, {error_count} errores")
        self.db.commit()
    
    def _parse_date(self, fecha_str):
        """Convierte string de fecha a objeto date de manera robusta"""
        if pd.isna(fecha_str) or str(fecha_str).strip() == '':
            raise ValueError("Fecha vacía")
        
        fecha_str = str(fecha_str).strip()
        
        if '/' in fecha_str:
            fecha_parts = fecha_str.split('/')
            if len(fecha_parts) != 3:
                raise ValueError(f"Formato de fecha inválido: {fecha_str}")
            
            day, month, year = fecha_parts
            
            if len(year) == 2:
                year = int(year)
                if year > 50:  
                    year = 1900 + year
                else:
                    year = 2000 + year
            else:
                year = int(year)
            
            return datetime(year, int(month), int(day)).date()
        
        elif '-' in fecha_str:
            return datetime.strptime(fecha_str, '%Y-%m-%d').date()
        
        else:
            raise ValueError(f"Formato de fecha no reconocido: {fecha_str}")
    
    def _safe_float_conversion(self, value):
        """Convierte un valor a float de manera segura"""
        if pd.isna(value) or str(value).strip() == '':
            return 0.0
        
        try:
            value_str = str(value).replace(',', '.')
            return float(value_str)
        except (ValueError, TypeError):
            print(f"⚠️  No se pudo convertir a float: {value}")
            return 0.0
    
    def _load_sample_data(self):
        """Carga datos de ejemplo si no hay CSV"""
        pass

def diagnose_csv_file(csv_file_path):
    """Función independiente para diagnosticar problemas en un archivo CSV"""
    loader = CSVLoader(csv_file_path)
    loader._diagnose_csv_issues()
