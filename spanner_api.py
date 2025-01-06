from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
from google.cloud import spanner
from google.cloud.spanner_v1 import param_types
from google.cloud.spanner_v1 import Mutation
from google.cloud.spanner import KeySet

from fastapi import HTTPException
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Aplicación FastAPI
app = FastAPI()

# Modelo Pydantic para solicitudes POST
class QueryRequest(BaseModel):
    query: str


class ValueObject(BaseModel):
    id: str
    status: str
    status_info: str = Field(alias="status_info")
    provider: str
    endpoint: str
    model: str
    scope: str
    criticality: str
    input_rows: int = Field(alias="input_rows")
    input_bytes: int = Field(alias="input_bytes")
    output_rows: int = Field(alias="output_rows")
    total_input_token: int = Field(alias="total_input_token")
    total_output_token: int = Field(alias="total_output_token")
    input_filepath: str = Field(alias="input_filepath")
    output_filepath: str = Field(alias="output_filepath")
    created_at: datetime
    updated_at: datetime


class MutationRequest(BaseModel):
    operation: str
    table: str
    columns: List[str]
    values: List[ValueObject]
    keySet: Optional[Dict] = None


# Configuración de Spanner
SPANNER_PROJECT_ID = "test-project"
INSTANCE_ID = "test-instance"
DATABASE_ID = "test-database"

# Inicializar cliente de Spanner
spanner_client = spanner.Client(project=SPANNER_PROJECT_ID)
instance = spanner_client.instance(INSTANCE_ID)
database = instance.database(DATABASE_ID)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    print("Payload recibido: &&&&&& ", await request.json())
    return JSONResponse(
        status_code=400,
        content={"error": "Invalid request payload", "details": exc.errors()},
    )


@app.post("/execute-query")
async def execute_query(request: dict):
    query = request.get("query")
    params = request.get("params", None)
    
    logger.info(f"Recibiendo consulta: {query} con parámetros: {params}")
    try:
        # Configurar los tipos de parámetros si existen
        param_types_map = (
            {key: param_types.STRING for key in params.keys()} if params else None
        )
        
        # Ejecutar consulta
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                query,
                params=params,
                param_types=param_types_map,
            )
            logger.info(f"Resultados crudos obtenidos: {results}")

            columns = [field.name for field in results.fields] if hasattr(results, "fields") else []


            # Si el objeto 'results' no tiene un tipo de fila, intenta convertir a lista
            if not hasattr(results, "row_type"):
                rows = results.to_dict_list()

                if not columns:
                    columns = list(rows[0].keys()) if rows else []

                logger.info(f"Resultados procesados (sin row_type): {rows}")
                return {"success": True, "data": rows, "columns": columns}

            # Verificar si el objeto 'results' es válido
            if results is None:
                logger.error(f"La consulta no devolvió un objeto válido. Resultados: {results}")
                return {"success": False, "error": "No se devolvieron resultados válidos."}

            # Obtener nombres de columnas
            columns = [field.name for field in results.fields] if hasattr(results, "fields") else []
            logger.info(f"Columnas detectadas: {columns}")
            
            # Verificar si se obtuvieron columnas
            if not columns:
                logger.error("No se pudo determinar la estructura de las columnas.")
                return {"success": False, "error": "No se encontraron columnas en el resultado."}
            
            # Convertir cada fila a un diccionario
            rows = [dict(zip(columns, row)) for row in results]
            logger.info(f"Resultados procesados: {rows}")
        
        logger.info("Consulta ejecutada exitosamente")
        return {"success": True, "data": rows, "columns": columns}

    except Exception as e:
        logger.error(f"Error al ejecutar la consulta: {e}")
        raise HTTPException(status_code=500, detail=str(e))




@app.post("/execute-mutation")
async def execute_mutation(request: MutationRequest):
    logger.info(f"Recibiendo mutación: {request}")

    try:

        with database.batch() as batch:
            if request.operation == "insert":
                for row in request.values:
                    batch.insert(
                        table=request.table,
                        columns=request.columns,
                        values=[[getattr(row, col) for col in request.columns]],
                    )
            elif request.operation == "update":
                for row in request.values:
                    batch.update(
                        table=request.table,
                        columns=request.columns,
                        values=[[getattr(row, col) for col in request.columns]],
                    )
            elif request.operation == "delete":
                if not request.keySet:
                    raise HTTPException(status_code=400, detail="KeySet requerido para delete")
                key_set = spanner.KeySet(keys=request.keySet["keys"])
                batch.delete(
                    table=request.table,
                    keyset=key_set,
                )
            else:
                raise HTTPException(status_code=400, detail=f"Operación no soportada: {request.operation}")


        logger.info("Mutación ejecutada exitosamente")
        return {"success": True, "message": "Mutación completada"}

    except ValueError as e:
        logger.error(f"Error al ejecutar la mutación: {e}")
        raise HTTPException(status_code=500, detail=str(e))
          

"""
from google.cloud import spanner
database_id = "test-database"
instance_id = "test-instance"
project_id = "test-project"
spanner_client = spanner.Client(project=project_id)
instance = spanner_client.instance(instance_id)
database = instance.database(database_id)

with database.snapshot() as snapshot:
    results = snapshot.execute_sql("SELECT table_name FROM information_schema.tables")
    for row in results:
        print(row)

"""


"""
import requests  

url = 'http://localhost:8080/batch/file-dispatch-rest'
myobj = {
"status": "file_process_started",
"statusInfo": {},
"provider": "test",
"endpoint": "test",
"model": "test",
"scope": "test",
"criticality": "test",
"inputRows": 1,
"inputBytes": 1,
"outputRows": 1,
"totalInputToken": 1,
"totalOutputToken": 1,
"inputFilepath": "test",
"outputFilepath": "test"
}
x = requests.post(url, json = myobj)
print(x.text)
"""



