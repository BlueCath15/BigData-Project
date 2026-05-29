from datetime import datetime

from fastapi import (
    APIRouter,
    UploadFile,
    File,
    BackgroundTasks
)

from pydantic import BaseModel

from app.documents.services import upload_document
from app.documents.transaction_loader import process_document

router = APIRouter(
    prefix="/documents",
    tags=["documents"]
)


# ─────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────

class UploadResponse(BaseModel):
    message: str
    document_id: str
    filename: str
    file_format: str


# ─────────────────────────────────────────────
# Endpoint
# ─────────────────────────────────────────────

@router.post(
    "/upload",
    response_model=UploadResponse,
    status_code=201
)
async def upload_document_endpoint(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):

    doc = await upload_document(file)

    background_tasks.add_task(
        process_document,
        doc["file_path"],
        doc["file_format"]
    )

    return UploadResponse(
        message="Documento recibido y en procesamiento",
        document_id=doc["id"],
        filename=doc["original_filename"],
        file_format=doc["file_format"]
    )