import uuid
from pathlib import Path

from fastapi import UploadFile, HTTPException

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────

ALLOWED_EXTENSIONS = {"pdf", "docx", "txt", "xlsx", "json", "csv"}

ALLOWED_CONTENT_TYPES = {
    "application/pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "text/plain": "txt",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "application/json": "json",
    "text/csv": "csv"
}

STORAGE_DIR = Path("app/data")
STORAGE_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _get_extension(filename: str) -> str:
    return Path(filename).suffix.lstrip(".").lower()


def validate_file(file: UploadFile) -> str:

    ext = _get_extension(file.filename or "")
    content_type = file.content_type or ""

    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Formato '{ext}' no soportado."
        )

    if content_type and content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Content-Type '{content_type}' no válido."
        )

    return ext


# ─────────────────────────────────────────────
# Upload
# ─────────────────────────────────────────────

async def upload_document(file: UploadFile):

    ext = validate_file(file)

    content = await file.read()

    doc_id = str(uuid.uuid4())

    stored_filename = f"{doc_id}.{ext}"

    storage_path = STORAGE_DIR / stored_filename

    storage_path.write_bytes(content)

    return {
        "id": doc_id,
        "filename": stored_filename,
        "original_filename": file.filename,
        "file_format": ext,
        "file_size_bytes": len(content),
        "file_path": str(storage_path)
    }