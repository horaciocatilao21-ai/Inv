"""
gdrive.py — Integración con Google Drive via Service Account
------------------------------------------------------------
Funciones disponibles:
  descargar_excel()          → descarga el Excel a disco temporal
  subir_excel(ruta)          → sube el Excel actualizado a Drive
  leer_contador_ocr()        → lee el contador de usos OCR desde Drive
  guardar_contador_ocr(data) → guarda el contador actualizado en Drive
"""

import os
import io
import json
import tempfile
import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload, MediaIoBaseUpload
from googleapiclient.errors import HttpError

SCOPES       = ["https://www.googleapis.com/auth/drive"]
CONTADOR_NOMBRE = "ocr_contador.json"
LIMITE_MENSUAL  = 1000


@st.cache_resource
def _get_drive_service():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


# ─────────────────────────────────────────────
# EXCEL
# ─────────────────────────────────────────────

def descargar_excel() -> str:
    service = _get_drive_service()
    file_id = st.secrets["gdrive"]["file_id"]
    request = service.files().get_media(fileId=file_id)
    buffer  = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fd, tmp_path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    with open(tmp_path, "wb") as f:
        f.write(buffer.getvalue())
    return tmp_path


def subir_excel(ruta_local: str):
    service = _get_drive_service()
    file_id = st.secrets["gdrive"]["file_id"]
    media   = MediaFileUpload(
        ruta_local,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=False)
    service.files().update(fileId=file_id, media_body=media).execute()


# ─────────────────────────────────────────────
# CONTADOR OCR — persiste en Drive como JSON
# ─────────────────────────────────────────────

def _obtener_folder_id() -> str:
    """Devuelve el folder_id de Drive donde vive el Excel (para guardar el JSON al lado)."""
    service = _get_drive_service()
    file_id = st.secrets["gdrive"]["file_id"]
    meta    = service.files().get(fileId=file_id, fields="parents").execute()
    parents = meta.get("parents", [])
    return parents[0] if parents else "root"


def _buscar_archivo_contador() -> str | None:
    """Busca el archivo ocr_contador.json en Drive. Devuelve su file_id o None."""
    service   = _get_drive_service()
    folder_id = _obtener_folder_id()
    query     = (f"name = '{CONTADOR_NOMBRE}' "
                 f"and '{folder_id}' in parents "
                 f"and trashed = false")
    result = service.files().list(q=query, fields="files(id, name)").execute()
    files  = result.get("files", [])
    return files[0]["id"] if files else None


def leer_contador_ocr() -> dict:
    """
    Lee el contador de usos OCR desde Drive.
    Estructura del JSON:
    {
      "mes_actual": "2026-03",
      "uso_mes":    42,
      "uso_total":  156,
      "historial":  [
        {"mes": "2026-03", "uso": 42},
        {"mes": "2026-02", "uso": 89},
        ...
      ]
    }
    Si no existe el archivo, devuelve un contador en cero.
    """
    try:
        service    = _get_drive_service()
        file_id_c  = _buscar_archivo_contador()

        if file_id_c is None:
            return _contador_vacio()

        request    = service.files().get_media(fileId=file_id_c)
        buffer     = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        data = json.loads(buffer.getvalue().decode("utf-8"))

        # Resetear uso_mes si cambió el mes
        mes_ahora = _mes_actual()
        if data.get("mes_actual") != mes_ahora:
            # Archivar el mes anterior en historial
            if data.get("uso_mes", 0) > 0:
                historial = data.get("historial", [])
                historial.insert(0, {
                    "mes": data.get("mes_actual", ""),
                    "uso": data.get("uso_mes", 0)
                })
                data["historial"] = historial[:24]  # guardar últimos 24 meses
            data["mes_actual"] = mes_ahora
            data["uso_mes"]    = 0
            # Guardar el reset inmediatamente
            guardar_contador_ocr(data)

        return data

    except Exception:
        return _contador_vacio()


def guardar_contador_ocr(data: dict):
    """
    Guarda el contador en Drive. Crea el archivo si no existe,
    lo actualiza si ya existe.
    """
    try:
        service   = _get_drive_service()
        contenido = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        media     = MediaIoBaseUpload(
            io.BytesIO(contenido),
            mimetype="application/json",
            resumable=False)

        file_id_c = _buscar_archivo_contador()

        if file_id_c:
            # Actualizar archivo existente
            service.files().update(
                fileId=file_id_c, media_body=media).execute()
        else:
            # Crear archivo nuevo en la misma carpeta que el Excel
            folder_id = _obtener_folder_id()
            meta_file = {
                "name":    CONTADOR_NOMBRE,
                "parents": [folder_id],
                "mimeType": "application/json",
            }
            service.files().create(
                body=meta_file, media_body=media, fields="id").execute()

    except Exception as e:
        # No interrumpir la app si falla el guardado del contador
        print(f"[contador OCR] Error al guardar: {e}")


def incrementar_contador_ocr() -> dict:
    """
    Incrementa el contador en +1 y lo guarda en Drive.
    Devuelve el estado actualizado del contador.
    """
    data              = leer_contador_ocr()
    data["uso_mes"]   = data.get("uso_mes",   0) + 1
    data["uso_total"] = data.get("uso_total", 0) + 1
    data["mes_actual"] = _mes_actual()

    # Registrar timestamp del último uso
    data["ultimo_uso"] = datetime.now(ZoneInfo("America/Santiago")).strftime("%d-%m-%Y %H:%M")

    guardar_contador_ocr(data)
    return data


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _mes_actual() -> str:
    return datetime.now(ZoneInfo("America/Santiago")).strftime("%Y-%m")


def _contador_vacio() -> dict:
    return {
        "mes_actual": _mes_actual(),
        "uso_mes":    0,
        "uso_total":  0,
        "ultimo_uso": None,
        "historial":  [],
    }
