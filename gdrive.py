"""
gdrive.py — Integración con Google Drive via Service Account
------------------------------------------------------------
Uso en inventario_streamlit.py:

    from gdrive import descargar_excel, subir_excel

Al inicio de la sesión:
    ruta_tmp = descargar_excel()

Después de guardar cambios:
    subir_excel(ruta_tmp)
"""

import os
import tempfile
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io


SCOPES = ['https://www.googleapis.com/auth/drive']


@st.cache_resource
def _get_drive_service():
    """
    Crea el cliente de Drive usando las credenciales de st.secrets.
    Se cachea para no reconectar en cada rerun de Streamlit.
    """
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES
    )
    return build('drive', 'v3', credentials=creds)


def descargar_excel() -> str:
    """
    Descarga el archivo Excel desde Drive al sistema de archivos temporal
    del servidor. Devuelve la ruta del archivo temporal.
    """
    service = _get_drive_service()
    file_id = st.secrets["gdrive"]["file_id"]

    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    # Guardar en archivo temporal persistente (no se borra entre reruns)
    fd, tmp_path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    with open(tmp_path, 'wb') as f:
        f.write(buffer.getvalue())

    return tmp_path


def subir_excel(ruta_local: str):
    """
    Sube el archivo local de vuelta a Drive, sobreescribiendo el original.
    Llama a esta función después de cada guardar_y_reportes().
    """
    service = _get_drive_service()
    file_id = st.secrets["gdrive"]["file_id"]

    media = MediaFileUpload(
        ruta_local,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        resumable=False
    )
    service.files().update(fileId=file_id, media_body=media).execute()
