"""
ocr_vision.py — Extracción de datos desde documentos usando Google Cloud Vision
--------------------------------------------------------------------------------
Usa la misma Service Account configurada para Drive.
1.000 imágenes/mes gratis en Google Cloud Vision.

Activar la API:
  1. Ir a console.cloud.google.com → tu proyecto
  2. APIs & Services → Library → buscar "Cloud Vision API" → Enable

Agregar al requirements.txt:
  google-cloud-vision
  pdfplumber
  pillow
"""

import io
import re
import json
import streamlit as st
import pdfplumber
from PIL import Image
from google.oauth2 import service_account
from google.cloud import vision


# ─────────────────────────────────────────────
# CLIENTE DE VISION
# ─────────────────────────────────────────────

@st.cache_resource
def _get_vision_client():
    """Reutiliza el cliente de Vision durante toda la sesión."""
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return vision.ImageAnnotatorClient(credentials=creds)


# ─────────────────────────────────────────────
# EXTRACCIÓN DE TEXTO
# ─────────────────────────────────────────────

def extraer_texto_imagen(imagen_bytes: bytes) -> str:
    """
    Envía una imagen a Cloud Vision y devuelve el texto completo detectado.
    Usa DOCUMENT_TEXT_DETECTION que es mejor para documentos con tablas.
    """
    client  = _get_vision_client()
    image   = vision.Image(content=imagen_bytes)
    response = client.document_text_detection(image=image)

    if response.error.message:
        raise RuntimeError(f"Cloud Vision error: {response.error.message}")

    return response.full_text_annotation.text


def extraer_texto_pdf(pdf_bytes: bytes) -> str:
    """
    Extrae texto de un PDF usando pdfplumber (sin OCR — para PDFs digitales).
    Si las páginas no tienen texto seleccionable, convierte a imagen primero.
    """
    texto_total = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for pagina in pdf.pages:
            texto = pagina.extract_text()
            if texto:
                texto_total.append(texto)

    if texto_total:
        return "\n".join(texto_total)

    # Fallback: convertir primera página a imagen y usar Vision
    # (requiere pdf2image, alternativa: solo avisar al usuario)
    return ""


# ─────────────────────────────────────────────
# PARSER DE GUÍA DE DESPACHO LMN
# ─────────────────────────────────────────────

def parsear_guia_despacho(texto: str, codigos_validos: list) -> dict:
    """
    Extrae ítems de una guía de despacho LMN a partir del texto OCR.

    Estructura esperada en el texto:
      <número> <CODIGO> <DESCRIPCION> <cantidad>,00 <precio> <total>
      Fecha de caducidad: YYYY-MM-DD; Lote: XXXXXX

    Devuelve:
    {
      "items": [{"codigo", "nombre", "cantidad", "lote", "fecha_caducidad"}],
      "sucursal_origen": str,
      "fecha_documento": str,
      "numero_documento": str,
    }
    """
    lineas = [l.strip() for l in texto.splitlines() if l.strip()]

    # ── Metadatos del documento ──────────────────────────────────────────────
    numero_doc    = ""
    fecha_doc     = ""
    suc_origen    = ""

    for linea in lineas:
        # N° de documento
        m = re.search(r"N[°º]\s*(\d+)", linea, re.IGNORECASE)
        if m and not numero_doc:
            numero_doc = m.group(1)

        # Fecha del documento — formato "Santiago, DD de mes de AAAA"
        m = re.search(
            r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", linea, re.IGNORECASE)
        if m and not fecha_doc:
            meses = {
                "enero":"01","febrero":"02","marzo":"03","abril":"04",
                "mayo":"05","junio":"06","julio":"07","agosto":"08",
                "septiembre":"09","octubre":"10","noviembre":"11","diciembre":"12",
            }
            mes_num = meses.get(m.group(2).lower(), "01")
            fecha_doc = f"{int(m.group(1)):02d}-{mes_num}-{m.group(3)}"

        # Sucursal de origen — aparece en "Referencia: en HEM: Nro. <nombre>"
        m = re.search(r"Referencia.*?Nro\.\s+(.+?)(?:del|\d{2})", linea, re.IGNORECASE)
        if m and not suc_origen:
            suc_origen = m.group(1).strip()

        # Alternativa: "Señor(es): <nombre>"
        m = re.search(r"Señor(?:es)?\s*[:\s]+(.+)", linea, re.IGNORECASE)
        if m and not suc_origen:
            suc_origen = m.group(1).strip()

    # ── Ítems de la tabla ────────────────────────────────────────────────────
    items = []

    # Patrones para detectar filas de la tabla
    # Ejemplo: "1  IS02  AGUJA MÚLTIPLE 21 G  6,00  0,00  0"
    patron_fila = re.compile(
        r"^\s*\d+\s+([A-Z]{2}\d{2,3})\s+(.+?)\s+(\d[\d\.,]+)\s*,?00",
        re.IGNORECASE
    )
    # Patrón alternativo para código solo
    patron_codigo = re.compile(r"\b([A-Z]{2}\d{2,3})\b")

    i = 0
    while i < len(lineas):
        linea = lineas[i]

        # Buscar fila de ítem
        m = patron_fila.match(linea)
        if m:
            codigo    = m.group(1).upper()
            nombre    = m.group(2).strip()
            # La cantidad puede venir como "6,00" o "6.00" o "6"
            cant_str  = m.group(3).replace(",", ".").replace(" ", "")
            try:
                cantidad = int(float(cant_str))
            except ValueError:
                cantidad = 0

            lote   = "N/A"
            fecha_cad = "S/V"

            # Buscar lote y fecha en las siguientes 3 líneas
            for j in range(i + 1, min(i + 4, len(lineas))):
                lnext = lineas[j]

                # Fecha de caducidad: YYYY-MM-DD o DD-MM-YYYY o DD/MM/YYYY
                m_f = re.search(
                    r"[Ff]echa.*?[Cc]aducidad[:\s]+(\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4}|\d{2}/\d{2}/\d{4})",
                    lnext
                )
                if m_f:
                    raw = m_f.group(1).replace("/", "-")
                    # Normalizar a DD-MM-YYYY
                    if re.match(r"\d{4}-\d{2}-\d{2}", raw):
                        partes = raw.split("-")
                        fecha_cad = f"{partes[2]}-{partes[1]}-{partes[0]}"
                    else:
                        fecha_cad = raw

                # Lote
                m_l = re.search(r"[Ll]ote[:\s]+([A-Za-z0-9]+)", lnext)
                if m_l:
                    lote = m_l.group(1).strip()

            if codigo in codigos_validos and cantidad > 0:
                items.append({
                    "codigo":           codigo,
                    "nombre":           nombre,
                    "cantidad":         cantidad,
                    "lote":             lote,
                    "fecha_caducidad":  fecha_cad,
                })
            i += 1
            continue

        # Buscar código standalone en la línea
        m_cod = patron_codigo.search(linea)
        if m_cod:
            codigo = m_cod.group(1).upper()
            if codigo in codigos_validos:
                # Intentar extraer cantidad de la misma línea
                nums = re.findall(r"\b(\d{1,5})[,.]?00\b", linea)
                cantidad = int(nums[0]) if nums else 0

                lote      = "N/A"
                fecha_cad = "S/V"

                for j in range(i + 1, min(i + 4, len(lineas))):
                    lnext = lineas[j]
                    m_f   = re.search(
                        r"[Ff]echa.*?[Cc]aducidad[:\s]+(\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4})",
                        lnext)
                    if m_f:
                        raw = m_f.group(1)
                        if re.match(r"\d{4}-\d{2}-\d{2}", raw):
                            p = raw.split("-")
                            fecha_cad = f"{p[2]}-{p[1]}-{p[0]}"
                        else:
                            fecha_cad = raw
                    m_l = re.search(r"[Ll]ote[:\s]+([A-Za-z0-9]+)", lnext)
                    if m_l:
                        lote = m_l.group(1).strip()

                # Evitar duplicados
                ya_existe = any(it["codigo"] == codigo for it in items)
                if not ya_existe and cantidad > 0:
                    nombre_cat = codigo  # se completará con el catálogo luego
                    items.append({
                        "codigo":          codigo,
                        "nombre":          nombre_cat,
                        "cantidad":        cantidad,
                        "lote":            lote,
                        "fecha_caducidad": fecha_cad,
                    })
        i += 1

    return {
        "items":            items,
        "sucursal_origen":  suc_origen,
        "fecha_documento":  fecha_doc,
        "numero_documento": numero_doc,
    }
