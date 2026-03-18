"""
=============================================================
  LMN Bicentenario — Sistema de Inventario (Service Account)
=============================================================
Persistencia via Google Drive con Service Account.
No requiere login del usuario.

Archivos necesarios en la misma carpeta:
  - inventario_sa.py        (este archivo)
  - gdrive.py               (módulo de Drive)
  - .streamlit/secrets.toml (credenciales)

secrets.toml:
  [gdrive]
  file_id = "1ABC_XYZ_id_del_archivo"

  [gcp_service_account]
  type = "service_account"
  project_id = "mi-proyecto"
  private_key_id = "abc123"
  private_key = "-----BEGIN RSA PRIVATE KEY-----\\n..."
  client_email = "inventario-bot@mi-proyecto.iam.gserviceaccount.com"
  client_id = "123456789"
  auth_uri = "https://accounts.google.com/o/oauth2/auth"
  token_uri = "https://oauth2.googleapis.com/token"
"""

import shutil
import io
import pandas as pd
import streamlit as st
from datetime import datetime
from pathlib import Path
from gdrive import descargar_excel, subir_excel

HEADER_ROW = 2
DIR_BACKUP  = "./Backups_Inventario"


# ═════════════════════════════════════════════
# CAPA 1: ACCESO A DATOS
# ═════════════════════════════════════════════

class InventarioRepo:
    def __init__(self, ruta: str):
        self.ruta = ruta

    def cargar_hoja(self, nombre_hoja: str) -> pd.DataFrame:
        df = pd.read_excel(self.ruta, sheet_name=nombre_hoja, skiprows=HEADER_ROW)
        return self._limpiar(df)

    def cargar_insumos(self)  -> pd.DataFrame: return self.cargar_hoja("Insumos")
    def cargar_ingresos(self) -> pd.DataFrame: return self.cargar_hoja("Ingresos")
    def cargar_salidas(self)  -> pd.DataFrame: return self.cargar_hoja("Salidas")

    def cargar_sucursales(self) -> list:
        df = pd.read_excel(self.ruta, sheet_name="Sucursales", skiprows=HEADER_ROW)
        df.columns = df.columns.str.strip()
        # Filtrar solo sucursales activas (evita filas vacías con "Activa" en col Estado)
        if "Estado" in df.columns:
            df = df[df["Estado"].astype(str).str.strip().str.lower() == "activa"]
        return df["Sucursal"].dropna().unique().tolist()

    def guardar_transaccion(self, df: pd.DataFrame, hoja: str):
        with pd.ExcelWriter(
            self.ruta, mode="a", engine="openpyxl", if_sheet_exists="replace"
        ) as w:
            df.to_excel(w, sheet_name=hoja, index=False, startrow=HEADER_ROW)

    def guardar_reportes(self, df_lote, df_suc, df_sin):
        with pd.ExcelWriter(
            self.ruta, mode="a", engine="openpyxl", if_sheet_exists="replace"
        ) as w:
            df_suc.to_excel(w,  sheet_name="Stock por Sucursal",  index=False, startrow=HEADER_ROW)
            df_lote.to_excel(w, sheet_name="Stock por Lote",      index=False, startrow=HEADER_ROW)
            if df_sin is not None and not df_sin.empty:
                df_sin.to_excel(w, sheet_name="Stock sin Lote ni Vencimiento",
                                index=False, startrow=HEADER_ROW)

    def hacer_backup(self) -> str:
        Path(DIR_BACKUP).mkdir(parents=True, exist_ok=True)
        ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
        destino = f"{DIR_BACKUP}/{Path(self.ruta).stem}_backup_{ts}.xlsx"
        shutil.copy2(self.ruta, destino)
        return destino

    def agregar_sucursal(self, nombre: str, direccion: str = "", responsable: str = "") -> None:
        """Agrega una nueva sucursal a la hoja Sucursales con estado Activa."""
        df = pd.read_excel(self.ruta, sheet_name="Sucursales", skiprows=HEADER_ROW)
        df.columns = df.columns.str.strip()
        nueva = pd.DataFrame([{
            "Sucursal":               nombre.strip(),
            "Dirección / referencia": direccion.strip(),
            "Responsable":            responsable.strip(),
            "Estado":                 "Activa",
        }])
        df_nueva = pd.concat([df, nueva], ignore_index=True)
        with pd.ExcelWriter(
            self.ruta, mode="a", engine="openpyxl", if_sheet_exists="replace"
        ) as w:
            df_nueva.to_excel(w, sheet_name="Sucursales", index=False, startrow=HEADER_ROW)

    def leer_archivo_externo(self, archivo) -> pd.DataFrame:
        nombre = archivo.name.lower()
        df = pd.read_csv(archivo, dtype=str) if nombre.endswith(".csv") \
             else pd.read_excel(archivo, dtype=str)
        df.columns = df.columns.str.strip()
        return df

    def get_bytes(self) -> bytes:
        with open(self.ruta, "rb") as f:
            return f.read()

    @staticmethod
    def _limpiar(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        for col in ("Código", "Lote"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.upper()
        if "Nombre del insumo" in df.columns:
            df["Nombre del insumo"] = df["Nombre del insumo"].astype(str).str.strip()
        if "Cantidad" in df.columns:
            df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0)
        return df


# ═════════════════════════════════════════════
# CAPA 2: LÓGICA DE NEGOCIO
# ═════════════════════════════════════════════

class StockService:
    def __init__(self, repo: InventarioRepo):
        self.repo = repo

    @staticmethod
    def _normalizar_fecha(serie: pd.Series) -> pd.Series:
        """Convierte fechas a string uniforme YYYY-MM-DD o deja el valor tal cual (S/V, etc.)"""
        def _conv(v):
            if pd.isna(v):
                return "S/V"
            if hasattr(v, "strftime"):
                return v.strftime("%Y-%m-%d")
            s = str(v).strip()
            return s if s else "S/V"
        return serie.apply(_conv)

    def stock_por_lote(self, codigo, df_ing, df_sal):
        codigo = codigo.strip().upper()
        ing    = df_ing[df_ing["Código"] == codigo].copy()
        sal    = df_sal[df_sal["Código"] == codigo].copy()
        if ing.empty:
            return pd.DataFrame()

        # Normalizar tipos para evitar error de merge entre datetime y string
        ing["Lote"]               = ing["Lote"].astype(str).str.strip().str.upper()
        ing["Fecha de caducidad"] = StockService._normalizar_fecha(ing["Fecha de caducidad"])
        resumen_ing = ing.groupby(["Lote", "Fecha de caducidad"])["Cantidad"].sum().reset_index()

        if not sal.empty and "Fecha de caducidad asociada" in sal.columns:
            sal["Lote"]                        = sal["Lote"].astype(str).str.strip().str.upper()
            sal["Fecha de caducidad asociada"] = StockService._normalizar_fecha(sal["Fecha de caducidad asociada"])
            resumen_sal = sal.groupby(
                ["Lote", "Fecha de caducidad asociada"])["Cantidad"].sum().reset_index()
            resumen_sal.columns = ["Lote", "Fecha de caducidad", "Cant_Salida"]
            df_l = resumen_ing.merge(
                resumen_sal, on=["Lote", "Fecha de caducidad"], how="left").fillna(0)
            df_l["Disponible"] = df_l["Cantidad"] - df_l["Cant_Salida"]
        else:
            df_l = resumen_ing.copy()
            df_l["Disponible"] = df_l["Cantidad"]
        return df_l[df_l["Disponible"] > 0].reset_index(drop=True)

    def vencimientos_proximos(self, df_ing, df_sal, dias: int = 60) -> pd.DataFrame:
        """
        Devuelve todos los lotes con stock disponible cuya fecha de caducidad
        está entre hoy y hoy + `dias`. Excluye lotes sin vencimiento (S/V).
        """
        df_lote = self.construir_stock_por_lote(df_ing, df_sal)
        df_lote = df_lote[df_lote["Stock disponible"] > 0].copy()

        hoy   = pd.Timestamp.now().normalize()
        limite = hoy + pd.Timedelta(days=dias)

        def _parse(v):
            if pd.isna(v) or str(v).strip().upper() in ("S/V", "NAN", ""):
                return pd.NaT
            try:
                return pd.to_datetime(v)
            except Exception:
                return pd.NaT

        df_lote["_fecha_dt"] = df_lote["Fecha de caducidad"].apply(_parse)
        prox = df_lote[
            df_lote["_fecha_dt"].notna() &
            (df_lote["_fecha_dt"] >= hoy) &
            (df_lote["_fecha_dt"] <= limite)
        ].copy()

        prox["Días restantes"] = (prox["_fecha_dt"] - hoy).dt.days

        def _semaforo(d):
            if d <= 15:  return "🔴 Crítico"
            if d <= 30:  return "🟠 Urgente"
            return "🟡 Próximo"

        prox["Estado"] = prox["Días restantes"].apply(_semaforo)
        prox["Fecha de caducidad"] = prox["_fecha_dt"].dt.strftime("%d-%m-%Y")
        return prox[["Código", "Nombre del insumo", "Lote", "Fecha de caducidad",
                     "Días restantes", "Estado", "Stock disponible"]]            .sort_values("Días restantes").reset_index(drop=True)

    def construir_stock_por_lote(self, df_ing, df_sal):
        ing_ag = df_ing.groupby(
            ["Código", "Nombre del insumo", "Lote", "Fecha de caducidad"]
        )["Cantidad"].sum().reset_index()
        sal_ag = df_sal.groupby(
            ["Código", "Lote", "Fecha de caducidad asociada"])["Cantidad"].sum().reset_index()
        sal_ag.columns = ["Código", "Lote", "Fecha de caducidad", "Cant_Salida"]
        df = ing_ag.merge(sal_ag, on=["Código", "Lote", "Fecha de caducidad"], how="left").fillna(0)
        df["Stock disponible"] = df["Cantidad"] - df["Cant_Salida"]
        return df.rename(columns={"Cantidad": "Ingresos", "Cant_Salida": "Salidas"})[
            ["Código", "Nombre del insumo", "Lote", "Fecha de caducidad",
             "Ingresos", "Salidas", "Stock disponible"]]

    def construir_stock_por_sucursal(self, df_ing, df_sal, df_insumos, lista_suc):
        todos  = df_insumos["Nombre del insumo"].unique()
        t_ing  = df_ing.groupby("Nombre del insumo")["Cantidad"].sum().reindex(todos, fill_value=0)
        if not df_sal.empty and "Destino" in df_sal.columns:
            matriz = df_sal.groupby(["Nombre del insumo", "Destino"])["Cantidad"].sum()\
                .unstack(fill_value=0).reindex(todos, fill_value=0)
        else:
            matriz = pd.DataFrame(0, index=todos, columns=[])
        rep = pd.DataFrame(index=todos)
        rep.index.name = "Nombre del insumo"
        rep["Usado en BC"] = matriz["Bodega Central"] if "Bodega Central" in matriz.columns else 0
        suc_ext = [s for s in lista_suc if s != "Bodega Central"]
        for s in suc_ext:
            rep[f"Enviado a {s}"] = matriz[s] if s in matriz.columns else 0
        for d in [d for d in matriz.columns if d != "Bodega Central" and d not in suc_ext]:
            rep[f"Enviado a {d}"] = matriz[d]
        cols_env = [c for c in rep.columns if c.startswith("Enviado a")]
        t_env    = rep[cols_env].sum(axis=1) if cols_env else 0
        rep["Stock Disponible (Bodega Central)"] = t_ing - rep["Usado en BC"] - t_env
        rep["STOCK TOTAL"] = rep["Stock Disponible (Bodega Central)"] + t_env
        return rep[["Stock Disponible (Bodega Central)", "Usado en BC"] + cols_env + ["STOCK TOTAL"]]\
            .fillna(0).reset_index()

    def construir_stock_sin_lote(self, df_ing, df_sal, lista_suc):
        ing_sin = df_ing[df_ing["Lote"].isin(["N/A", ""])].copy()
        sal_sin = df_sal[df_sal["Lote"].isin(["N/A", ""])].copy()
        if ing_sin.empty:
            return None
        t_ing  = ing_sin.groupby("Nombre del insumo")["Cantidad"].sum()
        t_sal  = sal_sin.groupby("Nombre del insumo")["Cantidad"].sum()
        stock  = (t_ing - t_sal).fillna(t_ing)
        matriz = sal_sin.groupby(["Nombre del insumo", "Destino"])["Cantidad"]\
            .sum().unstack(fill_value=0)
        rep    = pd.DataFrame(index=ing_sin["Nombre del insumo"].unique())
        rep["Stock Disponible (Bodega Central)"] = stock
        for s in lista_suc:
            if s != "Bodega Central":
                rep[f"Enviado a {s}"] = matriz[s] if s in matriz.columns else 0
        rep["STOCK TOTAL SIN LOTE"] = rep.sum(axis=1)
        return rep.fillna(0).reset_index().rename(columns={"index": "Nombre del insumo"})

    def validar_e_importar_inicial(self, df_raw, df_insumos):
        faltantes = {"Código", "Cantidad"} - set(df_raw.columns)
        if faltantes:
            raise ValueError(f"Faltan columnas obligatorias: {faltantes}")
        codigos_validos   = set(df_insumos["Código"].astype(str).str.strip().str.upper())
        nombre_por_codigo = df_insumos.set_index("Código")["Nombre del insumo"].to_dict()
        filas_ok, errores = [], []
        for i, row in df_raw.iterrows():
            n   = i + 1
            cod = str(row.get("Código", "")).strip().upper()
            if not cod or cod == "NAN":
                errores.append(f"Fila {n}: código vacío — omitida."); continue
            if cod not in codigos_validos:
                errores.append(f"Fila {n}: código '{cod}' no existe — omitida."); continue
            try:
                cant = float(str(row.get("Cantidad", "")).replace(",", "."))
                if cant <= 0: raise ValueError
            except ValueError:
                errores.append(f"Fila {n} ({cod}): cantidad inválida — omitida."); continue
            lote_raw = str(row.get("Lote", "")).strip().upper()
            lote     = lote_raw if lote_raw and lote_raw != "NAN" else "N/A"
            venc_raw = str(row.get("Fecha de caducidad", "")).strip()
            if venc_raw and venc_raw.upper() != "NAN":
                try:
                    venc = datetime.strptime(venc_raw, "%d-%m-%Y")
                except ValueError:
                    errores.append(f"Fila {n} ({cod}): fecha inválida — se usará 'S/V'.")
                    venc = "S/V"
            else:
                venc = "S/V"
            proveedor = str(row.get("Proveedor", "")).strip()
            proveedor = proveedor if proveedor and proveedor.upper() != "NAN" else ""
            obs_raw   = str(row.get("Observación", "")).strip()
            obs       = obs_raw if obs_raw and obs_raw.upper() != "NAN" else "Inventario inicial"
            filas_ok.append({
                "Fecha":             datetime.now(),
                "Código":            cod,
                "Nombre del insumo": nombre_por_codigo[cod],
                "Lote":              lote,
                "Cantidad":          cant,
                "Fecha de caducidad": venc,
                "Proveedor":         proveedor,
                "Observación":       obs,
            })
        return pd.DataFrame(filas_ok), errores


# ═════════════════════════════════════════════
# HELPERS DE SESIÓN
# ═════════════════════════════════════════════

def init_session():
    """
    Descarga el Excel desde Drive y carga todo en session_state.
    Se llama una sola vez al iniciar la app (o al recargar manualmente).
    """
    with st.spinner("Descargando archivo desde Google Drive..."):
        tmp = descargar_excel()

    repo = InventarioRepo(tmp)
    st.session_state.update({
        "repo":       repo,
        "servicio":   StockService(repo),
        "df_insumos": repo.cargar_insumos(),
        "lista_suc":  repo.cargar_sucursales(),
        "df_ing":     repo.cargar_ingresos(),
        "df_sal":     repo.cargar_salidas(),
        "tmp_path":   tmp,
        "cargado":    True,
    })


def guardar_y_reportes(df_nuevo: pd.DataFrame, hoja: str):
    """
    1. Backup local
    2. Guarda la hoja de transacciones en el archivo temporal
    3. Recalcula y guarda las hojas de reporte
    4. Sube el archivo actualizado a Google Drive
    """
    repo     = st.session_state.repo
    servicio = st.session_state.servicio

    # Backup local preventivo
    try:
        st.info(f"Backup local creado: `{repo.hacer_backup()}`")
    except Exception as e:
        st.warning(f"No se pudo crear backup local: {e}")

    # Guardar transacción
    try:
        repo.guardar_transaccion(df_nuevo, hoja)
    except Exception as e:
        return False, f"Error al guardar '{hoja}': {e}"

    # Recargar en memoria
    df_ing_n = repo.cargar_ingresos()
    df_sal_n = repo.cargar_salidas()
    st.session_state.df_ing = df_ing_n
    st.session_state.df_sal = df_sal_n

    # Reportes
    try:
        repo.guardar_reportes(
            servicio.construir_stock_por_lote(df_ing_n, df_sal_n),
            servicio.construir_stock_por_sucursal(
                df_ing_n, df_sal_n,
                st.session_state.df_insumos, st.session_state.lista_suc),
            servicio.construir_stock_sin_lote(
                df_ing_n, df_sal_n, st.session_state.lista_suc),
        )
    except Exception as e:
        return False, f"Transacción guardada pero error en reportes: {e}"

    # Subir a Drive
    try:
        with st.spinner("Sincronizando con Google Drive..."):
            subir_excel(st.session_state.tmp_path)
    except Exception as e:
        return False, f"Guardado localmente pero error al subir a Drive: {e}"

    return True, "¡Cambios guardados y sincronizados con Google Drive! ☁️"


# ═════════════════════════════════════════════
# APP
# ═════════════════════════════════════════════

st.set_page_config(
    page_title="LMN Bicentenario — Inventario",
    page_icon="📦",
    layout="wide",
)

st.title("📦 LMN Bicentenario — Sistema de Inventario")

# ── Carga automática al primer acceso ────────────────────────────────────────
if not st.session_state.get("cargado"):
    try:
        init_session()
        st.rerun()
    except Exception as e:
        st.error(f"No se pudo conectar con Google Drive: {e}")
        st.caption("Verifica que el `secrets.toml` esté configurado correctamente "
                   "y que el archivo de Drive esté compartido con la Service Account.")
        st.stop()

# Atajos
repo       = st.session_state.repo
servicio   = st.session_state.servicio
df_insumos = st.session_state.df_insumos
lista_suc  = st.session_state.lista_suc
df_ing     = st.session_state.df_ing
df_sal     = st.session_state.df_sal

# ── Barra lateral ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("☁️ Google Drive")
    st.success("Conectado vía Service Account")
    st.divider()
    st.caption(f"Insumos: {len(df_insumos)}")
    st.caption(f"Ingresos registrados: {len(df_ing)}")
    st.caption(f"Salidas registradas: {len(df_sal)}")
    st.divider()

    if st.button("🔄 Recargar desde Drive"):
        try:
            init_session()
            st.success("Datos actualizados.")
            st.rerun()
        except Exception as e:
            st.error(f"Error al recargar: {e}")

    st.download_button(
        "⬇️ Descargar copia local",
        data=repo.get_bytes(),
        file_name="inventario_copia_local.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.divider()
    # Badge de alertas rápidas en sidebar
    _df_alerta_sb = servicio.vencimientos_proximos(df_ing, df_sal, dias=30)
    _criticos     = len(_df_alerta_sb[_df_alerta_sb["Estado"] == "🔴 Crítico"])
    _urgentes     = len(_df_alerta_sb[_df_alerta_sb["Estado"] == "🟠 Urgente"])
    if _criticos:
        st.error(f"🔴 {_criticos} lote(s) vencen en ≤ 15 días")
    if _urgentes:
        st.warning(f"🟠 {_urgentes} lote(s) vencen en ≤ 30 días")
    if not _criticos and not _urgentes:
        st.success("✅ Sin vencimientos críticos (30 días)")

# ── Pestañas ──────────────────────────────────────────────────────────────────
tab_consulta, tab_ingreso, tab_salida, tab_carga, tab_reportes, tab_alertas = st.tabs([
    "🔍 Consultar Stock",
    "➕ Registrar Ingreso",
    "➖ Registrar Salida",
    "📂 Carga Inicial",
    "📊 Reportes",
    "⚠️ Alertas",
])


# ══════════════════════════════════════════════
# TAB 1 — CONSULTAR STOCK
# ══════════════════════════════════════════════
with tab_consulta:
    st.subheader("Consulta de stock disponible")
    termino = st.text_input("Buscar por código o nombre del insumo").strip().upper()
    if termino:
        mask = (
            (df_insumos["Código"] == termino) |
            (df_insumos["Nombre del insumo"].str.upper().str.contains(termino, na=False))
        )
        coincidencias = df_insumos[mask]
        if coincidencias.empty:
            st.warning("No se encontró ningún insumo con ese código o nombre.")
        else:
            opciones  = {f"{r['Código']} — {r['Nombre del insumo']}": r["Código"]
                         for _, r in coincidencias.iterrows()}
            seleccion = st.selectbox("Insumo encontrado", list(opciones.keys()))
            cod_sel   = opciones[seleccion]
            nom_sel   = coincidencias[coincidencias["Código"] == cod_sel].iloc[0]["Nombre del insumo"]
            lotes     = servicio.stock_por_lote(cod_sel, df_ing, df_sal)
            if lotes.empty:
                st.warning(f"**{nom_sel}** — Sin stock disponible.")
            else:
                ld = lotes.copy()
                ld["Fecha de caducidad"] = ld["Fecha de caducidad"].apply(
                    lambda x: x.strftime("%d-%m-%Y") if hasattr(x, "strftime") else str(x))
                ld["Disponible"] = ld["Disponible"].astype(int)
                col1, col2 = st.columns([3, 1])
                with col1: st.markdown(f"**{nom_sel}** `{cod_sel}`")
                with col2: st.metric("Total disponible", int(lotes["Disponible"].sum()))
                st.dataframe(ld[["Lote", "Fecha de caducidad", "Disponible"]],
                             use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════
# TAB 2 — REGISTRAR INGRESO (con carrito)
# ══════════════════════════════════════════════
with tab_ingreso:

    # Inicializar carrito en session_state
    if "carrito_ing" not in st.session_state:
        st.session_state.carrito_ing = []

    st.subheader("Registrar ingreso de insumos")

    # ── Formulario para agregar al carrito ────────────────────────────────────
    with st.container(border=True):
        col_tit_ing, col_fecha_ing = st.columns([3, 1])
        with col_tit_ing:
            st.markdown("##### Agregar ítem al ingreso")
        with col_fecha_ing:
            st.info(f"🕐 {datetime.now().strftime('%d-%m-%Y  %H:%M')}")
        opts_ing = {f"{r['Código']} — {r['Nombre del insumo']}": r["Código"]
                    for _, r in df_insumos.iterrows()}
        sel_ing  = st.selectbox("Insumo", list(opts_ing.keys()), key="ing_insumo")
        cod_ing  = opts_ing[sel_ing]
        nom_ing  = df_insumos[df_insumos["Código"] == cod_ing].iloc[0]["Nombre del insumo"]

        col_a, col_b = st.columns(2)
        with col_a:
            lote_ing = st.text_input("Lote (vacío = N/A)", key="ing_lote").strip().upper() or "N/A"
            cant_ing = st.number_input("Cantidad", min_value=0.0, step=1.0, key="ing_cant")
            prov_ing = st.text_input("Proveedor", key="ing_prov").strip()
        with col_b:
            fv       = st.date_input("Fecha vencimiento (opcional)", value=None, key="ing_venc")
            venc_ing = datetime.combine(fv, datetime.min.time()) if fv else "S/V"
            obs_ing  = st.text_area("Observación", key="ing_obs").strip()

        if st.button("➕ Agregar al ingreso", key="btn_agregar_ing"):
            if cant_ing <= 0:
                st.error("La cantidad debe ser mayor a 0.")
            else:
                st.session_state.carrito_ing.append({
                    "Código":            cod_ing,
                    "Nombre del insumo": nom_ing,
                    "Lote":              lote_ing,
                    "Cantidad":          cant_ing,
                    "Fecha de caducidad": (
                        venc_ing.strftime("%d-%m-%Y")
                        if hasattr(venc_ing, "strftime") else str(venc_ing)
                    ),
                    "Proveedor":         prov_ing,
                    "Observación":       obs_ing,
                    # guardamos el datetime real para el guardado
                    "_venc_raw":         venc_ing,
                })
                st.success(f"✔ **{nom_ing}** agregado al ingreso.")
                st.rerun()

    # ── Tabla del carrito ─────────────────────────────────────────────────────
    st.divider()
    carrito = st.session_state.carrito_ing

    if not carrito:
        st.info("📥 Aún no hay ítems en este ingreso. Agrega insumos con el formulario de arriba.")
    else:
        st.markdown(f"##### 📥 Ítems en este ingreso ({len(carrito)})")

        # Mostrar tabla con botón eliminar por fila
        cols_header = st.columns([2, 3, 2, 1, 2, 2, 1])
        for h, label in zip(cols_header, ["Código", "Nombre", "Lote", "Cantidad",
                                           "Vencimiento", "Proveedor", ""]):
            h.markdown(f"**{label}**")

        for i, item in enumerate(carrito):
            c1, c2, c3, c4, c5, c6, c7 = st.columns([2, 3, 2, 1, 2, 2, 1])
            c1.write(item["Código"])
            c2.write(item["Nombre del insumo"])
            c3.write(item["Lote"])
            c4.write(int(item["Cantidad"]))
            c5.write(item["Fecha de caducidad"])
            c6.write(item["Proveedor"] or "—")
            if c7.button("🗑️", key=f"del_ing_{i}", help="Eliminar este ítem"):
                st.session_state.carrito_ing.pop(i)
                st.rerun()

        # Totales rápidos
        st.divider()
        resumen = (
            pd.DataFrame(carrito)[["Nombre del insumo", "Cantidad"]]
            .groupby("Nombre del insumo")["Cantidad"]
            .sum()
            .reset_index()
            .rename(columns={"Cantidad": "Total a ingresar"})
        )
        col_res, col_btn = st.columns([3, 1])
        with col_res:
            with st.expander("📊 Ver resumen por insumo", expanded=False):
                st.dataframe(resumen, use_container_width=True, hide_index=True)
        with col_btn:
            st.markdown("&nbsp;", unsafe_allow_html=True)
            if st.button("🗑️ Vaciar carrito", key="btn_vaciar_ing"):
                st.session_state.carrito_ing = []
                st.rerun()

        # ── Confirmar y guardar todo ───────────────────────────────────────────
        st.divider()
        if st.button(
            f"✅ Confirmar ingreso ({len(carrito)} ítem{'s' if len(carrito) > 1 else ''})",
            type="primary", key="btn_confirmar_ing"
        ):
            ahora = datetime.now()
            nuevas_filas = []
            for item in carrito:
                nuevas_filas.append({
                    "Fecha":              ahora,
                    "Código":             item["Código"],
                    "Nombre del insumo":  item["Nombre del insumo"],
                    "Lote":               item["Lote"],
                    "Cantidad":           item["Cantidad"],
                    "Fecha de caducidad": item["_venc_raw"],
                    "Proveedor":          item["Proveedor"],
                    "Observación":        item["Observación"],
                })
            ok, msg = guardar_y_reportes(
                pd.concat([df_ing, pd.DataFrame(nuevas_filas)], ignore_index=True),
                "Ingresos"
            )
            if ok:
                st.session_state.carrito_ing = []
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)

    # ── Historial de ingresos registrados ─────────────────────────────────────
    st.divider()
    with st.expander("📋 Ver historial de ingresos registrados", expanded=False):
        if df_ing.empty:
            st.info("No hay ingresos registrados aún.")
        else:
            df_hist = df_ing.copy()
            # Formatear fecha de caducidad
            if "Fecha de caducidad" in df_hist.columns:
                df_hist["Fecha de caducidad"] = df_hist["Fecha de caducidad"].apply(
                    lambda x: x.strftime("%d-%m-%Y") if hasattr(x, "strftime") else str(x))
            if "Fecha" in df_hist.columns:
                df_hist["Fecha"] = df_hist["Fecha"].apply(
                    lambda x: x.strftime("%d-%m-%Y %H:%M") if hasattr(x, "strftime") else str(x))
            # Mostrar más recientes primero
            df_hist = df_hist.iloc[::-1].reset_index(drop=True)
            st.dataframe(df_hist, use_container_width=True, hide_index=True)
            buf_hist = io.BytesIO()
            df_hist.to_excel(buf_hist, index=False)
            st.download_button(
                "⬇️ Descargar historial",
                buf_hist.getvalue(),
                "historial_ingresos.xlsx",
                key="dl_hist_ing",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )


# ══════════════════════════════════════════════
# TAB 3 — REGISTRAR SALIDA (con carrito)
# ══════════════════════════════════════════════
with tab_salida:

    # Inicializar carrito de salidas en session_state
    if "carrito_sal" not in st.session_state:
        st.session_state.carrito_sal = []

    st.subheader("Registrar salida de insumos")

    # ── Formulario para agregar al carrito ────────────────────────────────────
    with st.container(border=True):
        col_tit_sal, col_fecha_sal = st.columns([3, 1])
        with col_tit_sal:
            st.markdown("##### Agregar ítem a la salida")
        with col_fecha_sal:
            st.info(f"🕐 {datetime.now().strftime('%d-%m-%Y  %H:%M')}")

        opts_sal = {f"{r['Código']} — {r['Nombre del insumo']}": r["Código"]
                    for _, r in df_insumos.iterrows()}
        sel_sal  = st.selectbox("Insumo", list(opts_sal.keys()), key="sal_insumo")
        cod_sal  = opts_sal[sel_sal]
        nom_sal  = df_insumos[df_insumos["Código"] == cod_sal].iloc[0]["Nombre del insumo"]

        # Calcular stock disponible descontando ya lo que está en el carrito de salidas
        cant_carrito = sum(
            item["Cantidad"] for item in st.session_state.carrito_sal
            if item["Código"] == cod_sal
        )
        lotes_sal = servicio.stock_por_lote(cod_sal, df_ing, df_sal)

        if lotes_sal.empty:
            st.warning("Sin stock disponible para este insumo.")
        else:
            ls = lotes_sal.copy()
            ls["Fecha de caducidad"] = ls["Fecha de caducidad"].apply(
                lambda x: x.strftime("%d-%m-%Y") if hasattr(x, "strftime") else str(x))
            opts_lote = {
                f"Lote {r['Lote']} | Vence {ls.loc[i, 'Fecha de caducidad']} | Stock {int(r['Disponible'])}": i
                for i, r in lotes_sal.iterrows()}
            sel_lote = st.selectbox("Lote disponible", list(opts_lote.keys()), key="sal_lote")
            idx_lote  = opts_lote[sel_lote]
            lote_sel  = lotes_sal.loc[idx_lote, "Lote"]
            venc_sel  = lotes_sal.loc[idx_lote, "Fecha de caducidad"]

            # Descontar lo ya agregado al carrito para ese lote específico
            cant_carrito_lote = sum(
                item["Cantidad"] for item in st.session_state.carrito_sal
                if item["Código"] == cod_sal and item["Lote"] == lote_sel
            )
            stock_max = max(0.0, float(lotes_sal.loc[idx_lote, "Disponible"]) - cant_carrito_lote)

            if cant_carrito_lote > 0:
                st.caption(f"⚠️ Ya tienes **{int(cant_carrito_lote)}** unidades de este lote en la salida actual.")

            col_c, col_d = st.columns(2)
            with col_c:
                # ── Selector de destino ────────────────────────────────────
                OPCION_OTRO = "➕ Agregar nuevo destino..."
                suc_destino = [s for s in lista_suc if s != "Bodega Central"] + [OPCION_OTRO]
                dest_sel = st.selectbox(
                    "Destino / Sucursal",
                    suc_destino,
                    key="sal_destino",
                    help="Si no aparece en la lista, elige '➕ Agregar nuevo destino...'"
                )

                if dest_sel == OPCION_OTRO:
                    st.markdown("##### ✏️ Nueva sucursal / destino")
                    dest_sal_txt  = st.text_input(
                        "Nombre *", key="sal_nuevo_nombre",
                        placeholder="Ej: Sucursal Norte"
                    ).strip()
                    dest_dir  = st.text_input(
                        "Dirección / referencia (opcional)", key="sal_nuevo_dir",
                        placeholder="Ej: Av. Principal 123"
                    ).strip()
                    dest_resp = st.text_input(
                        "Responsable (opcional)", key="sal_nuevo_resp",
                        placeholder="Ej: Juan Pérez"
                    ).strip()
                    guardar_en_lista = st.checkbox(
                        "Guardar en el catálogo de sucursales",
                        value=True, key="sal_guardar_suc",
                        help="Quedará disponible en el selector para futuros registros."
                    )
                    if dest_sal_txt:
                        st.info(f"✏️ Destino a registrar: **{dest_sal_txt}**")
                    dest_final       = dest_sal_txt
                else:
                    dest_final       = dest_sel
                    dest_dir         = ""
                    dest_resp        = ""
                    guardar_en_lista = False

                cant_sal = st.number_input(
                    f"Cantidad (máx {int(stock_max)})",
                    min_value=0.0, max_value=stock_max, step=1.0, key="sal_cant"
                )

            with col_d:
                obs_sal = st.text_area(
                    "Observación", key="sal_obs",
                    placeholder="Opcional: motivo, número de orden, etc."
                ).strip()

            if st.button("➕ Agregar a la salida", key="btn_agregar_sal"):
                if cant_sal <= 0:
                    st.error("La cantidad debe ser mayor a 0.")
                elif not dest_final:
                    st.error("Debes ingresar el nombre del destino.")
                else:
                    # Guardar nueva sucursal en catálogo si corresponde
                    if dest_sel == OPCION_OTRO and guardar_en_lista and dest_final not in lista_suc:
                        try:
                            repo.agregar_sucursal(dest_final, dest_dir, dest_resp)
                            st.session_state.lista_suc = repo.cargar_sucursales()
                            lista_suc = st.session_state.lista_suc
                            st.success(f"Sucursal **{dest_final}** agregada al catálogo. ✔")
                        except Exception as e:
                            st.warning(f"La salida se guardará, pero no se pudo agregar al catálogo: {e}")

                    venc_str = (
                        venc_sel.strftime("%d-%m-%Y")
                        if hasattr(venc_sel, "strftime") else str(venc_sel)
                    )
                    st.session_state.carrito_sal.append({
                        "Código":                      cod_sal,
                        "Nombre del insumo":           nom_sal,
                        "Lote":                        lote_sel,
                        "Cantidad":                    cant_sal,
                        "Fecha de caducidad asociada": venc_str,
                        "Destino":                     dest_final,
                        "Observación":                 obs_sal,
                        "_venc_raw":                   venc_sel,
                    })
                    st.success(f"✔ **{nom_sal}** agregado a la salida.")
                    st.rerun()

    # ── Tabla del carrito de salidas ──────────────────────────────────────────
    st.divider()
    carrito_s = st.session_state.carrito_sal

    if not carrito_s:
        st.info("📤 Aún no hay ítems en esta salida. Agrega insumos con el formulario de arriba.")
    else:
        st.markdown(f"##### 📤 Ítems en esta salida ({len(carrito_s)})")

        cols_header = st.columns([2, 3, 2, 1, 2, 2, 2, 1])
        for h, label in zip(cols_header, ["Código", "Nombre", "Lote", "Cantidad",
                                           "Vencimiento", "Destino", "Obs.", ""]):
            h.markdown(f"**{label}**")

        for i, item in enumerate(carrito_s):
            c1, c2, c3, c4, c5, c6, c7, c8 = st.columns([2, 3, 2, 1, 2, 2, 2, 1])
            c1.write(item["Código"])
            c2.write(item["Nombre del insumo"])
            c3.write(item["Lote"])
            c4.write(int(item["Cantidad"]))
            c5.write(item["Fecha de caducidad asociada"])
            c6.write(item["Destino"])
            c7.write(item["Observación"] or "—")
            if c8.button("🗑️", key=f"del_sal_{i}", help="Eliminar este ítem"):
                st.session_state.carrito_sal.pop(i)
                st.rerun()

        # Resumen por insumo y destino
        st.divider()
        resumen_s = (
            pd.DataFrame(carrito_s)[["Nombre del insumo", "Destino", "Cantidad"]]
            .groupby(["Nombre del insumo", "Destino"])["Cantidad"]
            .sum()
            .reset_index()
            .rename(columns={"Cantidad": "Total a salir"})
        )
        col_res_s, col_btn_s = st.columns([3, 1])
        with col_res_s:
            with st.expander("📊 Ver resumen por insumo y destino", expanded=False):
                st.dataframe(resumen_s, use_container_width=True, hide_index=True)
        with col_btn_s:
            st.markdown("&nbsp;", unsafe_allow_html=True)
            if st.button("🗑️ Vaciar salida", key="btn_vaciar_sal"):
                st.session_state.carrito_sal = []
                st.rerun()

        # ── Confirmar y guardar todo (con validación anti-sobredespacho) ────
        st.divider()
        if st.button(
            f"✅ Confirmar salida ({len(carrito_s)} ítem{'s' if len(carrito_s) > 1 else ''})",
            type="primary", key="btn_confirmar_sal"
        ):
            # Validar stock real en el momento de confirmar
            df_ing_actual = repo.cargar_ingresos()
            df_sal_actual = repo.cargar_salidas()
            errores_stock = []

            # Agrupar carrito por (Código, Lote) para validar totales
            from collections import defaultdict
            totales_carrito = defaultdict(float)
            for item in carrito_s:
                totales_carrito[(item["Código"], item["Lote"])] += item["Cantidad"]

            for (cod, lote), cant_total in totales_carrito.items():
                lotes_reales = servicio.stock_por_lote(cod, df_ing_actual, df_sal_actual)
                if lotes_reales.empty:
                    errores_stock.append(
                        f"❌ **{cod} — Lote {lote}**: sin stock disponible en este momento.")
                    continue
                fila_lote = lotes_reales[
                    lotes_reales["Lote"].astype(str).str.upper() == str(lote).upper()]
                if fila_lote.empty:
                    errores_stock.append(
                        f"❌ **{cod} — Lote {lote}**: lote no encontrado al momento de confirmar.")
                    continue
                stock_real = float(fila_lote.iloc[0]["Disponible"])
                if cant_total > stock_real:
                    nom = carrito_s[0]["Nombre del insumo"]
                    errores_stock.append(
                        f"❌ **{nom} — Lote {lote}**: solicitado {int(cant_total)}, "
                        f"disponible real {int(stock_real)}.")

            if errores_stock:
                st.error("**No se puede confirmar la salida. Stock insuficiente:**")
                for e in errores_stock:
                    st.markdown(e)
                st.warning("Edita el carrito antes de confirmar.")
            else:
                ahora = datetime.now()
                nuevas_filas = []
                for item in carrito_s:
                    nuevas_filas.append({
                        "Fecha":                       ahora,
                        "Código":                      item["Código"],
                        "Nombre del insumo":           item["Nombre del insumo"],
                        "Lote":                        item["Lote"],
                        "Cantidad":                    item["Cantidad"],
                        "Fecha de caducidad asociada": item["_venc_raw"],
                        "Destino":                     item["Destino"],
                        "Observación":                 item["Observación"],
                    })
                ok, msg = guardar_y_reportes(
                    pd.concat([df_sal, pd.DataFrame(nuevas_filas)], ignore_index=True),
                    "Salidas"
                )
                if ok:
                    st.session_state.carrito_sal = []
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

    # ── Historial de salidas registradas ──────────────────────────────────────
    st.divider()
    with st.expander("📋 Ver historial de salidas registradas", expanded=False):
        if df_sal.empty:
            st.info("No hay salidas registradas aún.")
        else:
            df_hist_s = df_sal.copy()
            if "Fecha de caducidad asociada" in df_hist_s.columns:
                df_hist_s["Fecha de caducidad asociada"] = df_hist_s["Fecha de caducidad asociada"].apply(
                    lambda x: x.strftime("%d-%m-%Y") if hasattr(x, "strftime") else str(x))
            if "Fecha" in df_hist_s.columns:
                df_hist_s["Fecha"] = df_hist_s["Fecha"].apply(
                    lambda x: x.strftime("%d-%m-%Y %H:%M") if hasattr(x, "strftime") else str(x))
            df_hist_s = df_hist_s.iloc[::-1].reset_index(drop=True)
            st.dataframe(df_hist_s, use_container_width=True, hide_index=True)
            buf_hist_s = io.BytesIO()
            df_hist_s.to_excel(buf_hist_s, index=False)
            st.download_button(
                "⬇️ Descargar historial",
                buf_hist_s.getvalue(),
                "historial_salidas.xlsx",
                key="dl_hist_sal",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )


# ══════════════════════════════════════════════
# TAB 4 — CARGA INICIAL
# ══════════════════════════════════════════════
with tab_carga:
    st.subheader("Carga masiva de inventario inicial")
    st.markdown(
        "Sube un Excel o CSV con columnas: **Código**, **Cantidad** *(obligatorias)* + "
        "Lote, Fecha de caducidad (DD-MM-AAAA), Proveedor, Observación *(opcionales)*."
    )
    arch = st.file_uploader("Archivo", type=["xlsx", "xls", "csv"], key="up_inicial")
    if arch:
        try:
            df_raw            = repo.leer_archivo_externo(arch)
            st.info(f"{len(df_raw)} filas encontradas.")
            df_filas, errores = servicio.validar_e_importar_inicial(df_raw, df_insumos)
            if errores:
                with st.expander(f"⚠️ {len(errores)} filas con problemas (serán omitidas)"):
                    for e in errores: st.caption(f"→ {e}")
            if df_filas.empty:
                st.error("No hay filas válidas para importar.")
            else:
                st.success(f"{len(df_filas)} filas válidas listas para importar.")
                st.dataframe(
                    df_filas[["Código", "Nombre del insumo", "Lote", "Cantidad"]].head(10),
                    use_container_width=True, hide_index=True)
                if st.button("📥 Confirmar importación", type="primary", key="btn_carga"):
                    ok, msg = guardar_y_reportes(
                        pd.concat([df_ing, df_filas], ignore_index=True), "Ingresos")
                    if ok:
                        st.success(f"¡Importación exitosa! {len(df_filas)} registros agregados.")
                        st.rerun()
                    else:
                        st.error(msg)
        except Exception as e:
            st.error(f"Error al procesar el archivo: {e}")


# ══════════════════════════════════════════════
# TAB 6 — ALERTAS
# ══════════════════════════════════════════════
with tab_alertas:
    st.subheader("⚠️ Alertas de vencimiento")

    col_dias, _ = st.columns([1, 3])
    with col_dias:
        dias_alerta = st.selectbox(
            "Mostrar lotes que vencen en los próximos:",
            options=[15, 30, 60, 90],
            index=1,
            format_func=lambda x: f"{x} días",
            key="sel_dias_alerta"
        )

    df_venc = servicio.vencimientos_proximos(df_ing, df_sal, dias=dias_alerta)

    if df_venc.empty:
        st.success(f"✅ No hay lotes con vencimiento en los próximos {dias_alerta} días.")
    else:
        # Métricas resumen
        criticos = df_venc[df_venc["Estado"] == "🔴 Crítico"]
        urgentes = df_venc[df_venc["Estado"] == "🟠 Urgente"]
        proximos = df_venc[df_venc["Estado"] == "🟡 Próximo"]

        m1, m2, m3 = st.columns(3)
        m1.metric("🔴 Críticos (≤ 15 días)", len(criticos))
        m2.metric("🟠 Urgentes (≤ 30 días)", len(urgentes))
        m3.metric("🟡 Próximos a vencer",    len(proximos))

        st.divider()

        # Filtro por estado
        estados_opciones = ["Todos"] + sorted(df_venc["Estado"].unique().tolist())
        filtro_estado = st.radio(
            "Filtrar por estado",
            estados_opciones,
            horizontal=True,
            key="filtro_estado_venc"
        )
        df_mostrar = df_venc if filtro_estado == "Todos"             else df_venc[df_venc["Estado"] == filtro_estado]

        # Colorear filas según estado
        def _color_fila(row):
            if row["Estado"] == "🔴 Crítico":
                return ["background-color: #3d0000; color: #ff9999"] * len(row)
            if row["Estado"] == "🟠 Urgente":
                return ["background-color: #3d2000; color: #ffcc88"] * len(row)
            return ["background-color: #3d3300; color: #ffee99"] * len(row)

        st.dataframe(
            df_mostrar.style.apply(_color_fila, axis=1),
            use_container_width=True,
            hide_index=True
        )

        # Descarga
        buf_venc = io.BytesIO()
        df_venc.to_excel(buf_venc, index=False)
        st.download_button(
            "⬇️ Descargar reporte de vencimientos",
            buf_venc.getvalue(),
            f"vencimientos_proximos_{dias_alerta}dias.xlsx",
            key="dl_venc",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


# ══════════════════════════════════════════════
# TAB 5 — REPORTES
# ══════════════════════════════════════════════
with tab_reportes:
    st.subheader("Reportes de stock")
    r1, r2, r3, r4, r5 = st.tabs([
        "Stock por lote",
        "Stock por sucursal",
        "Sin lote",
        "📥 Historial de ingresos",
        "📤 Historial de salidas",
    ])

    with r1:
        df_l = servicio.construir_stock_por_lote(df_ing, df_sal)
        st.dataframe(df_l, use_container_width=True, hide_index=True)
        buf = io.BytesIO(); df_l.to_excel(buf, index=False)
        st.download_button("⬇️ Descargar", buf.getvalue(), "stock_por_lote.xlsx",
                           key="dl_lote",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    with r2:
        df_s = servicio.construir_stock_por_sucursal(df_ing, df_sal, df_insumos, lista_suc)
        st.dataframe(df_s, use_container_width=True, hide_index=True)
        buf2 = io.BytesIO(); df_s.to_excel(buf2, index=False)
        st.download_button("⬇️ Descargar", buf2.getvalue(), "stock_por_sucursal.xlsx",
                           key="dl_suc",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    with r3:
        df_sin = servicio.construir_stock_sin_lote(df_ing, df_sal, lista_suc)
        if df_sin is None or df_sin.empty:
            st.info("No hay insumos sin lote registrados.")
        else:
            st.dataframe(df_sin, use_container_width=True, hide_index=True)
            buf3 = io.BytesIO(); df_sin.to_excel(buf3, index=False)
            st.download_button("⬇️ Descargar", buf3.getvalue(), "stock_sin_lote.xlsx",
                               key="dl_sin",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ── Historial de ingresos ──────────────────────────────────────────────────
    with r4:
        st.markdown("#### 📥 Historial de ingresos")
        if df_ing.empty:
            st.info("No hay ingresos registrados aún.")
        else:
            # ── Filtros ───────────────────────────────────────────────────────
            with st.expander("🔎 Filtros", expanded=False):
                fc1, fc2, fc3 = st.columns(3)
                with fc1:
                    fi_insumos = ["Todos"] + sorted(df_ing["Nombre del insumo"].dropna().unique().tolist())
                    fi_sel_ins = st.selectbox("Insumo", fi_insumos, key="fi_ing_insumo")
                with fc2:
                    fi_provs = ["Todos"] + sorted(df_ing["Proveedor"].dropna().astype(str).unique().tolist())                         if "Proveedor" in df_ing.columns else ["Todos"]
                    fi_sel_prov = st.selectbox("Proveedor", fi_provs, key="fi_ing_prov")
                with fc3:
                    fi_lotes = ["Todos"] + sorted(df_ing["Lote"].dropna().unique().tolist())
                    fi_sel_lote = st.selectbox("Lote", fi_lotes, key="fi_ing_lote")
                fd1, fd2 = st.columns(2)
                with fd1:
                    fi_fecha_desde = st.date_input("Desde", value=None, key="fi_ing_desde")
                with fd2:
                    fi_fecha_hasta = st.date_input("Hasta", value=None, key="fi_ing_hasta")

            df_hi = df_ing.copy()
            if fi_sel_ins  != "Todos":
                df_hi = df_hi[df_hi["Nombre del insumo"] == fi_sel_ins]
            if fi_sel_prov != "Todos" and "Proveedor" in df_hi.columns:
                df_hi = df_hi[df_hi["Proveedor"].astype(str) == fi_sel_prov]
            if fi_sel_lote != "Todos":
                df_hi = df_hi[df_hi["Lote"] == fi_sel_lote]
            if fi_fecha_desde and "Fecha" in df_hi.columns:
                df_hi = df_hi[pd.to_datetime(df_hi["Fecha"], errors="coerce") >= pd.Timestamp(fi_fecha_desde)]
            if fi_fecha_hasta and "Fecha" in df_hi.columns:
                df_hi = df_hi[pd.to_datetime(df_hi["Fecha"], errors="coerce") <= pd.Timestamp(fi_fecha_hasta) + pd.Timedelta(days=1)]

            # Formatear fechas para visualización
            df_hi_view = df_hi.copy().iloc[::-1].reset_index(drop=True)
            if "Fecha" in df_hi_view.columns:
                df_hi_view["Fecha"] = df_hi_view["Fecha"].apply(
                    lambda x: x.strftime("%d-%m-%Y %H:%M") if hasattr(x, "strftime") else str(x))
            if "Fecha de caducidad" in df_hi_view.columns:
                df_hi_view["Fecha de caducidad"] = df_hi_view["Fecha de caducidad"].apply(
                    lambda x: x.strftime("%d-%m-%Y") if hasattr(x, "strftime") else str(x))

            # Métricas resumen
            hm1, hm2, hm3 = st.columns(3)
            hm1.metric("Total registros", len(df_hi_view))
            hm2.metric("Total unidades ingresadas", int(df_hi["Cantidad"].sum()) if "Cantidad" in df_hi.columns else 0)
            hm3.metric("Insumos distintos", df_hi["Nombre del insumo"].nunique() if "Nombre del insumo" in df_hi.columns else 0)

            st.dataframe(df_hi_view, use_container_width=True, hide_index=True)

            buf_hi = io.BytesIO()
            df_hi_view.to_excel(buf_hi, index=False)
            st.download_button(
                "⬇️ Descargar historial filtrado",
                buf_hi.getvalue(),
                "historial_ingresos.xlsx",
                key="dl_rep_ing",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    # ── Historial de salidas ───────────────────────────────────────────────────
    with r5:
        st.markdown("#### 📤 Historial de salidas")
        if df_sal.empty:
            st.info("No hay salidas registradas aún.")
        else:
            # ── Filtros ───────────────────────────────────────────────────────
            with st.expander("🔎 Filtros", expanded=False):
                fs1, fs2, fs3 = st.columns(3)
                with fs1:
                    fs_insumos = ["Todos"] + sorted(df_sal["Nombre del insumo"].dropna().unique().tolist())
                    fs_sel_ins = st.selectbox("Insumo", fs_insumos, key="fs_sal_insumo")
                with fs2:
                    fs_destinos = ["Todos"] + sorted(df_sal["Destino"].dropna().astype(str).unique().tolist())                         if "Destino" in df_sal.columns else ["Todos"]
                    fs_sel_dest = st.selectbox("Destino", fs_destinos, key="fs_sal_dest")
                with fs3:
                    fs_lotes = ["Todos"] + sorted(df_sal["Lote"].dropna().unique().tolist())
                    fs_sel_lote = st.selectbox("Lote", fs_lotes, key="fs_sal_lote")
                fsd1, fsd2 = st.columns(2)
                with fsd1:
                    fs_fecha_desde = st.date_input("Desde", value=None, key="fs_sal_desde")
                with fsd2:
                    fs_fecha_hasta = st.date_input("Hasta", value=None, key="fs_sal_hasta")

            df_hs = df_sal.copy()
            if fs_sel_ins  != "Todos":
                df_hs = df_hs[df_hs["Nombre del insumo"] == fs_sel_ins]
            if fs_sel_dest != "Todos" and "Destino" in df_hs.columns:
                df_hs = df_hs[df_hs["Destino"].astype(str) == fs_sel_dest]
            if fs_sel_lote != "Todos":
                df_hs = df_hs[df_hs["Lote"] == fs_sel_lote]
            if fs_fecha_desde and "Fecha" in df_hs.columns:
                df_hs = df_hs[pd.to_datetime(df_hs["Fecha"], errors="coerce") >= pd.Timestamp(fs_fecha_desde)]
            if fs_fecha_hasta and "Fecha" in df_hs.columns:
                df_hs = df_hs[pd.to_datetime(df_hs["Fecha"], errors="coerce") <= pd.Timestamp(fs_fecha_hasta) + pd.Timedelta(days=1)]

            # Formatear fechas para visualización
            df_hs_view = df_hs.copy().iloc[::-1].reset_index(drop=True)
            if "Fecha" in df_hs_view.columns:
                df_hs_view["Fecha"] = df_hs_view["Fecha"].apply(
                    lambda x: x.strftime("%d-%m-%Y %H:%M") if hasattr(x, "strftime") else str(x))
            if "Fecha de caducidad asociada" in df_hs_view.columns:
                df_hs_view["Fecha de caducidad asociada"] = df_hs_view["Fecha de caducidad asociada"].apply(
                    lambda x: x.strftime("%d-%m-%Y") if hasattr(x, "strftime") else str(x))

            # Métricas resumen
            sm1, sm2, sm3 = st.columns(3)
            sm1.metric("Total registros", len(df_hs_view))
            sm2.metric("Total unidades despachadas", int(df_hs["Cantidad"].sum()) if "Cantidad" in df_hs.columns else 0)
            sm3.metric("Destinos distintos", df_hs["Destino"].nunique() if "Destino" in df_hs.columns else 0)

            st.dataframe(df_hs_view, use_container_width=True, hide_index=True)

            buf_hs = io.BytesIO()
            df_hs_view.to_excel(buf_hs, index=False)
            st.download_button(
                "⬇️ Descargar historial filtrado",
                buf_hs.getvalue(),
                "historial_salidas.xlsx",
                key="dl_rep_sal",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
