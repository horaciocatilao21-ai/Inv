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
 
    def cargar_insumos(self)    -> pd.DataFrame: return self.cargar_hoja("Insumos")
    def cargar_ingresos(self)   -> pd.DataFrame: return self.cargar_hoja("Ingresos")
    def cargar_salidas(self)    -> pd.DataFrame: return self.cargar_hoja("Salidas")
 
    def cargar_sucursales(self) -> list:
        df = pd.read_excel(self.ruta, sheet_name="Sucursales", skiprows=HEADER_ROW)
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
 
    def stock_por_lote(self, codigo, df_ing, df_sal):
        codigo = codigo.strip().upper()
        ing    = df_ing[df_ing["Código"] == codigo].copy()
        sal    = df_sal[df_sal["Código"] == codigo].copy()
        if ing.empty:
            return pd.DataFrame()
        resumen_ing = ing.groupby(["Lote", "Fecha de caducidad"])["Cantidad"].sum().reset_index()
        if not sal.empty:
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
                "Fecha": datetime.now(), "Código": cod,
                "Nombre del insumo": nombre_por_codigo[cod],
                "Lote": lote, "Cantidad": cant,
                "Fecha de caducidad": venc, "Proveedor": proveedor, "Observación": obs,
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
 
# ── Pestañas ──────────────────────────────────────────────────────────────────
tab_consulta, tab_ingreso, tab_salida, tab_carga, tab_reportes = st.tabs([
    "🔍 Consultar Stock",
    "➕ Registrar Ingreso",
    "➖ Registrar Salida",
    "📂 Carga Inicial",
    "📊 Reportes",
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
# TAB 2 — REGISTRAR INGRESO
# ══════════════════════════════════════════════
with tab_ingreso:
    st.subheader("Registrar ingreso de insumo")
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
    if st.button("✅ Guardar ingreso", type="primary", key="btn_ing"):
        if cant_ing <= 0:
            st.error("La cantidad debe ser mayor a 0.")
        else:
            fila = {
                "Fecha": datetime.now(), "Código": cod_ing, "Nombre del insumo": nom_ing,
                "Lote": lote_ing, "Cantidad": cant_ing, "Fecha de caducidad": venc_ing,
                "Proveedor": prov_ing, "Observación": obs_ing,
            }
            ok, msg = guardar_y_reportes(
                pd.concat([df_ing, pd.DataFrame([fila])], ignore_index=True), "Ingresos")
            if ok:  st.success(msg); st.rerun()
            else:   st.error(msg)
 
 
# ══════════════════════════════════════════════
# TAB 3 — REGISTRAR SALIDA
# ══════════════════════════════════════════════
with tab_salida:
    st.subheader("Registrar salida de insumo")
    opts_sal  = {f"{r['Código']} — {r['Nombre del insumo']}": r["Código"]
                 for _, r in df_insumos.iterrows()}
    sel_sal   = st.selectbox("Insumo", list(opts_sal.keys()), key="sal_insumo")
    cod_sal   = opts_sal[sel_sal]
    nom_sal   = df_insumos[df_insumos["Código"] == cod_sal].iloc[0]["Nombre del insumo"]
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
        sel_lote  = st.selectbox("Lote disponible", list(opts_lote.keys()), key="sal_lote")
        idx_lote  = opts_lote[sel_lote]
        stock_max = float(lotes_sal.loc[idx_lote, "Disponible"])
        lote_sel  = lotes_sal.loc[idx_lote, "Lote"]
        venc_sel  = lotes_sal.loc[idx_lote, "Fecha de caducidad"]
        col_c, col_d = st.columns(2)
        with col_c:
            dest_sal = st.selectbox("Destino", lista_suc, key="sal_destino")
            cant_sal = st.number_input(
                f"Cantidad (máx {int(stock_max)})",
                min_value=0.0, max_value=stock_max, step=1.0, key="sal_cant")
        with col_d:
            obs_sal = st.text_area("Observación", key="sal_obs").strip()
        if st.button("✅ Guardar salida", type="primary", key="btn_sal"):
            if cant_sal <= 0:
                st.error("La cantidad debe ser mayor a 0.")
            else:
                fila = {
                    "Fecha": datetime.now(), "Código": cod_sal, "Nombre del insumo": nom_sal,
                    "Lote": lote_sel, "Cantidad": cant_sal,
                    "Fecha de caducidad asociada": venc_sel,
                    "Destino": dest_sal, "Observación": obs_sal,
                }
                ok, msg = guardar_y_reportes(
                    pd.concat([df_sal, pd.DataFrame([fila])], ignore_index=True), "Salidas")
                if ok:  st.success(msg); st.rerun()
                else:   st.error(msg)
 
 
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
# TAB 5 — REPORTES
# ══════════════════════════════════════════════
with tab_reportes:
    st.subheader("Reportes de stock")
    r1, r2, r3 = st.tabs(["Stock por lote", "Stock por sucursal", "Sin lote"])
 
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
