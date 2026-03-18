"""
=============================================================
  LMN Bicentenario — Sistema de Inventario (Streamlit)
=============================================================
Cambios respecto a la versión Colab:
  - Eliminada dependencia de google.colab
  - UI completamente reescrita en Streamlit
  - Ruta del Excel configurable desde la barra lateral
  - Estado de sesión con st.session_state
  - Todas las capas de datos y lógica (InventarioRepo,
    StockService) se mantienen idénticas
"""

import shutil
import io
import pandas as pd
import streamlit as st
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIGURACIÓN GLOBAL
# ─────────────────────────────────────────────
HEADER_ROW = 2
DIR_BACKUP = './Backups_Inventario'


# ═════════════════════════════════════════════
# CAPA 1: ACCESO A DATOS (InventarioRepo)
# ═════════════════════════════════════════════

class InventarioRepo:
    def __init__(self, ruta: str):
        self.ruta = ruta

    def cargar_hoja(self, nombre_hoja: str) -> pd.DataFrame:
        df = pd.read_excel(self.ruta, sheet_name=nombre_hoja, skiprows=HEADER_ROW)
        return self._limpiar(df)

    def cargar_insumos(self) -> pd.DataFrame:
        return self.cargar_hoja('Insumos')

    def cargar_sucursales(self) -> list:
        df = pd.read_excel(self.ruta, sheet_name='Sucursales', skiprows=HEADER_ROW)
        return df['Sucursal'].dropna().unique().tolist()

    def cargar_ingresos(self) -> pd.DataFrame:
        return self.cargar_hoja('Ingresos')

    def cargar_salidas(self) -> pd.DataFrame:
        return self.cargar_hoja('Salidas')

    def guardar_transaccion(self, df: pd.DataFrame, hoja: str):
        with pd.ExcelWriter(
            self.ruta, mode='a', engine='openpyxl', if_sheet_exists='replace'
        ) as writer:
            df.to_excel(writer, sheet_name=hoja, index=False, startrow=HEADER_ROW)

    def guardar_reportes(self, df_stock_lote, df_stock_suc, df_sin_lote):
        with pd.ExcelWriter(
            self.ruta, mode='a', engine='openpyxl', if_sheet_exists='replace'
        ) as writer:
            df_stock_suc.to_excel(
                writer, sheet_name='Stock por Sucursal', index=False, startrow=HEADER_ROW)
            df_stock_lote.to_excel(
                writer, sheet_name='Stock por Lote', index=False, startrow=HEADER_ROW)
            if df_sin_lote is not None and not df_sin_lote.empty:
                df_sin_lote.to_excel(
                    writer, sheet_name='Stock sin Lote ni Vencimiento',
                    index=False, startrow=HEADER_ROW)

    def hacer_backup(self) -> str:
        Path(DIR_BACKUP).mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_original = Path(self.ruta).stem
        destino = f"{DIR_BACKUP}/{nombre_original}_backup_{ts}.xlsx"
        shutil.copy2(self.ruta, destino)
        return destino

    def leer_archivo_externo(self, archivo) -> pd.DataFrame:
        """Lee un UploadedFile de Streamlit (xlsx, xls, csv)."""
        nombre = archivo.name.lower()
        if nombre.endswith('.csv'):
            df = pd.read_csv(archivo, dtype=str)
        else:
            df = pd.read_excel(archivo, dtype=str)
        df.columns = df.columns.str.strip()
        return df

    @staticmethod
    def _limpiar(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        for col in ('Código', 'Lote'):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.upper()
        if 'Nombre del insumo' in df.columns:
            df['Nombre del insumo'] = df['Nombre del insumo'].astype(str).str.strip()
        if 'Cantidad' in df.columns:
            df['Cantidad'] = pd.to_numeric(df['Cantidad'], errors='coerce').fillna(0)
        return df


# ═════════════════════════════════════════════
# CAPA 2: LÓGICA DE NEGOCIO (StockService)
# ═════════════════════════════════════════════

class StockService:
    def __init__(self, repo: InventarioRepo):
        self.repo = repo

    def stock_por_lote(self, codigo: str, df_ing: pd.DataFrame, df_sal: pd.DataFrame) -> pd.DataFrame:
        codigo = codigo.strip().upper()
        ing = df_ing[df_ing['Código'] == codigo].copy()
        sal = df_sal[df_sal['Código'] == codigo].copy()
        if ing.empty:
            return pd.DataFrame()
        resumen_ing = (
            ing.groupby(['Lote', 'Fecha de caducidad'])['Cantidad'].sum().reset_index()
        )
        if not sal.empty:
            resumen_sal = (
                sal.groupby(['Lote', 'Fecha de caducidad asociada'])['Cantidad']
                .sum().reset_index()
            )
            resumen_sal.columns = ['Lote', 'Fecha de caducidad', 'Cant_Salida']
            df_lotes = resumen_ing.merge(
                resumen_sal, on=['Lote', 'Fecha de caducidad'], how='left'
            ).fillna(0)
            df_lotes['Disponible'] = df_lotes['Cantidad'] - df_lotes['Cant_Salida']
        else:
            df_lotes = resumen_ing.copy()
            df_lotes['Disponible'] = df_lotes['Cantidad']
        return df_lotes[df_lotes['Disponible'] > 0].reset_index(drop=True)

    def construir_stock_por_lote(self, df_ing, df_sal):
        ing_ag = df_ing.groupby(
            ['Código', 'Nombre del insumo', 'Lote', 'Fecha de caducidad']
        )['Cantidad'].sum().reset_index()
        sal_ag = df_sal.groupby(
            ['Código', 'Lote', 'Fecha de caducidad asociada']
        )['Cantidad'].sum().reset_index()
        sal_ag.columns = ['Código', 'Lote', 'Fecha de caducidad', 'Cant_Salida']
        df = ing_ag.merge(sal_ag, on=['Código', 'Lote', 'Fecha de caducidad'], how='left').fillna(0)
        df['Stock disponible'] = df['Cantidad'] - df['Cant_Salida']
        df = df.rename(columns={'Cantidad': 'Ingresos', 'Cant_Salida': 'Salidas'})
        return df[['Código', 'Nombre del insumo', 'Lote', 'Fecha de caducidad',
                   'Ingresos', 'Salidas', 'Stock disponible']]

    def construir_stock_por_sucursal(self, df_ing, df_sal, df_insumos, lista_suc):
        todos_los_insumos = df_insumos['Nombre del insumo'].unique()
        total_ing = (
            df_ing.groupby('Nombre del insumo')['Cantidad']
            .sum().reindex(todos_los_insumos, fill_value=0)
        )
        if not df_sal.empty and 'Destino' in df_sal.columns:
            matriz = (
                df_sal.groupby(['Nombre del insumo', 'Destino'])['Cantidad']
                .sum().unstack(fill_value=0).reindex(todos_los_insumos, fill_value=0)
            )
        else:
            matriz = pd.DataFrame(0, index=todos_los_insumos, columns=[])
        reporte = pd.DataFrame(index=todos_los_insumos)
        reporte.index.name = 'Nombre del insumo'
        reporte['Usado en BC'] = matriz['Bodega Central'] if 'Bodega Central' in matriz.columns else 0
        sucursales_externas = [s for s in lista_suc if s != 'Bodega Central']
        for suc in sucursales_externas:
            reporte[f'Enviado a {suc}'] = matriz[suc] if suc in matriz.columns else 0
        destinos_extra = [
            d for d in matriz.columns
            if d != 'Bodega Central' and d not in sucursales_externas
        ]
        for dest in destinos_extra:
            reporte[f'Enviado a {dest}'] = matriz[dest]
        cols_enviado = [c for c in reporte.columns if c.startswith('Enviado a')]
        total_enviado = reporte[cols_enviado].sum(axis=1) if cols_enviado else 0
        reporte['Stock Disponible (Bodega Central)'] = (
            total_ing - reporte['Usado en BC'] - total_enviado
        )
        reporte['STOCK TOTAL'] = reporte['Stock Disponible (Bodega Central)'] + total_enviado
        cols_orden = (
            ['Stock Disponible (Bodega Central)', 'Usado en BC']
            + cols_enviado + ['STOCK TOTAL']
        )
        return reporte[cols_orden].fillna(0).reset_index()

    def construir_stock_sin_lote(self, df_ing, df_sal, lista_suc):
        ing_sin = df_ing[df_ing['Lote'].isin(['N/A', ''])].copy()
        sal_sin = df_sal[df_sal['Lote'].isin(['N/A', ''])].copy()
        if ing_sin.empty:
            return None
        total_ing = ing_sin.groupby('Nombre del insumo')['Cantidad'].sum()
        total_sal = sal_sin.groupby('Nombre del insumo')['Cantidad'].sum()
        stock_bc = (total_ing - total_sal).fillna(total_ing)
        matriz = sal_sin.groupby(
            ['Nombre del insumo', 'Destino'])['Cantidad'].sum().unstack(fill_value=0)
        reporte = pd.DataFrame(index=ing_sin['Nombre del insumo'].unique())
        reporte['Stock Disponible (Bodega Central)'] = stock_bc
        for s in lista_suc:
            if s != 'Bodega Central':
                reporte[f'Enviado a {s}'] = matriz[s] if s in matriz.columns else 0
        reporte['STOCK TOTAL SIN LOTE'] = reporte.sum(axis=1)
        return reporte.fillna(0).reset_index().rename(columns={'index': 'Nombre del insumo'})

    def validar_e_importar_inicial(self, df_raw, df_insumos):
        COLS_REQUERIDAS = {'Código', 'Cantidad'}
        faltantes = COLS_REQUERIDAS - set(df_raw.columns)
        if faltantes:
            raise ValueError(
                f"Faltan columnas obligatorias: {faltantes}. "
                f"Detectadas: {list(df_raw.columns)}"
            )
        codigos_validos = set(df_insumos['Código'].astype(str).str.strip().str.upper())
        nombre_por_codigo = df_insumos.set_index('Código')['Nombre del insumo'].to_dict()
        filas_ok, errores = [], []
        for i, row in df_raw.iterrows():
            num_fila = i + 1
            cod = str(row.get('Código', '')).strip().upper()
            if not cod or cod == 'NAN':
                errores.append(f"Fila {num_fila}: código vacío — omitida.")
                continue
            if cod not in codigos_validos:
                errores.append(f"Fila {num_fila}: código '{cod}' no existe — omitida.")
                continue
            try:
                cant = float(str(row.get('Cantidad', '')).replace(',', '.'))
                if cant <= 0:
                    raise ValueError
            except ValueError:
                errores.append(f"Fila {num_fila} ({cod}): cantidad inválida — omitida.")
                continue
            lote_raw = str(row.get('Lote', '')).strip().upper()
            lote = lote_raw if lote_raw and lote_raw != 'NAN' else 'N/A'
            venc_raw = str(row.get('Fecha de caducidad', '')).strip()
            if venc_raw and venc_raw.upper() != 'NAN':
                try:
                    venc = datetime.strptime(venc_raw, "%d-%m-%Y")
                except ValueError:
                    errores.append(
                        f"Fila {num_fila} ({cod}): fecha '{venc_raw}' inválida — se usará 'S/V'.")
                    venc = 'S/V'
            else:
                venc = 'S/V'
            proveedor = str(row.get('Proveedor', '')).strip()
            proveedor = proveedor if proveedor and proveedor.upper() != 'NAN' else ''
            obs_raw = str(row.get('Observación', '')).strip()
            obs = obs_raw if obs_raw and obs_raw.upper() != 'NAN' else 'Inventario inicial'
            filas_ok.append({
                'Fecha': datetime.now(), 'Código': cod,
                'Nombre del insumo': nombre_por_codigo[cod],
                'Lote': lote, 'Cantidad': cant,
                'Fecha de caducidad': venc, 'Proveedor': proveedor, 'Observación': obs,
            })
        return pd.DataFrame(filas_ok), errores


# ═════════════════════════════════════════════
# HELPERS DE SESIÓN
# ═════════════════════════════════════════════

def init_session(ruta: str):
    """Carga o recarga todos los datos en st.session_state."""
    repo = InventarioRepo(ruta)
    st.session_state.repo      = repo
    st.session_state.servicio  = StockService(repo)
    st.session_state.df_insumos = repo.cargar_insumos()
    st.session_state.lista_suc  = repo.cargar_sucursales()
    st.session_state.df_ing     = repo.cargar_ingresos()
    st.session_state.df_sal     = repo.cargar_salidas()
    st.session_state.ruta_excel = ruta
    st.session_state.cargado    = True


def guardar_y_reportes(df_nuevo: pd.DataFrame, hoja: str):
    """Backup → guarda transacción → recarga → genera reportes. Devuelve (ok, mensaje)."""
    repo     = st.session_state.repo
    servicio = st.session_state.servicio

    try:
        backup = repo.hacer_backup()
        st.info(f"Backup creado: `{backup}`")
    except Exception as e:
        st.warning(f"No se pudo crear backup: {e}")

    try:
        repo.guardar_transaccion(df_nuevo, hoja)
    except Exception as e:
        return False, f"Error al guardar '{hoja}': {e}"

    # Recargar datos frescos
    df_ing_nuevo = repo.cargar_ingresos()
    df_sal_nuevo = repo.cargar_salidas()
    st.session_state.df_ing = df_ing_nuevo
    st.session_state.df_sal = df_sal_nuevo

    # Reportes
    try:
        df_lote = servicio.construir_stock_por_lote(df_ing_nuevo, df_sal_nuevo)
        df_suc  = servicio.construir_stock_por_sucursal(
            df_ing_nuevo, df_sal_nuevo,
            st.session_state.df_insumos, st.session_state.lista_suc
        )
        df_sin  = servicio.construir_stock_sin_lote(
            df_ing_nuevo, df_sal_nuevo, st.session_state.lista_suc
        )
        repo.guardar_reportes(df_lote, df_suc, df_sin)
    except Exception as e:
        return False, f"Transacción guardada, pero error en reportes: {e}"

    return True, "¡Excel actualizado con éxito en todas sus hojas!"


# ═════════════════════════════════════════════
# INTERFAZ STREAMLIT
# ═════════════════════════════════════════════

st.set_page_config(
    page_title="LMN Bicentenario — Inventario",
    page_icon="📦",
    layout="wide"
)

st.title("📦 LMN Bicentenario — Sistema de Inventario")

# ── Barra lateral: configuración ─────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Configuración")
    ruta_input = st.text_input(
        "Ruta del archivo Excel",
        value=st.session_state.get("ruta_excel", ""),
        placeholder="/ruta/al/Inventario.xlsx"
    )
    if st.button("Cargar / Recargar datos", type="primary"):
        if ruta_input and Path(ruta_input).exists():
            try:
                init_session(ruta_input)
                st.success("Datos cargados correctamente.")
            except Exception as e:
                st.error(f"Error al cargar: {e}")
        else:
            st.error("Ruta no válida o archivo no encontrado.")

    if st.session_state.get("cargado"):
        st.divider()
        st.caption(f"Archivo activo:")
        st.caption(f"`{st.session_state.ruta_excel}`")

# ── Guardia: datos no cargados ────────────────────────────────────────────────
if not st.session_state.get("cargado"):
    st.info("👈 Ingresa la ruta del archivo Excel en la barra lateral y presiona **Cargar / Recargar datos**.")
    st.stop()

# Atajos a objetos de sesión
repo     = st.session_state.repo
servicio = st.session_state.servicio
df_insumos = st.session_state.df_insumos
lista_suc  = st.session_state.lista_suc
df_ing     = st.session_state.df_ing
df_sal     = st.session_state.df_sal

# ── Pestañas principales ──────────────────────────────────────────────────────
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
            (df_insumos['Código'] == termino) |
            (df_insumos['Nombre del insumo'].str.upper().str.contains(termino, na=False))
        )
        coincidencias = df_insumos[mask]

        if coincidencias.empty:
            st.warning("No se encontró ningún insumo con ese código o nombre.")
        else:
            opciones = {
                f"{r['Código']} — {r['Nombre del insumo']}": r['Código']
                for _, r in coincidencias.iterrows()
            }
            seleccion = st.selectbox("Insumo encontrado", list(opciones.keys()))
            cod_sel = opciones[seleccion]
            nom_sel = coincidencias[coincidencias['Código'] == cod_sel].iloc[0]['Nombre del insumo']

            lotes = servicio.stock_por_lote(cod_sel, df_ing, df_sal)

            if lotes.empty:
                st.warning(f"**{nom_sel}** — Sin stock disponible.")
            else:
                # Formatear fecha para mostrar
                lotes_display = lotes.copy()
                lotes_display['Fecha de caducidad'] = lotes_display['Fecha de caducidad'].apply(
                    lambda x: x.strftime('%d-%m-%Y') if hasattr(x, 'strftime') else str(x)
                )
                lotes_display['Disponible'] = lotes_display['Disponible'].astype(int)

                total = int(lotes['Disponible'].sum())
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown(f"**{nom_sel}** `{cod_sel}`")
                with col2:
                    st.metric("Total disponible", total)

                st.dataframe(
                    lotes_display[['Lote', 'Fecha de caducidad', 'Disponible']],
                    use_container_width=True, hide_index=True
                )


# ══════════════════════════════════════════════
# TAB 2 — REGISTRAR INGRESO
# ══════════════════════════════════════════════
with tab_ingreso:
    st.subheader("Registrar ingreso de insumo")

    opciones_ing = {
        f"{r['Código']} — {r['Nombre del insumo']}": r['Código']
        for _, r in df_insumos.iterrows()
    }
    sel_ing = st.selectbox("Insumo", list(opciones_ing.keys()), key="ing_insumo")
    cod_ing = opciones_ing[sel_ing]
    nom_ing = df_insumos[df_insumos['Código'] == cod_ing].iloc[0]['Nombre del insumo']

    col_a, col_b = st.columns(2)
    with col_a:
        lote_ing = st.text_input("Lote (dejar vacío si no tiene)", key="ing_lote").strip().upper() or "N/A"
        cant_ing = st.number_input("Cantidad", min_value=0.0, step=1.0, key="ing_cant")
        prov_ing = st.text_input("Proveedor", key="ing_prov").strip()
    with col_b:
        fecha_venc = st.date_input("Fecha de vencimiento", value=None, key="ing_venc")
        venc_ing = datetime.combine(fecha_venc, datetime.min.time()) if fecha_venc else 'S/V'
        obs_ing  = st.text_area("Observación", key="ing_obs").strip()

    if st.button("✅ Guardar ingreso", type="primary", key="btn_ing"):
        if cant_ing <= 0:
            st.error("La cantidad debe ser mayor a 0.")
        else:
            nueva_fila = {
                'Fecha': datetime.now(), 'Código': cod_ing,
                'Nombre del insumo': nom_ing, 'Lote': lote_ing,
                'Cantidad': cant_ing, 'Fecha de caducidad': venc_ing,
                'Proveedor': prov_ing, 'Observación': obs_ing,
            }
            df_actualizado = pd.concat(
                [df_ing, pd.DataFrame([nueva_fila])], ignore_index=True
            )
            ok, msg = guardar_y_reportes(df_actualizado, 'Ingresos')
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)


# ══════════════════════════════════════════════
# TAB 3 — REGISTRAR SALIDA
# ══════════════════════════════════════════════
with tab_salida:
    st.subheader("Registrar salida de insumo")

    opciones_sal = {
        f"{r['Código']} — {r['Nombre del insumo']}": r['Código']
        for _, r in df_insumos.iterrows()
    }
    sel_sal = st.selectbox("Insumo", list(opciones_sal.keys()), key="sal_insumo")
    cod_sal = opciones_sal[sel_sal]
    nom_sal = df_insumos[df_insumos['Código'] == cod_sal].iloc[0]['Nombre del insumo']

    lotes_sal = servicio.stock_por_lote(cod_sal, df_ing, df_sal)

    if lotes_sal.empty:
        st.warning("Este insumo no tiene stock disponible.")
    else:
        lotes_sal_display = lotes_sal.copy()
        lotes_sal_display['Fecha de caducidad'] = lotes_sal_display['Fecha de caducidad'].apply(
            lambda x: x.strftime('%d-%m-%Y') if hasattr(x, 'strftime') else str(x)
        )
        lotes_sal_display['Disponible'] = lotes_sal_display['Disponible'].astype(int)

        opciones_lote = {
            f"Lote {r['Lote']} | Vence {lotes_sal_display.loc[i, 'Fecha de caducidad']} | Stock {int(r['Disponible'])}": i
            for i, r in lotes_sal.iterrows()
        }
        sel_lote = st.selectbox("Lote disponible", list(opciones_lote.keys()), key="sal_lote")
        idx_lote = opciones_lote[sel_lote]
        stock_max = float(lotes_sal.loc[idx_lote, 'Disponible'])
        lote_sel  = lotes_sal.loc[idx_lote, 'Lote']
        venc_sel  = lotes_sal.loc[idx_lote, 'Fecha de caducidad']

        col_c, col_d = st.columns(2)
        with col_c:
            dest_sal = st.selectbox("Destino", lista_suc, key="sal_destino")
            cant_sal = st.number_input(
                f"Cantidad (máx {int(stock_max)})",
                min_value=0.0, max_value=stock_max, step=1.0, key="sal_cant"
            )
        with col_d:
            obs_sal = st.text_area("Observación", key="sal_obs").strip()

        if st.button("✅ Guardar salida", type="primary", key="btn_sal"):
            if cant_sal <= 0:
                st.error("La cantidad debe ser mayor a 0.")
            else:
                nueva_fila = {
                    'Fecha': datetime.now(), 'Código': cod_sal,
                    'Nombre del insumo': nom_sal, 'Lote': lote_sel,
                    'Cantidad': cant_sal, 'Fecha de caducidad asociada': venc_sel,
                    'Destino': dest_sal, 'Observación': obs_sal,
                }
                df_actualizado = pd.concat(
                    [df_sal, pd.DataFrame([nueva_fila])], ignore_index=True
                )
                ok, msg = guardar_y_reportes(df_actualizado, 'Salidas')
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)


# ══════════════════════════════════════════════
# TAB 4 — CARGA INICIAL
# ══════════════════════════════════════════════
with tab_carga:
    st.subheader("Carga masiva de inventario inicial")
    st.markdown("""
    Sube un archivo Excel o CSV con al menos las columnas:
    - **Código** *(obligatorio)*
    - **Cantidad** *(obligatorio)*
    - Lote, Fecha de caducidad (DD-MM-AAAA), Proveedor, Observación *(opcionales)*
    """)

    archivo = st.file_uploader("Selecciona el archivo", type=['xlsx', 'xls', 'csv'])

    if archivo:
        try:
            df_raw = repo.leer_archivo_externo(archivo)
            st.info(f"{len(df_raw)} filas encontradas en el archivo.")

            df_filas, errores = servicio.validar_e_importar_inicial(df_raw, df_insumos)

            if errores:
                with st.expander(f"⚠️ {len(errores)} filas con problemas (serán omitidas)"):
                    for err in errores:
                        st.caption(f"→ {err}")

            if df_filas.empty:
                st.error("No hay filas válidas para importar.")
            else:
                st.success(f"{len(df_filas)} filas válidas listas para importar.")
                st.dataframe(
                    df_filas[['Código', 'Nombre del insumo', 'Lote', 'Cantidad']].head(10),
                    use_container_width=True, hide_index=True
                )

                if st.button("📥 Confirmar importación", type="primary", key="btn_carga"):
                    df_ing_actualizado = pd.concat(
                        [df_ing, df_filas], ignore_index=True
                    )
                    ok, msg = guardar_y_reportes(df_ing_actualizado, 'Ingresos')
                    if ok:
                        st.success(f"¡Importación exitosa! {len(df_filas)} registros agregados.")
                        st.rerun()
                    else:
                        st.error(msg)

        except ValueError as e:
            st.error(f"Error de estructura: {e}")
        except Exception as e:
            st.error(f"Error al procesar archivo: {e}")


# ══════════════════════════════════════════════
# TAB 5 — REPORTES
# ══════════════════════════════════════════════
with tab_reportes:
    st.subheader("Reportes de stock")

    rep1, rep2, rep3 = st.tabs(["Stock por lote", "Stock por sucursal", "Sin lote"])

    with rep1:
        df_lote = servicio.construir_stock_por_lote(df_ing, df_sal)
        st.dataframe(df_lote, use_container_width=True, hide_index=True)

        buf = io.BytesIO()
        df_lote.to_excel(buf, index=False)
        st.download_button(
            "⬇️ Descargar Excel",
            data=buf.getvalue(),
            file_name="stock_por_lote.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    with rep2:
        df_suc = servicio.construir_stock_por_sucursal(df_ing, df_sal, df_insumos, lista_suc)
        st.dataframe(df_suc, use_container_width=True, hide_index=True)

        buf2 = io.BytesIO()
        df_suc.to_excel(buf2, index=False)
        st.download_button(
            "⬇️ Descargar Excel",
            data=buf2.getvalue(),
            file_name="stock_por_sucursal.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_suc"
        )

    with rep3:
        df_sin = servicio.construir_stock_sin_lote(df_ing, df_sal, lista_suc)
        if df_sin is None or df_sin.empty:
            st.info("No hay insumos sin lote registrados.")
        else:
            st.dataframe(df_sin, use_container_width=True, hide_index=True)

            buf3 = io.BytesIO()
            df_sin.to_excel(buf3, index=False)
            st.download_button(
                "⬇️ Descargar Excel",
                data=buf3.getvalue(),
                file_name="stock_sin_lote.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_sin"
            )
