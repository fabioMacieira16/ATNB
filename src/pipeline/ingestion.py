"""
Camada de Ingestão (Bronze Layer)
----------------------------------
Responsável por ler os arquivos brutos da pasta data/ e retornar
DataFrames pandas com as colunas e tipos originais preservados.
Arquivos suportados:
  - acidentes2023.csv          → fatos de acidentes (2018-2025)
    - datatran2026.csv           → fatos de acidentes (layout PRF/Datatran)
  - Vitimas_DadosAbertos.csv   → vítimas por acidente
  - TipoVeiculo_DadosAbertos.csv → veículos envolvidos
  - Localidade_20260312.csv    → dimensão de localidade (município/UF)
  - Volume_trafego_mensal.csv  → volume de tráfego mensal (Fortaleza)
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Colunas que serão lidas de cada arquivo (projeção para economizar memória)
_ACIDENTES_COLS = [
    "num_acidente", "chv_localidade", "data_acidente", "uf_acidente",
    "ano_acidente", "mes_acidente", "dia_semana", "fase_dia", "hora_acidente",
    "tp_acidente", "cond_meteorologica", "cond_pista", "tp_rodovia",
    "tp_pavimento", "lim_velocidade", "tp_pista", "bairro_acidente",
    "end_acidente", "latitude_acidente", "longitude_acidente",
    "qtde_acidente", "qtde_acid_com_obitos", "qtde_envolvidos",
    "qtde_feridosilesos", "qtde_obitos",
]

_DATATRAN_COLS = [
    "id", "data_inversa", "dia_semana", "horario", "uf", "municipio",
    "causa_acidente", "tipo_acidente", "fase_dia", "condicao_metereologica",
    "tipo_pista", "br", "latitude", "longitude", "pessoas", "mortos", "feridos",
]

_VITIMAS_COLS = [
    "num_acidente", "chv_localidade", "data_acidente", "uf_acidente",
    "ano_acidente", "mes_acidente", "faixa_idade", "genero", "tp_envolvido",
    "gravidade_lesao", "equip_seguranca", "ind_motorista", "susp_alcool",
    "qtde_envolvidos", "qtde_feridosilesos", "qtde_obitos",
]

_TIPO_VEICULO_COLS = [
    "num_acidente", "tipo_veiculo", "ind_veic_estrangeiro", "qtde_veiculos",
]

_LOCALIDADE_COLS = [
    "chv_localidade", "ano_referencia", "mes_referencia", "regiao", "uf",
    "codigo_ibge", "municipio", "regiao_metropolitana",
    "qtde_habitantes", "frota_total", "frota_circulante",
]

_VOLUME_COLS = ["Sitio", "DATA", "ViaSentido", "VMD", "Lon", "Lat"]


def _read_csv_chunked(
    path: Path,
    usecols: list[str] | None,
    dtype: dict | None = None,
    chunksize: int = 500_000,
) -> pd.DataFrame:
    """Lê CSVs grandes em chunks e concatena. Economiza pico de memória RAM."""
    logger.info("Ingerindo %s ...", path.name)
    chunks = []
    reader = pd.read_csv(
        path,
        sep=";",
        encoding="latin-1",
        low_memory=False,
        usecols=usecols,
        dtype=dtype,
        chunksize=chunksize,
    )
    for chunk in reader:
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    logger.info("  → %d linhas lidas de %s", len(df), path.name)
    return df


def ingest_acidentes(data_dir: Path) -> pd.DataFrame:
    """Lê e unifica arquivos de acidentes em schema canônico."""
    frames: list[pd.DataFrame] = []

    legacy_path = data_dir / "acidentes2023.csv"
    if legacy_path.exists():
        dtype = {
            "hora_acidente": str,
            "bairro_acidente": str,
            "latitude_acidente": str,
            "longitude_acidente": str,
            "lim_velocidade": str,
        }
        frames.append(_read_csv_chunked(legacy_path, usecols=_ACIDENTES_COLS, dtype=dtype))

    datatran_path = data_dir / "datatran2026.csv"
    if datatran_path.exists():
        logger.info("Ingerindo layout Datatran (PRF): %s", datatran_path.name)
        df_dt = None
        for enc in ("utf-8", "latin-1"):
            try:
                df_dt = pd.read_csv(
                    datatran_path,
                    sep=";",
                    encoding=enc,
                    low_memory=False,
                    usecols=_DATATRAN_COLS,
                )
                break
            except UnicodeDecodeError:
                logger.warning("Falha de encoding (%s) em %s; tentando próximo...", enc, datatran_path.name)

        if df_dt is None:
            raise UnicodeDecodeError("datatran2026.csv", b"", 0, 1, "Não foi possível decodificar com utf-8 ou latin-1")

        dt_date = pd.to_datetime(df_dt["data_inversa"], errors="coerce")
        df_dt = df_dt.rename(columns={
            "id": "num_acidente",
            "data_inversa": "data_acidente",
            "uf": "uf_acidente",
            "horario": "hora_acidente",
            "tipo_acidente": "tp_acidente",
            "condicao_metereologica": "cond_meteorologica",
            "br": "tp_rodovia",
            "latitude": "latitude_acidente",
            "longitude": "longitude_acidente",
            "mortos": "qtde_obitos",
            "feridos": "qtde_feridosilesos",
            "pessoas": "qtde_envolvidos",
            "tipo_pista": "tp_pista",
        })
        df_dt["ano_acidente"] = dt_date.dt.year
        df_dt["mes_acidente"] = dt_date.dt.month
        df_dt["chv_localidade"] = pd.NA
        df_dt["cond_pista"] = pd.NA
        df_dt["tp_pavimento"] = pd.NA
        df_dt["lim_velocidade"] = pd.NA
        df_dt["bairro_acidente"] = pd.NA
        df_dt["end_acidente"] = pd.NA
        df_dt["qtde_acidente"] = 1
        df_dt["qtde_acid_com_obitos"] = (pd.to_numeric(df_dt["qtde_obitos"], errors="coerce").fillna(0) > 0).astype(int)

        out_cols = _ACIDENTES_COLS + ["municipio", "causa_acidente"]
        for col in out_cols:
            if col not in df_dt.columns:
                df_dt[col] = pd.NA
        frames.append(df_dt[out_cols])
        logger.info("  → %d linhas lidas de %s", len(df_dt), datatran_path.name)

    if not frames:
        raise FileNotFoundError("Nenhum arquivo de acidentes encontrado em data/ (acidentes2023.csv ou datatran2026.csv).")

    if len(frames) == 1:
        return frames[0]

    df = pd.concat(frames, ignore_index=True, sort=False)
    logger.info("  → total consolidado de acidentes: %d linhas", len(df))
    return df


def ingest_vitimas(data_dir: Path) -> pd.DataFrame:
    """Lê o arquivo de vítimas de acidentes."""
    path = data_dir / "Vitimas_DadosAbertos_20260312.csv"
    return _read_csv_chunked(path, usecols=_VITIMAS_COLS)


def ingest_tipo_veiculo(data_dir: Path) -> pd.DataFrame:
    """Lê o arquivo de tipo de veículo por acidente."""
    path = data_dir / "TipoVeiculo_DadosAbertos_20260312.csv"
    return _read_csv_chunked(path, usecols=_TIPO_VEICULO_COLS)


def ingest_localidade(data_dir: Path) -> pd.DataFrame:
    """Lê a dimensão de localidade (município/UF/habitantes/frota)."""
    path = data_dir / "Localidade_20260312.csv"
    return _read_csv_chunked(path, usecols=_LOCALIDADE_COLS, chunksize=200_000)


def ingest_volume_trafego(data_dir: Path) -> pd.DataFrame:
    """Lê o volume de tráfego mensal (dados de Fortaleza/CE)."""
    path = data_dir / "Volume_trafego_mensal.csv"
    logger.info("Ingerindo %s ...", path.name)
    df = pd.read_csv(
        path,
        sep=",",
        encoding="latin-1",
        index_col=0,
        quotechar='"',
    )
    df.columns = [c.strip().strip('"') for c in df.columns]
    # normalizar nomes das colunas para snake_case
    col_map = {
        "Sitio": "sitio",
        "DATA": "data",
        "ViaSentido": "via_sentido",
        "VMD": "vmd",
        "Lon": "longitude",
        "Lat": "latitude",
    }
    df = df.rename(columns=col_map)
    logger.info("  → %d linhas lidas de %s", len(df), path.name)
    return df
