#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ----------------------------
# CONFIGURAÇÕES
# ----------------------------

# MySQL externo
MYSQL_USER = "eugon2"
MYSQL_PASS = "Master45@net"
MYSQL_HOST = "187.73.33.163"
MYSQL_DB   = "eugon2"

# PostgreSQL local (para o Superset)
PG_USER = "geo_user"
PG_PASS = "mastersoundbh"
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB   = "geo"
PG_SCHEMA = "public"
PG_TABLE = "enderecos_geolocalizados"

# API Nominatim
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
REQUEST_DELAY = 1.5  # segundos entre requisições

# ----------------------------
# FUNÇÕES
# ----------------------------

def escape_password(password):
    """Escapa caracteres especiais na senha"""
    return quote_plus(password)

def geocode(address):
    """Consulta Nominatim e retorna latitude e longitude"""
    try:
        params = {
            "q": address,
            "format": "json",
            "addressdetails": 0,
            "limit": 1,
        }
        headers = {"User-Agent": "MasterGeoScript/1.0"}
        response = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"❌ Erro ao consultar '{address}': {e}")
    return None, None

def format_endereco(raw_address):
    """Limpa o texto e monta formato amigável pro Nominatim"""
    if not raw_address:
        return ""
    address = raw_address.replace("Endereço da Obra:", "")
    address = address.replace("Endereço Principal:", "")
    address = address.replace("Bairro:", "").replace("Cidade:", "")
    return f"{address.strip()}, Minas Gerais, Brasil"

# ----------------------------
# CONEXÃO COM MYSQL
# ----------------------------

password_escaped = escape_password(MYSQL_PASS)
mysql_engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{password_escaped}@{MYSQL_HOST}/{MYSQL_DB}"
)

query = """
SELECT EnderecoObra
FROM calendar AS a
WHERE CodServicosCab > 0
  AND DATE(a.start_date) = CURDATE() - INTERVAL 2 DAY
ORDER BY a.id DESC;
"""

with mysql_engine.connect() as conn:
    df_enderecos = pd.read_sql(text(query), conn)

print(f"🔍 {len(df_enderecos)} endereços encontrados no MySQL.")

# ----------------------------
# GEOCODIFICAÇÃO
# ----------------------------

results = []
for _, row in df_enderecos.iterrows():
    raw = row["EnderecoObra"]
    endereco_limpo = format_endereco(raw)
    lat, lon = geocode(endereco_limpo)
    results.append({
        "EnderecoOriginal": raw,
        "EnderecoFormatado": endereco_limpo,
        "Latitude": lat,
        "Longitude": lon
    })
    print(f"📍 {raw} -> ({lat}, {lon})")
    time.sleep(REQUEST_DELAY)

df_geo = pd.DataFrame(results)

# ----------------------------
# SALVAR NO POSTGRES
# ----------------------------

pg_engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

with pg_engine.begin() as conn:
    df_geo.to_sql(
        PG_TABLE,
        conn,
        schema=PG_SCHEMA,
        if_exists="replace",
        index=False
    )

print(f"✅ Dados salvos na tabela '{PG_SCHEMA}.{PG_TABLE}'.")
print("📦 Pronto para importar no Superset.")
