#!/usr/bin/env python3
import requests
import time
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ----------------------------
# CONFIGURAÇÕES
# ----------------------------

# MySQL externo (onde estão os endereços)
MYSQL_USER = "eugon2"
MYSQL_PASS = "Master45@net"  # senha literal
MYSQL_HOST = "187.73.33.163"
MYSQL_DB   = "eugon2"

# Banco local SQLite para o Superset
LOCAL_DB_URL = "sqlite:///geolocalizacao.db"

# API Nominatim
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
REQUEST_DELAY = 1.5  # segundos entre requisições

# ----------------------------
# FUNÇÕES
# ----------------------------

def escape_password(password):
    """Escapa caracteres especiais na senha para SQLAlchemy"""
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
        headers = {"User-Agent": "GeoScript/1.0"}
        response = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"❌ Erro ao consultar '{address}': {e}")
    return None, None

def format_endereco(raw_address):
    """Normaliza endereço para Nominatim"""
    # Remove prefixo e textos desnecessários
    address = raw_address.replace("Endereço da Obra:", "")
    address = address.replace("Bairro:", "").replace("Cidade:", "")
    return f"{address.strip()}, MG, Brasil"

# ----------------------------
# CONEXÃO COM MYSQL EXTERNO
# ----------------------------

password_escaped = escape_password(MYSQL_PASS)
mysql_engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{password_escaped}@{MYSQL_HOST}/{MYSQL_DB}"
)

query = """
SELECT 
  EnderecoObra
FROM
  calendar AS a
WHERE CodServicosCab > 0
AND DATE(a.start_date) = CURDATE() - INTERVAL 1 DAY
ORDER BY a.id DESC;
"""

with mysql_engine.connect() as conn:
    df_enderecos = pd.read_sql(text(query), conn)

print(f"🔍 {len(df_enderecos)} endereços encontrados no MySQL.")

# ----------------------------
# GEOCODIFICAR
# ----------------------------

results = []

for _, row in df_enderecos.iterrows():
    raw = row["EnderecoObra"]
    endereco_limpo = format_endereco(raw)
    lat, lon = geocode(endereco_limpo)
    results.append({
        "EnderecoObra": endereco_limpo,
        "Latitude": lat,
        "Longitude": lon
    })
    print(f"📍 Endereço da Obra: {endereco_limpo} -> ({lat}, {lon})")
    time.sleep(REQUEST_DELAY)

df_geo = pd.DataFrame(results)

# ----------------------------
# SALVAR NO BANCO LOCAL (SQLite)
# ----------------------------

local_engine = create_engine(LOCAL_DB_URL)
df_geo.to_sql("enderecos_geolocalizados", local_engine, if_exists="replace", index=False)

print("✅ Dados salvos na tabela 'enderecos_geolocalizados'.")
print("📦 Pronto para importar no Superset.")
