#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import time

# -------------------------
# CONFIGURAÇÕES DE BANCO
# -------------------------

# MySQL externo (onde estão os endereços)
MYSQL_USER = "net"
MYSQL_PASSWORD = "master45@net"  # senha original com @
MYSQL_HOST = "187.73.33.163"
MYSQL_PORT = 3306
MYSQL_DB = "eugon2"
MYSQL_TABLE = "calendar"

# PostgreSQL local (onde vamos salvar para Superset)
PG_USER = "geo_user"
PG_PASSWORD = "mastergeo"  # ajuste para sua senha real
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "geo"
PG_SCHEMA = "public"  # ou outro schema que seu usuário tenha permissão
PG_TABLE = "enderecos_geolocalizados"

# -------------------------
# FUNÇÕES
# -------------------------

def buscar_enderecos_mysql():
    """Puxa endereços do MySQL"""
    # Substitui @ por %40 na URL para não quebrar a conexão
    senha_url = MYSQL_PASSWORD.replace("@", "%40")
    mysql_url = f"mysql+pymysql://{MYSQL_USER}:{senha_url}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    engine_mysql = create_engine(mysql_url)
    
    query = f"SELECT EnderecoObra FROM {MYSQL_TABLE} WHERE EnderecoObra IS NOT NULL"
    
    df = pd.read_sql(query, engine_mysql)
    print(f"🔍 {len(df)} endereços encontrados no MySQL.")
    return df

def geolocalizar_endereco(endereco):
    """Consulta o Nominatim para obter latitude e longitude"""
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": endereco,
        "format": "json",
        "addressdetails": 0,
        "limit": 1
    }
    try:
        resp = requests.get(url, params=params, headers={"User-Agent": "MasterGeoScript"})
        data = resp.json()
        if data:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            return lat, lon
    except Exception as e:
        print(f"❌ Erro ao geolocalizar {endereco}: {e}")
    return None, None

# -------------------------
# EXECUÇÃO
# -------------------------

def main():
    # Pega os endereços
    df = buscar_enderecos_mysql()
    
    # Geolocaliza cada endereço
    latitudes = []
    longitudes = []
    for idx, row in df.iterrows():
        endereco = row["EnderecoObra"]
        lat, lon = geolocalizar_endereco(endereco)
        latitudes.append(lat)
        longitudes.append(lon)
        print(f"📍 {endereco} -> ({lat}, {lon})")
        time.sleep(1)  # evita bloqueio do Nominatim
    
    df["Latitude"] = latitudes
    df["Longitude"] = longitudes
    
    # Conecta ao PostgreSQL
    local_db_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine_pg = create_engine(local_db_url)
    
    # Salva no PostgreSQL dentro do schema
    try:
        df.to_sql(
            PG_TABLE,
            engine_pg,
            schema=PG_SCHEMA,
            if_exists="replace",
            index=False
        )
        print(f"✅ Dados salvos na tabela '{PG_SCHEMA}.{PG_TABLE}'.")
        print("📦 Pronto para importar no Superset.")
    except SQLAlchemyError as e:
        print("❌ Erro ao salvar no PostgreSQL:", e)

if __name__ == "__main__":
    main()
