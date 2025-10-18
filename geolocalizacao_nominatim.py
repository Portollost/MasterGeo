#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import time
import re


# -------------------------
# CONFIGURAÇÕES DE BANCO
# -------------------------

# MySQL externo (onde estão os endereços)
MYSQL_USER = "eugon2"
MYSQL_PASSWORD = "Master45@net"  # senha original com @
MYSQL_PASSWORD_ESC = MYSQL_PASSWORD.replace("@", "%40")  # escapando o @
MYSQL_HOST = "187.73.33.163"
MYSQL_PORT = 3306
MYSQL_DB = "eugon2"
MYSQL_TABLE = "calendar"

# PostgreSQL local (onde vamos salvar para Superset)
PG_USER = "geo_user"
PG_PASSWORD = "mastersoundbh"
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "geo"
PG_SCHEMA = "public"
PG_TABLE = "enderecos_geolocalizados"

# -------------------------
# FUNÇÕES
# -------------------------

def buscar_enderecos_mysql():
    """Puxa endereços do MySQL somente de ontem"""
    mysql_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD_ESC}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    engine_mysql = create_engine(mysql_url)
    
    query = """
    SELECT EnderecoObra 
    FROM calendar AS a 
    WHERE CodServicosCab > 0 
      AND DATE(a.start_date) = CURDATE() - INTERVAL 1 DAY 
    ORDER BY a.id DESC;
    """
    
    df = pd.read_sql(query, engine_mysql)
    print(f"🔍 {len(df)} endereços encontrados do dia anterior no MySQL.")
    return df


def limpar_endereco(endereco):
    """Limpa o endereço removendo prefixos como 'Endereço da Obra' ou 'Endereço Principal'."""
    if not endereco or not isinstance(endereco, str):
        return ""
    
    # Remove prefixos do começo da string
    endereco = re.sub(
        r'^\s*End[eé]re[cç]o\s+(da Obra|Principal)\s*:\s*',
        '',
        endereco,
        flags=re.IGNORECASE
    )
    
    # Remove múltiplos espaços, traços e organiza vírgulas
    endereco = re.sub(r'\s*-\s*', ', ', endereco)
    endereco = re.sub(r'\s+', ' ', endereco).strip()
    
    return endereco



def geolocalizar_endereco(endereco):
    """Consulta o Nominatim para obter latitude e longitude."""
    endereco_limpo = limpar_endereco(endereco)
    if not endereco_limpo:
        return None, None

    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": endereco_limpo + ", Minas Gerais, Brasil",
        "format": "json",
        "limit": 1
    }

    try:
        resp = requests.get(url, params=params, headers={"User-Agent": "MasterGeoScript"})
        data = resp.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
        else:
            partes = endereco_limpo.split(",")
            if len(partes) > 1:
                fallback = partes[-1].strip() + ", Minas Gerais, Brasil"
                params["q"] = fallback
                resp = requests.get(url, params=params, headers={"User-Agent": "MasterGeoScript"})
                data = resp.json()
                if data:
                    return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"❌ Erro ao geolocalizar {endereco}: {e}")

    return None, None


# -------------------------
# EXECUÇÃO
# -------------------------

def main():
    df = buscar_enderecos_mysql()
    
    latitudes = []
    longitudes = []
    for idx, row in df.iterrows():
        endereco = row["EnderecoObra"]
        lat, lon = geolocalizar_endereco(endereco)
        latitudes.append(lat)
        longitudes.append(lon)
        print(f"📍 {endereco} -> ({lat}, {lon})")
        time.sleep(1)  # Respeita limite do Nominatim
    
    df["Latitude"] = latitudes
    df["Longitude"] = longitudes
    
    local_db_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine_pg = create_engine(local_db_url)
    
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
