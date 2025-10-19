#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import re
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

# ----------------------------
# CONFIGURAÇÕES
# ----------------------------

# MySQL externo
MYSQL_USER = "eugon2"
MYSQL_PASS = "Master45@net"
MYSQL_HOST = "187.73.33.163"
MYSQL_DB   = "eugon2"

# Google Sheets
GOOGLE_SHEET_NAME = "enderecos"
SERVICE_ACCOUNT_JSON = "x-pathway-475010-f3-c694d792cffc.json"  # caminho para sua chave JSON

# API Nominatim
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
REQUEST_DELAY = 1.5  # segundos entre requisições

# ----------------------------
# FUNÇÕES
# ----------------------------

def escape_password(password):
    return quote_plus(password)

def limpar_endereco(endereco):
    if not endereco or not isinstance(endereco, str):
        return ""
    s = endereco.strip()
    s = re.sub(r'^\s*End[eé]re[cç]o\s+(da obra|principal)\s*:\s*', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\s*-\s*', ', ', s)
    s = re.sub(r'\b\d{1,3}\s*(?:º|ª)?\s*(?:andar|andar\.?)\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\b(apt|apto|apartamento)\.? ?\d+\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bbloco\b[:\s]*[A-Za-z0-9-]+\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\b(s\/n|sem numero|sem número)\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bBairro:\s*', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bCidade:\s*', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r',\s*,+', ', ', s)
    return s.strip(' ,')

def geocode(raw_address):
    endereco_limpo = limpar_endereco(raw_address)
    if not endereco_limpo:
        return None, None
    headers = {"User-Agent": "MasterGeoScript/1.0 (contato@mastersound.com)"}
    tentativas = [
        f"{endereco_limpo}, Minas Gerais, Brasil",
        re.sub(r'\bAv[\.]?\b', 'Avenida', endereco_limpo) + ", Minas Gerais, Brasil",
        endereco_limpo.split(',')[0] + ", Minas Gerais, Brasil"
    ]
    for q in tentativas:
        try:
            print(f"🔎 tentando: {q}")
            params = {"q": q, "format": "json", "limit": 1}
            resp = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    lat = float(data[0]["lat"])
                    lon = float(data[0]["lon"])
                    print(f"✅ encontrado: {q} -> ({lat}, {lon})")
                    return lat, lon
        except Exception as e:
            print(f"❌ erro ao consultar '{q}': {e}")
        time.sleep(REQUEST_DELAY)
    print(f"⚠️ Nenhuma coordenada encontrada para: {raw_address}")
    return None, None

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
    endereco_limpo = limpar_endereco(raw)
    lat, lon = geocode(raw)
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
# SALVAR NO GOOGLE SHEETS
# ----------------------------

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_JSON, scope)
gc = gspread.authorize(credentials)

# Abre ou cria a planilha
try:
    sh = gc.open(GOOGLE_SHEET_NAME)
except gspread.SpreadsheetNotFound:
    sh = gc.create(GOOGLE_SHEET_NAME)
    sh.share(None, perm_type='anyone', role='writer')  # qualquer pessoa com link pode editar

worksheet = sh.sheet1
set_with_dataframe(worksheet, df_geo)

print(f"✅ Dados salvos na Google Sheet '{GOOGLE_SHEET_NAME}'.")
