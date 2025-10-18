import requests
import time
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------
# CONFIGURAÇÕES
# ---------------------------------------------

# Banco externo (MySQL com os endereços)
MYSQL_USER = "eugon2"
MYSQL_PASS = "Master45@net"
MYSQL_HOST = "187.73.33.163"
MYSQL_DB = "eugon2"

# Banco local (exemplo: SQLite para o Superset)
LOCAL_DB_URL = "sqlite:///geolocalizacao.db"
# Pode trocar por:
# "postgresql://user:pass@localhost:5432/geolocalizacao"
# "mysql+pymysql://user:pass@localhost/geolocalizacao"

# URL base da API do Nominatim
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Delay entre consultas (evita bloqueio da API pública)
REQUEST_DELAY = 1.5  # segundos

# ---------------------------------------------
# ETAPA 1: Buscar endereços do banco MySQL externo
# ---------------------------------------------

mysql_engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}/{MYSQL_DB}"
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

# ---------------------------------------------
# ETAPA 2: Consultar Nominatim e gerar coordenadas
# ---------------------------------------------

def geocode(address):
    """Consulta o Nominatim e retorna lat/lon."""
    try:
        params = {
            "q": address,
            "format": "json",
            "addressdetails": 0,
            "limit": 1,
        }
        response = requests.get(NOMINATIM_URL, params=params, headers={"User-Agent": "GeoScript/1.0"})
        if response.status_code == 200:
            data = response.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"❌ Erro ao consultar '{address}': {e}")
    return None, None

results = []
for _, row in df_enderecos.iterrows():
    endereco = row["EnderecoObra"]
    lat, lon = geocode(endereco)
    results.append({
        "EnderecoObra": endereco,
        "Latitude": lat,
        "Longitude": lon
    })
    print(f"📍 {endereco} -> ({lat}, {lon})")
    time.sleep(REQUEST_DELAY)

df_geo = pd.DataFrame(results)

# ---------------------------------------------
# ETAPA 3: Gravar no banco local (SQLite/Postgre/MySQL)
# ---------------------------------------------

local_engine = create_engine(LOCAL_DB_URL)

df_geo.to_sql("enderecos_geolocalizados", local_engine, if_exists="replace", index=False)

print("✅ Dados salvos na tabela 'enderecos_geolocalizados'.")
print("📦 Pronto para importar no Superset.")
