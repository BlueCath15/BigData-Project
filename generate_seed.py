"""
generate_seed.py — Genera seed_data.parquet con transacciones sintéticas.

Ejecutar UNA sola vez antes de levantar el stack:
    python generate_seed.py

Salida:
    app/seed_data.parquet

Distribución de clases (intencionalmente desbalanceada, como fraude real):
    0 = approved  →  70 %
    1 = flagged   →  20 %
    2 = blocked   →  10 %

Columnas que genera (las mismas que produce Kafka en el streaming):
    user_id, amount, transaction_type, currency,
    destination_id, ip_address, created_at, label
"""

import os
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────

OUTPUT_PATH   = "/tmp/seed_data.parquet"
S3_SEED_PATH  = "s3://fraud-detection-992382522951/seed/seed_data.parquet"
N_ROWS        = 10_000
RANDOM_SEED   = 42

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# ─────────────────────────────────────────────
# Catálogos de valores posibles
# ─────────────────────────────────────────────

TX_TYPES   = ["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT"]
CURRENCIES = ["USD", "EUR", "COP", "MXN", "BRL"]

USER_POOL  = [f"user_{i:05d}" for i in range(1, 501)]   # 500 usuarios
DEST_POOL  = [f"dest_{i:05d}" for i in range(1, 301)]   # 300 destinos


# ─────────────────────────────────────────────
# Generadores por clase
# ─────────────────────────────────────────────

def make_approved(n):
    """
    Clase 0 — transacciones normales:
    - Montos bajos o moderados
    - Horas hábiles
    - Transferencias mayormente internas
    """
    amounts      = np.random.lognormal(mean=6.0, sigma=1.2, size=n).clip(1, 4999)
    tx_types     = np.random.choice(TX_TYPES, size=n, p=[0.25, 0.45, 0.20, 0.10])
    hours        = np.random.choice(range(8, 22), size=n)          # horas hábiles
    users        = np.random.choice(USER_POOL, size=n)
    # destinos: muchos son el mismo usuario (transferencias internas)
    destinations = [
        u if random.random() < 0.6 else random.choice(DEST_POOL)
        for u in users
    ]
    return amounts, tx_types, hours, users, destinations


def make_flagged(n):
    """
    Clase 1 — transacciones sospechosas:
    - Montos en umbrales redondos (999, 4999, 9999…)
    - Madrugada o fuera de horario
    - Transferencias externas frecuentes
    """
    # Montos en valores sospechosos típicos de structuring
    suspicious = [999, 1999, 2999, 4999, 9999, 14999, 19999, 49999]
    base       = np.random.lognormal(mean=7.5, sigma=1.0, size=n).clip(1000, 9999)
    mask       = np.random.random(n) < 0.5
    amounts    = np.where(
        mask,
        np.random.choice(suspicious, size=n),
        base
    )
    tx_types     = np.random.choice(TX_TYPES, size=n, p=[0.50, 0.20, 0.25, 0.05])
    hours        = np.random.choice(
        list(range(0, 6)) + list(range(22, 24)), size=n   # madrugada
    )
    users        = np.random.choice(USER_POOL, size=n)
    destinations = np.random.choice(DEST_POOL, size=n)    # siempre externo
    return amounts, tx_types, hours, users, destinations


def make_blocked(n):
    """
    Clase 2 — transacciones bloqueadas (fraude claro):
    - Montos muy altos
    - Redondos exactos en miles (structuring avanzado)
    - Madrugada
    - Destino siempre externo y distinto al usuario
    """
    round_amounts = np.random.choice(
        [10000, 20000, 30000, 50000, 75000, 100000], size=n
    )
    large_amounts = np.random.lognormal(mean=10.5, sigma=0.8, size=n).clip(10000, 200000)
    mask    = np.random.random(n) < 0.6
    amounts = np.where(mask, round_amounts, large_amounts)

    tx_types     = np.random.choice(TX_TYPES, size=n, p=[0.60, 0.05, 0.35, 0.00])
    hours        = np.random.choice(range(0, 5), size=n)   # siempre madrugada
    users        = np.random.choice(USER_POOL, size=n)
    # destino distinto del usuario, siempre externo
    destinations = [
        random.choice([d for d in DEST_POOL if d != u[:8]])
        for u in users
    ]
    return amounts, tx_types, hours, users, destinations


# ─────────────────────────────────────────────
# Construir DataFrame completo
# ─────────────────────────────────────────────

def build_dataframe():
    n_approved = int(N_ROWS * 0.70)
    n_flagged  = int(N_ROWS * 0.20)
    n_blocked  = N_ROWS - n_approved - n_flagged

    rows = []

    base_time = datetime(2024, 1, 1)

    for label, gen_fn, n in [
        (0, make_approved, n_approved),
        (1, make_flagged,  n_flagged),
        (2, make_blocked,  n_blocked),
    ]:
        amounts, tx_types, hours, users, destinations = gen_fn(n)

        for i in range(n):
            # created_at: fecha aleatoria en 2024 con la hora del generador
            day_offset = random.randint(0, 364)
            created_at = base_time + timedelta(
                days=day_offset,
                hours=int(hours[i]),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )

            rows.append({
                "user_id"          : users[i],
                "amount"           : round(float(amounts[i]), 2),
                "transaction_type" : tx_types[i],
                "currency"         : random.choice(CURRENCIES),
                "destination_id"   : destinations[i],
                "ip_address"       : f"{random.randint(1,255)}.{random.randint(0,255)}"
                                     f".{random.randint(0,255)}.{random.randint(0,255)}",
                "created_at"       : created_at,
                "label"            : float(label),   # float para Spark ML
            })

    df = pd.DataFrame(rows).sample(frac=1, random_state=RANDOM_SEED).reset_index(drop=True)
    return df


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

if __name__ == "__main__":
    os.makedirs("app", exist_ok=True)
    df = build_dataframe()
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"[seed-generator] {len(df):,} filas guardadas en {OUTPUT_PATH}")

    import subprocess
    subprocess.run([    
        "aws", "s3", "cp", OUTPUT_PATH, S3_SEED_PATH
    ])
    print(f"[seed-generator] Subido a {S3_SEED_PATH}")