"""
migrate_csv_to_postgres.py — Chargement des CSV du RNE vers PostgreSQL
=======================================================================
- Schéma cible : rne
- Types appliqués : DATE (DD/MM/YYYY), SMALLINT (codes CSP / AFE)
- Idempotent : un flag file évite de rejouer la migration
"""

import logging
import os
import re
import unicodedata

import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [migrate] %(message)s")
logger = logging.getLogger(__name__)

PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_USER     = os.getenv("POSTGRES_USER", "spark")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "spark")
PG_DB       = os.getenv("POSTGRES_DB",   "rne")
CSV_DIR     = os.getenv("CSV_DIR",       "/data/csv")
FLAG_FILE   = "/app/flag/done"
PG_SCHEMA   = "rne"

# ─── Mapping fichier CSV → table PostgreSQL ───────────────────────────────────
CSV_TABLE_MAP = {
    "elus-conseillers-municipaux-cm.csv":                  "conseillers_municipaux",
    "elus-maires-mai.csv":                                 "maires",
    "elus-conseillers-departementaux-cd.csv":              "conseillers_departementaux",
    "elus-conseillers-regionaux-cr.csv":                   "conseillers_regionaux",
    "elus-conseillers-communautaires-epci.csv":            "conseillers_communautaires",
    "elus-deputes-dep.csv":                                "deputes",
    "elus-senateurs-sen.csv":                              "senateurs",
    "elus-representants-parlement-europeen-rpe.csv":       "representants_parlement_europeen",
    "elus-assemblee-des-francais-de-letranger-afe.csv":    "assemblee_francais_etranger",
    "elus-conseillers-des-francais-de-letranger-cons.csv": "conseillers_francais_etranger",
    "elus-membres-dune-assemblee-ma.csv":                  "membres_assemblee",
    "elus-conseillers-darrondissements-ca.csv":            "conseillers_darrondissements",
}

# ─── Renommage : nom auto-normalisé → nom DDL ─────────────────────────────────
# Les clés sont les noms produits par normalize_col() appliqué aux en-têtes CSV.
# Les valeurs correspondent exactement aux colonnes définies dans init-postgres.sql.
COLUMN_RENAME = {
    # Géographie
    "code_du_departement":                                  "code_departement",
    "libelle_du_departement":                               "libelle_departement",
    "code_de_la_collectivite_a_statut_particulier":         "code_collectivite_statut_particulier",
    "libelle_de_la_collectivite_a_statut_particulier":      "libelle_collectivite_statut_particulier",
    "code_de_la_commune":                                   "code_commune",
    "libelle_de_la_commune":                                "libelle_commune",
    # Canton
    "code_du_canton":                                       "code_canton",
    "libelle_du_canton":                                    "libelle_canton",
    # Région / section
    "code_de_la_region":                                    "code_region",
    "libelle_de_la_region":                                 "libelle_region",
    "code_de_la_section_departementale":                    "code_section_departementale",
    "libelle_de_la_section_departementale":                 "libelle_section_departementale",
    # EPCI
    "n_siren":                                              "siren",
    "libelle_de_l_epci":                                    "libelle_epci",
    "code_de_la_commune_de_rattachement":                   "code_commune_rattachement",
    "libelle_de_la_commune_de_rattachement":                "libelle_commune_rattachement",
    # Circonscription législative
    "code_de_la_circonscription_legislative":               "code_circonscription_legislative",
    "libelle_de_la_circonscription_legislative":            "libelle_circonscription_legislative",
    # AFE (deux formes présentes dans les CSV)
    "code_de_la_circ_afe":                                  "code_circonscription_afe",
    "libelle_la_circ_afe":                                  "libelle_circonscription_afe",
    "code_de_la_circonscription_afe":                       "code_circonscription_afe",
    "libelle_la_circonscription_afe":                       "libelle_circonscription_afe",
    # Consulaire
    "code_de_la_circonscription_consulaire":                "code_circonscription_consulaire",
    "libelle_de_la_circonscription_consulaire":             "libelle_circonscription_consulaire",
    # Membres d'assemblée
    "code_de_la_section_collectivite_a_statut_particulier": "code_section_collectivite",
    "libelle_de_la_section_collectivite_a_statut_particulier": "libelle_section_collectivite",
    "code_de_la_circonscription_metropolitaine":            "code_circonscription_metropolitaine",
    "libelle_de_la_circonscription_metropolitaine":         "libelle_circonscription_metropolitaine",
    # Arrondissements
    "libelle_du_secteur":                                   "libelle_secteur",
    # Élu (commun à toutes les tables)
    "nom_de_l_elu":                                         "nom",
    "prenom_de_l_elu":                                      "prenom",
    "code_de_la_categorie_socio_professionnelle":           "code_csp",
    "libelle_de_la_categorie_socio_professionnelle":        "libelle_csp",
    "date_de_naissance":                                    "date_naissance",
    "date_de_debut_du_mandat":                              "date_debut_mandat",
    "libelle_de_la_fonction":                               "libelle_fonction",
    "date_de_debut_de_la_fonction":                         "date_debut_fonction",
}

# Colonnes à convertir en DATE (format source : JJ/MM/AAAA)
DATE_COLS = {"date_naissance", "date_debut_mandat", "date_debut_fonction"}

# Colonnes à convertir en entier nullable
INT_COLS = {"code_csp", "code_circonscription_afe", "code_circonscription_consulaire"}


def normalize_col(name: str) -> str:
    """Convertit un en-tête CSV français en snake_case ASCII."""
    name = name.strip().strip('"').strip("'")
    nfkd = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in nfkd if not unicodedata.combining(c))
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def detect_separator(filepath: str) -> str:
    with open(filepath, encoding="utf-8") as f:
        first_line = f.readline()
    return ";" if first_line.count(";") >= first_line.count(",") else ","


def cast_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.array(pd.to_numeric(df[col], errors="coerce"), dtype="Int16")
    return df


def load_csv(filepath: str, table_name: str, engine) -> None:
    sep = detect_separator(filepath)
    df = pd.read_csv(
        filepath,
        sep=sep,
        dtype=str,
        encoding="utf-8",
        quotechar='"',
        on_bad_lines="skip",
    )

    # 1. Normalisation des noms de colonnes
    df.columns = [normalize_col(c) for c in df.columns]

    # 2. Renommage vers les noms DDL
    df = df.rename(columns=COLUMN_RENAME)

    # 3. Chaînes vides → NULL
    df = df.where(df.notna(), None)
    df = df.where(df != "", None)

    # 4. Conversion des types
    df = cast_columns(df)

    logger.info(f"  {table_name}: {len(df):,} lignes — colonnes: {list(df.columns)}")

    df.to_sql(
        table_name,
        engine,
        schema=PG_SCHEMA,
        if_exists="append",   # insère dans les tables créées par le DDL
        index=False,
        method="multi",
        chunksize=5_000,
    )
    logger.info(f"  {table_name}: OK")


def main() -> None:
    if os.path.exists(FLAG_FILE):
        logger.info("Migration déjà effectuée. Fin.")
        return

    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )

    for csv_filename, table_name in CSV_TABLE_MAP.items():
        filepath = os.path.join(CSV_DIR, csv_filename)
        if not os.path.exists(filepath):
            logger.warning(f"Fichier introuvable: {filepath} — ignoré.")
            continue
        logger.info(f"Chargement {csv_filename} → {PG_SCHEMA}.{table_name}")
        load_csv(filepath, table_name, engine)

    os.makedirs(os.path.dirname(FLAG_FILE), exist_ok=True)
    with open(FLAG_FILE, "w") as f:
        f.write("done\n")
    logger.info("Migration terminée.")


if __name__ == "__main__":
    main()
