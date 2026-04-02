-- =============================================================
-- init-postgres.sql
-- Exécuté une seule fois au premier démarrage du conteneur postgres
-- Base : rne  (définie via POSTGRES_DB)
-- =============================================================

-- Base dédiée à Airflow (métadonnées du scheduler)
CREATE DATABASE airflow_db;

-- Schéma dédié au Répertoire National des Élus
CREATE SCHEMA IF NOT EXISTS rne;

-- =============================================================
-- CONSEILLERS MUNICIPAUX
-- Source : elus-conseillers-municipaux-cm.csv
-- =============================================================
CREATE TABLE rne.conseillers_municipaux (
    id                                      SERIAL          PRIMARY KEY,
    code_departement                        VARCHAR(3),
    libelle_departement                     TEXT,
    code_collectivite_statut_particulier    VARCHAR(10),
    libelle_collectivite_statut_particulier TEXT,
    code_commune                            VARCHAR(5),
    libelle_commune                         TEXT,
    nom                                     TEXT,
    prenom                                  TEXT,
    code_sexe                               CHAR(1),
    date_naissance                          DATE,
    code_csp                                SMALLINT,
    libelle_csp                             TEXT,
    date_debut_mandat                       DATE,
    libelle_fonction                        TEXT,
    date_debut_fonction                     DATE,
    code_nationalite                        VARCHAR(3)
);

-- =============================================================
-- MAIRES
-- Source : elus-maires-mai.csv
-- =============================================================
CREATE TABLE rne.maires (
    id                                      SERIAL          PRIMARY KEY,
    code_departement                        VARCHAR(3),
    libelle_departement                     TEXT,
    code_collectivite_statut_particulier    VARCHAR(10),
    libelle_collectivite_statut_particulier TEXT,
    code_commune                            VARCHAR(5),
    libelle_commune                         TEXT,
    nom                                     TEXT,
    prenom                                  TEXT,
    code_sexe                               CHAR(1),
    date_naissance                          DATE,
    code_csp                                SMALLINT,
    libelle_csp                             TEXT,
    date_debut_mandat                       DATE,
    date_debut_fonction                     DATE
);

-- =============================================================
-- CONSEILLERS DÉPARTEMENTAUX
-- Source : elus-conseillers-departementaux-cd.csv
-- =============================================================
CREATE TABLE rne.conseillers_departementaux (
    id                  SERIAL  PRIMARY KEY,
    code_departement    VARCHAR(3),
    libelle_departement TEXT,
    code_canton         VARCHAR(6),
    libelle_canton      TEXT,
    nom                 TEXT    NOT NULL,
    prenom              TEXT    NOT NULL,
    code_sexe           CHAR(1),
    date_naissance      DATE,
    code_csp            SMALLINT,
    libelle_csp         TEXT,
    date_debut_mandat   DATE,
    libelle_fonction    TEXT,
    date_debut_fonction DATE
);

-- =============================================================
-- CONSEILLERS RÉGIONAUX
-- Source : elus-conseillers-regionaux-cr.csv
-- =============================================================
CREATE TABLE rne.conseillers_regionaux (
    id                              SERIAL  PRIMARY KEY,
    code_region                     VARCHAR(3),
    libelle_region                  TEXT,
    code_section_departementale     VARCHAR(3),
    libelle_section_departementale  TEXT,
    nom                             TEXT    NOT NULL,
    prenom                          TEXT    NOT NULL,
    code_sexe                       CHAR(1),
    date_naissance                  DATE,
    code_csp                        SMALLINT,
    libelle_csp                     TEXT,
    date_debut_mandat               DATE,
    libelle_fonction                TEXT,
    date_debut_fonction             DATE
);

-- =============================================================
-- CONSEILLERS COMMUNAUTAIRES (EPCI)
-- Source : elus-conseillers-communautaires-epci.csv
-- =============================================================
CREATE TABLE rne.conseillers_communautaires (
    id                                      SERIAL      PRIMARY KEY,
    code_departement                        VARCHAR(3),
    libelle_departement                     TEXT,
    code_collectivite_statut_particulier    VARCHAR(10),
    libelle_collectivite_statut_particulier TEXT,
    siren                                   VARCHAR(9),
    libelle_epci                            TEXT,
    code_commune_rattachement               VARCHAR(5),
    libelle_commune_rattachement            TEXT,
    nom                                     TEXT        NOT NULL,
    prenom                                  TEXT        NOT NULL,
    code_sexe                               CHAR(1),
    date_naissance                          DATE,
    code_csp                                SMALLINT,
    libelle_csp                             TEXT,
    date_debut_mandat                       DATE,
    libelle_fonction                        TEXT,
    date_debut_fonction                     DATE
);

-- =============================================================
-- DÉPUTÉS
-- Source : elus-deputes-dep.csv
-- =============================================================
CREATE TABLE rne.deputes (
    id                                      SERIAL      PRIMARY KEY,
    code_departement                        VARCHAR(3),
    libelle_departement                     TEXT,
    code_collectivite_statut_particulier    VARCHAR(10),
    libelle_collectivite_statut_particulier TEXT,
    code_circonscription_legislative        VARCHAR(6),
    libelle_circonscription_legislative     TEXT,
    nom                                     TEXT        NOT NULL,
    prenom                                  TEXT        NOT NULL,
    code_sexe                               CHAR(1),
    date_naissance                          DATE,
    code_csp                                SMALLINT,
    libelle_csp                             TEXT,
    date_debut_mandat                       DATE
);

-- =============================================================
-- SÉNATEURS
-- Source : elus-senateurs-sen.csv
-- =============================================================
CREATE TABLE rne.senateurs (
    id                                      SERIAL      PRIMARY KEY,
    code_departement                        VARCHAR(3),
    libelle_departement                     TEXT,
    code_collectivite_statut_particulier    VARCHAR(10),
    libelle_collectivite_statut_particulier TEXT,
    nom                                     TEXT        NOT NULL,
    prenom                                  TEXT        NOT NULL,
    code_sexe                               CHAR(1),
    date_naissance                          DATE,
    code_csp                                SMALLINT,
    libelle_csp                             TEXT,
    date_debut_mandat                       DATE
);

-- =============================================================
-- REPRÉSENTANTS AU PARLEMENT EUROPÉEN
-- Source : elus-representants-parlement-europeen-rpe.csv
-- =============================================================
CREATE TABLE rne.representants_parlement_europeen (
    id                SERIAL  PRIMARY KEY,
    nom               TEXT    NOT NULL,
    prenom            TEXT    NOT NULL,
    code_sexe         CHAR(1),
    date_naissance    DATE,
    code_csp          SMALLINT,
    libelle_csp       TEXT,
    date_debut_mandat DATE
);

-- =============================================================
-- ASSEMBLÉE DES FRANÇAIS DE L'ÉTRANGER
-- Source : elus-assemblee-des-francais-de-letranger-afe.csv
-- =============================================================
CREATE TABLE rne.assemblee_francais_etranger (
    id                          SERIAL  PRIMARY KEY,
    code_circonscription_afe    SMALLINT,
    libelle_circonscription_afe TEXT,
    nom                         TEXT    NOT NULL,
    prenom                      TEXT    NOT NULL,
    code_sexe                   CHAR(1),
    date_naissance              DATE,
    code_csp                    SMALLINT,
    libelle_csp                 TEXT,
    date_debut_mandat           DATE,
    libelle_fonction            TEXT,
    date_debut_fonction         DATE
);

-- =============================================================
-- CONSEILLERS DES FRANÇAIS DE L'ÉTRANGER
-- Source : elus-conseillers-des-francais-de-letranger-cons.csv
-- =============================================================
CREATE TABLE rne.conseillers_francais_etranger (
    id                                  SERIAL  PRIMARY KEY,
    code_circonscription_afe            SMALLINT,
    libelle_circonscription_afe         TEXT,
    code_circonscription_consulaire     SMALLINT,
    libelle_circonscription_consulaire  TEXT,
    nom                                 TEXT    NOT NULL,
    prenom                              TEXT    NOT NULL,
    code_sexe                           CHAR(1),
    date_naissance                      DATE,
    code_csp                            SMALLINT,
    libelle_csp                         TEXT,
    date_debut_mandat                   DATE,
    libelle_fonction                    TEXT,
    date_debut_fonction                 DATE
);

-- =============================================================
-- MEMBRES D'UNE ASSEMBLÉE (collectivités à statut particulier)
-- Source : elus-membres-dune-assemblee-ma.csv
-- =============================================================
CREATE TABLE rne.membres_assemblee (
    id                                      SERIAL      PRIMARY KEY,
    code_region                             VARCHAR(3),
    libelle_region                          TEXT,
    code_departement                        VARCHAR(3),
    libelle_departement                     TEXT,
    code_collectivite_statut_particulier    VARCHAR(10),
    libelle_collectivite_statut_particulier TEXT,
    code_section_collectivite               VARCHAR(10),
    libelle_section_collectivite            TEXT,
    code_circonscription_metropolitaine     VARCHAR(10),
    libelle_circonscription_metropolitaine  TEXT,
    nom                                     TEXT        NOT NULL,
    prenom                                  TEXT        NOT NULL,
    code_sexe                               CHAR(1),
    date_naissance                          DATE,
    code_csp                                SMALLINT,
    libelle_csp                             TEXT,
    date_debut_mandat                       DATE,
    libelle_fonction                        TEXT,
    date_debut_fonction                     DATE
);

-- =============================================================
-- CONSEILLERS D'ARRONDISSEMENTS
-- Source : elus-conseillers-darrondissements-ca.csv
-- =============================================================
CREATE TABLE rne.conseillers_darrondissements (
    id                  SERIAL  PRIMARY KEY,
    code_departement    VARCHAR(3),
    libelle_departement TEXT,
    code_commune        VARCHAR(5),
    libelle_commune     TEXT,
    libelle_secteur     TEXT,
    nom                 TEXT    NOT NULL,
    prenom              TEXT    NOT NULL,
    code_sexe           CHAR(1),
    date_naissance      DATE,
    code_csp            SMALLINT,
    libelle_csp         TEXT,
    date_debut_mandat   DATE,
    libelle_fonction    TEXT,
    date_debut_fonction DATE
);
