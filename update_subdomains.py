#!/usr/bin/env python3
"""
Script de mise à jour des noms de commerce vers subdomains.
Remplace tous les "Nom Commerce" dans la colonne B par leurs subdomains correspondants.
"""

import os
import json
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ============================================================
# CONFIGURATION
# ============================================================
CONFIG_FILE = "config.json"
TOKEN_FILE = "token.json"
CREDENTIALS_FILE = "credentials.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


def load_config():
    """Charge la configuration depuis config.json."""
    config_path = os.path.join(os.path.dirname(__file__), CONFIG_FILE)
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


CONFIG = load_config()
SPREADSHEET_ID = CONFIG.get("spreadsheet_id", "")
SHEET_NAME = CONFIG.get("sheet_name", "Avis")
COMMERCE_TO_SUBDOMAIN = CONFIG.get("commerce_to_subdomain", {})


def get_credentials():
    """Authentification Google."""
    creds = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(TOKEN_FILE, "w") as token:
                token.write(creds.to_json())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            with open(TOKEN_FILE, "w") as token:
                token.write(creds.to_json())

    return creds


def convert_commerce_to_subdomain(nom_commerce):
    """Convertit le nom de commerce en subdomain selon le mapping."""
    if not nom_commerce:
        return nom_commerce

    # Recherche exacte d'abord
    if nom_commerce in COMMERCE_TO_SUBDOMAIN:
        return COMMERCE_TO_SUBDOMAIN[nom_commerce]

    # Recherche insensible à la casse
    nom_lower = nom_commerce.lower().strip()
    for commerce, subdomain in COMMERCE_TO_SUBDOMAIN.items():
        if commerce.lower().strip() == nom_lower:
            return subdomain

    # Pas de correspondance trouvée
    return nom_commerce


def main():
    print(f"\n{'='*60}")
    print(f"  Mise à jour des subdomains — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*60}\n")

    # Authentification
    print("Authentification...")
    creds = get_credentials()
    sheets_service = build("sheets", "v4", credentials=creds)
    print("   Connecte\n")

    # Lire la colonne B (Nom Commerce)
    print("Lecture des donnees existantes (colonne B)...")
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!B:B",
    ).execute()

    values = result.get("values", [])
    print(f"   {len(values)} lignes trouvees (incluant l'en-tete)")

    if len(values) <= 1:
        print("\n Aucune donnee a mettre a jour.")
        return

    # Preparer les nouvelles valeurs (garder l'en-tete)
    updated_values = [values[0]]  # En-tete
    converted_count = 0
    not_found = set()

    for i, row in enumerate(values[1:], start=2):
        if not row:
            updated_values.append([""])
            continue

        original = row[0]
        converted = convert_commerce_to_subdomain(original)

        if converted != original:
            converted_count += 1
        elif original and original not in COMMERCE_TO_SUBDOMAIN.values():
            # Nom pas dans le mapping et pas deja un subdomain
            if not original.endswith(".good-reviews.ch") and not original.endswith(".goodreviews.ch"):
                not_found.add(original)

        updated_values.append([converted])

    print(f"\n   {converted_count} valeurs a convertir")
    print(f"   {len(not_found)} valeurs non trouvees dans le mapping")

    if not_found:
        print("\n   Noms de commerce sans correspondance :")
        for name in sorted(not_found)[:20]:
            print(f"      - {name}")
        if len(not_found) > 20:
            print(f"      ... et {len(not_found) - 20} autres")

    if converted_count == 0:
        print("\n Aucune conversion necessaire.")
        return

    # Ecrire les nouvelles valeurs
    print(f"\nEcriture des {converted_count} conversions...")
    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!B1:B{len(updated_values)}",
        valueInputOption="RAW",
        body={"values": updated_values},
    ).execute()

    print(f"   {converted_count} lignes mises a jour !")

    print(f"\n Mise a jour terminee !")
    print(f"   Lien: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")


if __name__ == "__main__":
    main()
