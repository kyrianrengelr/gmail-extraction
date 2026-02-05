import os
import re
import json
import base64
from email.mime.text import MIMEText
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ============================================================
# CONFIGURATION
# ============================================================
SPREADSHEET_ID = "13LdV9dEeT42u51OkY3TGiof-Htd3C8KjeqJKbI_PXNo"
SHEET_NAME = "Avis"
NOTIFICATION_EMAIL = "kyrian.engel@gmail.com"
STATE_FILE = "last_processed.json"
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE = "token.json"

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.send",
]

# Mapping genre → sexe
GENDER_MAP = {
    "monsieur": "Homme",
    "madame": "Femme",
    "herr": "Homme",
    "frau": "Femme",
    "signor": "Homme",
    "signora": "Femme",
    "mx": "Autre",
}

# ============================================================
# FILTRAGE DES EMAILS INVALIDES
# ============================================================
# Emails à exclure (blacklist)
EMAIL_BLACKLIST = {
    "kyrian.engel@gmail.com",
    "kyrianr.engelr@gmail.com",
    "kyrian.kyrian.engel@gmail.com",
}

# Mots-clés suspects dans la partie locale de l'email
SPAM_KEYWORDS = {
    "test", "fake", "spam", "caca", "popo", "pipi", "prout",
    "asdf", "qwerty", "azerty", "aaa", "xxx", "zzz",
    "noreply", "no-reply", "donotreply",
}

# Domaines jetables connus
DISPOSABLE_DOMAINS = {
    "yopmail.com", "tempmail.com", "guerrillamail.com", "mailinator.com",
    "10minutemail.com", "throwaway.email", "fakeinbox.com", "trashmail.com",
    "temp-mail.org", "getnada.com", "maildrop.cc",
}


def is_valid_email(email):
    """Vérifie si un email est valide (filtrage modéré)."""
    if not email:
        return False

    email = email.lower().strip()

    # 1. Blacklist explicite
    if email in EMAIL_BLACKLIST:
        return False

    # 2. Format basique
    if "@" not in email or "." not in email:
        return False

    local_part, domain = email.rsplit("@", 1)

    # 3. Partie locale trop courte (moins de 3 caractères)
    if len(local_part) < 3:
        return False

    # 4. Domaines jetables
    if domain in DISPOSABLE_DOMAINS:
        return False

    # 5. Mots-clés spam (correspondance exacte de la partie locale)
    if local_part in SPAM_KEYWORDS:
        return False

    # 6. Que des chiffres dans la partie locale
    if local_part.isdigit():
        return False

    # 7. Répétition excessive (ex: aaaa, 1111)
    if len(set(local_part)) <= 2 and len(local_part) > 3:
        return False

    return True


# Noms suspects à filtrer
SUSPICIOUS_NAMES = {
    "test", "blabla", "bla", "toto", "tata", "titi", "tutu",
    "azerty", "qwerty", "asdf", "xxx", "zzz", "aaa", "bbb",
    "fake", "spam", "null", "none", "na", "n/a", "inconnu",
    "anonymous", "anonyme", "client", "user", "utilisateur",
}


def is_valid_name(nom, prenom):
    """Vérifie si le nom/prénom est valide."""
    nom = (nom or "").lower().strip()
    prenom = (prenom or "").lower().strip()

    # 1. Nom ou prénom trop court (1 caractère)
    if len(nom) <= 1 or len(prenom) <= 1:
        return False

    # 2. Noms suspects
    if nom in SUSPICIOUS_NAMES or prenom in SUSPICIOUS_NAMES:
        return False

    # 3. Que des chiffres
    if nom.isdigit() or prenom.isdigit():
        return False

    # 4. Répétition excessive (ex: aaaa, bbbb)
    if len(set(nom)) <= 1 and len(nom) > 1:
        return False
    if len(set(prenom)) <= 1 and len(prenom) > 1:
        return False

    return True


# Mots-clés suspects dans les commentaires
COMMENT_SPAM_KEYWORDS = {"test", "essai", "blabla", "asdf", "qwerty"}


def is_valid_comment(commentaire):
    """Vérifie si le commentaire est valide."""
    if not commentaire:
        return True  # Commentaire vide accepté

    commentaire_lower = commentaire.lower().strip()

    # 1. Commentaire qui contient "test" ou autres mots suspects
    for keyword in COMMENT_SPAM_KEYWORDS:
        if keyword in commentaire_lower:
            return False

    # 2. Commentaire trop court (moins de 3 caractères)
    if len(commentaire_lower) < 3:
        return False

    return True


# ============================================================
# AUTHENTIFICATION
# ============================================================
def get_credentials():
    creds = None

    # Mode GitHub Actions : lire depuis les variables d'environnement
    if os.environ.get("GOOGLE_TOKEN"):
        print("   (Mode GitHub Actions)")
        token_data = json.loads(os.environ["GOOGLE_TOKEN"])
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    # Mode local : lire depuis le fichier
    elif os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            # Sauvegarder le token rafraîchi (mode local uniquement)
            if not os.environ.get("GOOGLE_TOKEN") and os.path.exists(TOKEN_FILE):
                with open(TOKEN_FILE, "w") as token:
                    token.write(creds.to_json())
        else:
            # Première authentification (mode local uniquement)
            if os.environ.get("GOOGLE_TOKEN"):
                raise Exception("Token expiré. Régénérez-le localement.")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            with open(TOKEN_FILE, "w") as token:
                token.write(creds.to_json())

    return creds


# ============================================================
# ÉTAT (sauvegarde de la progression)
# ============================================================
STATE_SHEET_NAME = "_processed_ids"


def load_state(sheets_service=None):
    # Mode local : fichier JSON
    if not os.environ.get("GOOGLE_TOKEN"):
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        return {"processed_ids": []}

    # Mode GitHub Actions : lire depuis Google Sheets
    if sheets_service:
        try:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{STATE_SHEET_NAME}!A:A",
            ).execute()
            ids = [row[0] for row in result.get("values", []) if row]
            return {"processed_ids": ids}
        except Exception:
            # La feuille n'existe pas encore
            return {"processed_ids": []}

    return {"processed_ids": []}


def save_state(state, sheets_service=None):
    # Mode local : fichier JSON
    if not os.environ.get("GOOGLE_TOKEN"):
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
        return

    # Mode GitHub Actions : sauvegarder dans Google Sheets
    if sheets_service:
        try:
            # Vérifier si la feuille existe, sinon la créer
            spreadsheet = sheets_service.spreadsheets().get(
                spreadsheetId=SPREADSHEET_ID
            ).execute()

            sheet_exists = any(
                s["properties"]["title"] == STATE_SHEET_NAME
                for s in spreadsheet.get("sheets", [])
            )

            if not sheet_exists:
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=SPREADSHEET_ID,
                    body={
                        "requests": [{
                            "addSheet": {
                                "properties": {
                                    "title": STATE_SHEET_NAME,
                                    "hidden": True
                                }
                            }
                        }]
                    }
                ).execute()

            # Effacer et réécrire les IDs (garder les 10000 derniers)
            ids = state.get("processed_ids", [])[-10000:]
            if ids:
                # Effacer l'ancienne donnée
                sheets_service.spreadsheets().values().clear(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{STATE_SHEET_NAME}!A:A",
                ).execute()

                # Écrire les nouveaux IDs
                values = [[id_] for id_ in ids]
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{STATE_SHEET_NAME}!A1",
                    valueInputOption="RAW",
                    body={"values": values},
                ).execute()

        except Exception as e:
            print(f"   ⚠️ Erreur sauvegarde état: {e}")


# ============================================================
# EXTRACTION DU NOM DE COMMERCE (depuis le header From)
# ============================================================
def get_sender_name(message):
    headers = message.get("payload", {}).get("headers", [])
    for header in headers:
        if header["name"].lower() == "from":
            from_value = header["value"]
            # Format: "Burger King Thal <noreply@mail.carrd.site>"
            match = re.match(r'"?([^"<]+)"?\s*<', from_value)
            if match:
                return match.group(1).strip()
            return from_value
    return ""


# ============================================================
# EXTRACTION DES DONNÉES D'UN MAIL
# ============================================================
def parse_email_body(body_text):
    result = {
        "genre": "",
        "nom": "",
        "prenom": "",
        "email": "",
        "commentaire": "",
    }

    # Nettoyage du texte
    body_text = body_text.replace("\r\n", "\n").replace("\r", "\n").strip()

    # Pattern principal :
    # {Genre} {Nom} {Prénom} vient de vous suggérer ceci à {Lieu}:
    # {Commentaire}
    # Pour répondre: {Email}
    
    # Pattern flexible pour capturer genre, nom, prénom
    pattern = r"^(Monsieur|Madame|Herr|Frau|Signor|Signora|Mx)\s+(\S+)\s+(.+?)\s+vient de vous sugg[ée]rer ceci [àa]\s*.+?:"
    match = re.match(pattern, body_text, re.IGNORECASE | re.DOTALL)

    if match:
        genre_raw = match.group(1).strip().lower()
        result["genre"] = GENDER_MAP.get(genre_raw, "Autre")
        result["nom"] = match.group(2).strip()
        result["prenom"] = match.group(3).strip()

        # Extraire le reste après le match
        reste = body_text[match.end():].strip()

        # Chercher "Pour répondre:" ou "Pour r\u00e9pondre:" pour séparer commentaire et email
        email_pattern = r"Pour r[ée]pondre:\s*(\S+@\S+)"
        email_match = re.search(email_pattern, reste, re.IGNORECASE)

        if email_match:
            result["commentaire"] = reste[:email_match.start()].strip()
            result["email"] = email_match.group(1).strip()
        else:
            result["commentaire"] = reste.strip()
    else:
        # Tentative de parsing plus souple
        # Chercher au moins l'email
        email_pattern = r"Pour r[ée]pondre:\s*(\S+@\S+)"
        email_match = re.search(email_pattern, body_text, re.IGNORECASE)
        if email_match:
            result["email"] = email_match.group(1).strip()

        result["commentaire"] = f"[FORMAT NON RECONNU] {body_text[:500]}"

    return result


def get_email_body(message):
    payload = message.get("payload", {})

    # Cas simple : corps directement dans le payload
    if "body" in payload and payload["body"].get("data"):
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")

    # Cas multipart
    parts = payload.get("parts", [])
    for part in parts:
        if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
            return base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
        if part.get("parts"):
            for sub_part in part["parts"]:
                if sub_part.get("mimeType") == "text/plain" and sub_part.get("body", {}).get("data"):
                    return base64.urlsafe_b64decode(sub_part["body"]["data"]).decode("utf-8", errors="replace")

    # Fallback : text/html
    for part in parts:
        if part.get("mimeType") == "text/html" and part.get("body", {}).get("data"):
            html = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
            clean = re.sub(r"<[^>]+>", " ", html)
            clean = re.sub(r"\s+", " ", clean).strip()
            return clean

    return ""


# ============================================================
# EXPANSION AUTOMATIQUE DU GOOGLE SHEET
# ============================================================
def ensure_sheet_has_enough_rows(sheets_service, spreadsheet_id, sheet_name, required_rows):
    """Agrandit la feuille si elle n'a pas assez de lignes."""
    # Récupérer les métadonnées de la feuille
    spreadsheet = sheets_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id
    ).execute()

    # Trouver la feuille par son nom
    sheet_id = None
    current_rows = 0
    for sheet in spreadsheet.get("sheets", []):
        if sheet["properties"]["title"] == sheet_name:
            sheet_id = sheet["properties"]["sheetId"]
            current_rows = sheet["properties"]["gridProperties"]["rowCount"]
            break

    if sheet_id is None:
        raise Exception(f"Feuille '{sheet_name}' non trouvée")

    # Ajouter des lignes si nécessaire (avec marge de 1000)
    if required_rows > current_rows:
        rows_to_add = required_rows - current_rows + 1000
        print(f"   📈 Expansion de la feuille : {current_rows} → {current_rows + rows_to_add} lignes")

        request = {
            "requests": [{
                "appendDimension": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "length": rows_to_add
                }
            }]
        }
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=request
        ).execute()
        print(f"   ✅ Feuille agrandie")

    return current_rows


# ============================================================
# PROGRAMME PRINCIPAL
# ============================================================
def main():
    print(f"\n{'='*60}")
    print(f"  Extraction des avis — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*60}\n")

    # Authentification
    print("🔐 Authentification...")
    creds = get_credentials()
    gmail_service = build("gmail", "v1", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    print("   ✅ Connecté\n")

    # Charger l'état
    state = load_state(sheets_service)
    processed_ids = set(state.get("processed_ids", []))
    print(f"📋 Mails déjà traités : {len(processed_ids)}")

    # Récupérer tous les messages de la boîte de réception
    all_messages = []
    next_page_token = None

    print("📥 Récupération de la liste des mails...")
    while True:
        results = gmail_service.users().messages().list(
            userId="me",
            labelIds=["INBOX"],
            maxResults=500,
            pageToken=next_page_token,
        ).execute()

        messages = results.get("messages", [])
        all_messages.extend(messages)
        next_page_token = results.get("nextPageToken")

        print(f"   → {len(all_messages)} mails trouvés...")

        if not next_page_token:
            break

    print(f"\n📊 Total : {len(all_messages)} mails dans la boîte de réception")

    # Filtrer les mails déjà traités
    new_messages = [m for m in all_messages if m["id"] not in processed_ids]
    print(f"🆕 Nouveaux mails à traiter : {len(new_messages)}\n")

    if not new_messages:
        print("✅ Aucun nouveau mail. Fin.")
        return

    # Traiter chaque nouveau mail
    rows_to_add = []
    errors = []
    filtered_count = 0
    seen_emails = set()  # Pour dédoublonnage

    for i, msg_info in enumerate(new_messages):
        try:
            msg = gmail_service.users().messages().get(
                userId="me",
                id=msg_info["id"],
                format="full",
            ).execute()

            # Extraire les données
            nom_commerce = get_sender_name(msg)
            body = get_email_body(msg)
            parsed = parse_email_body(body)

            # Marquer comme traité dans tous les cas
            processed_ids.add(msg_info["id"])

            # Filtrer les emails invalides
            if not is_valid_email(parsed["email"]):
                filtered_count += 1
                continue

            # Dédoublonnage par email
            email_lower = parsed["email"].lower().strip()
            if email_lower in seen_emails:
                filtered_count += 1
                continue
            seen_emails.add(email_lower)

            # Filtrer les noms suspects
            if not is_valid_name(parsed["nom"], parsed["prenom"]):
                filtered_count += 1
                continue

            # Filtrer les commentaires suspects
            if not is_valid_comment(parsed["commentaire"]):
                filtered_count += 1
                continue

            # Colonnes : Nom commerce | Catégorie | Genre | Nom | Prénom | Nom complet | Email | Statut Email | Note | Commentaire
            row = [
                nom_commerce,           # Nom commerce
                "Fast-food",            # Catégorie
                parsed["genre"],        # Genre
                parsed["nom"],          # Nom
                parsed["prenom"],       # Prénom
                "",                     # Nom complet (vide)
                parsed["email"],        # Email
                "Pending",              # Statut Email
                1,                      # Note
                parsed["commentaire"],  # Commentaire
            ]
            rows_to_add.append(row)

            if (i + 1) % 50 == 0:
                print(f"   ⏳ {i + 1}/{len(new_messages)} mails traités...")

        except Exception as e:
            error_msg = f"Erreur mail ID {msg_info['id']}: {str(e)}"
            print(f"   ❌ {error_msg}")
            errors.append(error_msg)

    print(f"\n✅ Extraction terminée : {len(rows_to_add)} mails extraits, {filtered_count} filtrés, {len(errors)} erreurs")

    # Écrire dans Google Sheets
    if rows_to_add:
        print("\n📤 Écriture dans Google Sheets...")

        # Trouver la prochaine ligne vide
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:A",
        ).execute()
        existing_rows = len(result.get("values", []))
        next_row = existing_rows + 1

        # S'assurer que la feuille a assez de lignes
        total_rows_needed = next_row + len(rows_to_add)
        ensure_sheet_has_enough_rows(sheets_service, SPREADSHEET_ID, SHEET_NAME, total_rows_needed)

        # Écrire par lots de 500 pour éviter les timeout
        batch_size = 500
        for start in range(0, len(rows_to_add), batch_size):
            batch = rows_to_add[start:start + batch_size]
            body = {"values": batch}
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!A{next_row + start}",
                valueInputOption="RAW",
                body=body,
            ).execute()
            print(f"   → Lot {start + 1} à {start + len(batch)} écrit")

        print(f"   ✅ {len(rows_to_add)} lignes ajoutées")

    # Sauvegarder l'état
    state["processed_ids"] = list(processed_ids)
    state["last_run"] = datetime.now().isoformat()
    save_state(state, sheets_service)
    print("\n💾 État sauvegardé.")

    # Envoyer la notification
    send_notification(gmail_service, len(rows_to_add), errors)

    print("\n🎉 Terminé !")


# ============================================================
# NOTIFICATION PAR EMAIL
# ============================================================
def send_notification(gmail_service, count, errors=None):
    now = datetime.now().strftime("%d/%m/%Y à %H:%M")

    if count > 0:
        subject = f"Extraction avis : {count} nouveau(x) avis ajouté(s)"
        body_text = f"""Bonjour,

Le script d'extraction s'est exécuté le {now}.

Résumé :
- {count} nouveau(x) avis ajouté(s) au Google Sheet

Lien vers le Google Sheet :
https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit
"""
    else:
        subject = "Extraction avis : aucun nouvel avis"
        body_text = f"""Bonjour,

Le script d'extraction s'est exécuté le {now}.

Aucun nouvel avis à traiter.
"""

    if errors:
        body_text += f"\nAttention — {len(errors)} erreur(s) rencontrée(s) :\n"
        for err in errors[:10]:
            body_text += f"  - {err}\n"
        if len(errors) > 10:
            body_text += f"  ... et {len(errors) - 10} autres erreurs.\n"

    message = MIMEText(body_text)
    message["to"] = NOTIFICATION_EMAIL
    message["subject"] = subject

    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()

    try:
        gmail_service.users().messages().send(
            userId="me",
            body={"raw": raw},
        ).execute()
        print(f"\n📧 Notification envoyée à {NOTIFICATION_EMAIL}")
    except Exception as e:
        print(f"\n⚠️ Impossible d'envoyer la notification : {e}")


if __name__ == "__main__":
    main()
