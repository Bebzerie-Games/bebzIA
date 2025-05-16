import discord
from discord.ext import commands, tasks
import os
import datetime
import json
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import traceback # Pour les logs d'erreur détaillés
import pytz
import dateutil.parser # Importé pour un parsing de date plus robuste

print("DEBUG: Script starting...") # AJOUT DEBUG

# Charger les variables d'environnement
load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
ALLOWED_USER_ID_STR = os.getenv("ALLOWED_USER_ID")
COSMOS_DB_ENDPOINT = os.getenv("COSMOS_DB_ENDPOINT")
COSMOS_DB_KEY = os.getenv("COSMOS_DB_KEY")
DATABASE_NAME = os.getenv("DATABASE_NAME")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
TARGET_CHANNEL_ID_STR = os.getenv("TARGET_CHANNEL_ID")
LOG_CHANNEL_ID_STR = os.getenv("LOG_CHANNEL_ID")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")

print("DEBUG: Env variables loaded.") # AJOUT DEBUG


# --- Configuration Azure OpenAI (Nouvelle API v1.x.x) ---
from openai import AsyncAzureOpenAI, APIError, APIConnectionError, RateLimitError # Importer les exceptions nécessaires

azure_openai_client = None # Variable globale pour le client IA
IS_AZURE_OPENAI_CONFIGURED = False

if AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_KEY and AZURE_OPENAI_DEPLOYMENT_NAME:
    try:
        azure_openai_client = AsyncAzureOpenAI(
            api_version="2023-07-01-preview", # Vérifiez si une version plus récente est recommandée par Azure pour votre modèle
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=AZURE_OPENAI_KEY,
            # Le nom du déploiement (AZURE_OPENAI_DEPLOYMENT_NAME) sera utilisé dans l'appel .create()
        )
        IS_AZURE_OPENAI_CONFIGURED = True
        print("Client AsyncAzureOpenAI initialisé avec succès.")
    except Exception as e:
        print(f"ERREUR CRITIQUE lors de l'initialisation du client AsyncAzureOpenAI: {e}\n{traceback.format_exc()}")
        azure_openai_client = None # S'assurer qu'il est None en cas d'échec
else:
    print("AVERTISSEMENT: Variables d'environnement pour Azure OpenAI manquantes ou incomplètes. Les fonctionnalités IA seront désactivées.")

print("DEBUG: Azure OpenAI init complete.") # AJOUT DEBUG

# --- Définition de send_bot_log_message (avec paramètre source) ---
async def send_bot_log_message(message_content: str, source: str = "BOT"):
    """Envoie un message de log en utilisant l'instance bot principale, avec timestamp de Paris et source."""
    
    now_utc = discord.utils.utcnow()
    paris_tz = pytz.timezone('Europe/Paris')
    now_paris = now_utc.astimezone(paris_tz)
    
    timestamp_discord_display = now_paris.strftime('%Y-%m-%d %H:%M:%S %Z')
    timestamp_stdout_utc_display = now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')
    log_prefix = f"[{source.upper()}]"

    if 'bot' not in globals() or not globals().get('bot').is_ready() or not LOG_CHANNEL_ID:
        print(f"[{timestamp_stdout_utc_display}] {log_prefix} [LOG STDOUT - CANAL/BOT INDISPONSIBLE] {message_content}")
        return

    try:
        log_channel_obj = bot.get_channel(LOG_CHANNEL_ID)
        if log_channel_obj:
            # Tronquer le message si trop long pour Discord (limite 4000 chars dans le contenu d'un message)
            # Garder de la marge pour le préfixe et le code block
            max_discord_len = 3800
            if len(message_content) > max_discord_len:
                message_content = message_content[:max_discord_len] + "\n... (Tronqué car trop long pour Discord)"

            await log_channel_obj.send(f"```\n{log_prefix} {timestamp_discord_display}\n{message_content}\n```")
        else:
            print(f"[{timestamp_stdout_utc_display}] {log_prefix} [LOG STDOUT] Log channel ID {LOG_CHANNEL_ID} non trouvé. Msg: {message_content}")
    except Exception as e:
        print(f"[{timestamp_stdout_utc_display}] {log_prefix} [LOG STDOUT] Erreur envoi log Discord: {e}. Msg: {message_content}")
        print(traceback.format_exc())


# --- Fonction d'analyse IA pour la génération SQL ---
async def get_ai_analysis(user_query: str, requesting_user_name: str) -> str | None:
    """
    Interroge Azure OpenAI pour obtenir une requête SQL Cosmos DB basée sur la question de l'utilisateur.
    Retourne la chaîne de la requête SQL ou None en cas d'échec.
    """
    if not IS_AZURE_OPENAI_CONFIGURED or not azure_openai_client: # Vérification du client aussi
        print("AVERTISSEMENT: Tentative d'appel à get_ai_analysis alors qu'Azure OpenAI n'est pas configuré ou client non initialisé.")
        await send_bot_log_message("Tentative d'appel à l'IA (analyse SQL) alors que la configuration Azure OpenAI est manquante ou a échoué.", source="AI-QUERY-SQL-GEN")
        return None

    # Construction du system_prompt pour la génération SQL (comme précédemment)
    paris_tz = pytz.timezone('Europe/Paris')
    current_time_paris = datetime.datetime.now(paris_tz)
    system_current_time_reference = current_time_paris.strftime("%Y-%m-%d %H:%M:%S %Z")

    system_prompt = f"""
Tu es un assistant IA spécialisé dans la conversion de questions en langage naturel en requêtes SQL optimisées pour Azure Cosmos DB.
Ta tâche est d'analyser la question de l'utilisateur et de générer UNIQUEMENT la requête SQL correspondante pour interroger une base de données Cosmos DB contenant des messages Discord.

L'utilisateur actuel qui pose la question est : {requesting_user_name}

Contexte de la base de données :
- La base de données s'appelle '{DATABASE_NAME}' et le conteneur '{CONTAINER_NAME}'.
- Chaque document dans le conteneur représente un message Discord et a la structure JSON suivante (simplifiée) :
  {{
    "id": "string (identifiant unique du message)",
    "channel_id": "string",
    "author_id": "string",
    "author_name": "string (nom d'utilisateur Discord, ex: 'FlyXOwl')",
    "content": "string (contenu textuel du message)",
    "timestamp_iso": "string (timestamp ISO 8601 UTC, ex: '2023-10-15T12:30:45.123Z')",
    "attachments_count": "integer (nombre de pièces jointes)",
    "reactions_count": "integer (nombre total de réactions)"
  }}
- Le champ `timestamp_iso` est crucial pour les requêtes basées sur le temps. Il est stocké au format ISO 8601 UTC.
- La date et l'heure actuelles de référence (Paris) sont : {system_current_time_reference}.

Instructions pour la génération de la requête :
1.  Ta sortie doit être UNIQUEMENT la requête SQL. Ne fournis aucune explication, aucun texte avant ou après la requête.
2.  Utilise `c` comme alias pour le conteneur (par exemple, `SELECT * FROM c`).
3.  Pour les recherches de texte dans `c.content` (le contenu du message) ou `c.author_name` (le nom d'utilisateur), utilise la fonction `CONTAINS(c.field, "terme", true)` pour des recherches insensibles à la casse.
4.  Pour les dates (champ `c.timestamp_iso`) :
    * Utilise la date et l'heure de référence ({system_current_time_reference}) pour interpréter les références temporelles relatives. Convertis-les en filtres sur `c.timestamp_iso` au format UTC ISO 8601 :
    * Pour les périodes courtes ("aujourd'hui", "hier", "cette semaine", "la semaine dernière", "les N derniers jours/heures") : utilise des plages précises (`>=` et `<=`) ou `STARTSWITH("YYYY-MM-DD")`. Calcule les dates/heures UTC exactes correspondantes.
    * Pour les périodes plus longues ("il y a X mois", "en XXXX", "l'année dernière", "le mois dernier") : utilise `STARTSWITH("YYYY-MM")` pour un mois entier ou `STARTSWITH("YYYY")` pour une année entière. Ne calcule PAS une date précise au jour près.
    * Exemple Court : "messages d'hier" (si date réf 2025-05-16) -> `STARTSWITH(c.timestamp_iso, "2025-05-15")`
    * Exemple Long : "messages de y'a 3 mois" (si date réf en Mai 2025) -> `STARTSWITH(c.timestamp_iso, "2025-02")`
    * Exemple Long : "messages de l'année dernière" (si date réf en 2025) -> `STARTSWITH(c.timestamp_iso, "2024")`
    * Exemple Long : "messages de décembre 2023" -> `STARTSWITH(c.timestamp_iso, "2023-12")`
5.  Pour filtrer par l'auteur d'un message (en utilisant son nom d'utilisateur), utilise IMPÉRATIVEMENT le champ `c.author_name` avec la fonction `CONTAINS`. Lorsque l'utilisateur fait référence à lui-même ("moi", "j'ai envoyé", "mes messages"), utilise son nom d'utilisateur réel qui t'est fourni en début de prompt ("{requesting_user_name}"). Ne fais JAMAIS référence à un champ 'author' non défini à la racine.
6.  Utilise `c.reactions_count` ou `c.attachments_count` si besoin.
7.  Si la question est vague, retourne la chaîne "NO_QUERY_POSSIBLE".
8.  Par défaut, trie par `ORDER BY c.timestamp_iso DESC`.
9.  Pour " combien", utilise `SELECT VALUE COUNT(1) FROM c WHERE ...`.
10. Pour limiter le nombre de résultats retournés (par exemple, "le dernier message", "les 5 messages les plus récents"), utilise la clause `TOP N` (où N est le nombre désiré) **juste après le `SELECT`**.

Exemples (date de référence 2025-05-16 Paris) :
- Utilisateur: "Messages de FlyXOwl hier"
  IA: SELECT * FROM c WHERE CONTAINS(c.author_name, "FlyXOwl", true) AND STARTSWITH(c.timestamp_iso, "2025-05-15") ORDER BY c.timestamp_iso DESC
- Utilisateur: "Combien de messages le 1er janvier 2025 ?"
  IA: SELECT VALUE COUNT(1) FROM c WHERE STARTSWITH(c.timestamp_iso, "2025-01-01T")
- Utilisateur: "le premier message"
  IA: SELECT TOP 1 * FROM c ORDER BY c.timestamp_iso ASC
- Utilisateur: "les 5 derniers messages de FlyXOwl"
  IA: SELECT TOP 5 * FROM c WHERE CONTAINS(c.author_name, "FlyXOwl", true) ORDER BY c.timestamp_iso DESC
- Utilisateur: "messages de airzya d'il y a 3 mois"
  IA: SELECT * FROM c WHERE CONTAINS(c.author_name, "airzya", true) AND STARTSWITH(c.timestamp_iso, "2025-02") ORDER BY c.timestamp_iso DESC
- Utilisateur: "messages que j'ai envoyé hier"
  IA: SELECT * FROM c WHERE CONTAINS(c.author_name, "{requesting_user_name}", true) AND STARTSWITH(c.timestamp_iso, "2025-05-15") ORDER BY c.timestamp_iso DESC
- Utilisateur: "messages de bob"
  IA: SELECT * FROM c WHERE CONTAINS(c.author_name, "bob", true) ORDER BY c.timestamp_iso DESC
- Utilisateur: "Qui a dit 'exemple' pour la première fois ?"
  IA: SELECT TOP 1 c.author_name, c.content, c.timestamp_iso FROM c WHERE CONTAINS(c.content, "exemple", true) ORDER BY c.timestamp_iso ASC
- Utilisateur: "auteur du premier message contenant 'exemple'"
  IA: SELECT TOP 1 c.author_name FROM c WHERE CONTAINS(c.content, "exemple", true) ORDER BY c.timestamp_iso ASC


Question de l'utilisateur :
"""

    try:
        # Appel à l'API Azure OpenAI pour obtenir la requête SQL
        response = await azure_openai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ],
            temperature=0.2,
            max_tokens=250,
            top_p=0.95,
            frequency_penalty=0,
            presence_penalty=0,
            stop=None
        )

        if response.choices and response.choices[0].message and response.choices[0].message.content:
            generated_query = response.choices[0].message.content.strip()
            # print(f"Requête SQL générée par l'IA : {generated_query}") # Déjà loggué dans send_bot_log_message

            if "NO_QUERY_POSSIBLE" in generated_query:
                await send_bot_log_message(f"L'IA a déterminé qu'aucune requête n'est possible pour : '{user_query}'", source="AI-QUERY-SQL-GEN")
                return "NO_QUERY_POSSIBLE"

            if not generated_query.upper().startswith("SELECT"):
                await send_bot_log_message(f"L'IA a retourné une réponse inattendue (non SELECT) : '{generated_query}' pour la question : '{user_query}'", source="AI-QUERY-SQL-GEN")
                return "INVALID_QUERY_FORMAT"

            return generated_query
        else:
            await send_bot_log_message(f"Aucune réponse ou contenu de message valide reçu d'Azure OpenAI pour la question : '{user_query}'.", source="AI-QUERY-SQL-GEN")
            print(f"Aucune réponse ou contenu de message valide reçu d'Azure OpenAI. Réponse complète : {response}")
            return None

    except APIError as e:
        error_message = f"Erreur API Azure OpenAI (SQL Gen) : {e}"
        print(error_message)
        await send_bot_log_message(error_message, source="AI-QUERY-SQL-GEN")
        return None
    except APIConnectionError as e:
        error_message = f"Erreur de connexion Azure OpenAI (SQL Gen) : {e}"
        print(error_message)
        await send_bot_log_message(error_message, source="AI-QUERY-SQL-GEN")
        return None
    except RateLimitError as e:
        error_message = f"Erreur de limite de taux Azure OpenAI (SQL Gen) : {e}. Veuillez vérifier votre quota et votre utilisation."
        print(error_message)
        await send_bot_log_message(error_message, source="AI-QUERY-SQL-GEN")
        return None
    except Exception as e:
        error_message = f"Erreur inattendue lors de l'appel à Azure OpenAI (SQL Gen) : {e}\n{traceback.format_exc()}"
        print(error_message)
        await send_bot_log_message(error_message, source="AI-QUERY-SQL-GEN")
        return None

# --- Fonction d'analyse IA pour la synthèse des messages ---
async def get_ai_summary(messages_list: list[dict]) -> str | None:
    """
    Interroge Azure OpenAI pour obtenir un résumé d'une liste de messages.
    Retourne le texte du résumé ou None en cas d'échec.
    """
    if not IS_AZURE_OPENAI_CONFIGURED or not azure_openai_client:
        await send_bot_log_message("Tentative d'appel à l'IA (Synthèse) alors que la configuration Azure OpenAI est manquante ou a échoué.", source="AI-SUMMARY")
        return None
    
    if not messages_list:
        return "Aucun message à résumer."

    # Formater les messages pour les envoyer à l'IA
    # On limite le nombre de messages pour éviter de dépasser la taille du prompt de l'IA
    MAX_MESSAGES_FOR_SUMMARY = 50 
    formatted_messages = "" # Initialise la chaîne qui contiendra les messages formatés
    paris_tz = pytz.timezone('Europe/Paris')

    # Limiter aux N messages les plus récents (premiers de la liste triée par DESC par défaut)
    messages_to_summarize = messages_list[:MAX_MESSAGES_FOR_SUMMARY]

    # Formater chaque message - ASSURE-TOI QUE LES LIGNES CI-DESSOUS SONT BIEN INDENTÉES
    for item in messages_to_summarize:
        # Les lignes à l'intérieur de cette boucle FOR doivent être indentées d'un niveau supplémentaire
        author = item.get("author_name", "Auteur inconnu")
        # Utilise author_display_name pour le résumé si disponible, sinon author_name
        if item.get("author_display_name"):
             author = item.get("author_display_name")

        timestamp_str = item.get("timestamp_iso")
        content = item.get("content", "")

        date_fmt = "Date inconnue" # Default fallback
        dt_obj = None # Initialize dt_obj

        if timestamp_str:
             try:
                 # Try fromisoformat first
                 dt_obj = datetime.datetime.fromisoformat(timestamp_str)
             except ValueError:
                 # Fallback to dateutil if fromisoformat fails
                 try:
                     dt_obj = dateutil.parser.isoparse(timestamp_str)
                 except Exception as e_parse:
                      # Log parsing error, dt_obj remains None
                      print(f"Erreur parsing date (pour résumé) avec fromisoformat/dateutil pour timestamp '{timestamp_str}' (ID: {item.get('id', 'N/A')}): {e_parse}\n{traceback.format_exc()}")


             if dt_obj: # If parsing was successful
                 try:
                     # Ensure timezone-aware, assume UTC if naive
                     if dt_obj.tzinfo is None:
                         dt_obj = dt_obj.replace(tzinfo=datetime.timezone.utc)

                     # Convert to Paris time
                     dt_paris = dt_obj.astimezone(paris_tz)
                     # Format date for AI input (ISO format is good for consistency)
                     date_fmt = dt_paris.strftime("%Y-%m-%d %H:%M") # Format plus IA-friendly

                 except Exception as e_tz_format:
                     # Log errors during timezone conversion/formatting
                      print(f"Erreur conversion/formatage timezone (pour résumé) pour dt_obj '{dt_obj}' (timestamp '{timestamp_str}', ID: {item.get('id', 'N/A')}): {e_tz_format}\n{traceback.format_exc()}")
                     # date_fmt remains "Date inconnue"


        # Format each message for the AI prompt
        # Inclure la date formatée pour aider l'IA à situer chronologiquement
        formatted_messages += f"[{author}] ({date_fmt}): {content}\n---\n"


    # Le prompt pour l'IA pour la synthèse
    system_prompt = """
Tu es un assistant IA spécialisé dans la synthèse de conversations Discord.
Tu recevras une liste de messages Discord dans un format [NomAuteur] (AAAA-MM-JJ HH:MM): Contenu du message.
Chaque message est séparé par une ligne "---".
Ton objectif est de lire attentivement ces messages et de fournir un résumé concis et cohérent de la discussion qu'ils représentent.
Mets en évidence les sujets principaux, les points clés, et les informations importantes partagées.
Le résumé doit être un texte fluide, en français, et ne doit pas citer les messages textuellement.
Pour les pseudos des utilisateurs donc "author_name" ou "author_display_name", les noms réels des personnes sont : 
Lamerdeoffline ou Lamerde: Luka
hezek112 ou hezekiel : Enzo
FlyXOwl ou Fly: Théo
airzya ou azyria : Vincent
wkda_ledauphin ou ledauphin : Nathan
viv1dvivi ou vivi ou vivihihihi : victoire mais appelle la vivi
will.connect ou will : Justin
bastos0234 ou bastos : Bastien
ttv_yunix ou yunix : Liam
.fantaman ou fantaman : Khelyan
    - Tu peux les tutoyer dans tes réponses.
    - Tu peux les appeler par leur pseudo ou leur nom réel, selon le contexte de la question.
    - Tu peux utiliser les deux noms dans la même réponse si tu le souhaites.
    - Tu peux aussi utiliser des variantes comme "Lamerdeoffline" ou "Lamerde" selon le contexte de la question.
Essaie de toujours donner le nom de la personne qui a envoyé le message a partir de son pseudo ou de son nom réel comme indiqué ci-dessus. ainsi que le lien du message a la fin de ton résumé au format suivant : https://discord.com/channels/{channel_id}/{message_id} (ex: https://discord.com/channels/123456789012345678/123456789012345678)
Essaie de maintenir le résumé relativement court (quelques phrases, idéalement moins de 300 mots).
"""

    user_message = f"Voici les messages à résumer :\n\n---\n{formatted_messages}\n\nRésumé de la discussion :"

    try:
        # Appel à l'API Azure OpenAI pour obtenir le résumé
        response = await azure_openai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            temperature=0.7, # Une température un peu plus élevée pour un résumé plus fluide
            max_tokens=400, # Plus de tokens pour le résumé
            top_p=0.95,
            frequency_penalty=0,
            presence_penalty=0,
            stop=None
        )

        if response.choices and response.choices[0].message and response.choices[0].message.content:
            summary = response.choices[0].message.content.strip()
            await send_bot_log_message(f"Résumé IA généré pour {len(messages_to_summarize)} messages: {summary}", source="AI-SUMMARY")
            return summary
        else:
            await send_bot_log_message(f"Aucune réponse ou contenu valide reçu d'Azure OpenAI pour la synthèse. Réponse complète : {response}", source="AI-SUMMARY")
            print(f"Aucune réponse IA pour synthèse. Réponse complète : {response}")
            return None

    except APIError as e:
        error_message = f"Erreur API Azure OpenAI (Synthèse) : {e}"
        print(error_message)
        await send_bot_log_message(error_message, source="AI-SUMMARY")
        return None
    except APIConnectionError as e:
        error_message = f"Erreur de connexion Azure OpenAI (Synthèse) : {e}"
        print(error_message)
        await send_bot_log_message(error_message, source="AI-SUMMARY")
        return None
    except RateLimitError as e:
        error_message = f"Erreur de limite de taux Azure OpenAI (Synthèse) : {e}. Veuillez vérifier votre quota et votre utilisation."
        print(error_message)
        await send_bot_log_message(error_message, source="AI-SUMMARY")
        return None
    except Exception as e:
        error_message = f"Erreur inattendue lors de l'appel à Azure OpenAI (Synthèse) : {e}\n{traceback.format_exc()}"
        print(error_message)
        await send_bot_log_message(error_message, source="AI-SUMMARY")
        return None


print("DEBUG: AI functions defined.") # AJOUT DEBUG

# --- Configuration des Intents et du Bot ---
print("DEBUG: Initializing Discord Intents and Bot...") # AJOUT DEBUG
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True # Nécessaire pour bot.get_channel et gestion des membres/rôles/pseudos

bot = commands.Bot(command_prefix="!", intents=intents)
print("DEBUG: Discord Bot object created.") # AJOUT DEBUG

# --- Conversion des IDs de canaux ---
print("DEBUG: Converting IDs...") # AJOUT DEBUG
TARGET_CHANNEL_ID = None
LOG_CHANNEL_ID = None
ALLOWED_USER_ID = None

try:
    if TARGET_CHANNEL_ID_STR: TARGET_CHANNEL_ID = int(TARGET_CHANNEL_ID_STR)
    if LOG_CHANNEL_ID_STR: LOG_CHANNEL_ID = int(LOG_CHANNEL_ID_STR)
    # --- Conversion du nouvel ID utilisateur ---
    if ALLOWED_USER_ID_STR: ALLOWED_USER_ID = int(ALLOWED_USER_ID_STR)
    # ---------------------------------------
except ValueError:
    # Cette erreur est critique car les IDs sont essentiels
    print("ERREUR CRITIQUE: Un ID (canal ou utilisateur) n'est pas un entier valide. Vérifiez vos variables d'environnement.")
    # Quitter proprement si les IDs ne peuvent pas être convertis
    import sys
    sys.exit(1)
print("DEBUG: ID conversion complete.") # AJOUT DEBUG

# --- Initialisation du client Cosmos DB ---
print("DEBUG: Initializing Cosmos DB...")
cosmos_client_instance_global = None
container_client = None
if COSMOS_DB_ENDPOINT and COSMOS_DB_KEY and DATABASE_NAME and CONTAINER_NAME:
    try:
        cosmos_client_instance_global = CosmosClient(COSMOS_DB_ENDPOINT, credential=COSMOS_DB_KEY)
        
        # Créer la base de données si elle n'existe pas
        database_client = cosmos_client_instance_global.create_database_if_not_exists(id=DATABASE_NAME)
        print(f"Base de données '{DATABASE_NAME}' prête.")
        
        # Définir le chemin de la clé de partition
        partition_key_path = PartitionKey(path="/id") # Assure-toi que c'est bien ta clé de partition
        
        # Créer le conteneur s'il n'existe pas
        container_client = database_client.create_container_if_not_exists(
            id=CONTAINER_NAME,  # 'id' est correct ici pour le nom du conteneur à créer
            partition_key=partition_key_path,
            offer_throughput=400 # Optionnel, ajuste si besoin
        )
        print(f"Conteneur '{CONTAINER_NAME}' prêt dans la base '{DATABASE_NAME}'.")
        
        print("Connecté et configuré avec Cosmos DB avec succès.")

    except TypeError as te:
        print(f"ERREUR CRITIQUE (TypeError) lors de l'initialisation de Cosmos DB (probablement un mauvais argument pour une méthode client): {te}\n{traceback.format_exc()}")
        container_client = None
    except exceptions.CosmosHttpResponseError as e_http:
        # Capturer les erreurs HTTP spécifiques de Cosmos (permissions, etc.)
        print(f"ERREUR CRITIQUE (CosmosHttpResponseError) lors de l'initialisation de Cosmos DB: {e_http.message}\n{traceback.format_exc()}")
        container_client = None
    except Exception as e:
        print(f"ERREUR CRITIQUE lors de l'initialisation de Cosmos DB: {e}\n{traceback.format_exc()}")
        container_client = None
else:
    print("AVERTISSEMENT: Variables d'environnement pour Cosmos DB manquantes. La récupération des messages et les recherches seront désactivées.")
print("DEBUG: Cosmos DB init complete.")


# --- Fonctions utilitaires ---
def format_message_to_json(message: discord.Message):
    return {
        "id": str(message.id),
        "message_id_int": message.id,
        "channel_id": str(message.channel.id),
        "guild_id": str(message.guild.id) if message.guild else None,
        "author_id": str(message.author.id),
        # Utilise author_name qui est le nom d'utilisateur unique (ex: flyxowl)
        "author_name": message.author.name, 
        "author_discriminator": message.author.discriminator, # Discriminator est souvent '0000' maintenant
        # Utilise display_name qui est le pseudo du serveur, ou à défaut le nom d'utilisateur
        "author_display_name": message.author.display_name,
        "author_bot": message.author.bot,
        "content": message.content,
        "timestamp_iso": message.created_at.isoformat() + "Z",
        "timestamp_unix": int(message.created_at.timestamp()),
        "attachments": [{"id": str(att.id), "filename": att.filename, "url": att.url, "content_type": att.content_type, "size": att.size} for att in message.attachments],
        "attachments_count": len(message.attachments),
        "embeds": [embed.to_dict() for embed in message.embeds],
        "reactions": [{"emoji": str(reaction.emoji), "count": reaction.count} for reaction in message.reactions],
        "reactions_count": sum(reaction.count for reaction in message.reactions),
        "edited_timestamp_iso": message.edited_at.isoformat() + "Z" if message.edited_at else None,
    }


async def main_message_fetch_logic():
    log_source = "AUTO-FETCH"
    if not container_client:
        await send_bot_log_message("ERREUR: Client Cosmos DB non initialisé. Abandon.", source=log_source)
        return
    if not TARGET_CHANNEL_ID:
        await send_bot_log_message("ERREUR: TARGET_CHANNEL_ID non configuré. Abandon.", source=log_source)
        return

    await send_bot_log_message(f"Démarrage tâche pour canal ID: {TARGET_CHANNEL_ID}.", source=log_source)

    channel_to_fetch = None # Initialise la variable à None

    # --- Tenter de récupérer l'objet canal ---
    try:
        # Utilise get_channel en premier (cache)
        channel_to_fetch = bot.get_channel(TARGET_CHANNEL_ID)
        if not channel_to_fetch:
            # Si non trouvé en cache, fetch via API
            await send_bot_log_message(f"Canal {TARGET_CHANNEL_ID} non trouvé en cache, tentative de fetch via API.", source=log_source)
            # S'assurer que le bot est prêt avant de fetch
            await bot.wait_until_ready() # Attendre que le cache soit potentiellement rempli
            channel_to_fetch = await bot.fetch_channel(TARGET_CHANNEL_ID)

        # Vérifier si l'objet canal a été obtenu avec succès
        if not channel_to_fetch:
             await send_bot_log_message(f"ERREUR CRITIQUE: Canal {TARGET_CHANNEL_ID} introuvable ou inaccessible après bot.get_channel et bot.fetch_channel. Abandon.", source=log_source)
             return # Quitter la fonction si le canal n'est pas trouvé

    except discord.NotFound:
        await send_bot_log_message(f"ERREUR: Canal {TARGET_CHANNEL_ID} introuvable lors de la tentative de fetch.", source=log_source)
        return # Quitter la fonction en cas d'erreur spécifique
    except discord.Forbidden:
        await send_bot_log_message(f"ERREUR: Permissions insuffisantes pour accéder au canal {TARGET_CHANNEL_ID}.", source=log_source)
        return # Quitter la fonction en cas d'erreur spécifique
    except Exception as e: # Capturer toute autre erreur pendant la récupération du canal
        error_details = traceback.format_exc()
        await send_bot_log_message(f"ERREUR inattendue lors de la récupération du canal {TARGET_CHANNEL_ID}:\n{error_details}", source=log_source)
        return # Quitter la fonction en cas d'erreur

    # --- Si on arrive ici, channel_to_fetch est défini et valide ---

    # --- Configuration de la date de départ pour le fetch ---
    # Définis cette variable sur une date HISTORIQUE pour forcer une récupération complète.
    # METS CETTE LIGNE EN COMMENTAIRE (ou sur None) APRES AVOIR FAIT LA RECUPERATION HISTORIQUE !
    # force_historical_fetch_from = datetime.datetime(2022, 10, 1, tzinfo=datetime.timezone.utc) # <-- COMMENTEZ OU DEFINISSEZ SUR NONE
    force_historical_fetch_from = None # <-- DÉFINI À NONE POUR REPRENDRE LE COMPORTEMENT NORMAL

    if force_historical_fetch_from:
        # Si on force une date historique, on utilise celle-ci.
        after_date = force_historical_fetch_from
        await send_bot_log_message(f"Récupération historique FORCÉE depuis {after_date.isoformat()}.", source=log_source)
    else:
        # Logique normale : reprendre après le dernier message stocké en base.
        last_message_timestamp_unix = 0
        try:
            # Assurer que le bot est prêt avant de faire la requête MAX
            await bot.wait_until_ready() 
            query = f"SELECT VALUE MAX(c.timestamp_unix) FROM c WHERE c.channel_id = '{str(TARGET_CHANNEL_ID)}'"
            results = list(container_client.query_items(query=query, enable_cross_partition_query=True))
            if results and results[0] is not None:
                last_message_timestamp_unix = results[0]
        except Exception as e:
            await send_bot_log_message(f"AVERTISSEMENT: Récupération MAX timestamp échouée: {e}. Utilisation de la période par défaut.", source=log_source)

        if last_message_timestamp_unix > 0:
            # On ajoute 0.001 seconde pour ne pas refetcher le dernier message déjà stocké
            after_date = datetime.datetime.fromtimestamp(last_message_timestamp_unix + 0.001, tz=datetime.timezone.utc)
            await send_bot_log_message(f"Dernier msg stocké: {after_date.isoformat()}. Récupération après.", source=log_source)
        else:
            # Si aucun message précédent trouvé (base vide ou erreur), remonter sur les 14 derniers jours.
            after_date = discord.utils.utcnow() - datetime.timedelta(days=14)
            await send_bot_log_message(f"Aucun msg précédent/erreur MAX timestamp. Récupération depuis {after_date.isoformat()} (par défaut).", source=log_source)
    # --- Fin de la configuration de la date de départ ---

    # --- Début de la boucle de fetch Discord ---
    # La méthode history va chercher les messages APRÈS `after_date`, en commençant par les plus anciens si oldest_first=True
    await send_bot_log_message(f"Lancement de channel_to_fetch.history(after={after_date.isoformat()}, oldest_first=True, limit=None)..", source=log_source)

    fetched_in_pass = 0

    try:
        # Utilise une limite pour le fetch total pour éviter de bloquer indéfiniment sur un très grand historique
        # Ou s'assurer que le "limit=None" de history est bien géré par discord.py avec pagination
        # Pour l'instant, on garde limit=None pour tenter de tout avoir depuis after_date
        async for message in channel_to_fetch.history(limit=None, after=after_date, oldest_first=True):
            fetched_in_pass +=1
            message_json = format_message_to_json(message)
            item_id = message_json["id"]

            # --- Logique de récupération et stockage avec upsert_item ---
            try:
                container_client.upsert_item(body=message_json)
            except exceptions.CosmosHttpResponseError as e_upsert:
                 await send_bot_log_message(f"ERREUR Cosmos (upsert) msg {item_id}: {e_upsert.message}", source=log_source)
            except Exception as e:
                 await send_bot_log_message(f"ERREUR Inattendue pendant upsert msg {item_id}: {e}\n{traceback.format_exc()}", source=log_source)

            
            if fetched_in_pass > 0 and fetched_in_pass % 500 == 0:
                await send_bot_log_message(f"Progression: {fetched_in_pass} messages traités/mis à jour...", source=log_source)

        # --- Résumé de la tâche ---
        summary_message = (f"Récupération terminée pour '{channel_to_fetch.name}'.\n"
                            f"- Messages traités/mis à jour : {fetched_in_pass}")
        await send_bot_log_message(summary_message, source=log_source)

    except Exception as e: # Capturer les exceptions pendant la boucle de fetch history
        error_details = traceback.format_exc()
        await send_bot_log_message(f"ERREUR MAJEURE pendant boucle fetch history:\n{error_details}", source=log_source)


# --- Définition de la tâche en boucle ---
@tasks.loop(hours=12)
async def scheduled_message_fetch():
    log_source = "SCHEDULER"
    print(f"[{discord.utils.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] Tâche de récupération planifiée démarrée.")
    await send_bot_log_message("Démarrage tâche récupération planifiée.", source=log_source)
    try:
        await main_message_fetch_logic()
    except Exception as e:
        error_details = traceback.format_exc()
        await send_bot_log_message(f"ERREUR non gérée dans scheduled_message_fetch:\n{error_details}", source=log_source)
    await send_bot_log_message("Tâche récupération planifiée terminée.", source=log_source)

@scheduled_message_fetch.before_loop
async def before_scheduled_fetch():
    log_source = "SCHEDULER"
    # S'assurer que le bot est prêt avant de démarrer la boucle
    await bot.wait_until_ready() 
    print("Bot prêt, la tâche de récupération planifiée peut commencer.")
    valid_config = True
    if not TARGET_CHANNEL_ID:
        print("ERREUR CRITIQUE: TARGET_CHANNEL_ID non configuré. Tâche annulée.")
        await send_bot_log_message("ERREUR CRITIQUE: TARGET_CHANNEL_ID non configuré. Tâche désactivée.", source=log_source)
        valid_config = False
        # Utiliser bot.loop.create_task(scheduled_message_fetch.cancel()) si on est dans un contexte async
        # Ici, on est dans before_loop, un simple cancel() devrait suffire si valid_config est False
        if not valid_config:
            scheduled_message_fetch.cancel()
            await send_bot_log_message("Tâche récupération annulée (config invalide).", source=log_source)
            return # Sortir de la fonction si la config est invalide
            
    if not LOG_CHANNEL_ID:
        print("AVERTISSEMENT: LOG_CHANNEL_ID non configuré. Logs sur STDOUT.")
    
    # La vérification de container_client est cruciale aussi
    if not container_client:
         print("ERREUR CRITIQUE: Client Cosmos DB non initialisé. Tâche annulée.")
         await send_bot_log_message("ERREUR CRITIQUE: Cosmos DB non initialisé. Tâche désactivée.", source=log_source)
         valid_config = False
         # Annuler et sortir si Cosmos DB n'est pas prêt
         if not valid_config:
             scheduled_message_fetch.cancel()
             await send_bot_log_message("Tâche récupération annulée (Cosmos DB invalide).", source=log_source)
             return # Sortir

    # Si toutes les vérifications passent, la tâche peut démarrer
    print("Tâche de récupération planifiée : Vérifications initiales passées.") # Log de succès des vérifications initiales

# --- Événements du Bot ---
@bot.event
async def on_ready():
    log_source = "CORE-BOT"
    print(f'{bot.user} s\'est connecté à Discord!')
    print(f"ID du bot : {bot.user.id}")
    await send_bot_log_message(f"Bot {bot.user.name} connecté et prêt.", source=log_source)
    
    # Démarrer la tâche planifiée APRES les vérifications initiales (qui se font dans before_loop)
    # on_ready est le bon endroit pour initier le DÉMARRAGE si before_loop n'annule pas
    # La logique de before_loop gère déjà les conditions d'annulation.
    # Si on_ready est atteint et before_loop n'a pas annulé, c'est que les conditions de base sont OK.
    # On peut donc démarrer la tâche ici.
    print("on_ready atteint. Tentative de démarrage de la tâche de récupération si elle ne tourne pas déjà...")
    if not scheduled_message_fetch.is_running():
         # L'appel à .start() appelle lui-même before_loop avant la première exécution
         scheduled_message_fetch.start() 
         await send_bot_log_message("Tâche de récupération planifiée initiée via on_ready.", source="SCHEDULER")
    else:
         print("Tâche de récupération déjà en cours d'exécution (probablement relancée par Heroku).")
         await send_bot_log_message("Tâche de récupération déjà en cours (relance Heroku?).", source="SCHEDULER")

# --- Commandes du Bot ---
# Les décorateurs @bot.command() enregistrent les commandes auprès de l'objet bot
@bot.command(name='ping')
async def ping(ctx):
    # print(f"DEBUG: Commande !ping reçue de {ctx.author.name}") # Optionnel: debug
    await ctx.send(f'Pong! Latence: {round(bot.latency * 1000)}ms')

@bot.command(name='ask', help="Pose une question sur l'historique des messages. L'IA tentera de trouver les messages pertinents.")
async def ask_command(ctx, *, question: str):
    log_source = "ASK-CMD"
    # print(f"DEBUG: Commande !ask reçue de {ctx.author.name} avec question: '{question}'") # Optionnel: debug

    # --- Nouvelle vérification de l'utilisateur ---
    # Vérifie si ALLOWED_USER_ID est défini ET si l'ID de l'auteur n'est PAS l'ID autorisé
    if ALLOWED_USER_ID is not None and ctx.author.id != ALLOWED_USER_ID:
        # print(f"Tentative d'utiliser !ask par utilisateur non autorisé: {ctx.author.name} (ID: {ctx.author.id})") # Moins de logs direct sur Heroku pour ça
        await ctx.send("Désolé, cette commande est actuellement restreinte.")
        return
    # --- Fin de la vérification ---


    await ctx.send(f"Recherche en cours pour : \"{question}\" ... Veuillez patienter.")

    # --- Déplacer les vérifications AVANT l'appel à l'IA ou la base de données ---
    # Vérifie si l'IA est configurée et le client est prêt
    if not IS_AZURE_OPENAI_CONFIGURED or not azure_openai_client:
        await ctx.send("Désolé, le module d'intelligence artificielle n'est pas correctement configuré ou démarré.")
        await send_bot_log_message(f"Cmd !ask par {ctx.author.name} échouée : Azure OpenAI non configuré/client non prêt.", source=log_source)
        return

    # Vérifie si la connexion à la base de données est active
    if not container_client:
        await ctx.send("Désolé, la connexion à la base de données n'est pas active.")
        await send_bot_log_message(f"Cmd !ask par {ctx.author.name} échouée : Client Cosmos DB non initialisé.", source=log_source)
        return
    # --- Fin des vérifications ---

    # --- Étape 1 : Générer la requête SQL avec l'IA ---
    generated_sql_query = await get_ai_analysis(question, ctx.author.name) 
    
    # Gérer les cas où l'IA n'a pas pu générer de requête ou a retourné un format invalide
    if not generated_sql_query:
        await ctx.send("Je n'ai pas réussi à interpréter votre question (erreur interne/communication IA).")
        # get_ai_analysis a déjà loggué l'erreur spécifique
        return

    if generated_sql_query == "NO_QUERY_POSSIBLE":
        await ctx.send("Je suis désolé, je ne peux pas formuler de requête avec cette question. Essayez de reformuler.")
        return # Log déjà fait par get_ai_analysis
    
    if generated_sql_query == "INVALID_QUERY_FORMAT":
        await ctx.send("L'IA a retourné une réponse dans un format inattendu.")
        return # Log déjà fait par get_ai_analysis

    # Log la requête générée (utile pour le debug, n'est pas envoyé à l'utilisateur)
    await send_bot_log_message(f"Cmd !ask par {ctx.author.name} pour '{question}'. Requête IA : {generated_sql_query}", source=log_source)
    
    # --- Commente ou supprime les messages de debug envoyés à l'utilisateur (Requête générée) ---
    # if len(generated_sql_query) < 1900 : # Pour éviter un message de debug trop long
    #     await ctx.send(f"Requête générée (débogage) : ```sql\n{generated_sql_query}\n```")
    # --------------------------------------------------------------------

    # --- Étape 2 : Exécuter la requête sur Cosmos DB ---
    try:
        # Vérifie si le bot est prêt avant de faire la requête BD
        await bot.wait_until_ready()
        query_to_execute = generated_sql_query
        items = list(container_client.query_items(
            query=query_to_execute,
            enable_cross_partition_query=True
        ))

        # --- Étape 3 : Gérer les résultats ---
        if not items:
            await ctx.send("J'ai exécuté la recherche, mais aucun message ne correspond à votre demande.")
            await send_bot_log_message(f"Aucun résultat Cosmos DB pour : {query_to_execute}", source=log_source)
            return

        # Si la requête est un COUNT, afficher le résultat directement
        if query_to_execute.upper().startswith("SELECT VALUE COUNT(1)"):
            count = items[0] if items else 0
            await ctx.send(f"J'ai trouvé {count} message(s) correspondant à votre demande.")
            await send_bot_log_message(f"Résultat COUNT pour '{query_to_execute}': {count}", source=log_source)
            return

        # Si la requête a retourné des messages (pas un COUNT), informer l'utilisateur et générer un résumé
        await ctx.send(f"J'ai trouvé {len(items)} message(s). Génération du résumé...") 

        # --- Étape 4 : Générer le résumé avec l'IA ---
        ai_summary = await get_ai_summary(items)

        # --- Étape 5 : Afficher le résumé ou un message de secours ---
        if ai_summary:
            # Affiche le résumé retourné par l'IA
            # Utilise une Embed pour une meilleure présentation
            embed = discord.Embed(
                title=f"Résumé des messages trouvés ({len(items)} messages)",
                description=ai_summary,
                color=discord.Color.blue(), # Choisis une couleur
                timestamp=discord.utils.utcnow() # Ajoute un timestamp
            )
            embed.set_footer(text=f"Requête : \"{question}\"") # Ajoute la requête en footer

            # Vérifie si l'affichage Embed est possible dans le canal, sinon utilise un message texte simple
            try:
                await ctx.send(embed=embed)
            except discord.Forbidden:
                 await ctx.send(f"**Résumé des messages trouvés ({len(items)} messages) :**\n{ai_summary}\n*(Impossible d'envoyer une Embed dans ce canal)*")
            except Exception as e_embed:
                 await ctx.send(f"**Résumé des messages trouvés ({len(items)} messages) :**\n{ai_summary}\n*(Erreur lors de l'envoi de l'Embed : {e_embed})*")
                 print(f"Erreur lors de l'envoi de l'Embed: {e_embed}\n{traceback.format_exc()}")


            await send_bot_log_message(f"Synthèse réussie pour {len(items)} messages. Résultat envoyé.", source=log_source)
        else:
            # Si la synthèse a échoué
            await ctx.send("Désolé, je n'ai pas réussi à générer de résumé pour ces messages.")
            # Optionnel: Afficher les premiers messages en secours si la synthèse échoue (réactive le code précédent si tu veux ça)
            await send_bot_log_message(f"Synthèse échouée pour {len(items)} messages.", source=log_source)


    except exceptions.CosmosHttpResponseError as e:
        error_msg_user = "Une erreur s'est produite lors de la recherche dans la base de données."
        # Gérer les erreurs spécifiques de Cosmos DB (requête trop complexe/gourmande)
        if "Query exceeded memory limit" in str(e) or "Query exceeded maximum time limit" in str(e):
            error_msg_user = "Votre demande a généré une requête trop complexe/gourmande. Soyez plus spécifique (ex: période plus courte)."
        elif "Request rate is large" in str(e):
             error_msg_user = "La base de données est temporairement surchargée. Veuillez réessayer plus tard."
        
        await ctx.send(error_msg_user)
        await send_bot_log_message(f"Erreur Cosmos DB pour '{generated_sql_query}': {e}\n{traceback.format_exc()}", source=log_source)
        print(f"Erreur Cosmos DB: {e}")
    except Exception as e:
        # Gérer toute autre exception inattendue pendant l'exécution de la requête ou la synthèse
        await ctx.send("Une erreur inattendue s'est produite lors de l'exécution de la requête ou de la synthèse.")
        await send_bot_log_message(f"Erreur inattendue dans ask_command (exécution/synthèse) pour '{generated_sql_query}': {e}\n{traceback.format_exc()}", source=log_source)
        print(f"Erreur inattendue: {e}")


# --- Démarrage du Bot ---
print("DEBUG: Reaching main execution block.") # AJOUT DEBUG
if __name__ == "__main__":
    print("DEBUG: Inside __main__ block.") # AJOUT DEBUG
    if DISCORD_BOT_TOKEN:
        print("DEBUG: Tentative de démarrer le bot...")
        try:
            # La méthode run() est bloquante et maintient le bot en marche
            bot.run(DISCORD_BOT_TOKEN)
            print("DEBUG: bot.run() terminé. (Ce message ne devrait PAS s'afficher pour un bot en marche continue)") # Si ce message apparaît, le bot s'est arrêté prématurément
        except Exception as e:
            # Log les exceptions qui pourraient empêcher bot.run() de démarrer ou le faire planter
            print(f"ERREUR: Exception lors de bot.run(): {e}\n{traceback.format_exc()}")
            # Quitter le processus si bot.run() échoue de manière inattendue
            import sys
            sys.exit(1) # Quitter avec un statut d'erreur
    else:
        print("ERREUR: DISCORD_BOT_TOKEN non trouvé. Le bot ne peut pas démarrer. Vérifiez vos variables d'environnement Heroku.")
        # Quitter si le token n'est pas trouvé
        import sys
        sys.exit(1) # Quitter avec un statut d'erreur