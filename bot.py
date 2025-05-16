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

print("DEBUG: Script starting...")

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

print("DEBUG: Env variables loaded.")

# --- MODIFICATION : Définir comme constante globale ---
MAX_MESSAGES_FOR_SUMMARY_CONFIG = 100 
# ----------------------------------------------------

from openai import AsyncAzureOpenAI, APIError, APIConnectionError, RateLimitError

azure_openai_client = None
IS_AZURE_OPENAI_CONFIGURED = False

if AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_KEY and AZURE_OPENAI_DEPLOYMENT_NAME:
    try:
        azure_openai_client = AsyncAzureOpenAI(
            api_version="2023-07-01-preview",
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=AZURE_OPENAI_KEY,
        )
        IS_AZURE_OPENAI_CONFIGURED = True
        print("Client AsyncAzureOpenAI initialisé avec succès.")
    except Exception as e:
        print(f"ERREUR CRITIQUE lors de l'initialisation du client AsyncAzureOpenAI: {e}\n{traceback.format_exc()}")
        azure_openai_client = None
else:
    print("AVERTISSEMENT: Variables d'environnement pour Azure OpenAI manquantes ou incomplètes. Les fonctionnalités IA seront désactivées.")

print("DEBUG: Azure OpenAI init complete.")

async def send_bot_log_message(message_content: str, source: str = "BOT"):
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
            max_discord_len = 3800
            if len(message_content) > max_discord_len:
                message_content = message_content[:max_discord_len] + "\n... (Tronqué car trop long pour Discord)"
            await log_channel_obj.send(f"```\n{log_prefix} {timestamp_discord_display}\n{message_content}\n```")
        else:
            print(f"[{timestamp_stdout_utc_display}] {log_prefix} [LOG STDOUT] Log channel ID {LOG_CHANNEL_ID} non trouvé. Msg: {message_content}")
    except Exception as e:
        print(f"[{timestamp_stdout_utc_display}] {log_prefix} [LOG STDOUT] Erreur envoi log Discord: {e}. Msg: {message_content}")
        print(traceback.format_exc())

async def get_ai_analysis(user_query: str, requesting_user_name: str) -> str | None:
    if not IS_AZURE_OPENAI_CONFIGURED or not azure_openai_client:
        print("AVERTISSEMENT: Tentative d'appel à get_ai_analysis alors qu'Azure OpenAI n'est pas configuré ou client non initialisé.")
        await send_bot_log_message("Tentative d'appel à l'IA (analyse SQL) alors que la configuration Azure OpenAI est manquante ou a échoué.", source="AI-QUERY-SQL-GEN")
        return None

    paris_tz = pytz.timezone('Europe/Paris')
    # Utilisation de la date fournie pour la référence, sinon la date actuelle
    # Pour les tests, on peut fixer une date de référence ici. Pour la prod, datetime.datetime.now(paris_tz)
    # current_time_paris = datetime.datetime.strptime("2025-05-16 14:40:12", "%Y-%m-%d %H:%M:%S").replace(tzinfo=paris_tz)
    current_time_paris = datetime.datetime.now(paris_tz) # Utiliser l'heure actuelle en production
        
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
    "guild_id": "string (identifiant du serveur Discord)",
    "author_id": "string",
    "author_name": "string (nom d'utilisateur Discord, ex: 'FlyXOwl')",
    "author_display_name": "string (pseudo sur le serveur)",
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
3.  Pour les recherches de texte dans `c.content`, `c.author_name`, utilise `CONTAINS(c.field, "terme", true)` pour des recherches insensibles à la casse.
4.  Pour les dates (champ `c.timestamp_iso`) :
    * Utilise la date et l'heure de référence ({system_current_time_reference}) pour interpréter les références temporelles relatives. Convertis-les en filtres sur `c.timestamp_iso` au format UTC ISO 8601.
    * "aujourd'hui": `STARTSWITH(c.timestamp_iso, "{current_time_paris.strftime('%Y-%m-%d')}")`
    * "hier": `STARTSWITH(c.timestamp_iso, "{(current_time_paris - datetime.timedelta(days=1)).strftime('%Y-%m-%d')}")`
    * "cette semaine" (Lundi à Dimanche, Lundi étant weekday 0): 
        Le premier jour de cette semaine (Lundi) est `{(current_time_paris - datetime.timedelta(days=current_time_paris.weekday())).strftime('%Y-%m-%d')}T00:00:00.000Z`.
        Le dernier jour de cette semaine (Dimanche) est `{(current_time_paris + datetime.timedelta(days=(6 - current_time_paris.weekday()))).strftime('%Y-%m-%d')}T23:59:59.999Z`.
        Donc, la condition est `c.timestamp_iso >= "{(current_time_paris - datetime.timedelta(days=current_time_paris.weekday())).strftime('%Y-%m-%d')}T00:00:00.000Z" AND c.timestamp_iso <= "{(current_time_paris + datetime.timedelta(days=(6 - current_time_paris.weekday()))).strftime('%Y-%m-%d')}T23:59:59.999Z"`
    * "la semaine dernière": Calcule les dates du Lundi au Dimanche de la semaine précédente.
    * "il y a X mois", "en XXXX", "l'année dernière", "le mois dernier": `STARTSWITH("YYYY-MM")` ou `STARTSWITH("YYYY")`.
5.  Pour filtrer par auteur (nom d'utilisateur), utilise `CONTAINS(c.author_name, "...", true)`. Si l'utilisateur dit "moi", utilise "{requesting_user_name}".
6.  Si la question est vague, retourne la chaîne "NO_QUERY_POSSIBLE".
7.  **Sélection des champs :** Sélectionne TOUJOURS au minimum `c.id`, `c.channel_id`, `c.guild_id`, `c.author_name`, `c.author_display_name`, `c.content`, et `c.timestamp_iso`. Si "combien", utilise `SELECT VALUE COUNT(1) FROM c WHERE ...`.
8.  **Ordre de tri :** Par défaut `ORDER BY c.timestamp_iso DESC`. Pour "premier message" ou "plus ancien", utilise `ASC`.
9.  Pour "combien", utilise `SELECT VALUE COUNT(1) FROM c WHERE ...`.
10. Pour limiter le nombre de résultats ("le dernier message", "les 5 messages"), utilise `TOP N` après `SELECT`.

Exemples (date de référence {system_current_time_reference}) :
- Utilisateur: "Messages de FlyXOwl hier"
  IA: SELECT c.id, c.channel_id, c.guild_id, c.author_name, c.author_display_name, c.content, c.timestamp_iso FROM c WHERE CONTAINS(c.author_name, "FlyXOwl", true) AND STARTSWITH(c.timestamp_iso, "{(current_time_paris - datetime.timedelta(days=1)).strftime('%Y-%m-%d')}") ORDER BY c.timestamp_iso DESC
- Utilisateur: "Combien de messages le 1er janvier 2025 ?"
  IA: SELECT VALUE COUNT(1) FROM c WHERE STARTSWITH(c.timestamp_iso, "2025-01-01T")
- Utilisateur: "le premier message contenant 'salut'"
  IA: SELECT TOP 1 c.id, c.channel_id, c.guild_id, c.author_name, c.author_display_name, c.content, c.timestamp_iso FROM c WHERE CONTAINS(c.content, "salut", true) ORDER BY c.timestamp_iso ASC
- Utilisateur: "les 50 premiers messages"
  IA: SELECT TOP 50 c.id, c.channel_id, c.guild_id, c.author_name, c.author_display_name, c.content, c.timestamp_iso FROM c ORDER BY c.timestamp_iso ASC
- Utilisateur: "messages de cette semaine"
  IA: SELECT c.id, c.channel_id, c.guild_id, c.author_name, c.author_display_name, c.content, c.timestamp_iso FROM c WHERE c.timestamp_iso >= "{(current_time_paris - datetime.timedelta(days=current_time_paris.weekday())).strftime('%Y-%m-%d')}T00:00:00.000Z" AND c.timestamp_iso <= "{(current_time_paris + datetime.timedelta(days=(6 - current_time_paris.weekday()))).strftime('%Y-%m-%d')}T23:59:59.999Z" ORDER BY c.timestamp_iso DESC

Question de l'utilisateur :
"""
    try:
        response = await azure_openai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ],
            temperature=0.2,
            max_tokens=350, # Un peu plus de marge pour les dates calculées dans les exemples
            top_p=0.95,
            frequency_penalty=0,
            presence_penalty=0,
            stop=None
        )
        if response.choices and response.choices[0].message and response.choices[0].message.content:
            generated_query = response.choices[0].message.content.strip()
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
        print(error_message); await send_bot_log_message(error_message, source="AI-QUERY-SQL-GEN"); return None
    except APIConnectionError as e:
        error_message = f"Erreur de connexion Azure OpenAI (SQL Gen) : {e}"
        print(error_message); await send_bot_log_message(error_message, source="AI-QUERY-SQL-GEN"); return None
    except RateLimitError as e:
        error_message = f"Erreur de limite de taux Azure OpenAI (SQL Gen) : {e}."
        print(error_message); await send_bot_log_message(error_message, source="AI-QUERY-SQL-GEN"); return None
    except Exception as e:
        error_message = f"Erreur inattendue lors de l'appel à Azure OpenAI (SQL Gen) : {e}\n{traceback.format_exc()}"
        print(error_message); await send_bot_log_message(error_message, source="AI-QUERY-SQL-GEN"); return None

async def get_ai_summary(messages_list: list[dict]) -> str | None:
    if not IS_AZURE_OPENAI_CONFIGURED or not azure_openai_client:
        await send_bot_log_message("Tentative d'appel à l'IA (Synthèse) alors que la configuration Azure OpenAI est manquante ou a échoué.", source="AI-SUMMARY")
        return None
    if not messages_list:
        return "Aucun message à résumer."

    # Utiliser la constante globale définie en haut du script
    messages_to_summarize = messages_list[:MAX_MESSAGES_FOR_SUMMARY_CONFIG]
    
    formatted_messages = ""
    paris_tz = pytz.timezone('Europe/Paris')

    first_message_id_for_link = None
    first_channel_id_for_link = None
    first_guild_id_for_link = None

    for i, item in enumerate(messages_to_summarize):
        author = item.get("author_name", "Auteur inconnu")
        author_display = item.get("author_display_name")
        if author_display:
             author = author_display

        timestamp_str = item.get("timestamp_iso")
        content = item.get("content", "")
        
        message_id = item.get("id")
        channel_id = item.get("channel_id")
        guild_id = item.get("guild_id")

        if i == 0 and message_id and channel_id and guild_id:
            first_message_id_for_link = message_id
            first_channel_id_for_link = channel_id
            first_guild_id_for_link = guild_id

        date_fmt = "Date inconnue"
        if timestamp_str:
            dt_obj = None
            try: dt_obj = datetime.datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            except ValueError:
                try: dt_obj = dateutil.parser.isoparse(timestamp_str)
                except Exception: pass 
            if dt_obj:
                try:
                    if dt_obj.tzinfo is None: dt_obj = dt_obj.replace(tzinfo=datetime.timezone.utc)
                    dt_paris = dt_obj.astimezone(paris_tz)
                    date_fmt = dt_paris.strftime("%Y-%m-%d %H:%M")
                except Exception: date_fmt = timestamp_str 
            else: date_fmt = "Date inconnue" 
        else: date_fmt = "Date inconnue"
        
        formatted_messages += f"[{author}] ({date_fmt}): {content}\n---\n"

    await send_bot_log_message(f"DEBUG Lien: guild_id={first_guild_id_for_link}, chan_id={first_channel_id_for_link}, msg_id={first_message_id_for_link}", source="AI-SUMMARY-DEBUG")

    system_prompt = f"""
Tu es un assistant IA spécialisé dans la synthèse de conversations Discord.
Tu recevras une liste d'environ {len(messages_to_summarize)} messages Discord dans un format [NomAuteur] (AAAA-MM-JJ HH:MM): Contenu du message.
Chaque message est séparé par une ligne "---".
Ton objectif est de lire attentivement ces messages et de fournir un résumé concis et cohérent de la discussion qu'ils représentent, **en te basant UNIQUEMENT ET EXCLUSIVEMENT sur le contenu textuel et les auteurs des messages qui te sont fournis dans la section "Voici les messages à résumer :".**
**Ne mentionne AUCUN participant ni AUCUN sujet qui ne soit pas explicitement présent et identifiable dans les messages que tu analyses pour CE résumé spécifique.**
**Ignore toute connaissance préalable sur les membres du groupe qui ne serait pas confirmée par les messages actuels.**
Mets en évidence les sujets principaux, les points clés, et les informations importantes partagées DANS CES MESSAGES.
Le résumé doit être un texte fluide, en français, et ne doit pas citer les messages textuellement.

Contexte du groupe d'amis "La bebzerie" (échanges depuis 2022) et correspondances pseudos/prénoms (UTILISE CES INFOS SEULEMENT SI LES PSEUDOS SONT DANS LES MESSAGES FOURNIS):
Lamerdeoffline/Lamerde: Luka ( discord id : 292657007779905547)
hezek112/hezekiel: Enzo ( discord id : 957249973064446032)
FlyXOwl/Fly: Théo ( discord id : 532526003407290381)
airzya/azyria: Vincent ( discord id : 503242253272350741)
wkda_ledauphin/ledauphin: Nathan ( discord id : 728678866654330921)
viv1dvivi/vivi/vivihihihi: Victoire mais appelle-la Vivi ( discord id : 813047875591340072)
will.connect/will: Justin ( discord id : 525001001170763797)
bastos0234/bastos: Bastien ( discord id : 1150107575031963649)
ttv_yunix/yunix: Liam ( discord id : 735088185771819079)
.fantaman/fantaman: Khelyan ( discord id : 675351685521997875)

Tu peux tutoyer et utiliser prénoms ou pseudos, **mais seulement pour les personnes dont les messages sont effectivement présents dans la liste fournie pour ce résumé.**

À la fin de ton résumé, SI ET SEULEMENT SI les trois IDs (serveur, canal, message) pour le premier message pertinent t'ont été fournis ci-dessous et ne sont pas 'Non fourni', inclus un lien vers ce message.
L'ID du serveur (guild) du premier message pertinent est : {first_guild_id_for_link if first_guild_id_for_link else 'Non fourni'}
L'ID du canal du premier message pertinent est : {first_channel_id_for_link if first_channel_id_for_link else 'Non fourni'}
L'ID du premier message pertinent est : {first_message_id_for_link if first_message_id_for_link else 'Non fourni'}
Si ces TROIS IDs sont fournis et valides (pas 'Non fourni'), construis le lien comme suit : https://discord.com/channels/{first_guild_id_for_link}/{first_channel_id_for_link}/{first_message_id_for_link}
N'invente pas de lien si les IDs ne sont pas explicitement disponibles.
Ne mentionne pas les IDs dans le résumé lui-même, seulement le lien formaté à la fin s'il est applicable. Par exemple: [Lien vers le message](URL_CONSTRUITE)

Essaie de maintenir le résumé relativement court (quelques phrases, idéalement environ 300 mots mais tu peux aller sur les 1000-2000 mots pour des requetes avec beaucoup de messages).
"""
    user_message = f"Voici les messages à résumer :\n\n---\n{formatted_messages}\n\nRésumé de la discussion :"

    try:
        response = await azure_openai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            temperature=0.3, 
            max_tokens=1500, 
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
            await send_bot_log_message(f"Aucune réponse ou contenu valide reçu d'Azure OpenAI pour la synthèse.", source="AI-SUMMARY")
            print(f"Aucune réponse IA pour synthèse. Réponse complète : {response}")
            return None
    except APIError as e:
        error_message = f"Erreur API Azure OpenAI (Synthèse) : {e}"
        print(error_message); await send_bot_log_message(error_message, source="AI-SUMMARY"); return None
    except APIConnectionError as e:
        error_message = f"Erreur de connexion Azure OpenAI (Synthèse) : {e}"
        print(error_message); await send_bot_log_message(error_message, source="AI-SUMMARY"); return None
    except RateLimitError as e:
        error_message = f"Erreur de limite de taux Azure OpenAI (Synthèse) : {e}."
        # Spécifier le type d'erreur dans le message utilisateur peut être utile
        if "context_length_exceeded" in str(e):
             # Ce cas devrait être moins fréquent maintenant avec MAX_MESSAGES_FOR_SUMMARY_CONFIG plus bas
             # Mais on le garde au cas où.
            await send_bot_log_message(f"Erreur de limite de taux (Synthèse) - Dépassement de la longueur du contexte: {e}", source="AI-SUMMARY")
            # On pourrait vouloir retourner un message spécifique ou None pour que ask_command le gère
            # Pour l'instant, on logue et on retourne None, ce qui mènera à "Désolé, je n'ai pas réussi à générer de résumé..."
            return None 
        print(error_message); await send_bot_log_message(error_message, source="AI-SUMMARY"); return None
    except Exception as e:
        error_message = f"Erreur inattendue lors de l'appel à Azure OpenAI (Synthèse) : {e}\n{traceback.format_exc()}"
        print(error_message); await send_bot_log_message(error_message, source="AI-SUMMARY"); return None

print("DEBUG: AI functions defined.")

# ... (le reste du code reste identique jusqu'à ask_command) ...
print("DEBUG: Initializing Discord Intents and Bot...")
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents)
print("DEBUG: Discord Bot object created.")

print("DEBUG: Converting IDs...")
TARGET_CHANNEL_ID = None
LOG_CHANNEL_ID = None
ALLOWED_USER_ID = None
try:
    if TARGET_CHANNEL_ID_STR: TARGET_CHANNEL_ID = int(TARGET_CHANNEL_ID_STR)
    if LOG_CHANNEL_ID_STR: LOG_CHANNEL_ID = int(LOG_CHANNEL_ID_STR)
    if ALLOWED_USER_ID_STR: ALLOWED_USER_ID = int(ALLOWED_USER_ID_STR)
except ValueError:
    print("ERREUR CRITIQUE: Un ID (canal ou utilisateur) n'est pas un entier valide. Vérifiez vos variables d'environnement.")
    import sys; sys.exit(1)
print("DEBUG: ID conversion complete.")

print("DEBUG: Initializing Cosmos DB...")
cosmos_client_instance_global = None
container_client = None
if COSMOS_DB_ENDPOINT and COSMOS_DB_KEY and DATABASE_NAME and CONTAINER_NAME:
    try:
        cosmos_client_instance_global = CosmosClient(COSMOS_DB_ENDPOINT, credential=COSMOS_DB_KEY)
        database_client = cosmos_client_instance_global.create_database_if_not_exists(id=DATABASE_NAME)
        print(f"Base de données '{DATABASE_NAME}' prête.")
        partition_key_path = PartitionKey(path="/id") 
        container_client = database_client.create_container_if_not_exists(
            id=CONTAINER_NAME,
            partition_key=partition_key_path,
            offer_throughput=400
        )
        print(f"Conteneur '{CONTAINER_NAME}' prêt dans la base '{DATABASE_NAME}'.")
        print("Connecté et configuré avec Cosmos DB avec succès.")
    except TypeError as te:
        print(f"ERREUR CRITIQUE (TypeError) lors de l'initialisation de Cosmos DB: {te}\n{traceback.format_exc()}")
        container_client = None
    except exceptions.CosmosHttpResponseError as e_http:
        print(f"ERREUR CRITIQUE (CosmosHttpResponseError) lors de l'initialisation de Cosmos DB: {e_http.message}\n{traceback.format_exc()}")
        container_client = None
    except Exception as e:
        print(f"ERREUR CRITIQUE lors de l'initialisation de Cosmos DB: {e}\n{traceback.format_exc()}")
        container_client = None
else:
    print("AVERTISSEMENT: Variables d'environnement pour Cosmos DB manquantes. La récupération des messages et les recherches seront désactivées.")
print("DEBUG: Cosmos DB init complete.")

def format_message_to_json(message: discord.Message):
    return {
        "id": str(message.id),
        "message_id_int": message.id,
        "channel_id": str(message.channel.id),
        "guild_id": str(message.guild.id) if message.guild else None,
        "author_id": str(message.author.id),
        "author_name": message.author.name, 
        "author_discriminator": message.author.discriminator,
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
        await send_bot_log_message("ERREUR: Client Cosmos DB non initialisé. Abandon.", source=log_source); return
    if not TARGET_CHANNEL_ID:
        await send_bot_log_message("ERREUR: TARGET_CHANNEL_ID non configuré. Abandon.", source=log_source); return

    await send_bot_log_message(f"Démarrage tâche pour canal ID: {TARGET_CHANNEL_ID}.", source=log_source)
    channel_to_fetch = None
    try:
        channel_to_fetch = bot.get_channel(TARGET_CHANNEL_ID)
        if not channel_to_fetch:
            await send_bot_log_message(f"Canal {TARGET_CHANNEL_ID} non trouvé en cache, tentative de fetch via API.", source=log_source)
            await bot.wait_until_ready()
            channel_to_fetch = await bot.fetch_channel(TARGET_CHANNEL_ID)
        if not channel_to_fetch:
             await send_bot_log_message(f"ERREUR CRITIQUE: Canal {TARGET_CHANNEL_ID} introuvable. Abandon.", source=log_source); return
    except discord.NotFound:
        await send_bot_log_message(f"ERREUR: Canal {TARGET_CHANNEL_ID} introuvable (NotFound).", source=log_source); return
    except discord.Forbidden:
        await send_bot_log_message(f"ERREUR: Permissions insuffisantes pour canal {TARGET_CHANNEL_ID}.", source=log_source); return
    except Exception as e:
        await send_bot_log_message(f"ERREUR inattendue récup canal {TARGET_CHANNEL_ID}:\n{traceback.format_exc()}", source=log_source); return

    force_historical_fetch_from = None
    if force_historical_fetch_from:
        after_date = force_historical_fetch_from
        await send_bot_log_message(f"Récupération historique FORCÉE depuis {after_date.isoformat()}.", source=log_source)
    else:
        last_message_timestamp_unix = 0
        try:
            await bot.wait_until_ready() 
            query = f"SELECT VALUE MAX(c.timestamp_unix) FROM c WHERE c.channel_id = '{str(TARGET_CHANNEL_ID)}'"
            results = list(container_client.query_items(query=query, enable_cross_partition_query=True))
            if results and results[0] is not None: last_message_timestamp_unix = results[0]
        except Exception as e:
            await send_bot_log_message(f"AVERTISSEMENT: Récupération MAX timestamp échouée: {e}. Utilisation période défaut.", source=log_source)
        if last_message_timestamp_unix > 0:
            after_date = datetime.datetime.fromtimestamp(last_message_timestamp_unix + 0.001, tz=datetime.timezone.utc)
            await send_bot_log_message(f"Dernier msg stocké: {after_date.isoformat()}. Récupération après.", source=log_source)
        else:
            after_date = discord.utils.utcnow() - datetime.timedelta(days=14)
            await send_bot_log_message(f"Aucun msg précédent/erreur MAX timestamp. Récupération depuis {after_date.isoformat()} (défaut).", source=log_source)

    await send_bot_log_message(f"Lancement channel.history(after={after_date.isoformat()}, oldest_first=True, limit=None)..", source=log_source)
    fetched_in_pass = 0
    try:
        async for message in channel_to_fetch.history(limit=None, after=after_date, oldest_first=True):
            fetched_in_pass +=1
            message_json = format_message_to_json(message)
            item_id = message_json["id"]
            try:
                container_client.upsert_item(body=message_json)
            except exceptions.CosmosHttpResponseError as e_upsert:
                 await send_bot_log_message(f"ERREUR Cosmos (upsert) msg {item_id}: {e_upsert.message}", source=log_source)
            except Exception as e:
                 await send_bot_log_message(f"ERREUR Inattendue upsert msg {item_id}: {e}\n{traceback.format_exc()}", source=log_source)
            if fetched_in_pass > 0 and fetched_in_pass % 500 == 0:
                await send_bot_log_message(f"Progression: {fetched_in_pass} messages traités/mis à jour...", source=log_source)
        summary_message = (f"Récupération terminée pour '{channel_to_fetch.name}'.\n- Messages traités/mis à jour : {fetched_in_pass}")
        await send_bot_log_message(summary_message, source=log_source)
    except Exception as e:
        await send_bot_log_message(f"ERREUR MAJEURE pendant boucle fetch history:\n{traceback.format_exc()}", source=log_source)

@tasks.loop(hours=12)
async def scheduled_message_fetch():
    log_source = "SCHEDULER"
    print(f"[{discord.utils.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] Tâche récupération planifiée démarrée.")
    await send_bot_log_message("Démarrage tâche récupération planifiée.", source=log_source)
    try:
        await main_message_fetch_logic()
    except Exception as e:
        await send_bot_log_message(f"ERREUR non gérée dans scheduled_message_fetch:\n{traceback.format_exc()}", source=log_source)
    await send_bot_log_message("Tâche récupération planifiée terminée.", source=log_source)

@scheduled_message_fetch.before_loop
async def before_scheduled_fetch():
    log_source = "SCHEDULER"
    await bot.wait_until_ready() 
    print("Bot prêt, la tâche de récupération planifiée peut commencer.")
    valid_config = True
    if not TARGET_CHANNEL_ID:
        print("ERREUR CRITIQUE: TARGET_CHANNEL_ID non configuré. Tâche annulée.")
        await send_bot_log_message("ERREUR CRITIQUE: TARGET_CHANNEL_ID non configuré. Tâche désactivée.", source=log_source)
        valid_config = False
    if not container_client: 
         print("ERREUR CRITIQUE: Client Conteneur Cosmos DB non initialisé. Tâche annulée.")
         await send_bot_log_message("ERREUR CRITIQUE: Conteneur Cosmos DB non initialisé. Tâche désactivée.", source=log_source)
         valid_config = False
    
    if not valid_config: 
        scheduled_message_fetch.cancel()
        await send_bot_log_message("Tâche récupération annulée (config invalide ou Cosmos DB non prêt).", source=log_source)
        return
            
    if not LOG_CHANNEL_ID:
        print("AVERTISSEMENT: LOG_CHANNEL_ID non configuré. Logs sur STDOUT.")
    print("Tâche de récupération planifiée : Vérifications initiales passées.")

@bot.event
async def on_ready():
    log_source = "CORE-BOT"
    print(f'{bot.user} s\'est connecté à Discord!')
    print(f"ID du bot : {bot.user.id}")
    await send_bot_log_message(f"Bot {bot.user.name} connecté et prêt.", source=log_source)
    print("on_ready atteint. Tentative de démarrage de la tâche de récupération si elle ne tourne pas déjà...")
    if not scheduled_message_fetch.is_running():
         scheduled_message_fetch.start() 
         await send_bot_log_message("Tâche de récupération planifiée initiée via on_ready.", source="SCHEDULER")
    else:
         print("Tâche de récupération déjà en cours d'exécution.")
         await send_bot_log_message("Tâche de récupération déjà en cours.", source="SCHEDULER")

@bot.command(name='ping')
async def ping(ctx):
    await ctx.send(f'Pong! Latence: {round(bot.latency * 1000)}ms')

@bot.command(name='ask', help="Pose une question sur l'historique des messages. L'IA tentera de trouver les messages pertinents.")
async def ask_command(ctx, *, question: str):
    log_source = "ASK-CMD"
    if ALLOWED_USER_ID is not None and ctx.author.id != ALLOWED_USER_ID:
        await ctx.send("Désolé, cette commande est actuellement restreinte."); return
    await ctx.send(f"Recherche en cours pour : \"{question}\" ... Veuillez patienter.")

    if not IS_AZURE_OPENAI_CONFIGURED or not azure_openai_client:
        await ctx.send("Désolé, le module d'intelligence artificielle n'est pas correctement configuré ou démarré.")
        await send_bot_log_message(f"Cmd !ask par {ctx.author.name} échouée : Azure OpenAI non configuré/client non prêt.", source=log_source); return
    if not container_client:
        await ctx.send("Désolé, la connexion à la base de données n'est pas active.")
        await send_bot_log_message(f"Cmd !ask par {ctx.author.name} échouée : Client Cosmos DB non initialisé.", source=log_source); return

    generated_sql_query = await get_ai_analysis(question, ctx.author.name) 
    
    if not generated_sql_query:
        await ctx.send("Je n'ai pas réussi à interpréter votre question (erreur interne/communication IA)."); return
    if generated_sql_query == "NO_QUERY_POSSIBLE":
        await ctx.send("Je suis désolé, je ne peux pas formuler de requête avec cette question. Essayez de reformuler."); return
    if generated_sql_query == "INVALID_QUERY_FORMAT":
        await ctx.send("L'IA a retourné une réponse dans un format inattendu."); return

    await send_bot_log_message(f"Cmd !ask par {ctx.author.name} pour '{question}'. Requête IA : {generated_sql_query}", source=log_source)
    
    try:
        await bot.wait_until_ready()
        query_to_execute = generated_sql_query
        items = list(container_client.query_items(query=query_to_execute, enable_cross_partition_query=True))

        if not items:
            await ctx.send("J'ai exécuté la recherche, mais aucun message ne correspond à votre demande.")
            await send_bot_log_message(f"Aucun résultat Cosmos DB pour : {query_to_execute}", source=log_source); return

        if query_to_execute.upper().startswith("SELECT VALUE COUNT(1)"):
            count = items[0] if items else 0
            await ctx.send(f"J'ai trouvé {count} message(s) correspondant à votre demande.")
            await send_bot_log_message(f"Résultat COUNT pour '{query_to_execute}': {count}", source=log_source); return

        await ctx.send(f"J'ai trouvé {len(items)} message(s). Génération du résumé...") 
        ai_summary = await get_ai_summary(items) 

        if ai_summary:
            embed = discord.Embed(
                title=f"Résumé des messages trouvés ({len(items)} messages)",
                description=ai_summary, 
                color=discord.Color.blue(), 
                timestamp=discord.utils.utcnow()
            )
            embed.set_footer(text=f"Requête : \"{question}\"")
            try:
                await ctx.send(embed=embed)
            except discord.Forbidden:
                 await ctx.send(f"**Résumé ({len(items)} msgs):**\n{ai_summary}\n*(Pas de perm Embed)*")
            except Exception as e_embed:
                 await ctx.send(f"**Résumé ({len(items)} msgs):**\n{ai_summary}\n*(Erreur Embed: {e_embed})*")
                 print(f"Erreur Embed: {e_embed}\n{traceback.format_exc()}")
            # --- MODIFICATION : Utiliser la constante globale pour le log ---
            await send_bot_log_message(f"Synthèse réussie pour {len(items)} messages (résumé basé sur les {min(len(items), MAX_MESSAGES_FOR_SUMMARY_CONFIG)} premiers). Résultat envoyé.", source=log_source)
        else:
            await ctx.send("Désolé, je n'ai pas réussi à générer de résumé pour ces messages.")
            # Le log d'échec de la synthèse est déjà fait dans get_ai_summary si une exception API survient
            # Ajouter un log ici si get_ai_summary retourne None sans exception API (ex: liste de messages vide après filtrage)
            if not ai_summary : # S'assurer que le log n'est pas redondant si get_ai_summary a déjà loggué une erreur API
                await send_bot_log_message(f"Synthèse retournée comme None pour {len(items)} messages (raison non-API, ex: messages_to_summarize vide).", source=log_source)

    except exceptions.CosmosHttpResponseError as e:
        error_msg_user = "Une erreur s'est produite lors de la recherche dans la base de données."
        if "Query exceeded memory limit" in str(e) or "Query exceeded maximum time limit" in str(e):
            error_msg_user = "Votre demande a généré une requête trop complexe. Soyez plus spécifique."
        elif "Request rate is large" in str(e):
             error_msg_user = "Base de données temporairement surchargée. Réessayez plus tard."
        await ctx.send(error_msg_user)
        await send_bot_log_message(f"Erreur Cosmos DB pour '{generated_sql_query}': {e}\n{traceback.format_exc()}", source=log_source); print(f"Erreur Cosmos DB: {e}")
    except Exception as e:
        await ctx.send("Une erreur inattendue s'est produite.")
        await send_bot_log_message(f"Erreur inattendue dans ask_cmd pour '{generated_sql_query}': {e}\n{traceback.format_exc()}", source=log_source); print(f"Erreur inattendue: {e}")

print("DEBUG: Reaching main execution block.")
if __name__ == "__main__":
    print("DEBUG: Inside __main__ block.")
    if DISCORD_BOT_TOKEN:
        print("DEBUG: Tentative de démarrer le bot...")
        try:
            bot.run(DISCORD_BOT_TOKEN)
            print("DEBUG: bot.run() terminé.")
        except Exception as e:
            print(f"ERREUR: Exception lors de bot.run(): {e}\n{traceback.format_exc()}")
            import sys; sys.exit(1)
    else:
        print("ERREUR: DISCORD_BOT_TOKEN non trouvé. Le bot ne peut pas démarrer.")
        import sys; sys.exit(1)