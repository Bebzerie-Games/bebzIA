import discord
from discord.ext import commands, tasks
import os
import datetime
import json
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import traceback 
import pytz
import dateutil.parser 
import sys 
import collections 
import re # Ajouté pour l'extraction des causes de filtrage

print("DEBUG: Script starting...")

load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
ALLOWED_USER_IDS_STR = os.getenv("ALLOWED_USER_IDS") 
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

MAX_MESSAGES_FOR_SUMMARY_CONFIG = 100 
GLOBAL_ASK_COMMAND_LOGS = collections.deque(maxlen=50)

from openai import AsyncAzureOpenAI, APIError, APIConnectionError, RateLimitError

azure_openai_client = None
IS_AZURE_OPENAI_CONFIGURED = False

if AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_KEY and AZURE_OPENAI_DEPLOYMENT_NAME:
    try:
        azure_openai_client = AsyncAzureOpenAI(
            api_version="2023-07-01-preview", # Assurez-vous que cette version est compatible avec les détails de content_filter_result
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

LOG_CHANNEL_ID_VAR_FOR_SEND = None

async def send_bot_log_message(message_content: str, source: str = "BOT", send_to_discord_channel: bool = False, is_openai_filter_log: bool = False):
    now_utc = discord.utils.utcnow()
    timestamp_utc_display = now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')
    log_prefix = f"[{source.upper()}]"
    
    console_message = f"[{timestamp_utc_display}] {log_prefix} {message_content}"
    print(console_message)

    source_upper = source.upper()
    if source_upper.startswith("ASK-CMD") or \
       source_upper.startswith("AI-QUERY-SQL-GEN") or \
       source_upper.startswith("AI-SUMMARY") or \
       source_upper == "AI-TOKEN-USAGE":
        
        log_entry_for_caca = f"{timestamp_utc_display} {log_prefix} {message_content}"
        max_single_log_entry_len = 500 
        if len(log_entry_for_caca) > max_single_log_entry_len:
            log_entry_for_caca = log_entry_for_caca[:max_single_log_entry_len - 20] + "... (entry truncated)"
        GLOBAL_ASK_COMMAND_LOGS.append(log_entry_for_caca)

    if send_to_discord_channel and LOG_CHANNEL_ID_VAR_FOR_SEND and 'bot' in globals() and bot.is_ready():
        try:
            log_channel_obj = bot.get_channel(LOG_CHANNEL_ID_VAR_FOR_SEND)
            if log_channel_obj:
                discord_message_content = message_content
                if is_openai_filter_log: 
                    discord_message_content = f"❌ {message_content}"
                
                full_log_message_for_discord = f"{timestamp_utc_display} {log_prefix}\n{discord_message_content}"
                
                max_discord_len = 1900 
                if len(full_log_message_for_discord) > max_discord_len:
                    chars_to_remove = len(full_log_message_for_discord) - max_discord_len + 25 
                    if chars_to_remove > 0 :
                         original_content_len = len(discord_message_content)
                         discord_message_content = discord_message_content[:max(0, original_content_len - chars_to_remove)] + "... (Tronqué)"
                    full_log_message_for_discord = f"{timestamp_utc_display} {log_prefix}\n{discord_message_content}"

                await log_channel_obj.send(f"```\n{full_log_message_for_discord}\n```")
            else:
                print(f"[{timestamp_utc_display}] [SEND_LOG_WARN] Log channel ID {LOG_CHANNEL_ID_VAR_FOR_SEND} non trouvé pour envoi Discord.")
        except Exception as e:
            print(f"[{timestamp_utc_display}] [SEND_LOG_ERROR] Erreur envoi log Discord: {e}\n{traceback.format_exc()}")

def _extract_openai_filter_details(e: APIError) -> str:
    """Tente d'extraire les détails du filtrage de contenu d'une APIError."""
    filter_type_detected = "type de filtre inconnu"
    detailed_error_message = str(e)

    try:
        if hasattr(e, 'body') and e.body and isinstance(e.body, dict) and 'error' in e.body:
            error_obj = e.body['error']
            # Message d'erreur principal pour le log
            detailed_error_message = f"Code: {error_obj.get('code')}, Message: {error_obj.get('message', str(e))}"

            # Vérifier si c'est une erreur de filtrage de contenu
            is_content_filter_error = error_obj.get('code') == 'content_filter' or \
                                     (error_obj.get('innererror') and error_obj['innererror'].get('code') == 'ResponsibleAIPolicyViolation')

            if is_content_filter_error:
                filtered_categories_details = []
                # Tenter d'extraire les catégories spécifiques du champ content_filter_result
                if error_obj.get('innererror') and 'content_filter_result' in error_obj['innererror']:
                    content_filter_res = error_obj['innererror']['content_filter_result']
                    if isinstance(content_filter_res, dict): # S'assurer que c'est un dictionnaire
                        for category, result in content_filter_res.items():
                            if isinstance(result, dict) and result.get('filtered') is True:
                                severity = result.get('severity', 'N/A')
                                filtered_categories_details.append(f"{category} (sévérité: {severity})")
                
                # Si aucune catégorie spécifique n'est trouvée via la structure, essayer de parser le message
                if not filtered_categories_details and error_obj.get('message'):
                    match = re.search(r"Causes:\s*\[([^\]]+)\]", error_obj.get('message', ''))
                    if match:
                        causes_str = match.group(1)
                        parsed_causes = [cause.strip().strip("'\"") for cause in causes_str.split(',')]
                        if parsed_causes:
                            filtered_categories_details = parsed_causes
                
                if filtered_categories_details:
                    filter_type_detected = ", ".join(filtered_categories_details)
                else: # Fallback si aucune catégorie spécifique n'est extraite
                    filter_type_detected = error_obj.get('code', 'filtrage de contenu générique')
                
                return f"Violation de filtre de contenu. Type(s) détecté(s): {filter_type_detected}. (Détail API: {detailed_error_message})"
    except Exception as parse_exc:
        print(f"DEBUG: Exception lors du parsing de APIError pour les détails du filtre: {parse_exc}")
        # Retourner l'erreur originale si le parsing échoue
        return f"Erreur API OpenAI (parsing des détails du filtre échoué): {str(e)}"
        
    return f"Erreur API OpenAI : {detailed_error_message}"


async def get_ai_analysis(user_query: str, requesting_user_name_with_id: str) -> str | None:
    log_source_prefix = "AI-QUERY-SQL-GEN"
    if not IS_AZURE_OPENAI_CONFIGURED or not azure_openai_client:
        await send_bot_log_message(f"Tentative d'appel à l'IA (analyse SQL) alors que la configuration Azure OpenAI est manquante ou a échoué. Demandé par: {requesting_user_name_with_id}", source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True)
        return None

    paris_tz = pytz.timezone('Europe/Paris')
    current_time_paris = datetime.datetime.now(paris_tz) 
    system_current_time_reference = current_time_paris.strftime("%Y-%m-%d %H:%M:%S %Z")
    requesting_user_name_for_prompt = requesting_user_name_with_id.split(" (ID:")[0]

    system_prompt = f"""
Tu es un assistant IA spécialisé dans la conversion de questions en langage naturel en requêtes SQL optimisées pour Azure Cosmos DB.
Ta tâche est d'analyser la question de l'utilisateur et de générer UNIQUEMENT la requête SQL correspondante pour interroger une base de données Cosmos DB contenant des messages Discord.
L'utilisateur actuel qui pose la question est : {requesting_user_name_for_prompt}
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
5.  Pour filtrer par auteur (nom d'utilisateur), utilise `CONTAINS(c.author_name, "...", true)`. Si l'utilisateur dit "moi", utilise "{requesting_user_name_for_prompt}".
6.  Si la question est vague, retourne la chaîne "NO_QUERY_POSSIBLE".
7.  **Sélection des champs :** Sélectionne TOUJOURS au minimum `c.id`, `c.channel_id`, `c.guild_id`, `c.author_name`, `c.author_display_name`, `c.content`, et `c.timestamp_iso`. Si "combien", utilise `SELECT VALUE COUNT(1) FROM c WHERE ...`.
8.  **Ordre de tri :** Par défaut `ORDER BY c.timestamp_iso DESC`. Pour "premier message" ou "plus ancien", utilise `ASC`.
9.  Pour "combien", utilise `SELECT VALUE COUNT(1) FROM c WHERE ...`.
10. Pour limiter le nombre de résultats ("le dernier message", "les 5 messages"), utilise `TOP N` après `SELECT`.
"""
    try:
        response = await azure_openai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ],
            temperature=0.2, max_tokens=350, top_p=0.95,
            frequency_penalty=0, presence_penalty=0, stop=None
        )
        
        if response.usage:
            await send_bot_log_message(
                f"Utilisation des tokens (SQL Gen): Prompt={response.usage.prompt_tokens}, Completion={response.usage.completion_tokens}, Total={response.usage.total_tokens}\nDemandé par: {requesting_user_name_with_id} pour la question: '{user_query}'",
                source="AI-TOKEN-USAGE" 
            )

        if response.choices and response.choices[0].message and response.choices[0].message.content:
            generated_query = response.choices[0].message.content.strip()
            if "NO_QUERY_POSSIBLE" in generated_query:
                await send_bot_log_message(f"L'IA a déterminé 'NO_QUERY_POSSIBLE' pour : '{user_query}'. Demandé par: {requesting_user_name_with_id}", source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True)
                return "NO_QUERY_POSSIBLE"
            if not generated_query.upper().startswith("SELECT"):
                await send_bot_log_message(f"L'IA a retourné un format invalide (non SELECT) : '{generated_query}' pour : '{user_query}'. Demandé par: {requesting_user_name_with_id}", source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True)
                return "INVALID_QUERY_FORMAT"
            return generated_query
        else:
            await send_bot_log_message(f"Aucune réponse ou contenu de message valide d'Azure OpenAI pour : '{user_query}'. Demandé par: {requesting_user_name_with_id}", source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True)
            return None
    except APIError as e:
        error_details = _extract_openai_filter_details(e)
        error_message_to_log = f"{error_details}. Demandé par: {requesting_user_name_with_id} pour la question: '{user_query}'"
        await send_bot_log_message(error_message_to_log, source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True); return None
    except APIConnectionError as e:
        error_message = f"Erreur de connexion Azure OpenAI (SQL Gen) : {e}. Demandé par: {requesting_user_name_with_id}"
        await send_bot_log_message(error_message, source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True); return None
    except RateLimitError as e:
        error_message = f"Erreur de limite de taux Azure OpenAI (SQL Gen) : {e}. Demandé par: {requesting_user_name_with_id}"
        await send_bot_log_message(error_message, source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True); return None
    except Exception as e:
        error_message = f"Erreur inattendue lors de l'appel à Azure OpenAI (SQL Gen) : {e}\n{traceback.format_exc()}\nDemandé par: {requesting_user_name_with_id}"
        await send_bot_log_message(error_message, source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True); return None


async def get_ai_summary(messages_list: list[dict], requesting_user_name_with_id: str) -> str | None:
    log_source_prefix = "AI-SUMMARY"
    if not IS_AZURE_OPENAI_CONFIGURED or not azure_openai_client:
        await send_bot_log_message(f"Tentative d'appel à l'IA (Synthèse) alors que la configuration Azure OpenAI est manquante ou a échoué. Demandé par: {requesting_user_name_with_id}", source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True)
        return None
    if not messages_list:
        return "Aucun message à résumer."

    # ... (le reste de la préparation des messages pour get_ai_summary est identique)
    messages_to_summarize = messages_list[:MAX_MESSAGES_FOR_SUMMARY_CONFIG]
    formatted_messages = ""
    paris_tz = pytz.timezone('Europe/Paris')
    first_message_id_for_link, first_channel_id_for_link, first_guild_id_for_link = None, None, None

    for i, item in enumerate(messages_to_summarize):
        author = item.get("author_display_name", item.get("author_name", "Auteur inconnu"))
        timestamp_str, content = item.get("timestamp_iso"), item.get("content", "")
        message_id, channel_id, guild_id = item.get("id"), item.get("channel_id"), item.get("guild_id")

        if i == 0 and message_id and channel_id and guild_id:
            first_message_id_for_link, first_channel_id_for_link, first_guild_id_for_link = message_id, channel_id, guild_id

        date_fmt = "Date inconnue"
        if timestamp_str:
            try:
                dt_obj = dateutil.parser.isoparse(timestamp_str)
                if dt_obj.tzinfo is None: dt_obj = dt_obj.replace(tzinfo=datetime.timezone.utc)
                date_fmt = dt_obj.astimezone(paris_tz).strftime("%Y-%m-%d %H:%M")
            except Exception: date_fmt = timestamp_str 
        formatted_messages += f"[{author}] ({date_fmt}): {content}\n---\n"
    
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
Lamerdeoffline/Lamerde: Luka (292657007779905547)
hezek112/hezekiel: Enzo (957249973064446032)
FlyXOwl/Fly: Théo (532526003407290381)
airzya/azyria: Vincent (503242253272350741)
wkda_ledauphin/ledauphin: Nathan (728678866654330921)
viv1dvivi/vivi/vivihihihi: Victoire (Vivi) (813047875591340072)
will.connect/will: Justin (525001001170763797)
bastos0234/bastos: Bastien (1150107575031963649)
ttv_yunix/yunix: Liam (735088185771819079)
.fantaman/fantaman: Khelyan (675351685521997875)
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
            temperature=0.3, max_tokens=1500, top_p=0.95,
            frequency_penalty=0, presence_penalty=0, stop=None
        )

        if response.usage:
            await send_bot_log_message(
                f"Utilisation des tokens (Synthèse): Prompt={response.usage.prompt_tokens}, Completion={response.usage.completion_tokens}, Total={response.usage.total_tokens}\nPour {len(messages_to_summarize)} messages. Demandé par: {requesting_user_name_with_id}",
                source="AI-TOKEN-USAGE"
            )

        if response.choices and response.choices[0].message and response.choices[0].message.content:
            summary = response.choices[0].message.content.strip()
            return summary
        else:
            await send_bot_log_message(f"Aucune réponse ou contenu valide reçu d'Azure OpenAI pour la synthèse. Demandé par: {requesting_user_name_with_id}", source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True)
            return None
    except APIError as e:
        error_details = _extract_openai_filter_details(e)
        error_message_to_log = f"{error_details}. Demandé par: {requesting_user_name_with_id}"
        await send_bot_log_message(error_message_to_log, source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True); return None
    except APIConnectionError as e:
        error_message = f"Erreur de connexion Azure OpenAI (Synthèse) : {e}. Demandé par: {requesting_user_name_with_id}"
        await send_bot_log_message(error_message, source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True); return None
    except RateLimitError as e:
        error_message = f"Erreur de limite de taux Azure OpenAI (Synthèse) : {e}. Demandé par: {requesting_user_name_with_id}"
        await send_bot_log_message(error_message, source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True); return None
    except Exception as e:
        error_message = f"Erreur inattendue lors de l'appel à Azure OpenAI (Synthèse) : {e}\n{traceback.format_exc()}\nDemandé par: {requesting_user_name_with_id}"
        await send_bot_log_message(error_message, source=log_source_prefix, send_to_discord_channel=True, is_openai_filter_log=True); return None

# ----- LE RESTE DU CODE EST IDENTIQUE À LA VERSION PRÉCÉDENTE -----
# (Initialisation du bot, variables, CosmosDB, format_message_to_json, main_message_fetch_logic, scheduled_message_fetch, on_ready, ping, ask_command, caca_command)

print("DEBUG: AI functions defined.")

intents = discord.Intents.default()
intents.messages, intents.message_content, intents.guilds = True, True, True
bot = commands.Bot(command_prefix="!", intents=intents)
print("DEBUG: Discord Bot object created.")

TARGET_CHANNEL_ID, LOG_CHANNEL_ID_VAR, ALLOWED_USER_IDS_LIST = None, None, []

try:
    if TARGET_CHANNEL_ID_STR: TARGET_CHANNEL_ID = int(TARGET_CHANNEL_ID_STR)
    if LOG_CHANNEL_ID_STR: 
        LOG_CHANNEL_ID_VAR = int(LOG_CHANNEL_ID_STR)
        LOG_CHANNEL_ID_VAR_FOR_SEND = LOG_CHANNEL_ID_VAR # Assigner à la variable globale
    
    if ALLOWED_USER_IDS_STR:
        for user_id_str in ALLOWED_USER_IDS_STR.split(','):
            user_id_str = user_id_str.strip()
            if user_id_str: 
                try: ALLOWED_USER_IDS_LIST.append(int(user_id_str))
                except ValueError: print(f"AVERTISSEMENT: ID utilisateur '{user_id_str}' invalide.")
except ValueError:
    print("ERREUR CRITIQUE: ID de canal (TARGET ou LOG) invalide.")
    sys.exit(1)

if ALLOWED_USER_IDS_LIST: print(f"DEBUG: Allowed user IDs: {ALLOWED_USER_IDS_LIST}")
else: print("DEBUG: 'ask' non restreint (ALLOWED_USER_IDS vide/non défini).")
if LOG_CHANNEL_ID_VAR_FOR_SEND: print(f"DEBUG: Log Channel ID pour les erreurs OpenAI: {LOG_CHANNEL_ID_VAR_FOR_SEND}")
else: print("DEBUG: LOG_CHANNEL_ID non configuré, les erreurs OpenAI spécifiques ne seront pas envoyées sur un canal Discord.")

print("DEBUG: ID conversion complete.")

cosmos_client_instance_global, container_client = None, None
if all([COSMOS_DB_ENDPOINT, COSMOS_DB_KEY, DATABASE_NAME, CONTAINER_NAME]):
    try:
        cosmos_client_instance_global = CosmosClient(COSMOS_DB_ENDPOINT, credential=COSMOS_DB_KEY)
        database_client = cosmos_client_instance_global.create_database_if_not_exists(id=DATABASE_NAME)
        container_client = database_client.create_container_if_not_exists(
            id=CONTAINER_NAME, partition_key=PartitionKey(path="/id"), offer_throughput=400
        )
        print(f"Conteneur '{CONTAINER_NAME}' prêt.")
    except Exception as e:
        print(f"ERREUR CRITIQUE Cosmos DB: {e}\n{traceback.format_exc()}")
else:
    print("AVERTISSEMENT: Config Cosmos DB incomplète. Fonctions DB désactivées.")
print("DEBUG: Cosmos DB init complete.")

def format_message_to_json(message: discord.Message):
    return {
        "id": str(message.id), "message_id_int": message.id,
        "channel_id": str(message.channel.id),
        "guild_id": str(message.guild.id) if message.guild else None,
        "author_id": str(message.author.id), "author_name": message.author.name, 
        "author_discriminator": message.author.discriminator,
        "author_display_name": message.author.display_name, "author_bot": message.author.bot,
        "content": message.content, "timestamp_iso": message.created_at.isoformat() + "Z",
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
    if not container_client: await send_bot_log_message("ERREUR: Client Cosmos DB non initialisé.", source=log_source); return
    if not TARGET_CHANNEL_ID: await send_bot_log_message("ERREUR: TARGET_CHANNEL_ID non configuré.", source=log_source); return

    await send_bot_log_message(f"Démarrage tâche pour canal ID: {TARGET_CHANNEL_ID}.", source=log_source)
    try:
        channel_to_fetch = bot.get_channel(TARGET_CHANNEL_ID) or await bot.fetch_channel(TARGET_CHANNEL_ID)
    except (discord.NotFound, discord.Forbidden) as e:
        await send_bot_log_message(f"ERREUR canal {TARGET_CHANNEL_ID}: {e}", source=log_source); return
    except Exception as e:
        await send_bot_log_message(f"ERREUR récup canal {TARGET_CHANNEL_ID}:\n{traceback.format_exc()}", source=log_source); return

    after_date = None
    try:
        query = f"SELECT VALUE MAX(c.timestamp_unix) FROM c WHERE c.channel_id = '{str(TARGET_CHANNEL_ID)}'"
        results = list(container_client.query_items(query=query, enable_cross_partition_query=True))
        if results and results[0] is not None:
            after_date = datetime.datetime.fromtimestamp(results[0] + 0.001, tz=datetime.timezone.utc)
    except Exception as e:
        await send_bot_log_message(f"AVERTISSEMENT: Récup MAX timestamp échouée: {e}. Utilisation période défaut.", source=log_source)
    
    if not after_date: 
        after_date = discord.utils.utcnow() - datetime.timedelta(days=14)
        await send_bot_log_message(f"Récupération depuis {after_date.isoformat()} (défaut).", source=log_source)
    else:
        await send_bot_log_message(f"Dernier msg stocké: {after_date.isoformat()}. Récupération après.", source=log_source)

    fetched_in_pass = 0
    try:
        async for message in channel_to_fetch.history(limit=None, after=after_date, oldest_first=True):
            fetched_in_pass +=1
            message_json = format_message_to_json(message)
            try: container_client.upsert_item(body=message_json)
            except Exception as e_upsert:
                 await send_bot_log_message(f"ERREUR upsert msg {message_json['id']}: {e_upsert}", source=log_source) 
            if fetched_in_pass > 0 and fetched_in_pass % 500 == 0: 
                await send_bot_log_message(f"Progression: {fetched_in_pass} messages traités...", source=log_source)
        await send_bot_log_message(f"Récupération terminée pour '{channel_to_fetch.name}'. {fetched_in_pass} messages traités.", source=log_source)
    except Exception as e:
        await send_bot_log_message(f"ERREUR MAJEURE fetch history:\n{traceback.format_exc()}", source=log_source)

@tasks.loop(hours=12)
async def scheduled_message_fetch():
    await send_bot_log_message("Démarrage tâche récupération planifiée.", source="SCHEDULER")
    try: await main_message_fetch_logic()
    except Exception as e: await send_bot_log_message(f"ERREUR non gérée scheduled_fetch:\n{traceback.format_exc()}", source="SCHEDULER")
    await send_bot_log_message("Tâche récupération planifiée terminée.", source="SCHEDULER")

@scheduled_message_fetch.before_loop
async def before_scheduled_fetch():
    await bot.wait_until_ready() 
    valid_config = True
    if not TARGET_CHANNEL_ID: await send_bot_log_message("ERREUR: TARGET_CHANNEL_ID non configuré.", source="SCHEDULER"); valid_config = False
    if not container_client: await send_bot_log_message("ERREUR: Conteneur Cosmos DB non initialisé.", source="SCHEDULER"); valid_config = False
    if not LOG_CHANNEL_ID_VAR: print("AVERTISSEMENT SCHEDULER: LOG_CHANNEL_ID non configuré (pour les messages stdout de cette tâche).") 
    
    if not valid_config: scheduled_message_fetch.cancel(); await send_bot_log_message("Tâche récupération annulée.", source="SCHEDULER")
    else: print("Tâche récupération planifiée: Vérifications OK.")

@bot.event
async def on_ready():
    await send_bot_log_message(f"Bot {bot.user.name} (ID: {bot.user.id}) connecté et prêt.", source="CORE-BOT")
    if not scheduled_message_fetch.is_running():
         scheduled_message_fetch.start() 
         await send_bot_log_message("Tâche récupération planifiée initiée via on_ready.", source="SCHEDULER")

@bot.command(name='ping')
async def ping(ctx): await ctx.send(f'Pong! Latence: {round(bot.latency * 1000)}ms')

@bot.command(name='ask', help="Pose une question sur l'historique des messages.")
async def ask_command(ctx, *, question: str):
    log_source = "ASK-CMD" 
    user_name_for_log = f"{ctx.author.name} (ID: {ctx.author.id})"
    
    await send_bot_log_message(f"Commande !ask reçue de {user_name_for_log}. Question: '{question}'", source=f"{log_source}-INIT")

    if ALLOWED_USER_IDS_LIST and ctx.author.id not in ALLOWED_USER_IDS_LIST:
        await send_bot_log_message(f"Accès refusé à !ask pour {user_name_for_log}. Question: '{question}'", source=log_source)
        await ctx.send("Désolé, cette commande est actuellement restreinte."); return
    
    await ctx.send(f"Recherche en cours pour : \"{question}\" ... Veuillez patienter.")

    if not IS_AZURE_OPENAI_CONFIGURED or not azure_openai_client:
        await ctx.send("Désolé, le module d'intelligence artificielle n'est pas correctement configuré.")
        await send_bot_log_message(f"Cmd !ask par {user_name_for_log} échouée : Azure OpenAI non configuré. Q: '{question}'", source=log_source, send_to_discord_channel=True, is_openai_filter_log=True); return
    if not container_client:
        await ctx.send("Désolé, la connexion à la base de données n'est pas active.")
        # Note: is_openai_filter_log=False car ce n'est pas un filtre OpenAI
        await send_bot_log_message(f"Cmd !ask par {user_name_for_log} échouée : Client Cosmos DB non initialisé. Q: '{question}'", source=log_source, send_to_discord_channel=True, is_openai_filter_log=False); return 

    generated_sql_query = await get_ai_analysis(question, user_name_for_log) 
    
    if not generated_sql_query: await ctx.send("Je n'ai pas réussi à interpréter votre question."); return 
    if generated_sql_query == "NO_QUERY_POSSIBLE": await ctx.send("Je ne peux pas formuler de requête. Essayez de reformuler."); return 
    if generated_sql_query == "INVALID_QUERY_FORMAT": await ctx.send("L'IA a retourné une réponse inattendue."); return 
    
    await send_bot_log_message(f"Génération SQL pour '{question}' par {user_name_for_log} terminée. Requête : {generated_sql_query}", source="ASK-CMD-SQL-READY") 

    try:
        items = list(container_client.query_items(query=generated_sql_query, enable_cross_partition_query=True))

        if not items:
            await ctx.send("Aucun message ne correspond à votre demande.")
            await send_bot_log_message(f"Aucun résultat Cosmos DB pour '{generated_sql_query}'. Demandé par: {user_name_for_log}", source=log_source); return

        if generated_sql_query.upper().startswith("SELECT VALUE COUNT(1)"):
            count = items[0] if items else 0
            await ctx.send(f"J'ai trouvé {count} message(s) correspondant à votre demande.")
            await send_bot_log_message(f"Résultat COUNT pour '{generated_sql_query}': {count}. Demandé par: {user_name_for_log}", source=log_source); return

        await ctx.send(f"J'ai trouvé {len(items)} message(s). Génération du résumé...") 
        ai_summary = await get_ai_summary(items, user_name_for_log) 

        if ai_summary:
            MAX_EMBED_DESC_LENGTH = 4000 
            MAX_FALLBACK_MSG_LENGTH = 1900 
            TRUNCATION_SUFFIX = "\n... (Résumé tronqué)"
            TRUNCATION_MARGIN = len(TRUNCATION_SUFFIX) + 5 

            truncated_summary_for_embed = ai_summary
            if len(ai_summary) > MAX_EMBED_DESC_LENGTH:
                truncated_summary_for_embed = ai_summary[:MAX_EMBED_DESC_LENGTH - TRUNCATION_MARGIN] + TRUNCATION_SUFFIX
                await send_bot_log_message(f"Résumé IA tronqué pour l'embed (original: {len(ai_summary)}, tronqué: {len(truncated_summary_for_embed)}). Demandé par {user_name_for_log} Q: '{question}'", source=log_source)

            embed = discord.Embed(
                title=f"Résumé des messages trouvés ({len(items)} messages)",
                description=truncated_summary_for_embed, 
                color=discord.Color.blue(), 
                timestamp=discord.utils.utcnow()
            )
            embed.set_footer(text=f"Requête : \"{question}\"")
            
            try:
                await ctx.send(embed=embed)
            except discord.HTTPException as e_embed_send: 
                await send_bot_log_message(f"Erreur lors de l'envoi de l'embed (sera tenté en message normal): {e_embed_send}. Demandé par {user_name_for_log} Q: '{question}'", source=log_source)
                
                fallback_message_header = f"**Résumé ({len(items)} msgs):**\n"
                fallback_message_footer = f"\n*(Le résumé était trop long pour un embed. Version texte ci-dessus.)*" 
                
                remaining_space_for_summary = MAX_FALLBACK_MSG_LENGTH - len(fallback_message_header) - len(fallback_message_footer)
                
                truncated_summary_for_fallback = ai_summary
                if len(ai_summary) > remaining_space_for_summary:
                    truncated_summary_for_fallback = ai_summary[:remaining_space_for_summary - TRUNCATION_MARGIN] + TRUNCATION_SUFFIX
                    await send_bot_log_message(f"Résumé IA tronqué pour le message de fallback (original: {len(ai_summary)}, tronqué: {len(truncated_summary_for_fallback)}). Demandé par {user_name_for_log} Q: '{question}'", source=log_source)
                else:
                    truncated_summary_for_fallback = ai_summary

                try:
                    await ctx.send(f"{fallback_message_header}{truncated_summary_for_fallback}{fallback_message_footer}")
                except discord.HTTPException as e_fallback_send:
                    await send_bot_log_message(f"Erreur lors de l'envoi du message de fallback: {e_fallback_send}. Demandé par {user_name_for_log} Q: '{question}'. Le résumé était trop long.", source=log_source)
                    await ctx.send("Désolé, le résumé généré est trop long pour être affiché, même après avoir essayé de le raccourcir.")
            
            log_msg_succ = (f"Synthèse réussie pour {len(items)} messages. Demandé par: {user_name_for_log} Q: '{question}'. "
                            f"Résumé basé sur {min(len(items), MAX_MESSAGES_FOR_SUMMARY_CONFIG)} premiers.")
            await send_bot_log_message(log_msg_succ, source=log_source)
        else: 
            await ctx.send("Désolé, je n'ai pas réussi à générer de résumé pour ces messages.")
            # Le log d'erreur de get_ai_summary (si OpenAI a échoué) aura déjà été envoyé avec send_to_discord_channel=True

    except exceptions.CosmosHttpResponseError as e:
        error_msg_user = "Erreur lors de la recherche dans la base de données."
        if "Query exceeded memory limit" in str(e) or "Query exceeded maximum time limit" in str(e):
            error_msg_user = "Votre demande a généré une requête trop complexe. Soyez plus spécifique."
        elif "Request rate is large" in str(e):
             error_msg_user = "Base de données temporairement surchargée. Réessayez plus tard."
        await ctx.send(error_msg_user)
        await send_bot_log_message(f"Erreur Cosmos DB pour '{generated_sql_query}': {e}\nDemandé par: {user_name_for_log} Q: '{question}'\n{traceback.format_exc()}", source=log_source, send_to_discord_channel=True, is_openai_filter_log=False) # is_openai_filter_log=False
    except Exception as e:
        await ctx.send("Une erreur inattendue s'est produite.")
        await send_bot_log_message(f"Erreur inattendue ask_cmd pour '{generated_sql_query}': {e}\nDemandé par: {user_name_for_log} Q: '{question}'\n{traceback.format_exc()}", source=log_source, send_to_discord_channel=True, is_openai_filter_log=False) # is_openai_filter_log=False

@bot.command(name='caca', help="Affiche les 50 derniers logs pertinents des commandes !ask.")
async def caca_command(ctx):
    log_source = "CACA-CMD" 
    user_name_for_log = f"{ctx.author.name} (ID: {ctx.author.id})"

    if ALLOWED_USER_IDS_LIST and ctx.author.id not in ALLOWED_USER_IDS_LIST:
        await send_bot_log_message(f"Accès refusé à !caca pour {user_name_for_log}.", source=log_source)
        await ctx.send("Désolé, cette commande est actuellement restreinte."); return

    if not GLOBAL_ASK_COMMAND_LOGS:
        await ctx.send("Aucun log de commande !ask n'a encore été enregistré."); return

    embed = discord.Embed(
        title="🚽 Derniers Logs des Commandes !ask",
        color=discord.Color.gold(),
        timestamp=discord.utils.utcnow()
    )
    
    description_content_parts = []
    current_length = 0
    MAX_DESC_LENGTH = 4000 
    TRUNCATION_EMBED_NOTICE = "\n... (plus de logs tronqués pour tenir dans l'embed)"

    logs_to_display = list(GLOBAL_ASK_COMMAND_LOGS) 

    for log_entry in reversed(logs_to_display): 
        entry_with_newline = log_entry + "\n"
        # Réserver de la place pour le message de troncature potentiel
        if current_length + len(entry_with_newline) <= MAX_DESC_LENGTH - (len(TRUNCATION_EMBED_NOTICE) if len(logs_to_display) > (50 * 0.8) else 0) : # Heuristique pour ajouter la notice de troncature
            description_content_parts.append(entry_with_newline)
            current_length += len(entry_with_newline)
        else:
            if not any(TRUNCATION_EMBED_NOTICE in part for part in description_content_parts): # Ajouter une seule fois
                 description_content_parts.append(TRUNCATION_EMBED_NOTICE)
            await send_bot_log_message(f"!caca: Description de l'embed tronquée. Demandé par {user_name_for_log}", source=log_source)
            break 
            
    if not description_content_parts:
        embed.description = "Aucun log à afficher ou problème de formatage."
    else:
        embed.description = "".join(description_content_parts)
        
    embed.set_footer(text=f"Affichage des logs les plus récents.")

    try:
        await ctx.send(embed=embed)
    except discord.HTTPException as e:
        await send_bot_log_message(f"Erreur envoi embed !caca: {e}. Demandé par {user_name_for_log}", source=log_source)
        await ctx.send("Erreur lors de la création de l'embed des logs. Les logs sont peut-être trop volumineux.")


if __name__ == "__main__":
    if DISCORD_BOT_TOKEN:
        try: bot.run(DISCORD_BOT_TOKEN)
        except Exception as e: print(f"ERREUR bot.run(): {e}\n{traceback.format_exc()}"); sys.exit(1)
    else: print("ERREUR: DISCORD_BOT_TOKEN non trouvé."); sys.exit(1)
