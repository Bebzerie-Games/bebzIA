import discord
from discord.ext import commands, tasks
import os
import datetime
import json
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import traceback # Pour les logs d'erreur détaillés
import pytz

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
        print(f"[{timestamp_stdout_utc_display}] {log_prefix} [LOG STDOUT - CANAL/BOT INDISPONIBLE] {message_content}")
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


# --- Fonction d'analyse IA (Nouvelle API v1.x.x) ---
async def get_ai_analysis(user_query: str, requesting_user_name: str) -> str | None:
    """
    Interroge Azure OpenAI pour obtenir une requête SQL Cosmos DB basée sur la question de l'utilisateur.
    Retourne la chaîne de la requête SQL ou None en cas d'échec.
    """
    if not IS_AZURE_OPENAI_CONFIGURED or not azure_openai_client: # Vérification du client aussi
        print("AVERTISSEMENT: Tentative d'appel à get_ai_analysis alors qu'Azure OpenAI n'est pas configuré ou client non initialisé.")
        await send_bot_log_message("Tentative d'appel à l'IA alors que la configuration Azure OpenAI est manquante ou a échoué.", source="AI-QUERY")
        return None

    paris_tz = pytz.timezone('Europe/Paris')
    current_time_paris = datetime.datetime.now(paris_tz)
    system_current_time_reference = current_time_paris.strftime("%Y-%m-%d %H:%M:%S %Z")

    system_prompt = f"""
Tu es un assistant IA spécialisé dans la conversion de questions en langage naturel en requêtes SQL optimisées pour Azure Cosmos DB.
Ta tâche est d'analyser la question de l'utilisateur et de générer UNIQUEMENT la requête SQL correspondante pour interroger une base de données Cosmos DB contenant des messages Discord.

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
    * Exemple Court : "messages d'hier" (si date réf 2025-05-15) -> `STARTSWITH(c.timestamp_iso, "2025-05-14")`
    * Exemple Long : "messages de y'a 3 mois" (si date réf en Mai 2025) -> `STARTSWITH(c.timestamp_iso, "2025-02")`
    * Exemple Long : "messages de l'année dernière" (si date réf en 2025) -> `STARTSWITH(c.timestamp_iso, "2024")`
    * Exemple Long : "messages de décembre 2023" -> `STARTSWITH(c.timestamp_iso, "2023-12")`
5.  Pour filtrer par l'auteur d'un message (en utilisant son nom d'utilisateur), utilise IMPÉRATIVEMENT le champ c.author_name avec la fonction CONTAINS. Lorsque l'utilisateur fait référence à lui-même ("moi", "j'ai envoyé", "mes messages"), utilise son nom d'utilisateur réel qui t'est fourni en début de prompt ("{requesting_user_name}"). Ne fais JAMAIS référence à un champ 'author' non défini à la racine. 
6.  Utilise `c.reactions_count` ou `c.attachments_count` si besoin.
7.  Si la question est vague, retourne la chaîne "NO_QUERY_POSSIBLE".
8.  Par défaut, trie par `ORDER BY c.timestamp_iso DESC`.
9.  Pour "combien", utilise `SELECT VALUE COUNT(1) FROM c WHERE ...`.
10. Pour limiter le nombre de résultats retournés (par exemple, "le dernier message", "les 5 messages les plus récents"), utilise la clause `TOP N` (où N est le nombre désiré) **juste après le `SELECT`**.

Exemples (date de référence 2025-05-15 Paris) :
- Utilisateur: "Messages de FlyXOwl hier"
  IA: SELECT * FROM c WHERE CONTAINS(c.author_name, "FlyXOwl", true) AND STARTSWITH(c.timestamp_iso, "2025-05-14") ORDER BY c.timestamp_iso DESC
- Utilisateur: "Combien de messages le 1er janvier 2025 ?"
  IA: SELECT VALUE COUNT(1) FROM c WHERE STARTSWITH(c.timestamp_iso, "2025-01-01T")
- Utilisateur: "le premier message"
  IA: SELECT TOP 1 * FROM c ORDER BY c.timestamp_iso ASC
- Utilisateur: "les 5 derniers messages de FlyXOwl"
  IA: SELECT TOP 5 * FROM c WHERE CONTAINS(c.author_name, "FlyXOwl", true) ORDER BY c.timestamp_iso DESC
- Utilisateur: "messages de airzya d'il y a 3 mois"
  IA: SELECT * FROM c WHERE CONTAINS(c.author_name, "airzya", true) AND STARTSWITH(c.timestamp_iso, "2025-02") ORDER BY c.timestamp_iso DESC
- **(NOUVEL EXEMPLE SIMPLE AUTEUR)** Utilisateur: "messages de bob"
  IA: SELECT * FROM c WHERE CONTAINS(c.author_name, "bob", true) ORDER BY c.timestamp_iso DESC


Question de l'utilisateur :
"""

    try:
        print(f"Tentative d'appel à Azure OpenAI avec la question : {user_query}")
        # LIGNE CORRIGEE ICI : Utilisation de l'instance azure_openai_client
        response = await azure_openai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME, # 'model' est utilisé pour le nom du déploiement
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
            print(f"Requête SQL générée par l'IA : {generated_query}")

            if "NO_QUERY_POSSIBLE" in generated_query:
                await send_bot_log_message(f"L'IA a déterminé qu'aucune requête n'est possible pour : '{user_query}'", source="AI-QUERY")
                return "NO_QUERY_POSSIBLE"

            if not generated_query.upper().startswith("SELECT"):
                await send_bot_log_message(f"L'IA a retourné une réponse inattendue (non SELECT) : '{generated_query}' pour la question : '{user_query}'", source="AI-QUERY")
                return "INVALID_QUERY_FORMAT"

            return generated_query
        else:
            await send_bot_log_message(f"Aucune réponse ou contenu de message valide reçu d'Azure OpenAI pour la question : '{user_query}'.", source="AI-QUERY")
            print(f"Aucune réponse ou contenu de message valide reçu d'Azure OpenAI. Réponse complète : {response}")
            return None

    except APIError as e: # Utilise les exceptions importées directement
        error_message = f"Erreur API Azure OpenAI : {e}"
        print(error_message)
        await send_bot_log_message(error_message, source="AI-QUERY")
        return None
    except APIConnectionError as e:
        error_message = f"Erreur de connexion Azure OpenAI : {e}"
        print(error_message)
        await send_bot_log_message(error_message, source="AI-QUERY")
        return None
    except RateLimitError as e:
        error_message = f"Erreur de limite de taux Azure OpenAI : {e}. Veuillez vérifier votre quota et votre utilisation."
        print(error_message)
        await send_bot_log_message(error_message, source="AI-QUERY")
        return None
    except Exception as e:
        error_message = f"Erreur inattendue lors de l'appel à Azure OpenAI : {e}\n{traceback.format_exc()}"
        print(error_message)
        await send_bot_log_message(error_message, source="AI-QUERY")
        return None

# --- Configuration des Intents et du Bot ---
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents)

# --- Conversion des IDs de canaux ---
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
    print("ERREUR CRITIQUE: Un ID (canal ou utilisateur) n'est pas un entier valide.")

# --- Initialisation du client Cosmos DB ---
cosmos_client_instance_global = None # Renommé pour éviter confusion avec le module 'CosmosClient'
container_client = None
if COSMOS_DB_ENDPOINT and COSMOS_DB_KEY and DATABASE_NAME and CONTAINER_NAME:
    try:
        cosmos_client_instance_global = CosmosClient(COSMOS_DB_ENDPOINT, credential=COSMOS_DB_KEY)
        database_client = cosmos_client_instance_global.create_database_if_not_exists(id=DATABASE_NAME)
        partition_key_path = PartitionKey(path="/id")
        container_client = database_client.create_container_if_not_exists(
            id=CONTAINER_NAME,
            partition_key=partition_key_path,
            offer_throughput=400
        )
        print("Connecté à Cosmos DB avec succès.")
    except Exception as e:
        print(f"ERREUR CRITIQUE lors de l'initialisation de Cosmos DB: {e}\n{traceback.format_exc()}")
        container_client = None
else:
    print("AVERTISSEMENT: Variables d'environnement pour Cosmos DB manquantes. La récupération des messages sera désactivée.")

# --- Fonctions utilitaires ---
def format_message_to_json(message: discord.Message):
    return {
        "id": str(message.id),
        "message_id_int": message.id,
        "channel_id": str(message.channel.id),
        "guild_id": str(message.guild.id) if message.guild else None,
        "author_id": str(message.author.id),
        "author_name": message.author.name,
        "author_discriminator": message.author.discriminator,
        "author_nickname": message.author.nick if hasattr(message.author, 'nick') else None,
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

    try:
        channel_to_fetch = bot.get_channel(TARGET_CHANNEL_ID)
        if not channel_to_fetch:
            try:
                channel_to_fetch = await bot.fetch_channel(TARGET_CHANNEL_ID)
            except discord.NotFound:
                await send_bot_log_message(f"ERREUR: Canal {TARGET_CHANNEL_ID} introuvable.", source=log_source)
                return
            except discord.Forbidden:
                await send_bot_log_message(f"ERREUR: Permissions insuffisantes pour fetch canal {TARGET_CHANNEL_ID}.", source=log_source)
                return
            except Exception as e:
                await send_bot_log_message(f"ERREUR fetch_channel {TARGET_CHANNEL_ID}: {e}", source=log_source)
                return
        
        if not channel_to_fetch:
            await send_bot_log_message(f"ERREUR: Canal {TARGET_CHANNEL_ID} toujours introuvable.", source=log_source)
            return

        last_message_timestamp_unix = 0
        try:
            query = f"SELECT VALUE MAX(c.timestamp_unix) FROM c WHERE c.channel_id = '{str(TARGET_CHANNEL_ID)}'"
            results = list(container_client.query_items(query=query, enable_cross_partition_query=True))
            if results and results[0] is not None:
                last_message_timestamp_unix = results[0]
        except Exception as e:
            await send_bot_log_message(f"AVERTISSEMENT: Récupération MAX timestamp échouée: {e}. Période par défaut.", source=log_source)

        if last_message_timestamp_unix > 0:
            after_date = datetime.datetime.fromtimestamp(last_message_timestamp_unix + 0.001, tz=datetime.timezone.utc)
            await send_bot_log_message(f"Dernier msg stocké: {after_date.isoformat()}. Récupération après.", source=log_source)
        else:
            after_date = discord.utils.utcnow() - datetime.timedelta(days=14)
            await send_bot_log_message(f"Aucun msg précédent/erreur. Récupération depuis {after_date.isoformat()}.", source=log_source)

        # new_messages_count = 0 # Commenté ou supprimé car upsert ne distingue pas facilement
        # existing_messages_count = 0 # Commenté ou supprimé
        fetched_in_pass = 0 # Compteur pour le total traité/mis à jour

        async for message in channel_to_fetch.history(limit=None, after=after_date, oldest_first=True):
            fetched_in_pass +=1
            message_json = format_message_to_json(message)
            item_id = message_json["id"]

            try:
                # --- Logique de récupération et stockage avec upsert_item ---
                # Utilise upsert_item pour créer le document s'il n'existe pas,
                # ou le mettre à jour s'il existe déjà (avec le format actuel).
                container_client.upsert_item(body=message_json)
                # Ici, fetched_in_pass compte le nombre total de messages que nous avons tenté d'upsert.

            except exceptions.CosmosHttpResponseError as e_upsert:
                 await send_bot_log_message(f"ERREUR Cosmos (upsert) msg {item_id}: {e_upsert.message}", source=log_source)
            except Exception as e: # Capturer d'autres exceptions potentielles lors de l'upsert
                 await send_bot_log_message(f"ERREUR Inattendue pendant upsert msg {item_id}: {e}\n{traceback.format_exc()}", source=log_source)

            
            if fetched_in_pass > 0 and fetched_in_pass % 250 == 0: # Log moins fréquent
                await send_bot_log_message(f"Progression: {fetched_in_pass} messages traités/mis à jour...", source=log_source)

        # --- Résumé de la tâche (Compteur simplifié) ---
        summary_message = (f"Récupération terminée pour '{channel_to_fetch.name}'.\n"
                            f"- Messages traités/mis à jour : {fetched_in_pass}") # Résumé basé sur le compteur de traitement
        await send_bot_log_message(summary_message, source=log_source)

    except Exception as e: # Capturer les exceptions en dehors de la boucle de fetch
        error_details = traceback.format_exc()
        await send_bot_log_message(f"ERREUR MAJEURE pendant récupération:\n{error_details}", source=log_source)


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
    await bot.wait_until_ready()
    print("Bot prêt, la tâche de récupération planifiée peut commencer.")
    valid_config = True
    if not TARGET_CHANNEL_ID: # Vérifie la variable convertie
        print("ERREUR CRITIQUE: TARGET_CHANNEL_ID non configuré. Tâche annulée.")
        await send_bot_log_message("ERREUR CRITIQUE: TARGET_CHANNEL_ID non configuré. Tâche désactivée.", source=log_source)
        valid_config = False
    if not LOG_CHANNEL_ID:
        print("AVERTISSEMENT: LOG_CHANNEL_ID non configuré. Logs sur STDOUT.")
        # La tâche peut continuer, send_bot_log_message gère cela.
    if not container_client:
        print("ERREUR CRITIQUE: Client Cosmos DB non initialisé. Tâche annulée.")
        await send_bot_log_message("ERREUR CRITIQUE: Cosmos DB non initialisé. Tâche désactivée.", source=log_source)
        valid_config = False
        
    if not valid_config:
        scheduled_message_fetch.cancel()
        await send_bot_log_message("Tâche récupération annulée (config invalide).", source=log_source)

# --- Événements du Bot ---
@bot.event
async def on_ready():
    log_source = "CORE-BOT"
    print(f'{bot.user} s\'est connecté à Discord!')
    print(f"ID du bot : {bot.user.id}")
    await send_bot_log_message(f"Bot {bot.user.name} connecté et prêt.", source=log_source)
    
    if TARGET_CHANNEL_ID and container_client: # LOG_CHANNEL_ID est optionnel pour démarrer la tâche
        print("Configuration validée. Démarrage de la tâche de récupération...")
        scheduled_message_fetch.start()
        await send_bot_log_message("Tâche de récupération planifiée démarrée.", source="SCHEDULER")
    else:
        missing_configs = []
        if not TARGET_CHANNEL_ID: missing_configs.append("TARGET_CHANNEL_ID")
        if not container_client: missing_configs.append("Connexion Cosmos DB")
        # LOG_CHANNEL_ID manquant n'empêche pas la tâche, juste les logs Discord.
        
        if missing_configs: # Seulement si des configs critiques manquent
            msg = f"Tâche récupération NON démarrée. Manquant: {', '.join(missing_configs)}."
            print(f"AVERTISSEMENT CRITIQUE: {msg}")
            await send_bot_log_message(f"AVERTISSEMENT CRITIQUE: {msg}", source=log_source)
        else:
            await send_bot_log_message("Tâche de récupération démarrée (LOG_CHANNEL_ID peut manquer, logs sur STDOUT).", source="SCHEDULER")


# --- Commandes du Bot ---
@bot.command(name='ping')
async def ping(ctx):
    await ctx.send(f'Pong! Latence: {round(bot.latency * 1000)}ms')

@bot.command(name='ask', help="Pose une question sur l'historique des messages. L'IA tentera de trouver les messages pertinents.")
async def ask_command(ctx, *, question: str):
    log_source = "ASK-CMD"

    # --- Nouvelle vérification de l'utilisateur ---
    if ALLOWED_USER_ID is not None and ctx.author.id != ALLOWED_USER_ID:
        print(f"Tentative d'utiliser !ask par utilisateur non autorisé: {ctx.author.name} (ID: {ctx.author.id})")
        await ctx.send("Désolé, cette commande est actuellement restreinte.")
        return
    # --- Fin de la vérification ---

    await ctx.send(f"Recherche en cours pour : \"{question}\" ... Veuillez patienter.")

    # --- Déplacer les vérifications AVANT l'appel à l'IA ou la base de données ---
    if not IS_AZURE_OPENAI_CONFIGURED or not azure_openai_client:
        await ctx.send("Désolé, le module d'intelligence artificielle n'est pas correctement configuré.")
        await send_bot_log_message(f"Cmd !ask par {ctx.author.name} échouée : Azure OpenAI non configuré/client non prêt.", source=log_source)
        return

    if not container_client:
        await ctx.send("Désolé, la connexion à la base de données n'est pas active.")
        await send_bot_log_message(f"Cmd !ask par {ctx.author.name} échouée : Client Cosmos DB non initialisé.", source=log_source)
        return
    # --- Fin des vérifications ---

    # --- Appeler get_ai_analysis UNE SEULE fois et PASSER LE NOM DE L'UTILISATEUR ---
    generated_sql_query = await get_ai_analysis(question, ctx.author.name) # <-- Correct maintenant

    # --- Gérer le résultat de l'appel à l'IA ---
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

    # --- Le reste de ta fonction (affichage, exécution de la requête Cosmos DB, etc.) ---
    await send_bot_log_message(f"Cmd !ask par {ctx.author.name} pour '{question}'. Requête IA : {generated_sql_query}", source=log_source)
    if len(generated_sql_query) < 1900 : # Pour éviter un message de debug trop long
        await ctx.send(f"Requête générée (débogage) : ```sql\n{generated_sql_query}\n```")

    try:
        query_to_execute = generated_sql_query
        items = list(container_client.query_items(
            query=query_to_execute,
            enable_cross_partition_query=True
        ))

        # ... (Reste de la logique d'affichage des résultats) ...

        if not items:
            await ctx.send("J'ai exécuté la recherche, mais aucun message ne correspond à votre demande.")
            await send_bot_log_message(f"Aucun résultat Cosmos DB pour : {query_to_execute}", source=log_source)
            return

        if query_to_execute.upper().startswith("SELECT VALUE COUNT(1)"):
            count = items[0] if items else 0
            await ctx.send(f"J'ai trouvé {count} message(s) correspondant à votre demande.")
            await send_bot_log_message(f"Résultat COUNT pour '{query_to_execute}': {count}", source=log_source)
            return

        # --- Début de la boucle d'affichage des résultats (Déjà modifiée) ---
        response_parts = [f"Voici les messages que j'ai trouvés (jusqu'à {min(len(items), 5)} affichés) :\n"] # Ajuster le message si moins de 5 résultats
        max_messages_to_display = 5
        messages_displayed_count = 0

        for item in items[:max_messages_to_display]:
            # Tenter de lire le format ACTUEL (ajouté par format_message_to_json)
            author = item.get("author_name")
            timestamp_str = item.get("timestamp_iso") # Utilise un nom temporel pour éviter conflit

            # Si le format actuel n'est pas trouvé, tenter de lire l'ANCIEN format
            if author is None:
                author = item.get("author", {}).get("name", "Auteur inconnu") # Va chercher dans l'objet 'author'

            if timestamp_str is None:
                timestamp_str = item.get("timestamp", "Date inconnue") # Cherche l'ancienne clé 'timestamp'

            content = item.get("content", "*Contenu vide*")


            # --- Formatage de la date (Affiner pour différents formats ISO) ---
            date_fmt = "Date invalide ou manquante" # Valeur par défaut en cas d'échec
            if timestamp_str and timestamp_str != "Date inconnue":
                try:
                    # Tenter de nettoyer le timestamp si nécessaire avant de parser
                    # Gérer 'Z' et '+HH:MM' potentiellement ensemble ou séparément
                    cleaned_timestamp_str = timestamp_str
                    # Supprimer le 'Z' s'il y a déjà un offset pour fromisoformat
                    if '+' in cleaned_timestamp_str or '-' in cleaned_timestamp_str[10:]: # Présence d'un offset
                         if cleaned_timestamp_str.endswith('Z'):
                             cleaned_timestamp_str = cleaned_timestamp_str[:-1]
                    cleaned_timestamp_str = timestamp_str.replace("+00:00Z", "Z").replace(".000Z", "Z")


                    dt_obj = datetime.datetime.fromisoformat(cleaned_timestamp_str)
                    dt_paris = dt_obj.astimezone(pytz.timezone('Europe/Paris'))
                    date_fmt = dt_paris.strftime("%d/%m/%Y à %H:%M")
                except Exception as e:
                    # Log l'erreur de formatage de date détaillée
                    print(f"Erreur formatage date pour timestamp '{timestamp_str}' (ID: {item.get('id', 'N/A')}): {e}\n{traceback.format_exc()}")
                    date_fmt = f"Format date inconnu ({timestamp_str[:25]}...)" # Affiche le début du timestamp si échec
            # -------------------------------------------------------------

            display_content = (content[:150] + '...') if len(content) > 150 else content

            # --- Ajout à la réponse ---
            # Ajoute l'ID du message pour faciliter le debug si besoin
            response_parts.append(f"\n**De {author} (le {date_fmt}):** (ID: {item.get('id', 'N/A')})\n```\n{display_content}\n```\n---")
            messages_displayed_count += 1
        # --- Fin de la boucle d'affichage ---


        if len(items) > max_messages_to_display:
            response_parts.append(f"\n*Et {len(items) - max_messages_to_display} autre(s) message(s) trouvé(s).*")

        current_message = ""
        for part in response_parts:
            if len(current_message) + len(part) > 1950: # Marge pour les backticks et autres formatages
                await ctx.send(current_message)
                current_message = part
            else:
                current_message += part
        
        if current_message: # Envoyer le reste
            await ctx.send(current_message)
        
        await send_bot_log_message(f"{len(items)} résultats pour '{query_to_execute}'. {messages_displayed_count} affichés.", source=log_source)

    except exceptions.CosmosHttpResponseError as e:
        error_msg_user = "Une erreur s'est produite lors de la recherche dans la base de données."
        if "Query exceeded memory limit" in str(e) or "Query exceeded maximum time limit" in str(e):
            error_msg_user = "Votre demande a généré une requête trop complexe/gourmande. Soyez plus spécifique (ex: période plus courte)."
        
        await ctx.send(error_msg_user)
        await send_bot_log_message(f"Erreur Cosmos DB pour '{generated_sql_query}': {e}\n{traceback.format_exc()}", source=log_source)
        print(f"Erreur Cosmos DB: {e}")
    except Exception as e:
        await ctx.send("Une erreur inattendue s'est produite.")
        await send_bot_log_message(f"Erreur inattendue dans ask_command pour '{generated_sql_query}': {e}\n{traceback.format_exc()}", source=log_source)
        print(f"Erreur inattendue: {e}")

# --- Démarrage du Bot ---
if __name__ == "__main__":
    if DISCORD_BOT_TOKEN:
        bot.run(DISCORD_BOT_TOKEN)
    else:
        print("ERREUR: DISCORD_BOT_TOKEN non trouvé. Le bot ne peut pas démarrer.")