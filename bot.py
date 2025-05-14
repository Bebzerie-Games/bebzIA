import discord
from discord.ext import commands, tasks
import os
import datetime
import json
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import traceback # Pour les logs d'erreur détaillés
import pytz
import openai

# Charger les variables d'environnement
load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
COSMOS_DB_ENDPOINT = os.getenv("COSMOS_DB_ENDPOINT")
COSMOS_DB_KEY = os.getenv("COSMOS_DB_KEY")
DATABASE_NAME = os.getenv("DATABASE_NAME")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
TARGET_CHANNEL_ID_STR = os.getenv("TARGET_CHANNEL_ID")
LOG_CHANNEL_ID_STR = os.getenv("LOG_CHANNEL_ID")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")


# --- Configuration Azure OpenAI ---
IS_AZURE_OPENAI_CONFIGURED = False # Initialisation par défaut
if AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_KEY and AZURE_OPENAI_DEPLOYMENT_NAME:
    try:
        openai.api_type = "azure"
        openai.api_base = AZURE_OPENAI_ENDPOINT
        openai.api_version = "2023-07-01-preview"  # Vous pouvez ajuster cette version si nécessaire, mais c'est une version courante.
        openai.api_key = AZURE_OPENAI_KEY
        IS_AZURE_OPENAI_CONFIGURED = True
        print("Configuration Azure OpenAI initialisée avec succès.")
    except Exception as e:
        print(f"ERREUR CRITIQUE lors de l'initialisation de la configuration Azure OpenAI: {e}")
        # Pas besoin de send_bot_log_message ici car cela pourrait ne pas encore être prêt ou configuré
else:
    print("AVERTISSEMENT: Variables d'environnement pour Azure OpenAI manquantes ou incomplètes. Les fonctionnalités IA seront désactivées.")

# --- Définition de send_bot_log_message (Placée AVANT get_ai_analysis) ---
async def send_bot_log_message(message_content: str):
    """Envoie un message de log en utilisant l'instance bot principale, avec timestamp de Paris."""
    
    # S'assurer que 'bot' est défini et accessible globalement si ce n'est pas passé en argument
    # et que LOG_CHANNEL_ID est aussi accessible.
    # Pour cela, 'bot' et 'LOG_CHANNEL_ID' doivent être définis dans la portée globale avant que cette fonction soit appelée
    # par des tâches ou des événements qui s'exécutent après l'initialisation du bot.

    now_utc = discord.utils.utcnow()
    paris_tz = pytz.timezone('Europe/Paris')
    now_paris = now_utc.astimezone(paris_tz)
    
    timestamp_discord_display = now_paris.strftime('%Y-%m-%d %H:%M:%S %Z') 
    timestamp_stdout_utc_display = now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')

    # Vérifier si 'bot' est initialisé et prêt, et si LOG_CHANNEL_ID est défini
    # Cette vérification est cruciale si la fonction est appelée avant que 'on_ready' ait terminé
    # ou si LOG_CHANNEL_ID n'est pas correctement configuré.
    if 'bot' not in globals() or not globals().get('bot').is_ready() or not LOG_CHANNEL_ID:
        print(f"[{timestamp_stdout_utc_display}] [BOT LOG STDOUT - CANAL LOG INDISPONIBLE/BOT NON PRET OU LOG_CHANNEL_ID MANQUANT] {message_content}")
        return

    try:
        log_channel_obj = bot.get_channel(LOG_CHANNEL_ID) # Utilise la variable globale 'bot'
        if log_channel_obj:
            await log_channel_obj.send(f"```\n[Bot Auto-Fetch] {timestamp_discord_display}\n{message_content}\n```")
        else:
            print(f"[{timestamp_stdout_utc_display}] [BOT LOG STDOUT] Log channel ID {LOG_CHANNEL_ID} non trouvé. Message: {message_content}")
    except Exception as e:
        print(f"[{timestamp_stdout_utc_display}] [BOT LOG STDOUT] Erreur envoi log Discord: {e}. Message: {message_content}")
        print(traceback.format_exc())


# --- Fonction d'analyse IA ---
async def get_ai_analysis(user_query: str) -> str | None:
    """
    Interroge Azure OpenAI pour obtenir une requête SQL Cosmos DB basée sur la question de l'utilisateur.
    Retourne la chaîne de la requête SQL ou None en cas d'échec.
    """
    if not IS_AZURE_OPENAI_CONFIGURED:
        print("AVERTISSEMENT: Tentative d'appel à get_ai_analysis alors qu'Azure OpenAI n'est pas configuré.")
        await send_bot_log_message("Tentative d'appel à l'IA alors que la configuration Azure OpenAI est manquante ou a échoué.")
        return None

    paris_tz = pytz.timezone('Europe/Paris')
    current_time_paris = datetime.datetime.now(paris_tz)
    # Date et heure UTC actuelle pour le prompt système, basée sur l'heure de Paris pour référence
    # Mais l'IA doit générer des timestamps ISO 8601 UTC
    # La date fournie à l'IA est celle de Paris pour l'aider à interpréter "hier à Paris", etc.
    # Elle doit ensuite convertir cela en UTC pour la requête.
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
    "timestamp_iso": "string (timestamp ISO 8601 UTC, ex: '2023-10-15T12:30:45.123Z')", // Champ clé pour les dates
    "attachments_count": "integer (nombre de pièces jointes)", // Ajouté
    "reactions_count": "integer (nombre total de réactions)" // Ajouté
  }}
- Le champ `timestamp_iso` est crucial pour les requêtes basées sur le temps. Il est stocké au format ISO 8601 UTC.
- La date et l'heure actuelles de référence (Paris) sont : {system_current_time_reference}. Utilise cette information pour interpréter les références temporelles relatives (par exemple, "hier", "la semaine dernière"). Convertis TOUJOURS ces références en dates et heures UTC ISO 8601 spécifiques pour la requête. Par exemple, si la référence est "hier" et que l'heure actuelle de référence est '2025-05-14 ... Paris', alors "hier" correspond au jour '2025-05-13' UTC pour les filtres STARTSWITH.

Instructions pour la génération de la requête :
1.  Ta sortie doit être UNIQUEMENT la requête SQL. Ne fournis aucune explication, aucun texte avant ou après la requête.
2.  Utilise `c` comme alias pour le conteneur (par exemple, `SELECT * FROM c`).
3.  Pour les recherches de texte dans `c.content` ou `c.author_name`, utilise la fonction `CONTAINS(c.field, "terme", true)` pour des recherches insensibles à la casse.
4.  Pour les dates (champ `c.timestamp_iso`) :
    *   Si l'utilisateur demande des messages d'une date spécifique (ex: "15 octobre 2023"), la requête doit filtrer sur `STARTSWITH(c.timestamp_iso, "YYYY-MM-DD")`. Par exemple, pour le 15 octobre 2023, ce serait `STARTSWITH(c.timestamp_iso, "2023-10-15")`.
    *   Pour des plages de dates (ex: "entre le 10 et le 15 octobre"), utilise `c.timestamp_iso >= "YYYY-MM-DDT00:00:00.000Z" AND c.timestamp_iso <= "YYYY-MM-DDT23:59:59.999Z"`. Assure-toi que les dates sont bien en UTC.
    *   Pour "aujourd'hui", "hier", "cette semaine", "le mois dernier", etc., calcule les dates UTC exactes correspondantes basées sur la date et l'heure de référence fournies ci-dessus et utilise-les dans la requête.
5.  Si la question implique un auteur, filtre sur `c.author_name` (par exemple, `CONTAINS(c.author_name, "FlyXOwl", true)`).
6.  Si la question implique un nombre de réactions ou de pièces jointes, utilise `c.reactions_count` ou `c.attachments_count`.
7.  Si la question est vague ou ne peut pas être traduite en une requête SQL claire sur les champs disponibles, retourne la chaîne "NO_QUERY_POSSIBLE".
8.  Par défaut, si aucun tri n'est spécifié, trie les résultats par date de création la plus récente : `ORDER BY c.timestamp_iso DESC`.
9.  Si la question demande un "nombre" ou "combien", utilise `SELECT VALUE COUNT(1) FROM c WHERE ...`.
10. Si la question demande "le dernier message" ou "le message le plus récent" correspondant à certains critères, utilise `ORDER BY c.timestamp_iso DESC` et `TOP 1`.

Exemples de questions et de requêtes attendues (en supposant que la date de référence est le 2025-05-14 à Paris) :
- Utilisateur: "Messages de FlyXOwl hier"
  IA: SELECT * FROM c WHERE CONTAINS(c.author_name, "FlyXOwl", true) AND STARTSWITH(c.timestamp_iso, "2025-05-13") ORDER BY c.timestamp_iso DESC
- Utilisateur: "Combien de messages ont été envoyés le 1er janvier 2025 ?"
  IA: SELECT VALUE COUNT(1) FROM c WHERE STARTSWITH(c.timestamp_iso, "2025-01-01")
- Utilisateur: "Le dernier message contenant le mot 'projet'"
  IA: SELECT TOP 1 * FROM c WHERE CONTAINS(c.content, "projet", true) ORDER BY c.timestamp_iso DESC
- Utilisateur: "Messages avec plus de 5 réactions"
  IA: SELECT * FROM c WHERE c.reactions_count > 5 ORDER BY c.timestamp_iso DESC
- Utilisateur: "Quel temps fait-il ?"
  IA: NO_QUERY_POSSIBLE

Question de l'utilisateur :
"""

    try:
        print(f"Tentative d'appel à Azure OpenAI avec la question : {user_query}")
        response = await openai.ChatCompletion.acreate(
            engine=AZURE_OPENAI_DEPLOYMENT_NAME,
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
                await send_bot_log_message(f"L'IA a déterminé qu'aucune requête n'est possible pour : '{user_query}'")
                return "NO_QUERY_POSSIBLE"
            
            if not generated_query.upper().startswith("SELECT"):
                await send_bot_log_message(f"L'IA a retourné une réponse inattendue (non SELECT) : '{generated_query}' pour la question : '{user_query}'")
                return "INVALID_QUERY_FORMAT"

            return generated_query
        else:
            await send_bot_log_message(f"Aucune réponse ou contenu de message valide reçu d'Azure OpenAI pour la question : '{user_query}'.")
            print(f"Aucune réponse ou contenu de message valide reçu d'Azure OpenAI. Réponse complète : {response}")
            return None

    except openai.APIError as e:
        error_message = f"Erreur API Azure OpenAI : {e}"
        print(error_message)
        await send_bot_log_message(error_message)
        return None
    except openai.APIConnectionError as e:
        error_message = f"Erreur de connexion Azure OpenAI : {e}"
        print(error_message)
        await send_bot_log_message(error_message)
        return None
    except openai.RateLimitError as e:
        error_message = f"Erreur de limite de taux Azure OpenAI : {e}. Veuillez vérifier votre quota et votre utilisation."
        print(error_message)
        await send_bot_log_message(error_message)
        return None
    except Exception as e:
        error_message = f"Erreur inattendue lors de l'appel à Azure OpenAI : {e}\n{traceback.format_exc()}"
        print(error_message)
        await send_bot_log_message(error_message)
        return None

# --- Configuration des Intents et du Bot ---
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents) # 'bot' est maintenant défini globalement ici

# --- Conversion des IDs de canaux (placée après la définition de 'bot') ---
TARGET_CHANNEL_ID = None
LOG_CHANNEL_ID = None # Sera utilisé par send_bot_log_message
try:
    if TARGET_CHANNEL_ID_STR: TARGET_CHANNEL_ID = int(TARGET_CHANNEL_ID_STR)
    if LOG_CHANNEL_ID_STR: LOG_CHANNEL_ID = int(LOG_CHANNEL_ID_STR) 
except ValueError:
    print("ERREUR CRITIQUE: TARGET_CHANNEL_ID ou LOG_CHANNEL_ID n'est pas un entier valide dans les variables d'env.")
    # Si LOG_CHANNEL_ID n'est pas valide, send_bot_log_message utilisera stdout.

# --- Initialisation du client Cosmos DB ---
cosmos_client = None # Reste global pour être accessible par les commandes si besoin
container_client = None # 'container_client' doit être global pour 'main_message_fetch_logic' et potentiellement les commandes
if COSMOS_DB_ENDPOINT and COSMOS_DB_KEY and DATABASE_NAME and CONTAINER_NAME:
    try:
        cosmos_client_instance = CosmosClient(COSMOS_DB_ENDPOINT, credential=COSMOS_DB_KEY)
        database_client = cosmos_client_instance.create_database_if_not_exists(id=DATABASE_NAME)
        partition_key_path = PartitionKey(path="/id") # Supposant que 'id' du message est la clé de partition
        container_client = database_client.create_container_if_not_exists(
            id=CONTAINER_NAME,
            partition_key=partition_key_path,
            offer_throughput=400
        )
        print("Connecté à Cosmos DB avec succès.")
    except Exception as e:
        print(f"ERREUR CRITIQUE lors de l'initialisation de Cosmos DB: {e}")
        container_client = None # Assure que container_client est None en cas d'échec
else:
    print("AVERTISSEMENT: Variables d'environnement pour Cosmos DB manquantes. La récupération des messages sera désactivée.")


# --- Fonctions utilitaires (format_message_to_json reste ici) ---
def format_message_to_json(message: discord.Message):
    # J'ai remarqué que vous utilisiez "timestamp_iso" dans le prompt de l'IA,
    # mais votre format_message_to_json original utilisait "created_at_iso" et "timestamp_unix".
    # Pour la cohérence avec le prompt IA, je vais m'assurer que le champ clé pour la date est bien "timestamp_iso".
    # Votre fonction `format_message_to_json` a déjà `timestamp_iso` et `timestamp_unix`.
    # Le prompt IA fait référence à `timestamp_iso`. C'est cohérent.
    
    # J'ai aussi vu `author_name` dans le prompt, votre structure a `message.author.name`. C'est bien.
    # Et `attachments_count` et `reactions_count` sont aussi dans le prompt.
    # Assurons-nous que `format_message_to_json` les calcule si ce n'est pas direct.
    
    return {
        "id": str(message.id), # Clé de partition
        "message_id_int": message.id, # Pour référence si besoin
        "channel_id": str(message.channel.id),
        "guild_id": str(message.guild.id) if message.guild else None,
        "author_id": str(message.author.id), # Ajout pour plus de flexibilité
        "author_name": message.author.name, # Utilisé dans le prompt IA
        "author_discriminator": message.author.discriminator,
        "author_nickname": message.author.nick if hasattr(message.author, 'nick') else None,
        "author_bot": message.author.bot,
        "content": message.content,
        "timestamp_iso": message.created_at.isoformat() + "Z", # Champ clé pour les dates, format UTC ISO8601
        "timestamp_unix": int(message.created_at.timestamp()),
        "attachments": [{"id": str(att.id), "filename": att.filename, "url": att.url, "content_type": att.content_type, "size": att.size} for att in message.attachments],
        "attachments_count": len(message.attachments), # Calcul direct
        "embeds": [embed.to_dict() for embed in message.embeds],
        "reactions": [{"emoji": str(reaction.emoji), "count": reaction.count} for reaction in message.reactions],
        "reactions_count": sum(reaction.count for reaction in message.reactions), # Calcul direct
        "edited_timestamp_iso": message.edited_at.isoformat() + "Z" if message.edited_at else None,
    }

async def main_message_fetch_logic():
    if not container_client:
        await send_bot_log_message("ERREUR: Client Cosmos DB non initialisé. Abandon de la récupération.")
        return
    if not TARGET_CHANNEL_ID:
        await send_bot_log_message("ERREUR: TARGET_CHANNEL_ID non configuré. Abandon de la récupération.")
        return

    await send_bot_log_message(f"Démarrage de la tâche de récupération pour le canal ID: {TARGET_CHANNEL_ID}.")

    try:
        channel_to_fetch = bot.get_channel(TARGET_CHANNEL_ID)
        if not channel_to_fetch: # Tentative de fetch si get_channel échoue (par exemple, si le bot n'a pas le canal dans son cache initial)
            try:
                channel_to_fetch = await bot.fetch_channel(TARGET_CHANNEL_ID)
            except discord.NotFound:
                await send_bot_log_message(f"ERREUR: Impossible de trouver le canal à surveiller (ID {TARGET_CHANNEL_ID}) via get_channel ou fetch_channel.")
                return
            except discord.Forbidden:
                await send_bot_log_message(f"ERREUR: Permissions insuffisantes pour fetch_channel {TARGET_CHANNEL_ID}.")
                return
            except Exception as e:
                 await send_bot_log_message(f"ERREUR inattendue lors de fetch_channel {TARGET_CHANNEL_ID}: {e}")
                 return
        
        if not channel_to_fetch: # Double vérification après la tentative de fetch
            await send_bot_log_message(f"ERREUR: Canal ID {TARGET_CHANNEL_ID} toujours introuvable après tentatives.")
            return

        last_message_timestamp_unix = 0
        try:
            # Utilise c.timestamp_unix pour MAX car c'est un nombre, plus simple pour MAX que des chaînes ISO
            query = f"SELECT VALUE MAX(c.timestamp_unix) FROM c WHERE c.channel_id = '{str(TARGET_CHANNEL_ID)}'"
            results = list(container_client.query_items(query=query, enable_cross_partition_query=True))
            if results and results[0] is not None: # results[0] peut être 0 si tous les timestamps sont nuls ou si le conteneur est vide
                last_message_timestamp_unix = results[0]
        except Exception as e:
            await send_bot_log_message(f"AVERTISSEMENT: Impossible de récupérer le timestamp du dernier message stocké: {e}. Récupération sur une période par défaut.")

        if last_message_timestamp_unix > 0:
            # Ajoute une petite marge pour éviter de récupérer le même message si son timestamp est exactement celui stocké
            after_date = datetime.datetime.fromtimestamp(last_message_timestamp_unix + 0.001, tz=datetime.timezone.utc)
            await send_bot_log_message(f"Dernier message stocké à {datetime.datetime.fromtimestamp(last_message_timestamp_unix, tz=datetime.timezone.utc).isoformat()}. Récupération des messages après cette date.")
        else:
            after_date = discord.utils.utcnow() - datetime.timedelta(days=14) 
            await send_bot_log_message(f"Aucun message précédent trouvé ou erreur. Récupération des messages des {datetime.timedelta(days=14).days} derniers jours (depuis {after_date.isoformat()}).")


        new_messages_count = 0
        existing_messages_count = 0
        fetched_in_pass = 0

        async for message in channel_to_fetch.history(limit=None, after=after_date, oldest_first=True):
            fetched_in_pass +=1
            message_json = format_message_to_json(message)
            item_id = message_json["id"] # Utilise l'ID du message comme ID de l'item et clé de partition

            try:
                container_client.read_item(item=item_id, partition_key=item_id)
                existing_messages_count += 1
            except exceptions.CosmosResourceNotFoundError:
                try:
                    container_client.create_item(body=message_json)
                    new_messages_count += 1
                except exceptions.CosmosHttpResponseError as e_create:
                    await send_bot_log_message(f"ERREUR Cosmos DB (création) msg {item_id}: {e_create.message}") # Accès au message d'erreur
            except exceptions.CosmosHttpResponseError as e_read:
                await send_bot_log_message(f"ERREUR Cosmos DB (lecture) msg {item_id}: {e_read.message}") # Accès au message d'erreur
            
            if fetched_in_pass > 0 and fetched_in_pass % 100 == 0: # Log tous les 100 messages traités
                 await send_bot_log_message(f"Progression: {fetched_in_pass} messages traités pour cette passe...")

        summary_message = f"Récupération terminée pour '{channel_to_fetch.name}' (ID: {TARGET_CHANNEL_ID}).\n"
        summary_message += f"- Messages traités dans cette passe: {fetched_in_pass}\n"
        summary_message += f"- Nouveaux messages ajoutés : {new_messages_count}\n"
        summary_message += f"- Messages déjà existants ignorés : {existing_messages_count}"
        await send_bot_log_message(summary_message)

    except Exception as e:
        error_details = traceback.format_exc()
        await send_bot_log_message(f"ERREUR MAJEURE pendant la tâche de récupération des messages:\n{error_details}")

# --- Définition de la tâche en boucle ---
@tasks.loop(hours=12)
async def scheduled_message_fetch():
    print(f"[{discord.utils.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] Tâche de récupération planifiée démarrée.")
    await send_bot_log_message("Démarrage de la tâche de récupération planifiée.")
    try:
        await main_message_fetch_logic()
    except Exception as e:
        error_details = traceback.format_exc()
        await send_bot_log_message(f"ERREUR non gérée au niveau de la tâche scheduled_message_fetch:\n{error_details}")
    await send_bot_log_message("Tâche de récupération planifiée terminée.")

@scheduled_message_fetch.before_loop
async def before_scheduled_fetch():
    await bot.wait_until_ready()
    print("Le bot est prêt, la tâche de récupération planifiée peut commencer.")
    valid_config = True
    if not TARGET_CHANNEL_ID_STR or not TARGET_CHANNEL_ID:
        print("ERREUR CRITIQUE: TARGET_CHANNEL_ID n'est pas configuré ou invalide. La tâche de récupération ne démarrera pas.")
        await send_bot_log_message("ERREUR CRITIQUE: TARGET_CHANNEL_ID non configuré. Tâche de récupération désactivée.")
        valid_config = False
    if not LOG_CHANNEL_ID_STR or not LOG_CHANNEL_ID: # LOG_CHANNEL_ID est maintenant initialisé plus tôt
        print("AVERTISSEMENT: LOG_CHANNEL_ID n'est pas configuré ou invalide. Les logs iront sur STDOUT.")
        # La tâche peut continuer, send_bot_log_message gère cela.
    if not container_client:
        print("ERREUR CRITIQUE: Client Cosmos DB non initialisé. La tâche de récupération ne démarrera pas.")
        await send_bot_log_message("ERREUR CRITIQUE: Cosmos DB non initialisé. Tâche de récupération désactivée.")
        valid_config = False
        
    if not valid_config:
        scheduled_message_fetch.cancel()
        await send_bot_log_message("Tâche de récupération planifiée annulée en raison d'une configuration invalide.")


# --- Événements du Bot ---
@bot.event
async def on_ready():
    print(f'{bot.user} s\'est connecté à Discord!')
    print(f"ID du bot : {bot.user.id}")
    
    # Vérifier la configuration avant de démarrer la tâche
    if TARGET_CHANNEL_ID and LOG_CHANNEL_ID and container_client:
        print("Configuration validée. Démarrage de la tâche de récupération des messages planifiée...")
        scheduled_message_fetch.start()
        await send_bot_log_message(f"Bot {bot.user.name} connecté et prêt. Tâche de récupération démarrée.")
    else:
        missing_configs = []
        if not TARGET_CHANNEL_ID: missing_configs.append("TARGET_CHANNEL_ID")
        if not LOG_CHANNEL_ID: missing_configs.append("LOG_CHANNEL_ID (les logs iront sur STDOUT)")
        if not container_client: missing_configs.append("Connexion Cosmos DB")
        
        msg = f"La tâche de récupération des messages planifiée n'a PAS été démarrée. Configurations manquantes/invalides : {', '.join(missing_configs)}."
        print(f"AVERTISSEMENT CRITIQUE: {msg}")
        # send_bot_log_message peut être appelé ici car 'bot' est prêt, mais LOG_CHANNEL_ID pourrait manquer.
        # La fonction gère le cas où LOG_CHANNEL_ID n'est pas là en printant sur STDOUT.
        await send_bot_log_message(f"AVERTISSEMENT CRITIQUE: {msg}")


# --- Commandes du Bot ---
@bot.command(name='ping')
async def ping(ctx):
    await ctx.send(f'Pong! Latence: {round(bot.latency * 1000)}ms')



@bot.command(name='ask', help="Pose une question sur l'historique des messages du canal surveillé. L'IA tentera de trouver les messages pertinents.")
async def ask_command(ctx, *, question: str):
    """
    Commande principale pour interroger l'historique des messages via l'IA.
    """
    await ctx.send(f"Recherche en cours pour : \"{question}\" ... Veuillez patienter.")

    if not IS_AZURE_OPENAI_CONFIGURED:
        await ctx.send("Désolé, le module d'intelligence artificielle n'est pas correctement configuré.")
        await send_bot_log_message(f"Commande !ask par {ctx.author.name} échouée : Azure OpenAI non configuré.")
        return

    if not container_client:
        await ctx.send("Désolé, la connexion à la base de données n'est pas active.")
        await send_bot_log_message(f"Commande !ask par {ctx.author.name} échouée : Client Cosmos DB non initialisé.")
        return

    # 1. Obtenir la requête SQL de l'IA
    generated_sql_query = await get_ai_analysis(question)

    if not generated_sql_query:
        await ctx.send("Je n'ai pas réussi à interpréter votre question pour la base de données (erreur interne ou de communication avec l'IA).")
        await send_bot_log_message(f"Commande !ask par {ctx.author.name} pour '{question}': get_ai_analysis a retourné None.")
        return

    if generated_sql_query == "NO_QUERY_POSSIBLE":
        await ctx.send("Je suis désolé, je ne peux pas formuler de requête pour la base de données avec cette question. Essayez de la reformuler ou soyez plus spécifique sur les messages recherchés.")
        await send_bot_log_message(f"Commande !ask par {ctx.author.name} pour '{question}': NO_QUERY_POSSIBLE retourné par l'IA.")
        return
    
    if generated_sql_query == "INVALID_QUERY_FORMAT":
        await ctx.send("L'IA a retourné une réponse dans un format inattendu. Impossible de traiter la demande.")
        await send_bot_log_message(f"Commande !ask par {ctx.author.name} pour '{question}': INVALID_QUERY_FORMAT retourné par l'IA.")
        return

    await send_bot_log_message(f"Commande !ask par {ctx.author.name} pour '{question}'. Requête IA générée : {generated_sql_query}")
    await ctx.send(f"Requête générée par l'IA (pour débogage) : ```sql\n{generated_sql_query}\n```") # Pour le débogage initial

    # 2. Exécuter la requête sur Cosmos DB
    try:
        query_to_execute = generated_sql_query # On fait confiance à l'IA pour l'instant

        items = list(container_client.query_items(
            query=query_to_execute,
            enable_cross_partition_query=True # Nécessaire si la clé de partition n'est pas dans la requête ou si la requête est complexe
        ))

        if not items:
            await ctx.send("J'ai exécuté la recherche, mais je n'ai trouvé aucun message correspondant à votre demande.")
            await send_bot_log_message(f"Aucun résultat Cosmos DB pour la requête : {query_to_execute}")
            return

        # 3. Formater et envoyer les résultats
        # Si la requête était un COUNT
        if generated_sql_query.upper().startswith("SELECT VALUE COUNT(1)"):
            count = items[0] if items else 0
            await ctx.send(f"J'ai trouvé {count} message(s) correspondant à votre demande.")
            await send_bot_log_message(f"Résultat COUNT pour '{query_to_execute}': {count}")
            return

        # Pour les autres requêtes SELECT (affichage des messages)
        response_message = f"Voici les messages que j'ai trouvés (jusqu'à 5 affichés) :\n"
        
        # Limiter le nombre de messages affichés pour ne pas spammer
        max_messages_to_display = 5 
        messages_displayed_count = 0

        for item in items[:max_messages_to_display]:
            author_name = item.get("author_name", "Auteur inconnu")
            # Le champ de date est 'timestamp_iso' dans notre structure JSON
            timestamp_str = item.get("timestamp_iso", "Date inconnue")
            content = item.get("content", "Contenu vide")
            
            # Convertir le timestamp ISO en objet datetime pour un affichage plus lisible
            try:
                dt_object = datetime.datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                # Convertir en heure de Paris pour l'affichage
                paris_tz = pytz.timezone('Europe/Paris')
                dt_paris = dt_object.astimezone(paris_tz)
                formatted_date = dt_paris.strftime("%d/%m/%Y à %H:%M")
            except ValueError:
                formatted_date = timestamp_str # Afficher tel quel si le format est inattendu

            # Tronquer le contenu si trop long
            display_content = (content[:150] + '...') if len(content) > 150 else content
            
            response_message += f"\n**De {author_name} (le {formatted_date}):**\n```\n{display_content}\n```\n---\n"
            messages_displayed_count += 1
        
        if len(items) > max_messages_to_display:
            response_message += f"\n*Et {len(items) - max_messages_to_display} autre(s) message(s) trouvé(s) mais non affiché(s) ici.*"

        # Gérer les messages Discord longs
        if len(response_message) > 2000:
            await ctx.send(response_message[:1990] + "\n...(message tronqué car trop long)...")
        else:
            await ctx.send(response_message)
        
        await send_bot_log_message(f"{len(items)} résultats trouvés pour '{query_to_execute}'. {messages_displayed_count} affichés.")

    except exceptions.CosmosHttpResponseError as e:
        if "Query exceeded memory limit" in str(e) or "Query exceeded maximum time limit" in str(e):
            await ctx.send("Votre demande a généré une requête trop complexe ou gourmande pour la base de données. Essayez d'être plus spécifique (par exemple, en ajoutant une période de temps plus courte).")
            await send_bot_log_message(f"Erreur Cosmos DB (limite de ressources) pour '{generated_sql_query}': {e}")
        else:
            await ctx.send("Une erreur s'est produite lors de la recherche dans la base de données.")
            await send_bot_log_message(f"Erreur Cosmos DB pour '{generated_sql_query}': {e}\n{traceback.format_exc()}")
        print(f"Erreur Cosmos DB: {e}")
    except Exception as e:
        await ctx.send("Une erreur inattendue s'est produite.")
        await send_bot_log_message(f"Erreur inattendue dans ask_command pour '{generated_sql_query}': {e}\n{traceback.format_exc()}")
        print(f"Erreur inattendue: {e}")

# ... (avant if __name__ == "__main__":)

# --- Démarrage du Bot ---
if __name__ == "__main__":
    if DISCORD_BOT_TOKEN:
        bot.run(DISCORD_BOT_TOKEN)
    else:
        print("ERREUR: DISCORD_BOT_TOKEN non trouvé. Le bot ne peut pas démarrer.")
