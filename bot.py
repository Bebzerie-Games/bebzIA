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
COSMOS_DB_ENDPOINT = os.getenv("COSMOS_DB_ENDPOINT")
COSMOS_DB_KEY = os.getenv("COSMOS_DB_KEY")
DATABASE_NAME = os.getenv("DATABASE_NAME")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
TARGET_CHANNEL_ID_STR = os.getenv("TARGET_CHANNEL_ID")
LOG_CHANNEL_ID_STR = os.getenv("LOG_CHANNEL_ID")

async def send_bot_log_message(message_content: str):
    """Envoie un message de log en utilisant l'instance bot principale, avec timestamp de Paris."""
    
    # 1. Obtenir l'heure actuelle en UTC (objet datetime "aware")
    now_utc = discord.utils.utcnow() # Fait une seule fois au début

    # 2. Définir le fuseau horaire de Paris
    paris_tz = pytz.timezone('Europe/Paris')

    # 3. Convertir l'heure UTC en heure de Paris
    now_paris = now_utc.astimezone(paris_tz)

    # 4. Formater le timestamp pour l'affichage sur Discord
    timestamp_discord_display = now_paris.strftime('%Y-%m-%d %H:%M:%S %Z') # Ex: 2025-05-13 12:13:43 CEST

    # 5. Formater un timestamp UTC pour les logs console/stdout (optionnel, mais bon pour la cohérence avec Heroku)
    timestamp_stdout_utc_display = now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')

    if not LOG_CHANNEL_ID or not bot.is_ready():
        print(f"[{timestamp_stdout_utc_display}] [BOT LOG STDOUT - CANAL LOG INDISPONIBLE/BOT NON PRET] {message_content}")
        return

    try:
        log_channel = bot.get_channel(LOG_CHANNEL_ID)
        if log_channel:
            # C'EST ICI QU'ON UTILISE LE TIMESTAMP DE PARIS POUR DISCORD
            await log_channel.send(f"```\n[Bot Auto-Fetch] {timestamp_discord_display}\n{message_content}\n```")
        else:
            print(f"[{timestamp_stdout_utc_display}] [BOT LOG STDOUT] Log channel ID {LOG_CHANNEL_ID} non trouvé. Message: {message_content}")
    except Exception as e:
        print(f"[{timestamp_stdout_utc_display}] [BOT LOG STDOUT] Erreur envoi log Discord: {e}. Message: {message_content}")

# Conversion des IDs de canaux en entiers (à faire une fois, peut-être après le on_ready ou globalement avec vérification)
TARGET_CHANNEL_ID = None
LOG_CHANNEL_ID = None
try:
    if TARGET_CHANNEL_ID_STR: TARGET_CHANNEL_ID = int(TARGET_CHANNEL_ID_STR)
    if LOG_CHANNEL_ID_STR: LOG_CHANNEL_ID = int(LOG_CHANNEL_ID_STR)
except ValueError:
    print("ERREUR CRITIQUE: TARGET_CHANNEL_ID ou LOG_CHANNEL_ID n'est pas un entier valide dans les variables d'env.")
    # Vous pourriez vouloir empêcher le bot de démarrer ici ou gérer cela plus finement

# Configuration des Intents pour le bot principal
# Assurez-vous que ces intents sont suffisants pour TOUTES les fonctionnalités de votre bot
intents = discord.Intents.default()
intents.messages = True # Si votre bot doit lire des messages
intents.message_content = True # Si votre bot a besoin du contenu des messages (privilégié)
intents.guilds = True # Souvent nécessaire
# Ajoutez d'autres intents dont votre bot a besoin (members, presences, etc. si activés)

bot = commands.Bot(command_prefix="!", intents=intents) # Adaptez le préfixe

# --- Initialisation du client Cosmos DB (peut être fait une seule fois) ---
cosmos_client = None
container_client = None
if COSMOS_DB_ENDPOINT and COSMOS_DB_KEY and DATABASE_NAME and CONTAINER_NAME:
    try:
        cosmos_client_instance = CosmosClient(COSMOS_DB_ENDPOINT, credential=COSMOS_DB_KEY)
        database_client = cosmos_client_instance.create_database_if_not_exists(id=DATABASE_NAME)
        partition_key_path = PartitionKey(path="/id") # Assurez-vous que c'est votre clé de partition
        container_client = database_client.create_container_if_not_exists(
            id=CONTAINER_NAME,
            partition_key=partition_key_path,
            offer_throughput=400
        )
        print("Connecté à Cosmos DB avec succès.")
    except Exception as e:
        print(f"ERREUR CRITIQUE lors de l'initialisation de Cosmos DB dans bot.py: {e}")
        container_client = None # S'assurer qu'il est None si échec
else:
    print("AVERTISSEMENT: Variables d'environnement pour Cosmos DB manquantes. La récupération des messages sera désactivée.")


# --- Fonctions utilitaires (copiées et adaptées de fetch_new_messages.py) ---

async def send_bot_log_message(message_content: str):
    """Envoie un message de log en utilisant l'instance bot principale."""
    if not LOG_CHANNEL_ID or not bot.is_ready():
        print(f"[BOT LOG STDOUT] {message_content}")
        return
    try:
        log_channel = bot.get_channel(LOG_CHANNEL_ID)
        if log_channel:
            timestamp = discord.utils.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
            await log_channel.send(f"```\n[Bot Auto-Fetch] {timestamp}\n{message_content}\n```")
        else:
            print(f"[BOT LOG STDOUT] Log channel ID {LOG_CHANNEL_ID} non trouvé. Message: {message_content}")
    except Exception as e:
        print(f"[BOT LOG STDOUT] Erreur envoi log Discord: {e}. Message: {message_content}")

def format_message_to_json(message: discord.Message): # Identique à votre version
    return {
        "id": str(message.id),
        "message_id_int": message.id,
        "channel_id": str(message.channel.id),
        "guild_id": str(message.guild.id) if message.guild else None,
        "author": {
            "id": str(message.author.id),
            "name": message.author.name,
            "discriminator": message.author.discriminator,
            "nickname": message.author.nick if hasattr(message.author, 'nick') else None,
            "bot": message.author.bot,
        },
        "content": message.content,
        "timestamp_iso": message.created_at.isoformat() + "Z", # discord.py utilise déjà UTC pour created_at
        "timestamp_unix": int(message.created_at.timestamp()),
        "attachments": [{"id": str(att.id), "filename": att.filename, "url": att.url, "content_type": att.content_type, "size": att.size} for att in message.attachments],
        "embeds": [embed.to_dict() for embed in message.embeds],
        "reactions": [{"emoji": str(reaction.emoji), "count": reaction.count} for reaction in message.reactions],
        "edited_timestamp_iso": message.edited_at.isoformat() + "Z" if message.edited_at else None,
    }

async def main_message_fetch_logic():
    """Logique principale de récupération des messages, adaptée pour utiliser l'instance bot."""
    if not container_client:
        await send_bot_log_message("ERREUR: Client Cosmos DB non initialisé. Abandon de la récupération.")
        return
    if not TARGET_CHANNEL_ID:
        await send_bot_log_message("ERREUR: TARGET_CHANNEL_ID non configuré. Abandon de la récupération.")
        return

    await send_bot_log_message(f"Démarrage de la tâche de récupération pour le canal ID: {TARGET_CHANNEL_ID}.")

    try:
        channel_to_fetch = bot.get_channel(TARGET_CHANNEL_ID)
        if not channel_to_fetch:
            # Essayer de le fetch si get_channel ne le trouve pas (peut arriver au démarrage si le cache n'est pas plein)
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
        
        if not channel_to_fetch: # Double vérification
            await send_bot_log_message(f"ERREUR: Canal ID {TARGET_CHANNEL_ID} toujours introuvable après tentatives.")
            return


        last_message_timestamp_unix = 0
        try:
            query = f"SELECT VALUE MAX(c.timestamp_unix) FROM c WHERE c.channel_id = '{str(TARGET_CHANNEL_ID)}'"
            results = list(container_client.query_items(query=query, enable_cross_partition_query=True))
            if results and results[0] is not None:
                last_message_timestamp_unix = results[0]
        except Exception as e:
            await send_bot_log_message(f"AVERTISSEMENT: Impossible de récupérer le timestamp du dernier message stocké: {e}. Récupération sur une période par défaut.")

        if last_message_timestamp_unix > 0:
            after_date = datetime.datetime.fromtimestamp(last_message_timestamp_unix + 0.001, tz=datetime.timezone.utc) # +0.001 pour être strictement après
            await send_bot_log_message(f"Dernier message stocké à {datetime.datetime.fromtimestamp(last_message_timestamp_unix, tz=datetime.timezone.utc).isoformat()}. Récupération des messages après cette date.")
        else:
            after_date = discord.utils.utcnow() - datetime.timedelta(days=14) # Période par défaut
            await send_bot_log_message(f"Aucun message précédent trouvé ou erreur. Récupération des messages des 7 derniers jours (depuis {after_date.isoformat()}).")

        new_messages_count = 0
        existing_messages_count = 0
        fetched_in_pass = 0

        # Utiliser channel.history()
        async for message in channel_to_fetch.history(limit=None, after=after_date, oldest_first=True):
            fetched_in_pass +=1
            message_json = format_message_to_json(message)
            item_id = message_json["id"]

            try:
                container_client.read_item(item=item_id, partition_key=item_id)
                existing_messages_count += 1
            except exceptions.CosmosResourceNotFoundError:
                try:
                    container_client.create_item(body=message_json)
                    new_messages_count += 1
                except exceptions.CosmosHttpResponseError as e_create:
                    await send_bot_log_message(f"ERREUR Cosmos DB (création) msg {item_id}: {e_create}")
            except exceptions.CosmosHttpResponseError as e_read:
                await send_bot_log_message(f"ERREUR Cosmos DB (lecture) msg {item_id}: {e_read}")
            
            if fetched_in_pass % 100 == 0: # Log de progression
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
@tasks.loop(hours=12) # Exécute toutes les heures. Adaptez (minutes=X, seconds=X)
async def scheduled_message_fetch():
    print(f"[{discord.utils.utcnow().isoformat()}] Tâche de récupération planifiée démarrée.")
    await send_bot_log_message("Démarrage de la tâche de récupération planifiée.")
    try:
        await main_message_fetch_logic()
    except Exception as e:
        # Ce catch est une sécurité supplémentaire, main_message_fetch_logic devrait déjà logger ses erreurs.
        error_details = traceback.format_exc()
        await send_bot_log_message(f"ERREUR non gérée au niveau de la tâche scheduled_message_fetch:\n{error_details}")
    await send_bot_log_message("Tâche de récupération planifiée terminée.")

@scheduled_message_fetch.before_loop
async def before_scheduled_fetch():
    """S'assure que le bot est prêt avant que la boucle ne démarre."""
    await bot.wait_until_ready()
    print("Le bot est prêt, la tâche de récupération planifiée peut commencer.")
    # Vérification initiale des IDs de canaux
    global TARGET_CHANNEL_ID, LOG_CHANNEL_ID
    valid_config = True
    if not TARGET_CHANNEL_ID_STR or not TARGET_CHANNEL_ID:
        print("ERREUR CRITIQUE: TARGET_CHANNEL_ID n'est pas configuré ou invalide. La tâche de récupération ne démarrera pas.")
        await send_bot_log_message("ERREUR CRITIQUE: TARGET_CHANNEL_ID non configuré. Tâche de récupération désactivée.")
        valid_config = False
    if not LOG_CHANNEL_ID_STR or not LOG_CHANNEL_ID: # Moins critique, mais bon à savoir
        print("AVERTISSEMENT: LOG_CHANNEL_ID n'est pas configuré ou invalide. Les logs iront sur STDOUT.")
    if not container_client:
        print("ERREUR CRITIQUE: Client Cosmos DB non initialisé. La tâche de récupération ne démarrera pas.")
        await send_bot_log_message("ERREUR CRITIQUE: Cosmos DB non initialisé. Tâche de récupération désactivée.")
        valid_config = False
        
    if not valid_config:
        scheduled_message_fetch.cancel() # Annule la tâche si la configuration est mauvaise


# --- Événements du Bot ---
@bot.event
async def on_ready():
    print(f'{bot.user} s\'est connecté à Discord!')
    print(f"ID du bot : {bot.user.id}")
    print("Le bot est prêt à recevoir des commandes.")
    
    # Démarrer la tâche de récupération planifiée
    # Vérifier si la configuration est valide avant de démarrer
    if TARGET_CHANNEL_ID and LOG_CHANNEL_ID and container_client: # Assurez-vous que les conditions sont remplies
        print("Démarrage de la tâche de récupération des messages planifiée...")
        scheduled_message_fetch.start()
    else:
        msg = "La tâche de récupération des messages planifiée n'a PAS été démarrée en raison d'une configuration manquante (Canaux IDs ou CosmosDB)."
        print(msg)
        # Essayer d'envoyer un log si possible, même si LOG_CHANNEL_ID est manquant (ira sur stdout)
        await send_bot_log_message(f"AVERTISSEMENT CRITIQUE: {msg}")


# --- Commandes du Bot (si vous en avez) ---
@bot.command(name='ping')
async def ping(ctx):
    await ctx.send(f'Pong! Latence: {round(bot.latency * 1000)}ms')

# --- Démarrage du Bot ---
if __name__ == "__main__":
    if DISCORD_BOT_TOKEN:
        bot.run(DISCORD_BOT_TOKEN)
    else:
        print("ERREUR: DISCORD_BOT_TOKEN non trouvé. Le bot ne peut pas démarrer.")
