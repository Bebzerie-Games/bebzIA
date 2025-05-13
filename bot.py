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

# --- Conversion des IDs de canaux en entiers ---
# Doit être fait AVANT la première utilisation de LOG_CHANNEL_ID dans send_bot_log_message
# si cette fonction est appelée avant que 'bot' soit défini globalement.
# Cependant, send_bot_log_message utilise 'bot.is_ready()', donc 'bot' doit être défini.
# Plaçons la conversion des IDs après la définition de 'bot'.

# --- Définition de send_bot_log_message (Version corrigée avec heure de Paris) ---
async def send_bot_log_message(message_content: str):
    """Envoie un message de log en utilisant l'instance bot principale, avec timestamp de Paris."""
    
    now_utc = discord.utils.utcnow()
    paris_tz = pytz.timezone('Europe/Paris')
    now_paris = now_utc.astimezone(paris_tz)
    
    timestamp_discord_display = now_paris.strftime('%Y-%m-%d %H:%M:%S %Z') 
    timestamp_stdout_utc_display = now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')

    # Le print de débogage que nous avions ajouté peut être conservé ou supprimé
    # print(f"[{timestamp_stdout_utc_display}] [DEBUG DANS send_bot_log_message] Timestamp Paris calculé: {timestamp_discord_display}")

    if not LOG_CHANNEL_ID or not bot.is_ready(): # bot doit être accessible ici
        print(f"[{timestamp_stdout_utc_display}] [BOT LOG STDOUT - CANAL LOG INDISPONIBLE/BOT NON PRET] {message_content}")
        return

    try:
        log_channel = bot.get_channel(LOG_CHANNEL_ID)
        if log_channel:
            # Version avec la section de débogage (vous pouvez la simplifier si le test est concluant)
            # debug_timezone_info = f"Fuseau utilisé pour ce message: Europe/Paris. Timestamp calculé: {timestamp_discord_display}"
            # full_message_to_send = (
            #     f"```\n"
            #     f"[Bot Auto-Fetch] {timestamp_discord_display}\n"
            #     f"{message_content}\n"
            #     f"---\n"
            #     f"[Debug Info] {debug_timezone_info}\n"
            #     f"```"
            # )
            # await log_channel.send(full_message_to_send)
            
            # Version simplifiée sans la section de débogage supplémentaire une fois que ça marche :
            await log_channel.send(f"```\n[Bot Auto-Fetch] {timestamp_discord_display}\n{message_content}\n```")
        else:
            print(f"[{timestamp_stdout_utc_display}] [BOT LOG STDOUT] Log channel ID {LOG_CHANNEL_ID} non trouvé. Message: {message_content}")
    except Exception as e:
        print(f"[{timestamp_stdout_utc_display}] [BOT LOG STDOUT] Erreur envoi log Discord: {e}. Message: {message_content}")
        print(traceback.format_exc())


# --- Configuration des Intents et du Bot ---
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents)

# --- Conversion des IDs de canaux (placée après la définition de 'bot') ---
TARGET_CHANNEL_ID = None
LOG_CHANNEL_ID = None # Sera utilisé par send_bot_log_message
try:
    if TARGET_CHANNEL_ID_STR: TARGET_CHANNEL_ID = int(TARGET_CHANNEL_ID_STR)
    if LOG_CHANNEL_ID_STR: LOG_CHANNEL_ID = int(LOG_CHANNEL_ID_STR) # Initialise la variable globale
except ValueError:
    print("ERREUR CRITIQUE: TARGET_CHANNEL_ID ou LOG_CHANNEL_ID n'est pas un entier valide dans les variables d'env.")


# --- Initialisation du client Cosmos DB ---
cosmos_client = None
container_client = None
if COSMOS_DB_ENDPOINT and COSMOS_DB_KEY and DATABASE_NAME and CONTAINER_NAME:
    try:
        cosmos_client_instance = CosmosClient(COSMOS_DB_ENDPOINT, credential=COSMOS_DB_KEY)
        database_client = cosmos_client_instance.create_database_if_not_exists(id=DATABASE_NAME)
        partition_key_path = PartitionKey(path="/id")
        container_client = database_client.create_container_if_not_exists(
            id=CONTAINER_NAME,
            partition_key=partition_key_path,
            offer_throughput=400
        )
        print("Connecté à Cosmos DB avec succès.")
    except Exception as e:
        print(f"ERREUR CRITIQUE lors de l'initialisation de Cosmos DB dans bot.py: {e}")
        container_client = None
else:
    print("AVERTISSEMENT: Variables d'environnement pour Cosmos DB manquantes. La récupération des messages sera désactivée.")


# --- Fonctions utilitaires (format_message_to_json reste ici) ---
# LA DÉFINITION DUPLIQUÉE DE send_bot_log_message A ÉTÉ SUPPRIMÉE D'ICI

def format_message_to_json(message: discord.Message):
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
        "timestamp_iso": message.created_at.isoformat() + "Z",
        "timestamp_unix": int(message.created_at.timestamp()),
        "attachments": [{"id": str(att.id), "filename": att.filename, "url": att.url, "content_type": att.content_type, "size": att.size} for att in message.attachments],
        "embeds": [embed.to_dict() for embed in message.embeds],
        "reactions": [{"emoji": str(reaction.emoji), "count": reaction.count} for reaction in message.reactions],
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
        if not channel_to_fetch:
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
        
        if not channel_to_fetch:
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
            after_date = datetime.datetime.fromtimestamp(last_message_timestamp_unix + 0.001, tz=datetime.timezone.utc)
            await send_bot_log_message(f"Dernier message stocké à {datetime.datetime.fromtimestamp(last_message_timestamp_unix, tz=datetime.timezone.utc).isoformat()}. Récupération des messages après cette date.")
        else:
            # La période de récupération par défaut que vous aviez ajustée
            after_date = discord.utils.utcnow() - datetime.timedelta(days=14) 
            await send_bot_log_message(f"Aucun message précédent trouvé ou erreur. Récupération des messages des {datetime.timedelta(days=14).days} derniers jours (depuis {after_date.isoformat()}).")


        new_messages_count = 0
        existing_messages_count = 0
        fetched_in_pass = 0

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
            
            if fetched_in_pass % 100 == 0:
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
    # Utilisation de discord.utils.utcnow() pour le print console, qui est toujours en UTC
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
    # global TARGET_CHANNEL_ID, LOG_CHANNEL_ID # Déjà globales, pas besoin de redéclarer ici
    valid_config = True
    if not TARGET_CHANNEL_ID_STR or not TARGET_CHANNEL_ID:
        print("ERREUR CRITIQUE: TARGET_CHANNEL_ID n'est pas configuré ou invalide. La tâche de récupération ne démarrera pas.")
        await send_bot_log_message("ERREUR CRITIQUE: TARGET_CHANNEL_ID non configuré. Tâche de récupération désactivée.")
        valid_config = False
    if not LOG_CHANNEL_ID_STR or not LOG_CHANNEL_ID:
        print("AVERTISSEMENT: LOG_CHANNEL_ID n'est pas configuré ou invalide. Les logs iront sur STDOUT.")
    if not container_client:
        print("ERREUR CRITIQUE: Client Cosmos DB non initialisé. La tâche de récupération ne démarrera pas.")
        await send_bot_log_message("ERREUR CRITIQUE: Cosmos DB non initialisé. Tâche de récupération désactivée.")
        valid_config = False
        
    if not valid_config:
        scheduled_message_fetch.cancel()

# --- Événements du Bot ---
@bot.event
async def on_ready():
    print(f'{bot.user} s\'est connecté à Discord!')
    print(f"ID du bot : {bot.user.id}")
    print("Le bot est prêt à recevoir des commandes.")
    
    if TARGET_CHANNEL_ID and LOG_CHANNEL_ID and container_client:
        print("Démarrage de la tâche de récupération des messages planifiée...")
        scheduled_message_fetch.start()
    else:
        msg = "La tâche de récupération des messages planifiée n'a PAS été démarrée en raison d'une configuration manquante (Canaux IDs ou CosmosDB)."
        print(msg)
        await send_bot_log_message(f"AVERTISSEMENT CRITIQUE: {msg}") # Utilisera la bonne fonction send_bot_log_message

# --- Commandes du Bot ---
@bot.command(name='ping')
async def ping(ctx):
    await ctx.send(f'Pong! Latence: {round(bot.latency * 1000)}ms')

# --- Démarrage du Bot ---
if __name__ == "__main__":
    if DISCORD_BOT_TOKEN:
        bot.run(DISCORD_BOT_TOKEN)
    else:
        print("ERREUR: DISCORD_BOT_TOKEN non trouvé. Le bot ne peut pas démarrer.")
