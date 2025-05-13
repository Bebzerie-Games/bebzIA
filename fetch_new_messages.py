import discord
import os
import datetime
import json
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import asyncio

# Charger les variables d'environnement
load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
COSMOS_DB_ENDPOINT = os.getenv("COSMOS_DB_ENDPOINT")
COSMOS_DB_KEY = os.getenv("COSMOS_DB_KEY")
DATABASE_NAME = os.getenv("DATABASE_NAME")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
TARGET_CHANNEL_ID_STR = os.getenv("TARGET_CHANNEL_ID")
LOG_CHANNEL_ID_STR = os.getenv("LOG_CHANNEL_ID")

# --- Configuration Globale du Client Discord (pour les logs) ---
# Nous devons initialiser le client ici pour que la fonction send_log_message puisse l'utiliser
# même en cas d'échec avant la boucle principale.
intents_log = discord.Intents.default() # Pas besoin d'intents spéciaux pour envoyer un message si le bot est déjà sur le serveur
discord_log_client = discord.Client(intents=intents_log)
LOG_CHANNEL_ID = None
TARGET_CHANNEL_ID = None

async def send_log_message(message_content: str):
    if not LOG_CHANNEL_ID or not discord_log_client.is_ready():
        print(f"[LOG STDOUT] {message_content}") # Log sur stdout si le client n'est pas prêt ou ID non défini
        return
    try:
        log_channel = discord_log_client.get_channel(LOG_CHANNEL_ID)
        if log_channel:
            await log_channel.send(f"```\n[BebzIA Fetcher] {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n{message_content}\n```")
        else:
            print(f"[LOG STDOUT] Log channel with ID {LOG_CHANNEL_ID} not found. Message: {message_content}")
    except Exception as e:
        print(f"[LOG STDOUT] Error sending log message to Discord: {e}. Message: {message_content}")

async def main_fetch_logic():
    global TARGET_CHANNEL_ID # Pour pouvoir l'utiliser dans cette fonction
    global LOG_CHANNEL_ID # Pour pouvoir l'utiliser dans cette fonction

    # Vérifier les variables d'environnement
    if not all([DISCORD_BOT_TOKEN, COSMOS_DB_ENDPOINT, COSMOS_DB_KEY, DATABASE_NAME, CONTAINER_NAME, TARGET_CHANNEL_ID_STR, LOG_CHANNEL_ID_STR]):
        await send_log_message("ERREUR CRITIQUE: Des variables d'environnement sont manquantes. Vérifiez la configuration.")
        return

    try:
        TARGET_CHANNEL_ID = int(TARGET_CHANNEL_ID_STR)
        LOG_CHANNEL_ID = int(LOG_CHANNEL_ID_STR)
    except ValueError:
        await send_log_message(f"ERREUR CRITIQUE: TARGET_CHANNEL_ID ('{TARGET_CHANNEL_ID_STR}') ou LOG_CHANNEL_ID ('{LOG_CHANNEL_ID_STR}') n'est pas un ID de canal valide.")
        return

    await send_log_message(f"Démarrage du script de récupération pour le canal ID: {TARGET_CHANNEL_ID}.")

    # Initialisation du client Cosmos DB
    try:
        cosmos_client = CosmosClient(COSMOS_DB_ENDPOINT, credential=COSMOS_DB_KEY)
        database_client = cosmos_client.create_database_if_not_exists(id=DATABASE_NAME)
        partition_key_path = PartitionKey(path="/id")
        container_client = database_client.create_container_if_not_exists(
            id=CONTAINER_NAME,
            partition_key=partition_key_path,
            offer_throughput=400
        )
        await send_log_message(f"Connecté à Cosmos DB. Base: '{DATABASE_NAME}', Conteneur: '{CONTAINER_NAME}'.")
    except Exception as e:
        await send_log_message(f"ERREUR CRITIQUE lors de l'initialisation de Cosmos DB: {e}")
        return

    # Initialisation du client Discord pour la récupération
    # Note: discord_log_client est déjà initialisé globalement. Nous utilisons un client dédié pour le fetch.
    intents_fetch = discord.Intents.default()
    intents_fetch.messages = True
    intents_fetch.message_content = True
    discord_fetch_client = discord.Client(intents=intents_fetch)

    try:
        await discord_fetch_client.login(DISCORD_BOT_TOKEN)
        # Pas besoin de discord_fetch_client.wait_until_ready() ici car fetch_channel est un appel API direct
    except Exception as e:
        await send_log_message(f"ERREUR CRITIQUE: Échec de la connexion à Discord pour la récupération: {e}")
        return


    # Logique de récupération
    try:
        channel_to_fetch = await discord_fetch_client.fetch_channel(TARGET_CHANNEL_ID)
    except discord.NotFound:
        await send_log_message(f"ERREUR: Impossible de trouver le canal à surveiller avec l'ID {TARGET_CHANNEL_ID}.")
        await discord_fetch_client.close()
        return
    except discord.Forbidden:
        await send_log_message(f"ERREUR: Permissions insuffisantes pour accéder au canal {TARGET_CHANNEL_ID}.")
        await discord_fetch_client.close()
        return
    except Exception as e:
        await send_log_message(f"ERREUR inattendue lors de la récupération du canal {TARGET_CHANNEL_ID}: {e}")
        await discord_fetch_client.close()
        return

    last_message_timestamp_unix = 0
    try:
        query = f"SELECT VALUE MAX(c.timestamp_unix) FROM c WHERE c.channel_id = '{str(TARGET_CHANNEL_ID)}'"
        results = list(container_client.query_items(query=query, enable_cross_partition_query=True))
        if results and results[0] is not None:
            last_message_timestamp_unix = results[0]
    except Exception as e:
        await send_log_message(f"AVERTISSEMENT: Impossible de récupérer le timestamp du dernier message stocké: {e}. Récupération sur une période par défaut.")

    if last_message_timestamp_unix > 0:
        after_date = datetime.datetime.fromtimestamp(last_message_timestamp_unix + 1, tz=datetime.timezone.utc)
        await send_log_message(f"Dernier message stocké à {datetime.datetime.fromtimestamp(last_message_timestamp_unix, tz=datetime.timezone.utc).isoformat()}. Récupération des messages après cette date.")
    else:
        after_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7) # Période par défaut
        await send_log_message(f"Aucun message précédent trouvé ou erreur. Récupération des messages des 7 derniers jours (depuis {after_date.isoformat()}).")

    new_messages_count = 0
    existing_messages_count = 0

    try:
        async for message in channel_to_fetch.history(limit=None, after=after_date, oldest_first=True):
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
                    await send_log_message(f"ERREUR Cosmos DB (création) msg {item_id}: {e_create}")
            except exceptions.CosmosHttpResponseError as e_read:
                await send_log_message(f"ERREUR Cosmos DB (lecture) msg {item_id}: {e_read}")

        summary_message = f"Récupération terminée pour le canal '{channel_to_fetch.name}' (ID: {TARGET_CHANNEL_ID}).\n"
        summary_message += f"- Nouveaux messages ajoutés : {new_messages_count}\n"
        summary_message += f"- Messages déjà existants ignorés : {existing_messages_count}"
        await send_log_message(summary_message)

    except Exception as e:
        await send_log_message(f"ERREUR pendant la boucle de récupération/stockage des messages: {e}")
    finally:
        await discord_fetch_client.close()
        # print("Connexion Discord pour la récupération fermée.") # Log local si besoin

def format_message_to_json(message: discord.Message): # Identique à la version précédente
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

async def run_script():
    # Le client pour les logs doit être connecté pour envoyer des messages.
    # Il est préférable de le connecter une seule fois au début.
    # S'il y a une erreur de connexion, les logs iront sur stdout.
    try:
        # Le login pour le client de log doit être fait avant d'appeler main_fetch_logic
        # qui pourrait vouloir envoyer un message de log immédiatement.
        # On ne garde le client de log connecté que le temps de l'exécution du script.
        await discord_log_client.login(DISCORD_BOT_TOKEN)
        # On ne fait pas discord_log_client.run() car cela bloquerait.
        # On a juste besoin qu'il soit connecté pour pouvoir appeler client.get_channel().send().
        # Il faut attendre qu'il soit prêt.
        await discord_log_client.wait_until_ready() # Important!
        print(f"Client Discord pour les logs connecté en tant que {discord_log_client.user}")

        await main_fetch_logic() # Exécute la logique principale

    except discord.errors.LoginFailure:
        print(f"[LOG STDOUT] ERREUR CRITIQUE: Échec de la connexion du client de log Discord. Token incorrect ou intents manquants pour le bot sur le serveur de log.")
    except Exception as e:
        # Essayer d'envoyer un dernier message si possible, sinon print
        general_error_msg = f"ERREUR CRITIQUE non gérée dans run_script: {e}"
        if discord_log_client.is_ready() and LOG_CHANNEL_ID: # Vérifier si on peut encore envoyer
            try:
                log_channel = discord_log_client.get_channel(LOG_CHANNEL_ID)
                if log_channel:
                    await log_channel.send(f"```\n[BebzIA Fetcher] {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n{general_error_msg}\n```")
                else:
                    print(f"[LOG STDOUT] {general_error_msg}")
            except Exception as log_e:
                print(f"[LOG STDOUT] {general_error_msg} (erreur lors de l'envoi du log final: {log_e})")
        else:
            print(f"[LOG STDOUT] {general_error_msg}")
    finally:
        if discord_log_client.is_ready():
            await discord_log_client.close()
            print("Client Discord pour les logs déconnecté.")
        print("Script de récupération des messages terminé (run_script).")


if __name__ == "__main__":
    print("Démarrage de run_script...")
    try:
        asyncio.run(run_script())
    except KeyboardInterrupt:
        print("Script interrompu manuellement (KeyboardInterrupt).")
    except Exception as e:
        print(f"Erreur non gérée au niveau de asyncio.run : {e}")
    finally:
        print("Exécution de __main__ terminée.")
# ... (le reste de votre script au-dessus) ...

async def run_script():
    global LOG_CHANNEL_ID # Ensure it's accessible
    try:
        print("[DEBUG] run_script: Début de la fonction run_script.")
        print(f"[DEBUG] run_script: Vérification du DISCORD_BOT_TOKEN: {'Présent' if DISCORD_BOT_TOKEN else 'MANQUANT'}")
        print(f"[DEBUG] run_script: Vérification du LOG_CHANNEL_ID_STR: {LOG_CHANNEL_ID_STR}")


        print("[DEBUG] run_script: Tentative de discord_log_client.login()...")
        await discord_log_client.login(DISCORD_BOT_TOKEN)
        print("[DEBUG] run_script: discord_log_client.login() terminé.")

        print("[DEBUG] run_script: Tentative de discord_log_client.wait_until_ready()...")
        await discord_log_client.wait_until_ready()
        # Cette ligne ne s'affichera que si wait_until_ready se termine
        print(f"[DEBUG] run_script: discord_log_client.wait_until_ready() terminé. Client Discord pour les logs connecté en tant que {discord_log_client.user}")

        await main_fetch_logic()

    except discord.errors.LoginFailure:
        print(f"[LOG STDOUT] ERREUR CRITIQUE: Échec de la connexion du client de log Discord. Token incorrect.")
        # Vous pourriez vouloir ajouter plus de détails ici si possible, mais LoginFailure est souvent juste ça.
    except Exception as e:
        # Formatage pour mieux voir l'erreur et sa trace dans les logs Heroku
        import traceback
        error_details = traceback.format_exc()
        general_error_msg = f"ERREUR CRITIQUE non gérée dans run_script: {e}\nTrace:\n{error_details}"
        
        # Essayez d'envoyer un dernier message si possible, sinon print
        if discord_log_client.is_ready() and LOG_CHANNEL_ID:
            try:
                log_channel = discord_log_client.get_channel(LOG_CHANNEL_ID)
                if log_channel:
                    # Diviser en plusieurs messages si trop long pour Discord
                    limit = 1900 # Laisse de la marge pour le formatage
                    parts = [general_error_msg[i:i+limit] for i in range(0, len(general_error_msg), limit)]
                    for part in parts:
                        await log_channel.send(f"```\n[BebzIA Fetcher ERROR]\n{part}\n```")
                else:
                    print(f"[LOG STDOUT] {general_error_msg}")
            except Exception as log_e:
                print(f"[LOG STDOUT] {general_error_msg} (erreur lors de l'envoi du log final: {log_e})")
        else:
            print(f"[LOG STDOUT] {general_error_msg}")
    finally:
        if discord_log_client.is_ready():
            print("[DEBUG] run_script: Tentative de déconnexion du discord_log_client...")
            await discord_log_client.close()
            print("[DEBUG] run_script: discord_log_client déconnecté.")
        else:
            print("[DEBUG] run_script: discord_log_client n'était pas prêt, pas de déconnexion nécessaire ou possible proprement.")
        print("Script de récupération des messages terminé (run_script).")

# ... (le reste de votre script en dessous, y compris if __name__ == "__main__":) ...