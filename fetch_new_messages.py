print("[FETCH_SCRIPT_DEBUG] Le fichier fetch_new_messages.py est en cours d'exécution - VERSION AVEC DEBUG INTENSIF") # Optionnel, mais utile pour confirmer la version du fichier

import discord
import os
import datetime
import json
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import asyncio
import traceback

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
intents_log = discord.Intents.default()
discord_log_client = discord.Client(intents=intents_log)
LOG_CHANNEL_ID = None # Sera défini dans main_fetch_logic après conversion de LOG_CHANNEL_ID_STR
TARGET_CHANNEL_ID = None # Sera défini dans main_fetch_logic

async def send_log_message(message_content: str):
    # ... (votre code send_log_message, inchangé)
    if not LOG_CHANNEL_ID or not discord_log_client.is_ready():
        print(f"[LOG STDOUT] {message_content}") 
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
    # ... (votre code main_fetch_logic, inchangé)
    # N'oubliez pas que LOG_CHANNEL_ID et TARGET_CHANNEL_ID sont définis ici après la conversion depuis STR
    global TARGET_CHANNEL_ID
    global LOG_CHANNEL_ID

    if not all([DISCORD_BOT_TOKEN, COSMOS_DB_ENDPOINT, COSMOS_DB_KEY, DATABASE_NAME, CONTAINER_NAME, TARGET_CHANNEL_ID_STR, LOG_CHANNEL_ID_STR]):
        await send_log_message("ERREUR CRITIQUE: Des variables d'environnement sont manquantes. Vérifiez la configuration.")
        return
    try:
        TARGET_CHANNEL_ID = int(TARGET_CHANNEL_ID_STR)
        LOG_CHANNEL_ID = int(LOG_CHANNEL_ID_STR) # S'assurer que LOG_CHANNEL_ID global est bien un int
    except ValueError:
        await send_log_message(f"ERREUR CRITIQUE: TARGET_CHANNEL_ID ('{TARGET_CHANNEL_ID_STR}') ou LOG_CHANNEL_ID ('{LOG_CHANNEL_ID_STR}') n'est pas un ID de canal valide.")
        return
    # ... reste de main_fetch_logic

def format_message_to_json(message: discord.Message):
    # ... (votre code format_message_to_json, inchangé)
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

# C'est la version de run_script que nous voulons tester (celle avec les DEBUG prints)
async def run_script():
    # global LOG_CHANNEL_ID # LOG_CHANNEL_ID est déjà global, il sera initialisé dans main_fetch_logic
    print("[DEBUG] run_script: Entrée dans la fonction async run_script.")
    try:
        # Note: LOG_CHANNEL_ID (l'entier) n'est pas encore défini ici, il est défini dans main_fetch_logic.
        # send_log_message gère cela en loguant sur STDOUT si LOG_CHANNEL_ID n'est pas prêt.
        print(f"[DEBUG] run_script: Vérification du DISCORD_BOT_TOKEN: {'Présent' if DISCORD_BOT_TOKEN else 'MANQUANT'}")
        print(f"[DEBUG] run_script: Vérification du LOG_CHANNEL_ID_STR (avant conversion en int): {LOG_CHANNEL_ID_STR}")

        print("[DEBUG] run_script: Tentative de discord_log_client.login()...")
        await discord_log_client.login(DISCORD_BOT_TOKEN)
        print("[DEBUG] run_script: discord_log_client.login() terminé.")

        print("[DEBUG] run_script: Tentative de discord_log_client.wait_until_ready()...")
        await discord_log_client.wait_until_ready()
        print(f"[DEBUG] run_script: discord_log_client.wait_until_ready() terminé. Client Discord pour les logs connecté en tant que {discord_log_client.user}")

        # C'est ici que main_fetch_logic sera appelé, qui à son tour définira LOG_CHANNEL_ID (l'entier)
        await main_fetch_logic()

    except discord.errors.LoginFailure:
        # À ce stade, LOG_CHANNEL_ID (int) n'est peut-être pas défini si l'erreur s'est produite avant main_fetch_logic
        print(f"[LOG STDOUT] ERREUR CRITIQUE: Échec de la connexion du client de log Discord. Token incorrect.")
    except Exception as e:
        error_details = traceback.format_exc()
        general_error_msg = f"ERREUR CRITIQUE non gérée dans run_script: {e}\nTrace:\n{error_details}"
        
        # LOG_CHANNEL_ID (int) pourrait ne pas être défini ici si l'erreur s'est produite avant main_fetch_logic
        # send_log_message gérera cela et loguera sur STDOUT si nécessaire.
        # Pour être sûr, on peut vérifier si LOG_CHANNEL_ID (int) a été défini.
        log_channel_id_for_error = None
        try:
            if LOG_CHANNEL_ID and isinstance(LOG_CHANNEL_ID, int): # Vérifie si LOG_CHANNEL_ID est un int défini
                 log_channel_id_for_error = LOG_CHANNEL_ID
        except NameError: # Au cas où LOG_CHANNEL_ID n'est même pas dans le scope global (ne devrait pas arriver avec la structure actuelle)
            pass

        if discord_log_client.is_ready() and log_channel_id_for_error:
            try:
                log_channel_obj = discord_log_client.get_channel(log_channel_id_for_error)
                if log_channel_obj:
                    limit = 1900
                    parts = [general_error_msg[i:i+limit] for i in range(0, len(general_error_msg), limit)]
                    for part in parts:
                        await log_channel_obj.send(f"```\n[BebzIA Fetcher ERROR]\n{part}\n```")
                else:
                    print(f"[LOG STDOUT] {general_error_msg} (Log channel non trouvé avec ID {log_channel_id_for_error})")
            except Exception as log_e:
                print(f"[LOG STDOUT] {general_error_msg} (erreur lors de l'envoi du log final: {log_e})")
        else:
            print(f"[LOG STDOUT] {general_error_msg} (Client de log non prêt ou LOG_CHANNEL_ID (int) non défini pour l'erreur finale)")
    finally:
        if discord_log_client.is_ready():
            print("[DEBUG] run_script: Tentative de déconnexion du discord_log_client...")
            await discord_log_client.close()
            print("[DEBUG] run_script: discord_log_client déconnecté.")
        else:
            print("[DEBUG] run_script: discord_log_client n'était pas prêt, pas de déconnexion.")
        print("[DEBUG] run_script: Fin de la fonction async run_script (bloc finally).")


# UN SEUL bloc if __name__ == "__main__" à la fin
if __name__ == "__main__":
    print("[FETCH_SCRIPT_DEBUG] Entrée dans if __name__ == '__main__'.")
    print("Démarrage de run_script... (appel à asyncio.run)")
    try:
        asyncio.run(run_script())
    except KeyboardInterrupt:
        print("Script interrompu manuellement (KeyboardInterrupt).")
    except Exception as e:
        detailed_error = traceback.format_exc()
        print(f"Erreur non gérée au niveau de asyncio.run : {e}\nTrace:\n{detailed_error}")
    finally:
        print("[FETCH_SCRIPT_DEBUG] Exécution de __main__ terminée (bloc finally).")
