print("[FETCH_SCRIPT_DEBUG] Le fichier fetch_new_messages.py est en cours d'exécution - VERSION TEST MINIMAL CONNEXION") # Optionnel

import discord # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< CETTE LIGNE EST ESSENTIELLE
import os
import datetime
import json
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey, exceptions # Pas utilisé dans le test minimal, mais peut rester
import asyncio
import traceback

# Charger les variables d'environnement
load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
LOG_CHANNEL_ID_STR = os.getenv("LOG_CHANNEL_ID")
# COSMOS_DB_ENDPOINT etc. ne sont pas nécessaires pour ce test minimal mais peuvent rester

# --- Configuration Globale du Client Discord (pour les logs) ---
intents_log = discord.Intents.default()
discord_log_client = discord.Client(intents=intents_log)

# ... (le reste du code que je vous ai fourni pour run_script_minimal_connect_test et if __name__ == "__main__") ...

# ... (send_log_message, main_fetch_logic, format_message_to_json peuvent rester définies mais ne seront pas appelées dans ce test)

async def run_script_minimal_connect_test():
    print("[DEBUG] run_script_minimal_connect_test: Début du test de connexion minimal.")
    if not DISCORD_BOT_TOKEN:
        print("[DEBUG] run_script_minimal_connect_test: DISCORD_BOT_TOKEN MANQUANT.")
        return
    if not LOG_CHANNEL_ID_STR:
        print("[DEBUG] run_script_minimal_connect_test: LOG_CHANNEL_ID_STR MANQUANT.")
        # On peut continuer pour tester la connexion, mais le log ne fonctionnera pas
    
    log_channel_id_int = None
    try:
        log_channel_id_int = int(LOG_CHANNEL_ID_STR)
    except Exception as e:
        print(f"[DEBUG] run_script_minimal_connect_test: Erreur conversion LOG_CHANNEL_ID_STR: {e}")
        # On peut continuer pour tester la connexion

    try:
        print("[DEBUG] run_script_minimal_connect_test: Tentative de discord_log_client.login()...")
        await discord_log_client.login(DISCORD_BOT_TOKEN)
        print("[DEBUG] run_script_minimal_connect_test: discord_log_client.login() terminé.")

        print("[DEBUG] run_script_minimal_connect_test: Tentative de discord_log_client.wait_until_ready() avec timeout de 30s...")
        # asyncio.wait_for peut être utilisé pour ajouter un timeout à une coroutine
        await asyncio.wait_for(discord_log_client.wait_until_ready(), timeout=30.0)
        print(f"[DEBUG] run_script_minimal_connect_test: discord_log_client.wait_until_ready() terminé. Client connecté: {discord_log_client.user}")

        if log_channel_id_int:
            log_channel_obj = discord_log_client.get_channel(log_channel_id_int)
            if log_channel_obj:
                print(f"[DEBUG] run_script_minimal_connect_test: Tentative d'envoi de message de test au canal {log_channel_id_int}")
                await log_channel_obj.send("Message de test depuis run_script_minimal_connect_test Heroku.")
                print("[DEBUG] run_script_minimal_connect_test: Message de test envoyé.")
            else:
                print(f"[DEBUG] run_script_minimal_connect_test: Canal de log {log_channel_id_int} non trouvé.")
        else:
            print("[DEBUG] run_script_minimal_connect_test: Pas d'ID de canal de log valide pour envoyer un message test.")

    except asyncio.TimeoutError:
        print("[DEBUG] run_script_minimal_connect_test: TIMEOUT ! discord_log_client.wait_until_ready() n'a pas terminé en 30 secondes.")
    except discord.errors.LoginFailure as lf:
        print(f"[DEBUG] run_script_minimal_connect_test: LoginFailure - {lf}")
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"[DEBUG] run_script_minimal_connect_test: ERREUR INATTENDUE - {e}\nTrace:\n{error_details}")
    finally:
        if discord_log_client.is_ready():
            print("[DEBUG] run_script_minimal_connect_test: Tentative de déconnexion...")
            await discord_log_client.close()
            print("[DEBUG] run_script_minimal_connect_test: Déconnecté.")
        else:
            print("[DEBUG] run_script_minimal_connect_test: Client non prêt, pas de déconnexion nécessaire.")
        print("[DEBUG] run_script_minimal_connect_test: Test de connexion minimal terminé.")


if __name__ == "__main__":
    print("[FETCH_SCRIPT_DEBUG] Le fichier fetch_new_messages.py est en cours d'exécution - VERSION TEST MINIMAL CONNEXION")
    print("[FETCH_SCRIPT_DEBUG] Entrée dans if __name__ == '__main__'.")
    print("Démarrage de run_script_minimal_connect_test... (appel à asyncio.run)")
    try:
        # Assurez-vous que worker.1 est toujours à 0 pour ce test
        print("RAPPEL: Assurez-vous que le dyno worker (bot.py) est arrêté (heroku ps:scale worker=0)")
        asyncio.run(run_script_minimal_connect_test()) # APPEL DE LA NOUVELLE FONCTION DE TEST
    except KeyboardInterrupt:
        print("Script interrompu manuellement (KeyboardInterrupt).")
    except Exception as e:
        import traceback
        detailed_error = traceback.format_exc()
        print(f"Erreur non gérée au niveau de asyncio.run : {e}\nTrace:\n{detailed_error}")
    finally:
        print("[FETCH_SCRIPT_DEBUG] Exécution de __main__ terminée (bloc finally).")