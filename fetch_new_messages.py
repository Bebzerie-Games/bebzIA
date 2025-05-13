print("[FETCH_SCRIPT_DEBUG] Le fichier fetch_new_messages.py est en cours d'exécution - VERSION TEST MINIMAL CONNEXION (Intents.all + discord logging)")

import discord
import os
# import datetime # Pas utilisé dans ce test minimal spécifique
# import json # Pas utilisé dans ce test minimal spécifique
from dotenv import load_dotenv
# from azure.cosmos import CosmosClient, PartitionKey, exceptions # Pas utilisé
import asyncio
import traceback
import logging # <<<<<<< AJOUTER CET IMPORT

# Charger les variables d'environnement
load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
LOG_CHANNEL_ID_STR = os.getenv("LOG_CHANNEL_ID")


# --- Configuration Globale du Client Discord (pour les logs) ---
# intents_log = discord.Intents.default() # Ancienne version
intents_log = discord.Intents.all() # <<<<<<< MODIFICATION POUR TESTER AVEC TOUS LES INTENTS
print(f"[DEBUG] Intents pour discord_log_client: Value={intents_log.value}, Flags={intents_log}") # Pour voir les flags activés
discord_log_client = discord.Client(intents=intents_log)


async def run_script_minimal_connect_test():
    print("[DEBUG] run_script_minimal_connect_test: Début du test de connexion minimal.")
    if not DISCORD_BOT_TOKEN:
        print("[DEBUG] run_script_minimal_connect_test: DISCORD_BOT_TOKEN MANQUANT.")
        return
    
    log_channel_id_int = None
    if LOG_CHANNEL_ID_STR:
        try:
            log_channel_id_int = int(LOG_CHANNEL_ID_STR)
        except ValueError:
            print(f"[DEBUG] run_script_minimal_connect_test: LOG_CHANNEL_ID_STR ('{LOG_CHANNEL_ID_STR}') n'est pas un entier valide.")
    else:
        print("[DEBUG] run_script_minimal_connect_test: LOG_CHANNEL_ID_STR MANQUANT ou vide.")


    try:
        print("[DEBUG] run_script_minimal_connect_test: Tentative de discord_log_client.login()...")
        await discord_log_client.login(DISCORD_BOT_TOKEN)
        print("[DEBUG] run_script_minimal_connect_test: discord_log_client.login() terminé.")

        print("[DEBUG] run_script_minimal_connect_test: Tentative de discord_log_client.wait_until_ready() avec timeout de 45s...") # Augmentation légère du timeout
        await asyncio.wait_for(discord_log_client.wait_until_ready(), timeout=45.0) # <<<<<<< TIMEOUT AUGMENTÉ
        print(f"[DEBUG] run_script_minimal_connect_test: discord_log_client.wait_until_ready() terminé. Client connecté: {discord_log_client.user}")

        if log_channel_id_int and discord_log_client.is_ready(): # Vérifier is_ready ici aussi
            log_channel_obj = discord_log_client.get_channel(log_channel_id_int)
            if log_channel_obj:
                print(f"[DEBUG] run_script_minimal_connect_test: Tentative d'envoi de message de test au canal {log_channel_id_int}")
                await log_channel_obj.send("Message de test (Intents.all) depuis run_script_minimal_connect_test Heroku.")
                print("[DEBUG] run_script_minimal_connect_test: Message de test envoyé.")
            else:
                print(f"[DEBUG] run_script_minimal_connect_test: Canal de log {log_channel_id_int} non trouvé.")
        elif not log_channel_id_int:
             print("[DEBUG] run_script_minimal_connect_test: Pas d'ID de canal de log valide pour envoyer un message test.")
        elif not discord_log_client.is_ready():
            print("[DEBUG] run_script_minimal_connect_test: Client non prêt, impossible d'envoyer le message de test.")


    except asyncio.TimeoutError:
        print("[DEBUG] run_script_minimal_connect_test: TIMEOUT ! discord_log_client.wait_until_ready() n'a pas terminé en 45 secondes.")
    except discord.errors.LoginFailure as lf:
        print(f"[DEBUG] run_script_minimal_connect_test: LoginFailure - {lf}")
    except discord.errors.PrivilegedIntentsRequired as pir: # Attraper spécifiquement cette erreur
        print(f"[DEBUG] run_script_minimal_connect_test: ERREUR PrivilegedIntentsRequired - {pir}. Vérifiez vos intents dans le Developer Portal Discord !")
    except Exception as e:
        error_details = traceback.format_exc()
        print(f"[DEBUG] run_script_minimal_connect_test: ERREUR INATTENDUE - {e}\nTrace:\n{error_details}")
    finally:
        if discord_log_client.is_ready():
            print("[DEBUG] run_script_minimal_connect_test: Tentative de déconnexion...")
            await discord_log_client.close()
            print("[DEBUG] run_script_minimal_connect_test: Déconnecté.")
        else:
            print("[DEBUG] run_script_minimal_connect_test: Client non prêt, pas de déconnexion nécessaire ou tentative de fermeture échouée.")
        print("[DEBUG] run_script_minimal_connect_test: Test de connexion minimal terminé.")


if __name__ == "__main__":
    # --- Configuration du logging pour voir les messages de discord.py ---
    # Mettez cela au tout début de if __name__ == "__main__":
    log_format = '%(asctime)s %(levelname)-8s %(name)-15s %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_format) # Configure le root logger
    
    # Vous pouvez aussi configurer le logger de discord.py spécifiquement si besoin
    # discord_py_logger = logging.getLogger('discord')
    # discord_py_logger.setLevel(logging.DEBUG)
    # handler = logging.StreamHandler()
    # handler.setFormatter(logging.Formatter(log_format))
    # discord_py_logger.addHandler(handler)
    # discord_py_logger.propagate = False # Pour éviter la double sortie si le root logger est aussi en DEBUG

    print("[FETCH_SCRIPT_DEBUG] Entrée dans if __name__ == '__main__'. Configuration du logging effectuée.")
    print("Démarrage de run_script_minimal_connect_test... (appel à asyncio.run)")
    try:
        print("RAPPEL: Assurez-vous que le dyno worker (bot.py) est arrêté (heroku ps:scale worker=0)")
        asyncio.run(run_script_minimal_connect_test())
    except KeyboardInterrupt:
        print("Script interrompu manuellement (KeyboardInterrupt).")
    # except Exception as e: # Le traceback est déjà dans run_script_minimal_connect_test
    #     detailed_error = traceback.format_exc()
    #     print(f"Erreur non gérée au niveau de asyncio.run : {e}\nTrace:\n{detailed_error}")
    finally:
        print("[FETCH_SCRIPT_DEBUG] Exécution de __main__ terminée (bloc finally).")
