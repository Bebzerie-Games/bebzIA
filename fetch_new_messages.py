print("[FETCH_SCRIPT_DEBUG] Le fichier fetch_new_messages.py est en cours d'exécution - VERSION TEST MINIMAL (login -> connect -> wait_until_ready)")

import discord
import os
from dotenv import load_dotenv
import asyncio
import traceback
import logging

# Charger les variables d'environnement
load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
LOG_CHANNEL_ID_STR = os.getenv("LOG_CHANNEL_ID")

# --- Configuration Globale du Client Discord ---
intents_log = discord.Intents.all()
print(f"[DEBUG] Intents pour discord_log_client: Value={intents_log.value}")
discord_log_client = discord.Client(intents=intents_log)

async def run_script_minimal_connect_test():
    print("[DEBUG] run_script_minimal_connect_test: Début du test.")
    if not DISCORD_BOT_TOKEN:
        print("[DEBUG] DISCORD_BOT_TOKEN MANQUANT.")
        return
    
    log_channel_id_int = None
    if LOG_CHANNEL_ID_STR:
        try:
            log_channel_id_int = int(LOG_CHANNEL_ID_STR)
        except ValueError:
            print(f"[DEBUG] LOG_CHANNEL_ID_STR ('{LOG_CHANNEL_ID_STR}') n'est pas un entier.")
    else:
        print("[DEBUG] LOG_CHANNEL_ID_STR MANQUANT.")

    try:
        # Étape 1: Login (valide le token et récupère des infos HTTP)
        print("[DEBUG] Étape 1: Tentative de discord_log_client.login()...")
        await asyncio.wait_for(discord_log_client.login(DISCORD_BOT_TOKEN), timeout=15.0)
        print("[DEBUG] discord_log_client.login() terminé.")

        # Étape 2: Connect (connexion à la passerelle WebSocket)
        # La méthode connect() gère get_gateway_bot et la connexion WebSocket.
        print("[DEBUG] Étape 2: Tentative de discord_log_client.connect()... (timeout 30s)")
        # Par défaut, reconnect=True. Pour un script unique, reconnect=False pourrait être plus approprié,
        # mais testons avec True d'abord.
        await asyncio.wait_for(discord_log_client.connect(reconnect=True), timeout=30.0)
        print("[DEBUG] discord_log_client.connect() terminé (ou a timeouté).")

        # Étape 3: Wait until ready (attend l'événement READY de la passerelle)
        # Si connect() a réussi, cela devrait être rapide.
        print("[DEBUG] Étape 3: Tentative de discord_log_client.wait_until_ready()... (timeout 15s)")
        await asyncio.wait_for(discord_log_client.wait_until_ready(), timeout=15.0)
        print(f"[DEBUG] discord_log_client.wait_until_ready() terminé. Client connecté: {discord_log_client.user}")

        # Étape 4: Test d'envoi de message si tout a réussi
        if log_channel_id_int and discord_log_client.is_ready():
            log_channel_obj = discord_log_client.get_channel(log_channel_id_int)
            if log_channel_obj:
                print(f"[DEBUG] Tentative d'envoi de message test au canal {log_channel_id_int}")
                await log_channel_obj.send("Message de test (login->connect->wait_until_ready) Heroku.")
                print("[DEBUG] Message de test envoyé.")
            else:
                print(f"[DEBUG] Canal de log {log_channel_id_int} non trouvé.")
        elif not log_channel_id_int:
             print("[DEBUG] Pas d'ID de canal de log pour message test.")
        elif not discord_log_client.is_ready():
            print("[DEBUG] Client non prêt, impossible d'envoyer message test.")

    except asyncio.TimeoutError as te:
        print(f"[DEBUG] TIMEOUT ! Une des étapes n'a pas terminé dans le délai imparti. Erreur: {te}")
    except discord.errors.LoginFailure as lf:
        print(f"[DEBUG] LoginFailure - {lf}")
    except discord.errors.PrivilegedIntentsRequired as pir:
        print(f"[DEBUG] ERREUR PrivilegedIntentsRequired - {pir}.")
    except Exception as e:
        error_details = traceback.format_exc()
        print(f"[DEBUG] ERREUR INATTENDUE - {e}\nTrace:\n{error_details}")
    finally:
        if discord_log_client.is_connected(): # is_connected() vérifie la connexion WebSocket
            print("[DEBUG] Tentative de déconnexion (client.close)...")
            await discord_log_client.close()
            print("[DEBUG] discord_log_client.close() appelé.")
        else:
            print("[DEBUG] Client non connecté via WebSocket, pas de client.close() nécessaire ou tentative de fermeture échouée.")
        print("[DEBUG] Test de connexion minimal terminé (run_script_minimal_connect_test).")

if __name__ == "__main__":
    log_format = '%(asctime)s %(levelname)-8s %(name)-22s %(message)s' # Augmenté name width
    logging.basicConfig(level=logging.DEBUG, format=log_format)
    
    print("[FETCH_SCRIPT_DEBUG] Entrée dans if __name__ == '__main__'.")
    print("Démarrage de run_script_minimal_connect_test...")
    try:
        print("RAPPEL: Assurez-vous que le dyno worker (bot.py) est arrêté.")
        asyncio.run(run_script_minimal_connect_test())
    except Exception as e: # Capturer les erreurs au niveau de asyncio.run au cas où
        print(f"ERREUR FATALE au niveau de asyncio.run: {e}")
        traceback.print_exc()
    finally:
        print("[FETCH_SCRIPT_DEBUG] Exécution de __main__ terminée.")