import asyncio
import json
import logging
import re
import websockets
from datetime import datetime

# ------------------------------------------------------------------------------
# LOGGING CONFIGURATION
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ------------------------------------------------------------------------------
# GLOBAL SERVER CONFIGURATION & LOBBY STATE
# ------------------------------------------------------------------------------
MIN_PLAYERS_TO_START = 2
MAX_PLAYERS = 4
HOST_IP = "0.0.0.0"  # Listens on all LAN interfaces
PORT = 8080

# Single authoritative LAN lobby
LOBBY_STATE = {
    "id": "PUBLIC-LOBBY",
    "host": "",
    "betMultiplier": 1,
    "isPrivate": False,
    "inProgress": False,
    "currentRound": 0,
    "roundCount": 5,
    "playersExpected": MAX_PLAYERS,
    "useTimer": True,
    "lastActivity": datetime.now().isoformat(),
    "players": []
}

# Connected sockets: client_id -> websocket instance
CONNECTED_CLIENTS = {}

# Async lock to prevent concurrent modifications to lobby state
STATE_LOCK = asyncio.Lock()


# ------------------------------------------------------------------------------
# HELPER FUNCTIONS
# ------------------------------------------------------------------------------
def extract_int(data_val, default=0):
    """Extracts integers safely from numbers or strings like 'RoundChange: 5'."""
    if isinstance(data_val, int):
        return data_val
    if isinstance(data_val, str):
        match = re.search(r'\d+', data_val)
        if match:
            return int(match.group(0))
    return default

def get_available_player_number():
    """Finds the lowest open slot index (0 to 3) in the lobby."""
    taken = {p["playerNumber"] for p in LOBBY_STATE["players"]}
    for slot in range(MAX_PLAYERS):
        if slot not in taken:
            return slot
    return None

def reindex_player_numbers():
    """Reindexes player numbers contiguous (0..N-1) on drop."""
    for idx, player in enumerate(LOBBY_STATE["players"]):
        player["playerNumber"] = idx

async def broadcast_lobby_state():
    """Broadcasts current LobbyUpdated state to all connected LAN players."""
    async with STATE_LOCK:
        LOBBY_STATE["lastActivity"] = datetime.now().isoformat()
        payload = {
            "type": "LobbyUpdated",
            "player": "System",
            "data": LOBBY_STATE
        }
        msg = json.dumps(payload)
        target_sockets = list(CONNECTED_CLIENTS.values())

    for ws in target_sockets:
        try:
            if ws.state.name == "OPEN":
                await ws.send(msg)
        except Exception as err:
            logging.error(f"Error broadcasting lobby state: {err}")

    logging.info(f"[>] Broadcasted 'LobbyUpdated' | Players: {len(LOBBY_STATE['players'])}/{MAX_PLAYERS}")

async def send_packet(websocket, msg_type, pid="", name="", login="", data=""):
    """Sends a targeted packet directly to a specific websocket."""
    payload = {
        "type": msg_type,
        "id": pid,
        "name": name,
        "login": login,
        "data": data
    }
    try:
        await websocket.send(json.dumps(payload))
        logging.info(f"[>] Sent '{msg_type}' to {pid or websocket.remote_address}")
    except Exception as err:
        logging.error(f"Failed sending '{msg_type}' to {pid}: {err}")

async def broadcast_game_packet(msg_type, pid="", name="", login="", data=""):
    """Broadcasts non-state game actions (cards, bets) to all clients."""
    payload = {
        "type": msg_type,
        "id": pid,
        "name": name,
        "login": login,
        "data": data
    }
    msg = json.dumps(payload)
    
    async with STATE_LOCK:
        target_sockets = list(CONNECTED_CLIENTS.values())

    for ws in target_sockets:
        try:
            if ws.state.name == "OPEN":
                await ws.send(msg)
        except Exception as err:
            logging.error(f"Error broadcasting game packet '{msg_type}': {err}")

async def broadcast_chat_packet(sender_ws, msg_type, pid="", name="", login="", data=""):
    """Broadcasts chat messages strictly to OTHER clients (prevents double printing)."""
    payload = {
        "type": msg_type,
        "id": pid,
        "name": name,
        "login": login,
        "data": data
    }
    msg = json.dumps(payload)
    
    async with STATE_LOCK:
        target_sockets = [ws for ws in CONNECTED_CLIENTS.values() if ws != sender_ws]

    for ws in target_sockets:
        try:
            if ws.state.name == "OPEN":
                await ws.send(msg)
        except Exception as err:
            logging.error(f"Error broadcasting chat packet '{msg_type}': {err}")


# ------------------------------------------------------------------------------
# CLIENT HANDLER
# ------------------------------------------------------------------------------
async def handle_game_client(websocket):
    client_id = None
    peer_addr = websocket.remote_address
    logging.info(f"[+] LAN Connection accepted from {peer_addr}")
    
    try:
        async for message in websocket:
            try:
                packet = json.loads(message)
            except json.JSONDecodeError:
                logging.warning(f"[!] Non-JSON payload received from {peer_addr}: {message}")
                continue

            msg_type = packet.get("type")
            pid = packet.get("id") or ""
            pname = packet.get("name") or "Player"
            plogin = packet.get("login") or ""
            pdata = packet.get("data")
            
            logging.info(f"[<] Received '{msg_type}' from {pid or peer_addr} ({pname})")

            # 1. HANDSHAKE / CONNECTION / PUBLIC MATCHMAKING FIX
            if msg_type in ("Hello", "CreateLobby", "JoinLobby", "PublicGame", "SearchMatch"):
                async with STATE_LOCK:
                    # Enforce player cap
                    if len(LOBBY_STATE["players"]) >= MAX_PLAYERS and pid not in [p["id"] for p in LOBBY_STATE["players"]]:
                        logging.warning(f"[!] Lobby Full! Rejecting connection from {peer_addr}")
                        await send_packet(websocket, "Error", pid=pid, data="LAN Server is full.")
                        await websocket.close(1008, "Lobby Full")
                        return

                    # Handle multiple instances on same machine
                    base_id = pid if pid else f"LAN-Player-{peer_addr[1]}"
                    counter = 2
                    actual_id = base_id
                    while actual_id in CONNECTED_CLIENTS and CONNECTED_CLIENTS[actual_id] != websocket:
                        actual_id = f"{base_id}-Client{counter}"
                        pname = f"{packet.get('name') or 'Player'} ({counter})"
                        counter += 1

                    client_id = actual_id
                    CONNECTED_CLIENTS[client_id] = websocket

                    # Assign host if lobby is empty
                    if not LOBBY_STATE["host"]:
                        LOBBY_STATE["host"] = client_id

                    # Register player in lobby
                    existing_player = next((p for p in LOBBY_STATE["players"] if p["id"] == client_id), None)
                    if not existing_player:
                        p_num = get_available_player_number()
                        if p_num is None:
                            p_num = len(LOBBY_STATE["players"])

                        existing_player = {
                            "name": pname,
                            "id": client_id,
                            "playerNumber": p_num,
                            "registered": True,
                            "color": {"r": 1.0, "g": 1.0, "b": 1.0},
                            "inGame": False,
                            "ready": False,
                            "isServerBot": False,
                            "coins": 30
                        }
                        LOBBY_STATE["players"].append(existing_player)
                    
                    assigned_pnum = existing_player["playerNumber"]

                # Acknowledge connection
                await send_packet(websocket, "Hello", pid=client_id, name=pname, login=plogin, data=str(assigned_pnum))
                # Sync lobby state immediately so Public Matchmaking doesn't freeze/break UI
                await broadcast_lobby_state()

            # 2. START MATCH (REQUIRES AT LEAST 2 PLAYERS)
            elif msg_type == "Start":
                async with STATE_LOCK:
                    num_players = len(LOBBY_STATE["players"])
                    if num_players < MIN_PLAYERS_TO_START:
                        logging.warning(f"[!] Start rejected: Only {num_players} player(s) in lobby. Minimum required: {MIN_PLAYERS_TO_START}")
                        await send_packet(websocket, "Error", pid=pid or client_id, data=f"Cannot start: At least {MIN_PLAYERS_TO_START} players required.")
                        continue

                    LOBBY_STATE["inProgress"] = True
                    LOBBY_STATE["currentRound"] = 1
                    for p in LOBBY_STATE["players"]:
                        p["inGame"] = True
                await broadcast_lobby_state()

            # 3. ROUND & READY STATES (FALLBACK HANDLING FOR PUBLIC MATCHES)
            elif msg_type in ("GameReady", "ReadyForNextRound"):
                async with STATE_LOCK:
                    # If player sends GameReady before registration, register them on the fly
                    if client_id not in CONNECTED_CLIENTS:
                        client_id = pid or f"LAN-Player-{peer_addr[1]}"
                        CONNECTED_CLIENTS[client_id] = websocket
                        if client_id not in [p["id"] for p in LOBBY_STATE["players"]]:
                            p_num = get_available_player_number() or 0
                            LOBBY_STATE["players"].append({
                                "name": pname,
                                "id": client_id,
                                "playerNumber": p_num,
                                "registered": True,
                                "color": {"r": 1.0, "g": 1.0, "b": 1.0},
                                "inGame": True,
                                "ready": True,
                                "isServerBot": False,
                                "coins": 30
                            })

                    for p in LOBBY_STATE["players"]:
                        if p["id"] == pid or p["id"] == client_id:
                            p["ready"] = True

                await broadcast_lobby_state()

            # 4. COIN SYNC
            elif msg_type == "MyCoins":
                coin_val = extract_int(pdata, default=30)
                async with STATE_LOCK:
                    for p in LOBBY_STATE["players"]:
                        if p["id"] == pid or p["id"] == client_id:
                            p["coins"] = coin_val
                await broadcast_lobby_state()

            # 5. LOBBY SETTINGS (ROUNDS & TIMER)
            elif msg_type in ("SetRoundCount", "LobbyRoundChange"):
                new_rounds = extract_int(pdata, default=LOBBY_STATE["roundCount"])
                async with STATE_LOCK:
                    LOBBY_STATE["roundCount"] = new_rounds
                await broadcast_lobby_state()

            elif msg_type == "SetTimer":
                async with STATE_LOCK:
                    if isinstance(pdata, bool):
                        LOBBY_STATE["useTimer"] = pdata
                    elif isinstance(pdata, str):
                        LOBBY_STATE["useTimer"] = pdata.lower() == "true"
                await broadcast_lobby_state()

            # 6. IN-GAME ACTION BROADCASTS
            elif msg_type in ("SelectedCardsChanged", "DrawHoldPressed", "BetAmountChanged"):
                await broadcast_game_packet(msg_type, pid=pid or client_id, name=pname, data=pdata)

            # 7. CHAT MESSAGES
            elif msg_type == "ChatMessage":
                await broadcast_chat_packet(websocket, "ChatMessage", pid=pid or client_id, name=pname, login=plogin, data=pdata)

            # 8. HANDLE DISCONNECT COMMAND FROM CLIENT MENU
            elif msg_type == "Disconnect":
                logging.info(f"[-] Disconnect requested by {client_id or peer_addr}")
                break

            else:
                logging.warning(f"[!] Unhandled LAN packet: {msg_type} | Data: {pdata}")

    except websockets.exceptions.ConnectionClosedOK:
        logging.info(f"[-] Client {client_id or peer_addr} disconnected cleanly.")
    except websockets.exceptions.ConnectionClosedError as err:
        logging.warning(f"[-] Client {client_id or peer_addr} disconnected abruptly: {err}")
    except Exception as err:
        logging.error(f"[!] Error handling client {client_id or peer_addr}: {err}", exc_info=True)
    finally:
        async with STATE_LOCK:
            if client_id and client_id in CONNECTED_CLIENTS:
                del CONNECTED_CLIENTS[client_id]

            LOBBY_STATE["players"] = [p for p in LOBBY_STATE["players"] if p["id"] != client_id]
            reindex_player_numbers()

            # Migrate host if needed
            if LOBBY_STATE["host"] == client_id:
                LOBBY_STATE["host"] = LOBBY_STATE["players"][0]["id"] if LOBBY_STATE["players"] else ""

            if not LOBBY_STATE["players"]:
                LOBBY_STATE["inProgress"] = False
                LOBBY_STATE["currentRound"] = 0
                logging.info("[-] All players disconnected. LAN Lobby reset to idle.")
            else:
                await broadcast_lobby_state()


# ------------------------------------------------------------------------------
# MAIN SERVER INITIALIZATION
# ------------------------------------------------------------------------------
async def main():
    logging.info("========================================")
    logging.info(f" LOCAL NETWORK SERVER ACTIVE")
    logging.info(f" Listening on: ws://{HOST_IP}:{PORT}")
    logging.info(" Public Matchmaking & Private Mode Enabled")
    logging.info("========================================\n")
    
    async with websockets.serve(
        handle_game_client, 
        HOST_IP, 
        PORT, 
        ping_interval=20, 
        ping_timeout=10
    ):
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\n[!] LAN Server shut down.")
