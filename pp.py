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
# GLOBAL CONSTANTS & ROOM STATE
# ------------------------------------------------------------------------------
MAX_PLAYERS = 4
HOST_IP = "0.0.0.0"
PORT = 8080

ROOM_STATE = {
    "id": "LOCAL",
    "host": "",
    "betMultiplier": 1,
    "isPrivate": True,
    "inProgress": False,
    "currentRound": 0,
    "roundCount": 5,
    "playersExpected": MAX_PLAYERS,
    "useTimer": True,
    "lastActivity": "",
    "players": []
}

CONNECTED_CLIENTS = {}
STATE_LOCK = asyncio.Lock()


# ------------------------------------------------------------------------------
# HELPER FUNCTIONS
# ------------------------------------------------------------------------------
def extract_int(data_val, default=0):
    """Safely extracts an integer from numbers or string payloads like 'RoundChange: 5'."""
    if isinstance(data_val, int):
        return data_val
    if isinstance(data_val, str):
        match = re.search(r'\d+', data_val)
        if match:
            return int(match.group(0))
    return default

def get_available_player_number():
    """Finds the lowest available playerNumber slot between 0 and MAX_PLAYERS - 1."""
    taken_numbers = {p["playerNumber"] for p in ROOM_STATE["players"]}
    for slot in range(MAX_PLAYERS):
        if slot not in taken_numbers:
            return slot
    return None

def reindex_player_numbers():
    """Re-indexes player slots after a disconnect to ensure contiguous 0..N indices."""
    for idx, player in enumerate(ROOM_STATE["players"]):
        player["playerNumber"] = idx

async def broadcast_lobby():
    """Sends the authoritative LobbyUpdated packet to all active clients."""
    async with STATE_LOCK:
        ROOM_STATE["lastActivity"] = datetime.now().isoformat()
        payload = {
            "type": "LobbyUpdated",
            "player": "System",
            "data": ROOM_STATE
        }
        msg = json.dumps(payload)
        active_clients = list(CONNECTED_CLIENTS.values())

    for ws in active_clients:
        try:
            if ws.state.name == "OPEN":
                await ws.send(msg)
        except Exception as err:
            logging.error(f"Error broadcasting lobby to socket: {err}")

    logging.info(f"[>] Broadcasted 'LobbyUpdated' | Players: {len(ROOM_STATE['players'])}/{MAX_PLAYERS}")

async def send_packet(websocket, msg_type, pid="", name="", login="", data=""):
    payload = {
        "type": msg_type,
        "id": pid,
        "name": name,
        "login": login,
        "data": data
    }
    try:
        await websocket.send(json.dumps(payload))
        logging.info(f"[>] Sent '{msg_type}' to {pid}")
    except Exception as err:
        logging.error(f"Failed sending '{msg_type}' to {pid}: {err}")

async def broadcast_packet(msg_type, pid="", name="", login="", data=""):
    payload = {
        "type": msg_type,
        "id": pid,
        "name": name,
        "login": login,
        "data": data
    }
    msg = json.dumps(payload)
    
    async with STATE_LOCK:
        active_clients = list(CONNECTED_CLIENTS.values())

    for ws in active_clients:
        try:
            if ws.state.name == "OPEN":
                await ws.send(msg)
        except Exception as err:
            logging.error(f"Error broadcasting packet '{msg_type}': {err}")
            
    logging.info(f"[>] Broadcasted '{msg_type}' | From: {pid}")


# ------------------------------------------------------------------------------
# CLIENT HANDLER
# ------------------------------------------------------------------------------
async def handle_game_client(websocket):
    client_id = None
    peer_addr = websocket.remote_address
    logging.info(f"[+] Connection accepted from {peer_addr}")
    
    try:
        async for message in websocket:
            try:
                packet = json.loads(message)
            except json.JSONDecodeError:
                logging.warning(f"[!] Non-JSON payload received: {message}")
                continue

            msg_type = packet.get("type")
            pid = packet.get("id") or ""
            pname = packet.get("name") or "Player"
            plogin = packet.get("login") or ""
            pdata = packet.get("data")
            
            logging.info(f"[<] Incoming '{msg_type}' from {pid} ({pname})")

            # 1. HELLO / HANDSHAKE
            if msg_type == "Hello":
                async with STATE_LOCK:
                    if len(ROOM_STATE["players"]) >= MAX_PLAYERS and pid not in [p["id"] for p in ROOM_STATE["players"]]:
                        logging.warning(f"[!] Rejecting connection from {pid}: Room Full ({MAX_PLAYERS}/{MAX_PLAYERS})")
                        await send_packet(websocket, "Error", pid=pid, data="Room is full.")
                        await websocket.close(1008, "Room Full")
                        return

                    base_id = pid
                    counter = 2
                    while pid in CONNECTED_CLIENTS and CONNECTED_CLIENTS[pid] != websocket:
                        pid = f"{base_id}-Client{counter}"
                        pname = f"{packet.get('name') or 'Player'} ({counter})"
                        counter += 1

                    client_id = pid
                    CONNECTED_CLIENTS[client_id] = websocket

                    if not ROOM_STATE["host"]:
                        ROOM_STATE["host"] = pid

                    existing_player = next((p for p in ROOM_STATE["players"] if p["id"] == pid), None)
                    if not existing_player:
                        p_num = get_available_player_number()
                        if p_num is None:
                            p_num = len(ROOM_STATE["players"])

                        existing_player = {
                            "name": pname,
                            "id": pid,
                            "playerNumber": p_num,
                            "registered": True,
                            "color": {"r": 1.0, "g": 1.0, "b": 1.0},
                            "inGame": False,
                            "ready": False,
                            "isServerBot": False,
                            "coins": 30
                        }
                        ROOM_STATE["players"].append(existing_player)
                    
                    assigned_pnum = existing_player["playerNumber"]

                await send_packet(websocket, "Hello", pid=pid, name=pname, login=plogin, data=str(assigned_pnum))
                await broadcast_lobby()

            # 2. START MATCH
            elif msg_type == "Start":
                async with STATE_LOCK:
                    ROOM_STATE["inProgress"] = True
                    ROOM_STATE["currentRound"] = 1
                    for p in ROOM_STATE["players"]:
                        p["inGame"] = True
                await broadcast_lobby()

            # 3. GAME READY & ROUND TRANSITIONS
            elif msg_type in ("GameReady", "ReadyForNextRound"):
                async with STATE_LOCK:
                    for p in ROOM_STATE["players"]:
                        if p["id"] == pid:
                            p["ready"] = True
                await broadcast_lobby()

            # 4. COIN SYNC
            elif msg_type == "MyCoins":
                coin_val = extract_int(pdata, default=0)
                async with STATE_LOCK:
                    for p in ROOM_STATE["players"]:
                        if p["id"] == pid:
                            p["coins"] = coin_val
                await broadcast_lobby()

            # 5. LOBBY SETTINGS (ROUND COUNT & TIMER)
            elif msg_type in ("SetRoundCount", "LobbyRoundChange"):
                new_rounds = extract_int(pdata, default=ROOM_STATE["roundCount"])
                async with STATE_LOCK:
                    ROOM_STATE["roundCount"] = new_rounds
                await broadcast_lobby()

            elif msg_type == "SetTimer":
                async with STATE_LOCK:
                    if isinstance(pdata, bool):
                        ROOM_STATE["useTimer"] = pdata
                    elif isinstance(pdata, str):
                        ROOM_STATE["useTimer"] = pdata.lower() == "true"
                await broadcast_lobby()

            # 6. IN-GAME ACTIONS (CARDS, BETS, HOLD/DRAW)
            elif msg_type == "SelectedCardsChanged":
                await broadcast_packet("SelectedCardsChanged", pid=pid, name=pname, data=pdata)

            elif msg_type == "DrawHoldPressed":
                await broadcast_packet("DrawHoldPressed", pid=pid, name=pname, data=pdata)

            elif msg_type == "BetAmountChanged":
                await broadcast_packet("BetAmountChanged", pid=pid, name=pname, data=pdata)

            # 7. CHAT
            elif msg_type == "ChatMessage":
                await broadcast_packet("ChatMessage", pid=pid, name=pname, login=plogin, data=f"{pname}: {pdata}")

            else:
                logging.warning(f"[!] Unhandled packet type: {msg_type} | Data: {pdata}")

    except websockets.exceptions.ConnectionClosedOK:
        logging.info(f"[-] Client {client_id or peer_addr} disconnected cleanly.")
    except websockets.exceptions.ConnectionClosedError as err:
        logging.warning(f"[-] Client {client_id or peer_addr} disconnected abruptly: {err}")
    except Exception as err:
        logging.error(f"[!] Unexpected error handling client {client_id or peer_addr}: {err}", exc_info=True)
    finally:
        async with STATE_LOCK:
            if client_id and client_id in CONNECTED_CLIENTS:
                del CONNECTED_CLIENTS[client_id]
            
            ROOM_STATE["players"] = [p for p in ROOM_STATE["players"] if p["id"] != client_id]
            reindex_player_numbers()

            if ROOM_STATE["host"] == client_id:
                ROOM_STATE["host"] = ROOM_STATE["players"][0]["id"] if ROOM_STATE["players"] else ""
                
            if not ROOM_STATE["players"]:
                ROOM_STATE["inProgress"] = False
                ROOM_STATE["currentRound"] = 0
                
        await broadcast_lobby()


# ------------------------------------------------------------------------------
# MAIN SERVER INITIALIZATION
# ------------------------------------------------------------------------------
async def main():
    logging.info("========================================")
    logging.info(f" SERVER ACTIVE: ws://{HOST_IP}:{PORT}")
    logging.info(f" Max Players: {MAX_PLAYERS}")
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
        logging.info("\n[!] Server shutting down cleanly.")
