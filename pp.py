#!/usr/bin/env python3
import asyncio
import json
import logging
import re
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

HOST_IP = "0.0.0.0"
PORT = 8080

LOBBIES = {}
STATE_LOCK = asyncio.Lock()


def extract_int(val, default=0):
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        match = re.search(r'\d+', val)
        if match:
            return int(match.group(0))
    return default


def get_or_create_lobby(lobby_id):
    if lobby_id not in LOBBIES:
        LOBBIES[lobby_id] = {
            "state": {
                "id": lobby_id,
                "host": "",
                "betMultiplier": 1,
                "isPrivate": False,
                "inProgress": False,
                "currentRound": 0,
                "roundCount": 5,
                "playersExpected": 4,
                "useTimer": True,
                "players": []
            },
            "clients": {}
        }
    return LOBBIES[lobby_id]


async def broadcast_to_lobby(lobby_id, message, exclude_ws=None):
    lobby = LOBBIES.get(lobby_id)
    if not lobby:
        return

    for cid, ws in list(lobby["clients"].items()):
        if ws != exclude_ws and ws.state.name == "OPEN":
            try:
                await ws.send(message)
            except Exception as err:
                logging.error(f"Failed to send to {cid}: {err}")


async def broadcast_lobby_state(room_id):
    lobby = LOBBIES.get(room_id)
    if not lobby:
        return
    
    lobby_update = {
        "type": "LobbyUpdated",
        "player": "System",
        "data": lobby["state"]
    }
    await broadcast_to_lobby(room_id, json.dumps(lobby_update))


async def handle_game_client(websocket):
    current_lobby_id = "PUBLIC"
    client_id = None
    peer_addr = websocket.remote_address

    logging.info(f"[+] Client connected from {peer_addr}")

    try:
        async for message in websocket:
            try:
                packet = json.loads(message)
            except (json.JSONDecodeError, TypeError):
                continue

            msg_type = packet.get("type")
            pid = packet.get("id") or f"Player-{peer_addr[1]}"
            pname = packet.get("name") or "Player"
            pdata = packet.get("data")

            room_id = packet.get("lobbyId") or packet.get("room") or current_lobby_id

            async with STATE_LOCK:
                lobby = get_or_create_lobby(room_id)
                current_lobby_id = room_id

                # 1. CONNECT & LOBBY REGISTRATION
                if msg_type in ("Hello", "CreateLobby", "JoinLobby", "PublicGame", "SearchMatch"):
                    client_id = pid
                    lobby["clients"][client_id] = websocket

                    if not lobby["state"]["host"]:
                        lobby["state"]["host"] = client_id

                    existing_p = next((p for p in lobby["state"]["players"] if p["id"] == client_id), None)
                    if not existing_p:
                        slot_num = len(lobby["state"]["players"])
                        existing_p = {
                            "name": pname,
                            "id": client_id,
                            "playerNumber": slot_num,
                            "registered": True,
                            "color": {"r": 1.0, "g": 1.0, "b": 1.0},
                            "inGame": False,
                            "ready": False,
                            "isServerBot": False,
                            "coins": 30
                        }
                        lobby["state"]["players"].append(existing_p)

                    ack_packet = {
                        "type": "Hello",
                        "id": client_id,
                        "name": pname,
                        "data": str(existing_p["playerNumber"])
                    }
                    await websocket.send(json.dumps(ack_packet))
                    await broadcast_lobby_state(room_id)
                    logging.info(f"[Lobby {room_id}] Player '{pname}' ({client_id}) joined.")

                # 2. SERVER-AUTHORITATIVE ROUND & GAME CONFIGURATION
                elif msg_type == "Start":
                    lobby["state"]["inProgress"] = True
                    lobby["state"]["currentRound"] = 1
                    for p in lobby["state"]["players"]:
                        p["inGame"] = True
                        p["ready"] = False
                    
                    await broadcast_lobby_state(room_id)
                    await broadcast_to_lobby(room_id, message, exclude_ws=websocket)

                elif msg_type in ("SetRoundCount", "LobbyRoundChange"):
                    new_rounds = extract_int(pdata, default=lobby["state"]["roundCount"])
                    lobby["state"]["roundCount"] = new_rounds
                    await broadcast_lobby_state(room_id)

                elif msg_type in ("GameReady", "ReadyForNextRound"):
                    for p in lobby["state"]["players"]:
                        if p["id"] == client_id or p["id"] == pid:
                            p["ready"] = True
                    
                    # Advance round counter if everyone is ready for the next round
                    if all(p["ready"] for p in lobby["state"]["players"]):
                        lobby["state"]["currentRound"] += 1
                        for p in lobby["state"]["players"]:
                            p["ready"] = False

                    await broadcast_lobby_state(room_id)
                    await broadcast_to_lobby(room_id, message, exclude_ws=websocket)

                elif msg_type == "LobbyUpdated":
                    if isinstance(pdata, dict):
                        lobby["state"].update(pdata)
                    await broadcast_to_lobby(room_id, message, exclude_ws=websocket)

                # 3. DIRECT P2P RELAY (Cards, Holds, Bets, Chats)
                else:
                    await broadcast_to_lobby(room_id, message, exclude_ws=websocket)

    except (websockets.exceptions.ConnectionClosedOK, websockets.exceptions.ConnectionClosedError):
        pass
    finally:
        async with STATE_LOCK:
            lobby = LOBBIES.get(current_lobby_id)
            if lobby and client_id in lobby["clients"]:
                del lobby["clients"][client_id]
                lobby["state"]["players"] = [p for p in lobby["state"]["players"] if p["id"] != client_id]

                for idx, player in enumerate(lobby["state"]["players"]):
                    player["playerNumber"] = idx

                if lobby["state"]["host"] == client_id:
                    lobby["state"]["host"] = lobby["state"]["players"][0]["id"] if lobby["state"]["players"] else ""

                if not lobby["clients"]:
                    del LOBBIES[current_lobby_id]
                    logging.info(f"[-] Lobby {current_lobby_id} closed (empty).")
                else:
                    await broadcast_lobby_state(current_lobby_id)

        logging.info(f"[-] Client {peer_addr} disconnected.")


async def main():
    logging.info("========================================")
    logging.info(f" HYBRID P2P / ROUND SYNC SERVER ACTIVE")
    logging.info(f" Listening on: ws://{HOST_IP}:{PORT}")
    logging.info("========================================")

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
        pass
