#!/usr/bin/env python3
import asyncio
import json
import logging
import re
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("log.txt", mode="a"),
        logging.StreamHandler()
    ]
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
                "roundCount": 7,  # Default updated to match 7-round game config from client logs
                "playersExpected": 2,  # Configured for standard 2-player Picture Poker matches
                "useTimer": True,
                "players": []
            },
            "clients": {}
        }
    return LOBBIES[lobby_id]


async def send_payload(websocket, message, recipient_info="Client"):
    try:
        await websocket.send(message)
        logging.info(f"[>] SEND ({recipient_info}): {message}")
    except Exception as err:
        logging.error(f"Failed to send to {recipient_info}: {err}")


async def broadcast_to_lobby(lobby_id, message, exclude_ws=None):
    lobby = LOBBIES.get(lobby_id)
    if not lobby:
        return

    active_clients = list(lobby["clients"].items())
    for cid, ws in active_clients:
        if ws != exclude_ws and ws.state.name == "OPEN":
            await send_payload(ws, message, recipient_info=f"Player:{cid}")


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
            logging.info(f"[<] RECV ({peer_addr}): {message}")

            try:
                packet = json.loads(message)
            except (json.JSONDecodeError, TypeError):
                continue

            msg_type = packet.get("type")
            pid = packet.get("id") or f"Player-{peer_addr[1]}"
            pname = packet.get("name") or "Player"
            pdata = packet.get("data")
            room_id = packet.get("lobbyId") or packet.get("room") or current_lobby_id

            should_broadcast_state = False
            round_advanced = False
            message_to_relay = None
            exclude_relay_sender = True

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
                            "inGame": lobby["state"]["inProgress"],
                            "ready": False,
                            "isServerBot": False,
                            "coins": 30,
                            "connected": True
                        }
                        lobby["state"]["players"].append(existing_p)
                    else:
                        existing_p["connected"] = True
                        if lobby["state"]["inProgress"]:
                            existing_p["inGame"] = True

                    ack_packet = {
                        "type": "Hello",
                        "id": client_id,
                        "name": pname,
                        "data": str(existing_p["playerNumber"])
                    }
                    await send_payload(websocket, json.dumps(ack_packet), recipient_info=f"Player:{client_id}")
                    should_broadcast_state = True

                # 2. LOBBY CONFIGURATION
                elif msg_type in ("SetRoundCount", "LobbyRoundChange"):
                    if lobby["state"]["inProgress"]:
                        logging.warning(f"Ignored {msg_type} from {client_id}: Game in progress.")
                    else:
                        new_rounds = extract_int(pdata, default=lobby["state"]["roundCount"])
                        lobby["state"]["roundCount"] = new_rounds
                        should_broadcast_state = True

                elif msg_type == "Start":
                    lobby["state"]["inProgress"] = True
                    lobby["state"]["currentRound"] = 1
                    for p in lobby["state"]["players"]:
                        p["inGame"] = True
                        p["ready"] = False
                    
                    should_broadcast_state = True
                    message_to_relay = message

                # 3. GAMEPLAY & ROUND SYNC
                elif msg_type == "GameReady":
                    for p in lobby["state"]["players"]:
                        if p["id"] in (client_id, pid):
                            if lobby["state"]["inProgress"]:
                                p["inGame"] = True
                    should_broadcast_state = True

                elif msg_type == "ReadyForNextRound":
                    for p in lobby["state"]["players"]:
                        if p["id"] in (client_id, pid):
                            p["ready"] = True

                    # Advance round ONLY when all active players confirm round completion
                    active_players = [p for p in lobby["state"]["players"] if p.get("connected", True)]
                    if len(active_players) > 0 and all(p["ready"] for p in active_players):
                        lobby["state"]["currentRound"] += 1
                        for p in lobby["state"]["players"]:
                            p["ready"] = False
                        round_advanced = True

                    should_broadcast_state = True
                    message_to_relay = message

                # Handle coin/poker hand state updates explicitly
                elif msg_type == "UpdatePlayerCoins":
                    new_coins = extract_int(pdata, default=None)
                    if new_coins is not None:
                        for p in lobby["state"]["players"]:
                            if p["id"] in (client_id, pid):
                                p["coins"] = new_coins
                        should_broadcast_state = True
                    message_to_relay = message

                elif msg_type == "LobbyUpdated":
                    if isinstance(pdata, dict) and not lobby["state"]["inProgress"]:
                        lobby["state"].update(pdata)
                        should_broadcast_state = True

                # 4. DIRECT RELAY (Cards dealt, held, drawn, shown down)
                else:
                    message_to_relay = message

            # Execute I/O and broadcasts outside STATE_LOCK
            if should_broadcast_state:
                await broadcast_lobby_state(room_id)

            if round_advanced:
                next_round_event = json.dumps({
                    "type": "RoundStart",
                    "round": lobby["state"]["currentRound"]
                })
                await broadcast_to_lobby(room_id, next_round_event, exclude_ws=None)

            if message_to_relay:
                exclude = websocket if exclude_relay_sender else None
                await broadcast_to_lobby(room_id, message_to_relay, exclude_ws=exclude)

    except (websockets.exceptions.ConnectionClosedOK, websockets.exceptions.ConnectionClosedError):
        pass
    finally:
        async with STATE_LOCK:
            lobby = LOBBIES.get(current_lobby_id)
            if lobby and client_id in lobby["clients"]:
                del lobby["clients"][client_id]
                
                p_record = next((p for p in lobby["state"]["players"] if p["id"] == client_id), None)
                if p_record:
                    p_record["connected"] = False

                if not lobby["clients"]:
                    del LOBBIES[current_lobby_id]
                    logging.info(f"[-] Lobby {current_lobby_id} closed (empty).")
                else:
                    should_broadcast_state = True

        if current_lobby_id in LOBBIES:
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
