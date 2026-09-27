#!/usr/bin/env python3
import asyncio
import base64
import hashlib
import json
import logging
import random
import sys
import urllib.parse
from datetime import datetime, timezone

# Detect if running in IDLE or non-TTY shell
IS_IDLE = "idlelib" in sys.modules or not sys.stdout.isatty()

class ConsoleColorFormatter(logging.Formatter):
    GREY = "\x1b[38;20m"
    GREEN = "\x1b[32;20m"
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    RESET = "\x1b[0m"

    FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
    FORMATS = {
        logging.DEBUG: GREY + FORMAT + RESET,
        logging.INFO: GREEN + FORMAT + RESET,
        logging.WARNING: YELLOW + FORMAT + RESET,
        logging.ERROR: RED + FORMAT + RESET,
    }

    def format(self, record):
        if IS_IDLE:
            log_fmt = self.FORMAT
        else:
            log_fmt = self.FORMATS.get(record.levelno, self.FORMAT)
        formatter = logging.Formatter(log_fmt, datefmt="%H:%M:%S")
        return formatter.format(record)


logger = logging.getLogger("PicturePoker")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler("log.txt", mode="a", encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(ConsoleColorFormatter())

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

HOST_IP = "0.0.0.0"
PORT = 4444

LOBBIES = {}
LAST_CREATED_LOBBY = "PUBLIC"
STATE_LOCK = asyncio.Lock()


class WSMsgType:
    MyCards = "MyCards"
    MyCoins = "MyCoins"
    Hello = "Hello"
    DrawHoldPressed = "DrawHoldPressed"
    SelectedCardsChanged = "SelectedCardsChanged"
    LobbyUpdated = "LobbyUpdated"
    ChatMessage = "ChatMessage"
    SetRoundCount = "SetRoundCount"
    Start = "Start"
    GameReady = "GameReady"
    ReadyForNextRound = "ReadyForNextRound"
    Disconnect = "Disconnect"
    LobbyBetChange = "LobbyBetChange"
    LobbyRoundChange = "LobbyRoundChange"
    SetTimer = "SetTimer"


def get_now_iso():
    return datetime.now(timezone.utc).isoformat()


def get_or_create_lobby(lobby_id):
    if lobby_id not in LOBBIES:
        logger.info(f"[LOBBY CREATED] Initialized lobby state: '{lobby_id}'")
        LOBBIES[lobby_id] = {
            "state": {
                "id": lobby_id,
                "code": lobby_id,
                "roomCode": lobby_id,
                "host": "",
                "betMultiplier": 1,
                "isPrivate": (lobby_id != "PUBLIC"),
                "inProgress": False,
                "currentRound": 0,
                "roundCount": 5,
                "playersExpected": 0,
                "useTimer": True,
                "players": [],
                "lastActivity": get_now_iso()
            },
            "clients": {}
        }
    return LOBBIES[lobby_id]


async def read_exact(reader, num_bytes):
    data = b""
    while len(data) < num_bytes:
        try:
            chunk = await reader.read(num_bytes - len(data))
            if not chunk:
                return None
            data += chunk
        except Exception:
            return None
    return data


async def send_ws_frame(writer, payload_str, opcode=0x1):
    try:
        payload_bytes = payload_str.encode("utf-8") if isinstance(payload_str, str) else payload_str
        frame_header = bytearray([0x80 | (opcode & 0x0F)])
        length = len(payload_bytes)
        
        if length <= 125:
            frame_header.append(length)
        elif length <= 65535:
            frame_header.append(126)
            frame_header.extend(length.to_bytes(2, 'big'))
        else:
            frame_header.append(127)
            frame_header.extend(length.to_bytes(8, 'big'))

        writer.write(frame_header + payload_bytes)
        await writer.drain()
    except Exception as err:
        logger.debug(f"[FRAME ERROR] Failed to send WS frame: {err}")


async def close_ws_connection(writer):
    try:
        close_frame = bytearray([0x88, 0x00])
        writer.write(close_frame)
        await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def broadcast_to_lobby(lobby_id, message, exclude_writer=None):
    lobby = LOBBIES.get(lobby_id)
    if not lobby:
        return
    for cid, client_info in list(lobby["clients"].items()):
        writer = client_info["writer"]
        if writer != exclude_writer:
            await send_ws_frame(writer, message)


async def broadcast_lobby_state(room_id):
    lobby = LOBBIES.get(room_id)
    if not lobby:
        return
    
    lobby["state"]["lastActivity"] = get_now_iso()
    
    lobby_update = {
        "type": WSMsgType.LobbyUpdated,
        "player": "System",
        "data": lobby["state"]
    }
    
    payload_str = json.dumps(lobby_update)
    logger.info(f"[LOBBY STATE BROADCAST] Syncing lobby '{room_id}' (Players: {len(lobby['state']['players'])})")
    
    for cid, client_info in list(lobby["clients"].items()):
        await send_ws_frame(client_info["writer"], payload_str)


async def handle_client(reader, writer):
    global LAST_CREATED_LOBBY
    peer_addr = writer.get_extra_info('peername')
    logger.info(f"[CONNECTED] Client connected from {peer_addr}")

    current_lobby_id = "PUBLIC"
    client_id = None

    try:
        request_data = b""
        while b"\r\n\r\n" not in request_data:
            chunk = await reader.read(1024)
            if not chunk:
                break
            request_data += chunk

        if not request_data:
            await close_ws_connection(writer)
            return

        header_text = request_data.decode("utf-8", errors="ignore")
        lines = header_text.split("\r\n")
        request_line = lines[0] if lines else ""
        parts = request_line.split(" ")
        
        if len(parts) < 2:
            await close_ws_connection(writer)
            return

        method, path = parts[0], parts[1]
        clean_path = path.lower().rstrip('/')

        headers = {
            line.split(":", 1)[0].strip().lower(): line.split(":", 1)[1].strip()
            for line in lines[1:] if ":" in line
        }

        # REST Endpoints
        if method in ("GET", "POST") and any(endpoint in clean_path for endpoint in ["createlobby", "publiclobby", "joinlobby"]):
            logger.info(f"[REST REQ] Method: {method} | Path: {path}")
            
            room_code = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=4)) if "public" not in clean_path else "PUBLIC"
            get_or_create_lobby(room_code)
            LAST_CREATED_LOBBY = room_code

            response_payload = json.dumps({
                "code": room_code,
                "roomCode": room_code,
                "id": room_code,
                "success": True
            }).encode("utf-8")

            http_response = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json; charset=UTF-8\r\n"
                f"Content-Length: {len(response_payload)}\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "Connection: close\r\n\r\n"
            )
            writer.write(http_response.encode("utf-8") + response_payload)
            await writer.drain()
            logger.info(f"[REST RESP] Created lobby '{room_code}'")
            
            writer.close()
            await writer.wait_closed()
            return

        # WebSocket Handshake
        if headers.get("upgrade", "").lower() == "websocket":
            ws_key = headers.get("sec-websocket-key", "")
            guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
            sha1 = hashlib.sha1((ws_key + guid).encode("utf-8")).digest()
            ws_accept = base64.b64encode(sha1).decode("utf-8")

            handshake_str = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {ws_accept}\r\n"
                "Access-Control-Allow-Origin: *\r\n\r\n"
            )
            writer.write(handshake_str.encode("utf-8"))
            await writer.drain()

            # Room Code Extraction
            parsed_url = urllib.parse.urlparse(path)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            path_parts = [p for p in parsed_url.path.split('/') if p]

            if path_parts and path_parts[0].lower() not in ("ws", "websocket", "lobbies"):
                current_lobby_id = path_parts[0].upper()
            elif len(path_parts) >= 2 and path_parts[0].lower() == "lobbies":
                current_lobby_id = path_parts[1].upper()
            elif "room" in query_params:
                current_lobby_id = query_params["room"][0].upper()
            elif "code" in query_params:
                current_lobby_id = query_params["code"][0].upper()
            else:
                current_lobby_id = LAST_CREATED_LOBBY

            if len(current_lobby_id) > 4 and current_lobby_id != "PUBLIC":
                current_lobby_id = current_lobby_id[:4]

            logger.info(f"[WS HANDSHAKE] Connected to lobby '{current_lobby_id}'")

            while True:
                frame_header = await read_exact(reader, 2)
                if not frame_header:
                    break
                
                b1, b2 = frame_header[0], frame_header[1]
                opcode = b1 & 0x0F
                masked = (b2 & 0x80) != 0
                payload_len = b2 & 0x7F

                if payload_len == 126:
                    ext_len = await read_exact(reader, 2)
                    if not ext_len: break
                    payload_len = int.from_bytes(ext_len, 'big')
                elif payload_len == 127:
                    ext_len = await read_exact(reader, 8)
                    if not ext_len: break
                    payload_len = int.from_bytes(ext_len, 'big')

                mask_key = await read_exact(reader, 4) if masked else b""
                if masked and not mask_key: break

                encoded_payload = await read_exact(reader, payload_len) if payload_len > 0 else b""
                if payload_len > 0 and not encoded_payload: break

                if masked:
                    decoded_payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(encoded_payload))
                else:
                    decoded_payload = encoded_payload

                if opcode == 0x8:  # Close
                    await send_ws_frame(writer, b"", opcode=0x8)
                    break
                elif opcode == 0x9:  # Ping
                    await send_ws_frame(writer, decoded_payload, opcode=0xA)
                    continue
                elif opcode in (0x1, 0x2):  # Handles BOTH Text (0x1) and Binary (0x2) WebSocket Opcodes
                    raw_text = decoded_payload.decode('utf-8', errors='ignore').replace('\x00', '').strip()
                    logger.info(f"👉 [ACTION RECEIVED ({current_lobby_id})] {raw_text}")

                    try:
                        packet = json.loads(raw_text)
                    except Exception:
                        packet = {}

                    msg_type = packet.get("type")
                    pid = packet.get("id") or f"Player-{peer_addr[1]}"
                    pname = packet.get("name") or pid
                    plogin = packet.get("login") or ""
                    room_id = packet.get("lobbyId") or packet.get("room") or current_lobby_id

                    broadcast_state = False
                    relay_msg = True

                    async with STATE_LOCK:
                        lobby = get_or_create_lobby(room_id)
                        current_lobby_id = room_id

                        if msg_type in (WSMsgType.Hello, WSMsgType.GameReady):
                            client_id = pid
                            lobby["clients"][client_id] = {"writer": writer, "reader": reader}
                            
                            # FIXED: Only send empty lobby state if the game is NOT in progress
                            if not lobby["state"]["inProgress"]:
                                empty_lobby_state = {
                                    "type": WSMsgType.LobbyUpdated,
                                    "player": "System",
                                    "data": {
                                        "players": [{
                                            "name": "",
                                            "id": "",
                                            "playerNumber": 0,
                                            "registered": False,
                                            "color": {"r": 1.0, "g": 1.0, "b": 1.0},
                                            "inGame": False,
                                            "ready": False,
                                            "isServerBot": False,
                                            "coins": 0
                                        }],
                                        "id": room_id,
                                        "host": "",
                                        "betMultiplier": lobby["state"]["betMultiplier"],
                                        "isPrivate": lobby["state"]["isPrivate"],
                                        "inProgress": False,
                                        "currentRound": 0,
                                        "roundCount": lobby["state"]["roundCount"],
                                        "playersExpected": 0,
                                        "useTimer": lobby["state"]["useTimer"],
                                        "lastActivity": get_now_iso()
                                    }
                                }
                                await send_ws_frame(writer, json.dumps(empty_lobby_state))

                                chat_join_msg = json.dumps({
                                    "type": WSMsgType.ChatMessage,
                                    "player": "System",
                                    "id": "",
                                    "login": "",
                                    "data": f"{pname} joined the lobby"
                                })
                                await send_ws_frame(writer, chat_join_msg)

                            if not lobby["state"]["host"]:
                                lobby["state"]["host"] = client_id
                                logger.info(f"[HOST ASSIGNED] Host: '{pname}' ({client_id})")

                            existing_p = next((p for p in lobby["state"]["players"] if p["id"] == client_id), None)
                            if not existing_p:
                                slot_num = len(lobby["state"]["players"])
                                lobby["state"]["players"].append({
                                    "name": pname,
                                    "id": client_id,
                                    "login": plogin,
                                    "playerNumber": slot_num,
                                    "registered": True,
                                    "color": {"r": 1.0, "g": 1.0, "b": 1.0},
                                    "inGame": lobby["state"]["inProgress"],
                                    "ready": False,
                                    "isServerBot": False,
                                    "coins": 100
                                })
                                logger.info(f"[JOINED] '{pname}' registered in slot #{slot_num}")
                            else:
                                existing_p["name"] = pname
                                existing_p["login"] = plogin
                                existing_p["inGame"] = lobby["state"]["inProgress"]

                            broadcast_state = True
                            relay_msg = False

                        # FIXED: Properly swallow and process client-sent coin updates
                        elif msg_type == WSMsgType.MyCoins:
                            existing_p = next((p for p in lobby["state"]["players"] if p["id"] == pid), None)
                            if existing_p and "data" in packet:
                                try:
                                    existing_p["coins"] = int(packet["data"])
                                    logger.info(f"[COINS UPDATED] '{pname}' coins -> {existing_p['coins']}")
                                except (ValueError, TypeError):
                                    pass
                            relay_msg = False
                            broadcast_state = False

                        elif msg_type == WSMsgType.ReadyForNextRound:
                            existing_p = next((p for p in lobby["state"]["players"] if p["id"] == pid), None)
                            if existing_p:
                                existing_p["ready"] = bool(packet.get("data", True))
                                logger.info(f"[BUTTON PRESS] '{pname}' toggled ready -> {existing_p['ready']}")
                            broadcast_state = True

                        elif msg_type in (WSMsgType.SetRoundCount, WSMsgType.LobbyRoundChange):
                            if "data" in packet:
                                lobby["state"]["roundCount"] = packet["data"]
                                logger.info(f"[BUTTON PRESS] Round count updated -> {packet['data']}")
                            broadcast_state = True

                        elif msg_type == WSMsgType.LobbyBetChange:
                            if "data" in packet:
                                lobby["state"]["betMultiplier"] = packet["data"]
                                logger.info(f"[BUTTON PRESS] Bet multiplier updated -> {packet['data']}")
                            broadcast_state = True

                        elif msg_type == WSMsgType.SetTimer:
                            if "data" in packet:
                                lobby["state"]["useTimer"] = packet["data"]
                                logger.info(f"[BUTTON PRESS] Timer updated -> {packet['data']}")
                            broadcast_state = True

                        elif msg_type == WSMsgType.Start:
                            lobby["state"]["inProgress"] = True
                            lobby["state"]["currentRound"] = 1
                            logger.info(f"[GAME START] Host triggered game start in lobby '{room_id}'")

                            suits = ["Star", "Mushroom", "Yoshi", "Mario", "Luigi", "Cloud"]

                            for client_pid, client_info in lobby["clients"].items():
                                player_hand = [random.choice(suits) for _ in range(5)]
                                
                                cards_packet = json.dumps({
                                    "type": WSMsgType.MyCards,
                                    "player": "System",
                                    "data": player_hand
                                })
                                
                                coins_packet = json.dumps({
                                    "type": WSMsgType.MyCoins,
                                    "player": "System",
                                    "data": 100
                                })

                                await send_ws_frame(client_info["writer"], cards_packet)
                                await send_ws_frame(client_info["writer"], coins_packet)
                                logger.info(f"[DEALT HAND] Sent initial hand {player_hand} to '{client_pid}'")

                            broadcast_state = True

                    if broadcast_state:
                        await broadcast_lobby_state(room_id)

                    if relay_msg:
                        await broadcast_to_lobby(room_id, raw_text, exclude_writer=writer)

    except Exception as err:
        logger.error(f"[EXC] Error handling client {peer_addr}: {err}")
    finally:
        async with STATE_LOCK:
            lobby = LOBBIES.get(current_lobby_id)
            if lobby and client_id in lobby["clients"]:
                del lobby["clients"][client_id]
                lobby["state"]["players"] = [p for p in lobby["state"]["players"] if p["id"] != client_id]
                
                if lobby["state"]["host"] == client_id:
                    lobby["state"]["host"] = lobby["state"]["players"][0]["id"] if lobby["state"]["players"] else ""
                        
                await broadcast_lobby_state(current_lobby_id)

        await close_ws_connection(writer)
        logger.info(f"[DISCONNECTED] Clean exit for {peer_addr}")


async def main():
    server = await asyncio.start_server(handle_client, HOST_IP, PORT)
    logger.info(f"[SERVER STARTED] Listening on ws://{HOST_IP}:{PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("[SERVER STOPPED] Shutting down...")
