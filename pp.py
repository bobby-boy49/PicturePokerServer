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

IS_IDLE = "idlelib" in sys.modules or not sys.stdout.isatty()

class ConsoleColorFormatter(logging.Formatter):
    CYAN = "\x1b[36;20m"
    GREEN = "\x1b[32;20m"
    RESET = "\x1b[0m"

    def format(self, record):
        record.asctime = self.formatTime(record, "%H:%M:%S")
        msg = record.getMessage()
        
        if IS_IDLE:
            return f"{record.asctime} [{record.levelname}] {msg}"
        
        color = self.GREEN if "[SEND]" in msg else (self.CYAN if "[RECV]" in msg else self.RESET)
        return f"{color}{record.asctime} [{record.levelname}] {msg}{self.RESET}"


logger = logging.getLogger("RoomServer")
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(ConsoleColorFormatter())
logger.addHandler(stream_handler)

HOST_IP = "0.0.0.0"
PORT = 4444

LOBBIES = {}
LAST_CREATED_LOBBY = "PUBLIC"
STATE_LOCK = asyncio.Lock()
DEFAULT_ROUND_TIME = 15


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


def log_packet(direction, peer, raw_text, room_id):
    try:
        parsed = json.loads(raw_text)
        msg_type = parsed.get("type", "UNKNOWN")
        payload = json.dumps(parsed)
        logger.info(f"[{direction}] [{peer}] [Room: {room_id}] {msg_type} -> {payload}")
    except Exception:
        logger.info(f"[{direction}] [{peer}] [Room: {room_id}] RAW -> {raw_text}")


def get_now_iso():
    return datetime.now(timezone.utc).isoformat()


def extract_room_id_from_path(path):
    parsed_url = urllib.parse.urlparse(path)
    query_params = urllib.parse.parse_qs(parsed_url.query)
    
    for key in ("room", "code", "lobbyId", "lobby_id", "roomId", "room_id"):
        if key in query_params:
            return query_params[key][0].upper().strip()

    path_parts = [p.strip().upper() for p in parsed_url.path.split('/') if p.strip()]
    ignored_keywords = {"WS", "WEBSOCKET", "LOBBY", "LOBBIES", "JOIN", "API", "CREATELOBBY", "PUBLICLOBBY"}
    
    filtered_parts = [p for p in path_parts if p not in ignored_keywords]
    if filtered_parts:
        candidate = filtered_parts[-1]
        if 3 <= len(candidate) <= 8:
            return candidate
        
    return None


def generate_room_code():
    return "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=4))


def get_or_create_lobby(lobby_id):
    lobby_id = lobby_id.strip().upper() if lobby_id else "PUBLIC"
    if lobby_id not in LOBBIES:
        logger.info(f"[SERVER] Created room/lobby '{lobby_id}'")
        LOBBIES[lobby_id] = {
            "state": {
                "id": lobby_id,
                "code": lobby_id,
                "roomCode": lobby_id,
                "room_code": lobby_id,
                "lobbyId": lobby_id,
                "lobby_id": lobby_id,
                "room": lobby_id,
                "roomId": lobby_id,
                "room_id": lobby_id,
                "host": None,
                "betMultiplier": 1,
                "isPrivate": (lobby_id != "PUBLIC"),
                "inProgress": False,
                "phase": "LOBBY",
                "currentRound": 1,
                "roundCount": 5,
                "roundsRemaining": 5,
                "playersExpected": 0,
                "useTimer": True,
                "timerSeconds": DEFAULT_ROUND_TIME,
                "players": [],
                "lastActivity": get_now_iso()
            },
            "clients": {},
            "round_ready": set(),
            "timer_task": None,
            "timer_remaining": DEFAULT_ROUND_TIME
        }
    return LOBBIES[lobby_id]


async def read_exact(reader, num_bytes):
    data = b""
    while len(data) < num_bytes:
        try:
            chunk = await reader.read(num_bytes - len(data))
            if not chunk: return None
            data += chunk
        except Exception:
            return None
    return data


async def send_ws_frame(writer, payload_str, opcode=0x2, room_id="N/A"):
    try:
        peer_addr = writer.get_extra_info('peername')
        if payload_str:
            log_packet("SEND", peer_addr, payload_str, room_id)

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
    except Exception:
        pass


async def close_ws_connection(writer):
    try:
        writer.write(bytearray([0x88, 0x00]))
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
    if not lobby: return
    for cid, client_info in list(lobby["clients"].items()):
        if client_info["writer"] != exclude_writer:
            await send_ws_frame(client_info["writer"], message, room_id=lobby_id)


async def broadcast_lobby_state(room_id, target_writer=None):
    lobby = LOBBIES.get(room_id)
    if not lobby: return
    
    lobby["state"]["lastActivity"] = get_now_iso()
    payload_str = json.dumps({
        "type": WSMsgType.LobbyUpdated,
        "player": "System",
        "id": room_id,
        "playerId": "System",
        "lobbyId": room_id,
        "lobby_id": room_id,
        "code": room_id,
        "roomCode": room_id,
        "room_code": room_id,
        "room": room_id,
        "roomId": room_id,
        "room_id": room_id,
        "players": lobby["state"]["players"],
        "data": lobby["state"]
    })

    if target_writer:
        await send_ws_frame(target_writer, payload_str, room_id=room_id)
    else:
        for cid, client_info in list(lobby["clients"].items()):
            await send_ws_frame(client_info["writer"], payload_str, room_id=room_id)


def stop_timer(lobby):
    if lobby["timer_task"] and not lobby["timer_task"].done():
        lobby["timer_task"].cancel()
    lobby["timer_task"] = None


def start_timer_if_enabled(lobby, room_id):
    stop_timer(lobby)
    if not lobby["state"]["useTimer"]:
        return

    lobby["timer_remaining"] = DEFAULT_ROUND_TIME

    async def timer_loop():
        try:
            while lobby["timer_remaining"] > 0:
                await asyncio.sleep(1)
                async with STATE_LOCK:
                    if not lobby["state"]["useTimer"]:
                        break
                    
                    lobby["timer_remaining"] -= 1
                    tick_packet = json.dumps({
                        "type": "TimerTick",
                        "player": "System",
                        "id": "System",
                        "playerId": "System",
                        "lobbyId": room_id,
                        "data": lobby["timer_remaining"]
                    })
                    for cid, client_info in list(lobby["clients"].items()):
                        await send_ws_frame(client_info["writer"], tick_packet, room_id=room_id)

        except asyncio.CancelledError:
            pass

    lobby["timer_task"] = asyncio.create_task(timer_loop())


async def send_round_start(lobby, room_id):
    total_rounds = lobby["state"]["roundCount"]
    current_round = lobby["state"]["currentRound"]
    lobby["state"]["roundsRemaining"] = max(0, total_rounds - current_round)
    lobby["round_ready"].clear()
    
    game_started_payload = json.dumps({
        "type": "GameStarted",
        "player": "System",
        "id": "System",
        "playerId": "System",
        "lobbyId": room_id,
        "data": {
            "currentRound": current_round,
            "roundCount": total_rounds,
            "roundsRemaining": lobby["state"]["roundsRemaining"]
        }
    })
    
    # Broadcast GameStarted to ALL active clients
    for cid, client_info in list(lobby["clients"].items()):
        await send_ws_frame(client_info["writer"], game_started_payload, room_id=room_id)
        
    start_timer_if_enabled(lobby, room_id)


async def advance_round(lobby, room_id):
    lobby["round_ready"].clear()
    lobby["state"]["inProgress"] = True
    lobby["state"]["phase"] = "GAME"
    
    lobby["state"]["currentRound"] += 1
    total_rounds = lobby["state"]["roundCount"]
    
    if lobby["state"]["currentRound"] > total_rounds:
        lobby["state"]["inProgress"] = False
        lobby["state"]["phase"] = "LOBBY"
        lobby["state"]["currentRound"] = 1
        lobby["state"]["roundsRemaining"] = total_rounds
        stop_timer(lobby)
        await broadcast_lobby_state(room_id)
        
        await broadcast_to_lobby(room_id, json.dumps({
            "type": "GameEnded",
            "player": "System",
            "id": "System",
            "playerId": "System",
            "lobbyId": room_id,
            "data": {
                "message": "Match complete",
                "totalRounds": total_rounds
            }
        }))
    else:
        await send_round_start(lobby, room_id)


async def handle_client(reader, writer):
    global LAST_CREATED_LOBBY
    peer_addr = writer.get_extra_info('peername')
    current_lobby_id = "PUBLIC"
    client_id = None

    try:
        request_data = b""
        while b"\r\n\r\n" not in request_data:
            chunk = await reader.read(1024)
            if not chunk: break
            request_data += chunk

        if not request_data:
            await close_ws_connection(writer)
            return

        header_text = request_data.decode("utf-8", errors="ignore")
        lines = header_text.split("\r\n")
        parts = lines[0].split(" ") if lines else []
        if len(parts) < 2:
            await close_ws_connection(writer)
            return

        method, path = parts[0], parts[1]
        clean_path = path.lower()

        headers = {
            line.split(":", 1)[0].strip().lower(): line.split(":", 1)[1].strip()
            for line in lines[1:] if ":" in line
        }

        if method in ("GET", "POST") and any(ep in clean_path for ep in ["createlobby", "publiclobby", "joinlobby"]):
            room_code = "PUBLIC" if "public" in clean_path else generate_room_code()
            lobby = get_or_create_lobby(room_code)
            LAST_CREATED_LOBBY = room_code

            response_payload = json.dumps({
                "lobbyToUse": room_code,
                "opponentsInLobby": len(lobby["state"]["players"])
            }).encode("utf-8")

            writer.write(
                f"HTTP/1.1 200 OK\r\n"
                f"Content-Type: application/json; charset=UTF-8\r\n"
                f"Content-Length: {len(response_payload)}\r\n"
                f"Access-Control-Allow-Origin: *\r\n"
                f"Connection: close\r\n\r\n".encode("utf-8") + response_payload
            )
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return

        if headers.get("upgrade", "").lower() == "websocket":
            ws_key = headers.get("sec-websocket-key", "")
            ws_accept = base64.b64encode(hashlib.sha1((ws_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("utf-8")).digest()).decode("utf-8")
            writer.write(f"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {ws_accept}\r\nAccess-Control-Allow-Origin: *\r\n\r\n".encode("utf-8"))
            await writer.drain()

            extracted_id = extract_room_id_from_path(path)
            current_lobby_id = extracted_id if extracted_id else LAST_CREATED_LOBBY

            while True:
                frame_header = await read_exact(reader, 2)
                if not frame_header: break
                
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

                decoded_payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(encoded_payload)) if masked else encoded_payload

                if opcode == 0x8:
                    await send_ws_frame(writer, b"", opcode=0x8, room_id=current_lobby_id)
                    break
                elif opcode == 0x9:
                    await send_ws_frame(writer, decoded_payload, opcode=0xA, room_id=current_lobby_id)
                    continue
                elif opcode in (0x1, 0x2):
                    raw_text = decoded_payload.decode('utf-8', errors='ignore').replace('\x00', '').strip()

                    try:
                        packet = json.loads(raw_text)
                    except json.JSONDecodeError:
                        continue

                    msg_type = packet.get("type")
                    pid = packet.get("playerId") or packet.get("id") or packet.get("player") or packet.get("sender") or client_id
                    pname = packet.get("name") or pid
                    plogin = packet.get("login") or ""
                    
                    room_id = packet.get("lobbyId") or packet.get("room") or packet.get("code") or current_lobby_id
                    log_packet("RECV", peer_addr, raw_text, room_id=room_id)

                    handled = False

                    async with STATE_LOCK:
                        lobby = get_or_create_lobby(room_id)
                        current_lobby_id = room_id

                        if pid:
                            client_id = pid
                            lobby["clients"][client_id] = {"writer": writer, "reader": reader}

                        if msg_type in (WSMsgType.Hello, WSMsgType.GameReady, "PlayerReady", "Join"):
                            handled = True
                            # Lock host to the first player who joins or creates
                            if not lobby["state"]["host"]:
                                lobby["state"]["host"] = client_id

                            existing_p = next((p for p in lobby["state"]["players"] if p["id"] == client_id), None)
                            if not existing_p:
                                slot_num = len(lobby["state"]["players"])
                                existing_p = {
                                    "name": pname, 
                                    "id": client_id, 
                                    "login": plogin,
                                    "playerNumber": slot_num, 
                                    "registered": True,
                                    "color": {"r": 1.0, "g": 1.0, "b": 1.0},
                                    "inGame": False, 
                                    "ready": True,
                                    "isServerBot": False, 
                                    "coins": 100, 
                                    "connected": True
                                }
                                lobby["state"]["players"].append(existing_p)
                            else:
                                existing_p["name"] = pname
                                if plogin:
                                    existing_p["login"] = plogin
                                existing_p["connected"] = True

                            await broadcast_lobby_state(room_id)

                        elif msg_type == WSMsgType.Start:
                            handled = True
                            lobby["state"]["inProgress"] = True
                            lobby["state"]["phase"] = "GAME"
                            lobby["state"]["currentRound"] = 1
                            await send_round_start(lobby, room_id)

                        elif msg_type == WSMsgType.ReadyForNextRound or msg_type in ("RoundComplete", "NextRound"):
                            handled = True
                            await advance_round(lobby, room_id)

                        elif msg_type in (WSMsgType.SetRoundCount, WSMsgType.LobbyRoundChange, "UpdateRoundCount"):
                            handled = True
                            if "data" in packet:
                                try:
                                    rounds = int(packet["data"])
                                    if rounds > 0:
                                        lobby["state"]["roundCount"] = rounds
                                        lobby["state"]["roundsRemaining"] = rounds
                                        await broadcast_lobby_state(room_id)
                                except (ValueError, TypeError):
                                    pass

                        elif msg_type in (WSMsgType.SetTimer, "ToggleTimer"):
                            handled = True
                            use_timer = bool(packet["data"]) if "data" in packet else not lobby["state"]["useTimer"]
                            lobby["state"]["useTimer"] = use_timer
                            
                            if not lobby["state"]["useTimer"]:
                                stop_timer(lobby)
                            elif lobby["state"]["inProgress"]:
                                start_timer_if_enabled(lobby, room_id)
                                
                            await broadcast_lobby_state(room_id)

                        elif msg_type == WSMsgType.LobbyBetChange:
                            handled = True
                            if "data" in packet:
                                lobby["state"]["betMultiplier"] = packet["data"]
                                await broadcast_lobby_state(room_id)

                        elif msg_type == WSMsgType.Disconnect:
                            handled = True
                            p_record = next((p for p in lobby["state"]["players"] if p["id"] == pid), None)
                            if p_record:
                                p_record["connected"] = False
                            await broadcast_lobby_state(room_id)

                    if not handled:
                        await broadcast_to_lobby(room_id, raw_text, exclude_writer=writer)

    except Exception:
        pass
    finally:
        async with STATE_LOCK:
            lobby = LOBBIES.get(current_lobby_id)
            if lobby and client_id:
                # Do not immediately reassign host if player disconnects during active gameplay/reconnection
                if client_id in lobby["clients"] and lobby["clients"][client_id]["writer"] == writer:
                    del lobby["clients"][client_id]
                
                p_record = next((p for p in lobby["state"]["players"] if p["id"] == client_id), None)
                if p_record:
                    p_record["connected"] = False

                if not any(p.get("connected") for p in lobby["state"]["players"]):
                    stop_timer(lobby)

                await broadcast_lobby_state(current_lobby_id)

        await close_ws_connection(writer)


async def main():
    server = await asyncio.start_server(handle_client, HOST_IP, PORT)
    logger.info(f"[SERVER] Room & Menu server active on ws://{HOST_IP}:{PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("[SERVER] Shutting down...")
