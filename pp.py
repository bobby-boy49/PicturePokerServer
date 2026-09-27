#!/usr/bin/env python3
import asyncio
import base64
import hashlib
import json
import logging
import urllib.parse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("log.txt", mode="a"),
        logging.StreamHandler()
    ]
)

HOST_IP = "0.0.0.0"
PORT = 4444

LOBBIES = {}
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


def get_or_create_lobby(lobby_id):
    if lobby_id not in LOBBIES:
        logging.info(f"[+] Init lobby state: {lobby_id}")
        LOBBIES[lobby_id] = {
            "state": {
                "id": lobby_id,
                "code": lobby_id,
                "roomCode": lobby_id,
                "host": "",
                "betMultiplier": 1,
                "isPrivate": True,
                "inProgress": False,
                "currentRound": 0,
                "roundCount": 5,
                "playersExpected": 2,
                "useTimer": True,
                "players": []
            },
            "clients": {}
        }
    return LOBBIES[lobby_id]


async def read_exact(reader, num_bytes):
    data = b""
    while len(data) < num_bytes:
        chunk = await reader.read(num_bytes - len(data))
        if not chunk:
            return None
        data += chunk
    return data


async def send_ws_frame(writer, payload_str):
    payload_bytes = payload_str.encode("utf-8")
    frame_header = bytearray([0x81])
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


async def broadcast_to_lobby(lobby_id, message, exclude_writer=None):
    lobby = LOBBIES.get(lobby_id)
    if not lobby:
        return
    for cid, client_info in list(lobby["clients"].items()):
        writer = client_info["writer"]
        if writer != exclude_writer:
            try:
                await send_ws_frame(writer, message)
            except Exception as err:
                logging.error(f"[!] Broadcast failed to {cid}: {err}")


async def broadcast_lobby_state(room_id):
    lobby = LOBBIES.get(room_id)
    if not lobby:
        return
    lobby_update = {
        "type": WSMsgType.LobbyUpdated,
        "id": "Server",
        "name": "Server",
        "login": "Server",
        "data": lobby["state"]
    }
    for cid, client_info in list(lobby["clients"].items()):
        try:
            await send_ws_frame(client_info["writer"], json.dumps(lobby_update))
        except Exception as err:
            logging.error(f"[!] State send failed to {cid}: {err}")


async def handle_client(reader, writer):
    peer_addr = writer.get_extra_info('peername')
    logging.info(f"[+] Client connected from {peer_addr}")

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
            writer.close()
            await writer.wait_closed()
            return

        header_text = request_data.decode("utf-8", errors="ignore")
        lines = header_text.split("\r\n")
        request_line = lines[0] if lines else ""
        parts = request_line.split(" ")
        
        if len(parts) < 2:
            writer.close()
            await writer.wait_closed()
            return

        method, path = parts[0], parts[1]
        headers = {
            line.split(":", 1)[0].strip().lower(): line.split(":", 1)[1].strip()
            for line in lines[1:] if ":" in line
        }

        # REST lobby creation handler
        if "createlobby" in path or "create" in path or (method == "POST" and "lobby" in path):
            lobby_code = "PUBLIC"
            response_payload = json.dumps({"code": lobby_code, "roomCode": lobby_code, "id": lobby_code}).encode("utf-8")
            http_response = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json; charset=UTF-8\r\n"
                f"Content-Length: {len(response_payload)}\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "Connection: close\r\n\r\n"
            )
            writer.write(http_response.encode("utf-8") + response_payload)
            await writer.drain()
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

            parsed_url = urllib.parse.urlparse(path)
            path_parts = [p for p in parsed_url.path.split('/') if p]
            current_lobby_id = path_parts[1] if len(path_parts) >= 2 else "PUBLIC"

            logging.info(f"[+] WS Handshake Complete on lobby: {current_lobby_id}")

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

                if opcode == 0x8:
                    break
                elif opcode == 0x1:
                    message = decoded_payload.decode('utf-8', errors='ignore')
                    logging.info(f"[<-- RECV WS] {message}")

                    try:
                        packet = json.loads(message)
                    except Exception:
                        packet = {}

                    msg_type = packet.get("type")
                    pid = packet.get("id") or f"Player-{peer_addr[1]}"
                    pname = packet.get("name") or pid
                    room_id = packet.get("lobbyId") or packet.get("room") or current_lobby_id

                    broadcast_state = False
                    relay_msg = True

                    async with STATE_LOCK:
                        lobby = get_or_create_lobby(room_id)
                        current_lobby_id = room_id

                        if msg_type in (WSMsgType.Hello, WSMsgType.GameReady):
                            client_id = pid
                            lobby["clients"][client_id] = {"writer": writer, "reader": reader}
                            
                            if not lobby["state"]["host"]:
                                lobby["state"]["host"] = client_id

                            existing_p = next((p for p in lobby["state"]["players"] if p["id"] == client_id), None)
                            if not existing_p:
                                slot_num = len(lobby["state"]["players"])
                                lobby["state"]["players"].append({
                                    "name": pname,
                                    "id": client_id,
                                    "playerNumber": slot_num,
                                    "registered": True,
                                    "color": {"r": 1.0, "g": 1.0, "b": 1.0},
                                    "inGame": lobby["state"]["inProgress"],
                                    "ready": True,
                                    "isServerBot": False,
                                    "coins": 100,
                                    "connected": True
                                })
                            else:
                                existing_p["connected"] = True
                                existing_p["name"] = pname
                                
                            broadcast_state = True
                            relay_msg = False

                        elif msg_type in (WSMsgType.SetRoundCount, WSMsgType.LobbyRoundChange):
                            if "data" in packet:
                                lobby["state"]["roundCount"] = packet["data"]

                        elif msg_type == WSMsgType.SetTimer:
                            if "data" in packet:
                                lobby["state"]["useTimer"] = packet["data"]

                    if broadcast_state:
                        await broadcast_lobby_state(room_id)

                    if relay_msg:
                        await broadcast_to_lobby(room_id, message, exclude_writer=writer)

    except Exception as err:
        logging.error(f"[!] Error handling client {peer_addr}: {err}")
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        logging.info(f"[-] Connection closed: {peer_addr}")


async def main():
    server = await asyncio.start_server(handle_client, HOST_IP, PORT)
    logging.info(f"[*] Server listening on {HOST_IP}:{PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
