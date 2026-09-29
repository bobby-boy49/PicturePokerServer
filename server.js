const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = 4444;
const DEFAULT_ROUND_TIME = 15;
const DISCONNECT_GRACE_PERIOD_MS = 5000;

const WSMsgType = {
    MyCards: "MyCards",
    MyCoins: "MyCoins",
    Hello: "Hello",
    DrawHoldPressed: "DrawHoldPressed",
    SelectedCardsChanged: "SelectedCardsChanged",
    LobbyUpdated: "LobbyUpdated",
    ChatMessage: "ChatMessage",
    SetRoundCount: "SetRoundCount",
    Start: "Start",
    GameReady: "GameReady",
    ReadyForNextRound: "ReadyForNextRound",
    Disconnect: "Disconnect",
    LobbyBetChange: "LobbyBetChange",
    LobbyRoundChange: "LobbyRoundChange",
    SetTimer: "SetTimer"
};

const LOBBIES = {};

function getNowIso() {
    return new Date().toISOString();
}

function generateRoomCode() {
    const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    while (true) {
        let code = "";
        for (let i = 0; i < 4; i++) {
            code += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        if (!LOBBIES[code] && code !== "PUBLIC") {
            return code;
        }
    }
}

function extractRoomIdFromPath(reqUrl) {
    if (!reqUrl) return null;

    const queryMatch = reqUrl.match(/[?&](room|code|lobbyId|lobby_id|roomId|room_id)=([^&]+)/i);
    if (queryMatch && queryMatch[2]) {
        const val = decodeURIComponent(queryMatch[2]).trim().toUpperCase();
        if (val) return val;
    }

    const cleanUrl = reqUrl.split('?')[0];
    const parts = cleanUrl.split('/').map(p => p.trim().toUpperCase()).filter(Boolean);
    const ignored = new Set(["WS", "WEBSOCKET", "LOBBY", "LOBBIES", "JOIN", "API", "CREATELOBBY", "PUBLICLOBBY", "JOINLOBBY"]);
    const filtered = parts.filter(p => !ignored.has(p));

    if (filtered.length > 0) {
        const candidate = filtered[filtered.length - 1];
        if (candidate.length >= 3 && candidate.length <= 12) {
            return candidate;
        }
    }

    return null;
}

function getOrCreateLobby(lobbyId) {
    if (!lobbyId) return null;
    const id = lobbyId.trim().toUpperCase();
    if (!LOBBIES[id]) {
        console.log(`[SERVER] Created room/lobby '${id}'`);
        LOBBIES[id] = {
            state: {
                id: id,
                code: id,
                roomCode: id,
                room_code: id,
                lobbyId: id,
                lobby_id: id,
                room: id,
                roomId: id,
                room_id: id,
                host: null,
                betMultiplier: 1,
                isPrivate: (id !== "PUBLIC"),
                inProgress: false,
                phase: "LOBBY",
                currentRound: 1,
                roundCount: 5,
                roundsRemaining: 5,
                playersExpected: 0,
                useTimer: true,
                timerSeconds: DEFAULT_ROUND_TIME,
                players: [],
                lastActivity: getNowIso()
            },
            clients: new Map(),
            wsToClientId: new Map(),
            disconnectTasks: new Map(),
            roundReadyPerRound: {},
            playerRounds: {},
            startTime: 0,
            roundAdvancing: false,
            cardsDealtForRound: false
        };
    }
    return LOBBIES[id];
}

function broadcastToLobby(roomId, messageStr, excludeWs = null) {
    const lobby = LOBBIES[roomId];
    if (!lobby) return;

    lobby.clients.forEach(({ ws }) => {
        if (ws !== excludeWs && ws.readyState === WebSocket.OPEN) {
            ws.send(messageStr);
        }
    });
}

function broadcastLobbyState(roomId, targetWs = null) {
    const lobby = LOBBIES[roomId];
    if (!lobby) return;

    const cur = lobby.state.currentRound || 1;
    const tot = lobby.state.roundCount || 5;

    if (lobby.state.inProgress) {
        lobby.state.roundsRemaining = Math.max(0, tot - cur + 1);
    } else {
        lobby.state.roundsRemaining = tot;
    }

    lobby.state.lastActivity = getNowIso();
    lobby.state.players.sort((a, b) => (a.playerNumber || 0) - (b.playerNumber || 0));

    const payloadStr = JSON.stringify({
        type: WSMsgType.LobbyUpdated,
        player: "System",
        id: roomId,
        playerId: "System",
        lobbyId: roomId,
        lobby_id: roomId,
        code: roomId,
        roomCode: roomId,
        room_code: roomId,
        room: roomId,
        roomId: roomId,
        room_id: roomId,
        players: lobby.state.players,
        data: lobby.state
    });

    if (targetWs) {
        if (targetWs.readyState === WebSocket.OPEN) {
            targetWs.send(payloadStr);
        }
    } else {
        broadcastToLobby(roomId, payloadStr);
    }
}

function delayedPurgeClient(roomId, clientId) {
    const lobby = LOBBIES[roomId];
    if (!lobby) return;

    const playerIndex = lobby.state.players.findIndex(p => p.id === clientId);
    if (playerIndex !== -1) {
        const p = lobby.state.players[playerIndex];
        if (!p.connected) {
            if (!lobby.state.inProgress) {
                lobby.state.players.splice(playerIndex, 1);
            }

            if (lobby.state.host === clientId) {
                const activePlayers = lobby.state.players.filter(pl => pl.connected);
                lobby.state.host = activePlayers.length > 0 ? activePlayers[0].id : null;
            }

            broadcastLobbyState(roomId);
        }
    }

    lobby.disconnectTasks.delete(clientId);
}

function purgeClientConnection(ws, roomId, clientId = null) {
    const lobby = LOBBIES[roomId];
    if (!lobby) return;

    let targetClientId = clientId || lobby.wsToClientId.get(ws);

    if (targetClientId) {
        lobby.clients.delete(targetClientId);
        lobby.wsToClientId.delete(ws);

        const player = lobby.state.players.find(p => p.id === targetClientId);
        if (player) {
            player.connected = false;

            if (lobby.disconnectTasks.has(targetClientId)) {
                clearTimeout(lobby.disconnectTasks.get(targetClientId));
            }

            const timeoutHandle = setTimeout(() => {
                delayedPurgeClient(roomId, targetClientId);
            }, DISCONNECT_GRACE_PERIOD_MS);

            lobby.disconnectTasks.set(targetClientId, timeoutHandle);
        }

        broadcastLobbyState(roomId);
    }
}

const app = express();
app.use(express.json());

app.all('*', (req, res, next) => {
    const cleanPath = req.path.toLowerCase();
    
    if (['createlobby', 'publiclobby', 'joinlobby'].some(ep => cleanPath.includes(ep))) {
        const extracted = extractRoomIdFromPath(req.url);

        if (cleanPath.includes("createlobby")) {
            const roomCode = generateRoomCode();
            getOrCreateLobby(roomCode);
            const lobby = LOBBIES[roomCode];
            return res.json({
                lobbyToUse: roomCode,
                opponentsInLobby: lobby.state.players.length
            });
        } 

        if (cleanPath.includes("publiclobby")) {
            getOrCreateLobby("PUBLIC");
            const lobby = LOBBIES["PUBLIC"];
            return res.json({
                lobbyToUse: "PUBLIC",
                opponentsInLobby: lobby.state.players.length
            });
        }

        if (cleanPath.includes("joinlobby")) {
            if (!extracted || !LOBBIES[extracted]) {
                console.warn(`[SERVER] Attempted to join invalid/missing room: '${extracted}'`);
                return res.status(404).send("Not Found");
            }
            const lobby = LOBBIES[extracted];
            return res.json({
                lobbyToUse: extracted,
                opponentsInLobby: lobby.state.players.length
            });
        }
    }

    next();
});

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    const extractedId = extractRoomIdFromPath(request.url);

    if (!extractedId || !LOBBIES[extractedId]) {
        console.warn(`[SERVER] WebSocket rejected: Invalid or non-existent room '${extractedId}'`);
        socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
        socket.destroy();
        return;
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request, extractedId);
    });
});

wss.on('connection', (ws, req, extractedId) => {
    let currentLobbyId = extractedId;
    let clientId = null;

    ws.on('message', (data) => {
        const rawText = data.toString('utf-8').replace(/\0/g, '').trim();
        let packet;
        
        try {
            packet = JSON.parse(rawText);
        } catch (e) {
            return;
        }

        const msgType = packet.type;
        const targetPid = packet.playerId || packet.id || packet.player || packet.sender || clientId;
        const pname = packet.name || targetPid;
        const plogin = packet.login || "";
        const roomId = packet.lobbyId || packet.room || packet.code || currentLobbyId;

        if (!roomId || !LOBBIES[roomId]) {
            console.warn(`[SERVER] Invalid room '${roomId}' in packet. Killing connection.`);
            ws.send(JSON.stringify({ type: "Error", message: "Invalid Room ID" }));
            return ws.close();
        }

        console.log(`[RECV] [Room: ${roomId}] ${msgType} -> ${rawText}`);

        let handled = false;
        const lobby = LOBBIES[roomId];
        currentLobbyId = roomId;

        if (targetPid) {
            clientId = targetPid;
            lobby.clients.set(clientId, { ws });
            lobby.wsToClientId.set(ws, clientId);

            if (lobby.disconnectTasks.has(clientId)) {
                clearTimeout(lobby.disconnectTasks.get(clientId));
                lobby.disconnectTasks.delete(clientId);
            }
        }

        if ([WSMsgType.Hello, WSMsgType.GameReady, "PlayerReady", "Join"].includes(msgType)) {
            handled = true;
            if (!lobby.state.host) {
                lobby.state.host = clientId;
            }

            let existingP = lobby.state.players.find(p => p.id === clientId);
            let stateChanged = false;

            if (!existingP) {
                const usedSlots = new Set(lobby.state.players.map(p => p.playerNumber || 0));
                let slotNum = 0;
                while (usedSlots.has(slotNum)) slotNum++;

                existingP = {
                    name: pname,
                    id: clientId,
                    login: plogin,
                    playerNumber: slotNum,
                    registered: true,
                    color: { r: 1.0, g: 1.0, b: 1.0 },
                    inGame: false,
                    ready: true,
                    isServerBot: false,
                    coins: 0,
                    coins_received: false,
                    connected: true
                };
                lobby.state.players.push(existingP);
                stateChanged = true;
            } else {
                if (existingP.name !== pname || !existingP.connected) {
                    existingP.name = pname;
                    existingP.connected = true;
                    stateChanged = true;
                }
                if (plogin && existingP.login !== plogin) {
                    existingP.login = plogin;
                }
            }

            if (packet.coins !== undefined) {
                const parsedCoins = parseInt(packet.coins, 10);
                if (!isNaN(parsedCoins) && existingP.coins !== parsedCoins) {
                    existingP.coins = parsedCoins;
                    existingP.coins_received = true;
                    stateChanged = true;
                }
            }

            if (stateChanged) {
                broadcastLobbyState(roomId);
            } else {
                broadcastLobbyState(roomId, ws);
            }

        } else if (msgType === WSMsgType.MyCards || msgType === WSMsgType.DrawHoldPressed || msgType === WSMsgType.SelectedCardsChanged) {
            lobby.cardsDealtForRound = true;
            broadcastToLobby(roomId, rawText, ws);

        } else if (msgType === WSMsgType.ReadyForNextRound) {
            handled = true;
            
            // Send ReadyForNextRound to the opponent so client state machines don't freeze
            broadcastToLobby(roomId, rawText, ws);

            if (lobby.state.inProgress && lobby.state.phase === "GAME" && targetPid) {
                const activeRound = lobby.state.currentRound;

                if (!lobby.roundReadyPerRound) lobby.roundReadyPerRound = {};
                if (!lobby.roundReadyPerRound[activeRound]) {
                    lobby.roundReadyPerRound[activeRound] = new Set();
                }

                lobby.roundReadyPerRound[activeRound].add(targetPid);

                const activePlayers = lobby.state.players.filter(p => p.connected);

                if (!lobby.roundAdvancing && activePlayers.length > 0 && lobby.roundReadyPerRound[activeRound].size >= activePlayers.length) {
                    lobby.roundAdvancing = true;

                    if (lobby.cardsDealtForRound) {
                        if (lobby.state.currentRound < lobby.state.roundCount) {
                            lobby.state.currentRound += 1;
                            lobby.cardsDealtForRound = false;
                            console.log(`[SERVER] Room '${roomId}' advancing to Round ${lobby.state.currentRound}/${lobby.state.roundCount}`);
                        } else {
                            console.log(`[SERVER] Match complete in room '${roomId}'. Final round reached (${lobby.state.roundCount}/${lobby.state.roundCount}).`);
                            lobby.state.inProgress = false;
                            lobby.state.phase = "FINISHED";
                        }
                    } else {
                        console.log(`[SERVER] Room '${roomId}': All players synced for Round ${lobby.state.currentRound}. Ready to deal.`);
                    }

                    setTimeout(() => {
                        lobby.roundAdvancing = false;
                        broadcastLobbyState(roomId);
                    }, 100);
                }
            }

        } else if (msgType === WSMsgType.MyCoins) {
            handled = true;
            const coinVal = packet.data;
            if (coinVal !== undefined && targetPid) {
                const parsedCoins = parseInt(coinVal, 10);
                if (!isNaN(parsedCoins)) {
                    const targetP = lobby.state.players.find(p => p.id === targetPid);
                    if (targetP && targetP.coins !== parsedCoins) {
                        targetP.coins = parsedCoins;
                        targetP.coins_received = true;
                        broadcastLobbyState(roomId);
                    }
                }
            }
            broadcastToLobby(roomId, rawText, ws);

        } else if (msgType === WSMsgType.Start) {
            handled = true;
            if (clientId === lobby.state.host && !lobby.state.inProgress) {
                lobby.state.inProgress = true;
                lobby.state.phase = "GAME";
                lobby.state.currentRound = 1;
                lobby.state.roundsRemaining = lobby.state.roundCount;
                lobby.roundReadyPerRound = {};
                lobby.roundAdvancing = false;
                lobby.cardsDealtForRound = false;
                lobby.startTime = Date.now();
                broadcastLobbyState(roomId);
            }

        } else if ([WSMsgType.SetRoundCount, WSMsgType.LobbyRoundChange, "UpdateRoundCount"].includes(msgType)) {
            handled = true;
            
            // Allow setting total rounds only in the pre-game lobby
            if (!lobby.state.inProgress && clientId === lobby.state.host) {
                if (packet.data !== undefined) {
                    const val = parseInt(packet.data, 10);
                    if (!isNaN(val) && val > 0) {
                        lobby.state.roundCount = val;
                        lobby.state.roundsRemaining = val;
                        broadcastLobbyState(roomId);
                    }
                }
            }
            // During inProgress = true, silently drop incoming SetRoundCount packets to prevent client-side early exits.

        } else if ([WSMsgType.SetTimer, "ToggleTimer"].includes(msgType)) {
            handled = true;
            if (!lobby.state.inProgress && clientId === lobby.state.host) {
                const useTimer = packet.data !== undefined ? Boolean(packet.data) : !lobby.state.useTimer;
                lobby.state.useTimer = useTimer;
                broadcastLobbyState(roomId);
            }

        } else if (msgType === WSMsgType.LobbyBetChange) {
            handled = true;
            if (!lobby.state.inProgress && packet.data !== undefined && clientId === lobby.state.host) {
                lobby.state.betMultiplier = packet.data;
                broadcastLobbyState(roomId);
            }

        } else if (msgType === WSMsgType.Disconnect) {
            handled = true;
            purgeClientConnection(ws, roomId, targetPid || clientId);
        }

        if (!handled) {
            broadcastToLobby(roomId, rawText, ws);
        }
    });

    ws.on('close', () => {
        if (currentLobbyId) {
            purgeClientConnection(ws, currentLobbyId, clientId);
        }
    });

    ws.on('error', (err) => {
        console.error(`[WS ERROR]`, err.message);
    });
});

server.listen(PORT, () => {
    console.log(`========================================================`);
    console.log(`  Picture Poker Node.js Server Active on Port ${PORT}`);
    console.log(`========================================================`);
});