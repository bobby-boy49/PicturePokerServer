# Picture Poker Server Protocol & Architecture Specification

This is an overview of my current understanding of how the client/server connection "works" please keep in mind that this isn't 100% and is subject to change if I realize something new or just realize that something is just wrong.

## 1. Overview & Network Framing
This document specifies the network communication protocol and game state management rules for the Picture Poker multiplayer backend server.

* **Transport Layer:** WebSockets over HTTP/WS.
* **Data Serialization:** UTF-8 encoded JSON strings.
* **Frame Normalization:** Client frameworks (specifically `websocket-sharp`) may append trailing null bytes (`\0`). All raw incoming socket messages **must** be trimmed and cleaned of `\0` before JSON parsing.
* **Room Addressing:** The target Room ID is extracted from the WebSocket HTTP request path or query string (e.g., `/ws/JEZQ` or `?room=JEZQ`).

---

## 2. Server Data Models

### A. Lobby Model (`Lobby`)
```json
{
  "id": "JEZQ",
  "host": "Player-1234",
  "inProgress": false,
  "phase": "LOBBY",
  "currentRound": 1,
  "roundCount": 5,
  "roundsRemaining": 5,
  "useTimer": true,
  "timerSeconds": 15,
  "betMultiplier": 1,
  "players": []
}
```

### B. Player Model (`Player`)
```json
{
  "id": "Player-1234",
  "name": "wombo",
  "login": "",
  "playerNumber": 0,
  "coins": 100,
  "connected": true,
  "ready": true
}
```

---

## 3. Game State Machine Workflow

```text
+-------------------------------------------------------------+
|                                                             |
v                                                             |
[ Phase: LOBBY ] ---> (Host sends "Start") ---> [ Phase: GAME ]
                                                       |
                                         (Both send "ReadyForNextRound")
                                                       |
                                                       v
                                               [ Advance Round ]
                                                       |
                                                       |
[ Phase: FINISHED ] <--- (Final Round complete) -------+
```

---

## 4. Packet Specifications

### A. Connection & Handshake

#### `Hello` / `GameReady` / `PlayerReady`
* **Direction:** Client -> Server
* **Purpose:** Initial join or reconnection handshake.
* **Payload Example:**
  ```json
  { "type": "Hello", "id": "Player-1234", "name": "wombo", "coins": 50 }
  ```
* **Server Logic:**
  1. Register or update player entry in `lobby.players`. Assign `playerNumber` index if new.
  2. Assign `lobby.host = packet.id` if no host is currently set.
  3. Broadcast an updated `LobbyUpdated` frame to all connected room clients.

---

### B. Lobby Management

#### `LobbyUpdated`
* **Direction:** Server -> Client (Broadcast)
* **Purpose:** Pushes state updates and active player lists to clients.
* **Payload Structure:**
  ```json
  {
    "type": "LobbyUpdated",
    "player": "System",
    "id": "JEZQ",
    "players": [ /* Array of Player Objects */ ],
    "data": { /* Complete Lobby State Object */ }
  }
  ```

#### `SetRoundCount` / `LobbyRoundChange`
* **Direction:** Client -> Server
* **Purpose:** Sets total match rounds during pre-game setup.
* **Payload Example:** `{ "type": "SetRoundCount", "data": 5 }`
* **Critical Guardrail:** **Ingress packets must be IGNORED while `inProgress === true`.** Client state engines calculate local round counters, and receiving mid-game `SetRoundCount` updates causes clients to exit active games prematurely.

#### `SetTimer` / `LobbyBetChange`
* **Direction:** Client -> Server (Host only)
* **Purpose:** Configures timer toggle or bet multiplier settings.
* **Server Logic:** Update corresponding fields on lobby and broadcast `LobbyUpdated`.

---

### C. Gameplay Progression

#### `Start`
* **Direction:** Client (Host) -> Server
* **Purpose:** Begins the match.
* **Server Logic:**
  1. Set `inProgress = true`, `phase = "GAME"`, and `currentRound = 1`.
  2. Reset internal ready-state counters.
  3. Broadcast `LobbyUpdated`.

#### `MyCards` / `DrawHoldPressed` / `SelectedCardsChanged`
* **Direction:** Client -> Server
* **Purpose:** Synchronizes card distribution, holds, and UI selections between players.
* **Server Logic:**
  1. Set local flag `cardsDealtForRound = true`.
  2. **Passthrough:** Forward unhandled raw packet string directly to opponent clients.

#### `ReadyForNextRound`
* **Direction:** Client -> Server
* **Purpose:** Sent when a player's round animations and score tallies complete.
* **Payload Example:** `{ "type": "ReadyForNextRound", "id": "Player-1234" }`
* **Synchronization Logic:**
  1. **Immediate Echo:** Broadcast `ReadyForNextRound` to opposing clients to prevent local state machine deadlocks.
  2. **Round Advancement:**
     * Log player readiness for `currentRound`.
     * **When ALL connected players submit `ReadyForNextRound` for the active round:**
       * If `cardsDealtForRound === true`:
         * If `currentRound < totalRounds`: Increment `currentRound += 1` and set `cardsDealtForRound = false`.
         * Else: Set `inProgress = false` and `phase = "FINISHED"`.
       * Broadcast `LobbyUpdated` to sync state across all clients.

---

### D. Currency & Disconnections

#### `MyCoins`
* **Direction:** Client -> Server
* **Purpose:** Updates player coin totals post-round.
* **Server Logic:** Update target player's `coins` in room state and echo packet to opponents.

#### `Disconnect` / Network Drop
* **Direction:** Client / WebSocket Transport
* **Server Logic:**
  1. Flag target player as `connected = false`.
  2. Initiate a 5-second grace period timer.
  3. If client fails to reconnect before timer expiration, drop player from `lobby.players` (if in lobby) and migrate host status if needed.
  4. Broadcast `LobbyUpdated`.
