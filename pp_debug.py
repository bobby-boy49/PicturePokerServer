import asyncio
import websockets

async def handle_game_client(websocket):
    print("\n[+] Game connected successfully!")
    
    try:
        # Continuously listen for incoming messages from the game
        async for message in websocket:
            print(f"\n[<] Received Payload ({len(message)} bytes):")
            print(message)
            
            # Optional: Send a generic reply back if the game expects a response
            # await websocket.send("OK")
            
    except websockets.exceptions.ConnectionClosedError:
        print("[-] Game disconnected.")
    except Exception as e:
        print(f"[!] Error: {e}")

async def main():
    print("========================================")
    print(" Listening on ws://127.0.0.1:8080 ... ")
    print(" Launch Picture Poker now!")
    print("========================================\n")
    
    async with websockets.serve(handle_game_client, "127.0.0.1", 8080):
        await asyncio.Future()  # Keeps the server running indefinitely

if __name__ == "__main__":
    asyncio.run(main())
