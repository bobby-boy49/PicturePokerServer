using BepInEx;
using BepInEx.Configuration;
using HarmonyLib;
using System;
using UnityEngine;

namespace PicturePokerRedirector
{
    [BepInPlugin("com.yourname.picturepoker.redirector", "Picture Poker Server Redirector", "1.0.0")]
    public class Plugin : BaseUnityPlugin
    {
        // Default host/IP address
        public const string DEFAULT_SERVER_HOST = "picturepoker.rui2015.me";

        public static ConfigEntry<string> TargetServerHost;

        // UI state & Window Sizing
        private bool showUi = false;
        private string inputServerHost = "";
        
        // Initial window dimensions (Width: 500px, Height: 260px)
        private Rect windowRect = new Rect(30, 30, 500, 260);

        private void Awake()
        {
            TargetServerHost = Config.Bind(
                "Server Settings",
                "ServerHost",
                DEFAULT_SERVER_HOST,
                "The target server host/IP address (e.g. 192.168.1.50:4444 or myserver.com)."
            );

            inputServerHost = TargetServerHost.Value;

            Harmony harmony = new Harmony("com.yourname.picturepoker.redirector");
            harmony.PatchAll();

            Logger.LogInfo("[Redirector Mod] Active! Press F2 to open Server Switcher.");
        }

        private void Update()
        {
            if (Input.GetKeyDown(KeyCode.F2))
            {
                showUi = !showUi;
            }
        }

        private void OnGUI()
        {
            if (!showUi) return;

            // Clamp window size so it never exceeds screen resolution or shrinks below minimums
            windowRect.width = Mathf.Clamp(windowRect.width, 350f, Screen.width);
            windowRect.height = Mathf.Clamp(windowRect.height, 220f, Screen.height);

            // Clamp window position inside game screen bounds
            windowRect.x = Mathf.Clamp(windowRect.x, 0f, Screen.width - windowRect.width);
            windowRect.y = Mathf.Clamp(windowRect.y, 0f, Screen.height - windowRect.height);

            windowRect = GUILayout.Window(999123, windowRect, DrawServerWindow, "Server Switcher (F2 to Toggle)");
        }

        private void DrawServerWindow(int windowID)
        {
            GUILayout.Space(5);
            
            // Custom Styling for Larger Text/Fields
            GUIStyle labelStyle = new GUIStyle(GUI.skin.label) { fontSize = 14 };
            GUIStyle fieldStyle = new GUIStyle(GUI.skin.textField) { fontSize = 14, fixedHeight = 28 };
            GUIStyle buttonStyle = new GUIStyle(GUI.skin.button) { fontSize = 13, fixedHeight = 35 };

            GUILayout.Label("Server Address (IP:Port or Domain):", labelStyle);
            inputServerHost = GUILayout.TextField(inputServerHost, fieldStyle);

            GUILayout.Space(15);

            // Display current active URLs for clarity
            GUILayout.Label($"<b>WS:</b> {GetFormattedWsUrl()}", GUI.skin.label);
            GUILayout.Label($"<b>HTTP:</b> {GetFormattedHttpUrl()}", GUI.skin.label);

            GUILayout.FlexibleSpace();

            GUILayout.BeginHorizontal();

            if (GUILayout.Button("Apply & Redirect", buttonStyle))
            {
                string cleaned = CleanHostInput(inputServerHost);
                inputServerHost = cleaned;
                TargetServerHost.Value = cleaned;

                Config.Save();
                Logger.LogInfo($"[Redirector Mod] Server redirected -> Host: {TargetServerHost.Value}");
            }

            if (GUILayout.Button("Reset Default", buttonStyle))
            {
                inputServerHost = DEFAULT_SERVER_HOST;
                TargetServerHost.Value = DEFAULT_SERVER_HOST;

                Config.Save();
                Logger.LogInfo("[Redirector Mod] Server reset to default address.");
            }

            GUILayout.EndHorizontal();

            GUILayout.Space(5);

            if (GUILayout.Button("Close Menu", buttonStyle))
            {
                showUi = false;
            }

            // Allow dragging from anywhere in the window
            GUI.DragWindow();
        }

        public static string CleanHostInput(string host)
        {
            if (string.IsNullOrEmpty(host)) return DEFAULT_SERVER_HOST;

            host = host.Trim();
            if (host.StartsWith("https://", StringComparison.OrdinalIgnoreCase)) host = host.Substring(8);
            else if (host.StartsWith("http://", StringComparison.OrdinalIgnoreCase)) host = host.Substring(7);
            else if (host.StartsWith("wss://", StringComparison.OrdinalIgnoreCase)) host = host.Substring(6);
            else if (host.StartsWith("ws://", StringComparison.OrdinalIgnoreCase)) host = host.Substring(5);

            return host.TrimEnd('/');
        }

        public static string GetFormattedWsUrl()
        {
            string host = CleanHostInput(TargetServerHost.Value);
            
            // Support all common local network address blocks and loopbacks
            bool isLocal = host.StartsWith("127.0.0.1") || 
                           host.StartsWith("localhost") || 
                           host.StartsWith("192.168.") || 
                           host.StartsWith("10.") || 
                           host.StartsWith("172.");

            string scheme = isLocal ? "ws://" : "wss://";
            return $"{scheme}{host}/";
        }

        public static string GetFormattedHttpUrl()
        {
            string host = CleanHostInput(TargetServerHost.Value);
            
            bool isLocal = host.StartsWith("127.0.0.1") || 
                           host.StartsWith("localhost") || 
                           host.StartsWith("192.168.") || 
                           host.StartsWith("10.") || 
                           host.StartsWith("172.");

            string scheme = isLocal ? "http://" : "https://";
            return $"{scheme}{host}/";
        }
    }

    // Intercept WebSocket URL Getter
    [HarmonyPatch(typeof(GameSettings), "serverWSUrl", MethodType.Getter)]
    public static class ServerWsUrlGetterPatch
    {
        public static void Postfix(ref string __result)
        {
            __result = Plugin.GetFormattedWsUrl();
        }
    }

    // Intercept HTTP URL Getter
    [HarmonyPatch(typeof(GameSettings), "serverHttpUrl", MethodType.Getter)]
    public static class ServerHttpUrlGetterPatch
    {
        public static void Postfix(ref string __result)
        {
            __result = Plugin.GetFormattedHttpUrl();
        }
    }

    // Prevent redirected URLs from polluting settings.json on disk
    [HarmonyPatch(typeof(GameSettings), "Save")]
    public static class GameSettingsSavePatch
    {
        private static string savedHttp;
        private static string savedWs;

        public static void Prefix()
        {
            if (GameSettings.instance != null)
            {
                savedHttp = GameSettings.instance.serverHttpUrl;
                savedWs = GameSettings.instance.serverWSUrl;

                GameSettings.instance.serverHttpUrl = $"https://{Plugin.DEFAULT_SERVER_HOST}/";
                GameSettings.instance.serverWSUrl = $"wss://{Plugin.DEFAULT_SERVER_HOST}/";
            }
        }

        public static void Postfix()
        {
            if (GameSettings.instance != null)
            {
                GameSettings.instance.serverHttpUrl = savedHttp;
                GameSettings.instance.serverWSUrl = savedWs;
            }
        }
    }
}