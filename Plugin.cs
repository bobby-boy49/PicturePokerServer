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

        // UI state
        private bool showUi = false;
        private string inputServerHost = "";
        private Rect windowRect = new Rect(20, 20, 380, 160);

        private void Awake()
        {
            // Bind config with a single target host address
            TargetServerHost = Config.Bind(
                "Server Settings",
                "ServerHost",
                DEFAULT_SERVER_HOST,
                "The target server host/IP address (e.g. 127.0.0.1:4444 or myserver.com)."
            );

            // Sync UI input field
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

            windowRect = GUILayout.Window(999123, windowRect, DrawServerWindow, "Server Switcher (F2 to Toggle)");
        }

        private void DrawServerWindow(int windowID)
        {
            GUILayout.Label("Server Address (IP:Port or Domain):");
            inputServerHost = GUILayout.TextField(inputServerHost);

            GUILayout.Space(10);

            GUILayout.BeginHorizontal();

            // Apply custom server address typed in UI
            if (GUILayout.Button("Apply & Redirect"))
            {
                string cleaned = CleanHostInput(inputServerHost);
                inputServerHost = cleaned;
                TargetServerHost.Value = cleaned;

                Config.Save();
                Logger.LogInfo($"[Redirector Mod] Server redirected -> Host: {TargetServerHost.Value}");
            }

            // Reset back to default
            if (GUILayout.Button("Reset to Default"))
            {
                inputServerHost = DEFAULT_SERVER_HOST;
                TargetServerHost.Value = DEFAULT_SERVER_HOST;

                Config.Save();
                Logger.LogInfo("[Redirector Mod] Server reset to default address.");
            }

            GUILayout.EndHorizontal();

            GUILayout.Space(5);

            if (GUILayout.Button("Close Menu"))
            {
                showUi = false;
            }

            GUI.DragWindow();
        }

        // Strips protocol prefixes/slashes if user enters http(s):// or ws(s)://
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

        // Helper methods to construct formatted URLs
        public static string GetFormattedWsUrl()
        {
            string host = CleanHostInput(TargetServerHost.Value);
            bool isLocal = host.StartsWith("127.0.0.1") || host.StartsWith("localhost") || host.StartsWith("192.168.");
            string scheme = isLocal ? "ws://" : "wss://";
            return $"{scheme}{host}/";
        }

        public static string GetFormattedHttpUrl()
        {
            string host = CleanHostInput(TargetServerHost.Value);
            bool isLocal = host.StartsWith("127.0.0.1") || host.StartsWith("localhost") || host.StartsWith("192.168.");
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

                // Always write official defaults to settings.json
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