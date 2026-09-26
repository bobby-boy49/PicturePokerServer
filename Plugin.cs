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
        // Hardcoded original production server addresses
        public const string DEFAULT_WS_URL = "wss://picturepoker.rui2015.me/";
        public const string DEFAULT_HTTP_URL = "https://picturepoker.rui2015.me/";

        public static ConfigEntry<string> TargetWsUrl;
        public static ConfigEntry<string> TargetHttpUrl;

        // UI state
        private bool showUi = false;
        private string inputWsUrl = "";
        private string inputHttpUrl = "";
        private Rect windowRect = new Rect(20, 20, 400, 210);

        private void Awake()
        {
            // Bind config with hardcoded defaults
            TargetWsUrl = Config.Bind(
                "Server Settings",
                "ServerWSUrl",
                DEFAULT_WS_URL,
                "The target WebSocket URL for multiplayer traffic."
            );

            TargetHttpUrl = Config.Bind(
                "Server Settings",
                "ServerHttpUrl",
                DEFAULT_HTTP_URL,
                "The target HTTP URL for API traffic."
            );

            // Sync UI input fields
            inputWsUrl = TargetWsUrl.Value;
            inputHttpUrl = TargetHttpUrl.Value;

            Harmony harmony = new Harmony("com.yourname.picturepoker.redirector");
            harmony.PatchAll();

            Logger.LogInfo("[Redirector Mod] Active! Hardcoded defaults set. Press F2 to open Server Switcher.");
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
            GUILayout.Label("WebSocket URL:");
            inputWsUrl = GUILayout.TextField(inputWsUrl);

            GUILayout.Space(5);

            GUILayout.Label("HTTP API URL:");
            inputHttpUrl = GUILayout.TextField(inputHttpUrl);

            GUILayout.Space(10);

            GUILayout.BeginHorizontal();

            // Apply custom server typed in UI
            if (GUILayout.Button("Apply & Redirect"))
            {
                if (!inputWsUrl.EndsWith("/")) inputWsUrl += "/";
                if (!inputHttpUrl.EndsWith("/")) inputHttpUrl += "/";

                TargetWsUrl.Value = inputWsUrl;
                TargetHttpUrl.Value = inputHttpUrl;

                Config.Save();
                Logger.LogInfo($"[Redirector Mod] Server redirected -> WS: {TargetWsUrl.Value} | HTTP: {TargetHttpUrl.Value}");
            }

            // Reset back to hardcoded defaults
            if (GUILayout.Button("Reset to Default"))
            {
                inputWsUrl = DEFAULT_WS_URL;
                inputHttpUrl = DEFAULT_HTTP_URL;

                TargetWsUrl.Value = DEFAULT_WS_URL;
                TargetHttpUrl.Value = DEFAULT_HTTP_URL;

                Config.Save();
                Logger.LogInfo("[Redirector Mod] Server reset to official production defaults.");
            }

            GUILayout.EndHorizontal();

            GUILayout.Space(5);

            if (GUILayout.Button("Close Menu"))
            {
                showUi = false;
            }

            GUI.DragWindow();
        }
    }

    // Intercept WebSocket URL Getter
    [HarmonyPatch(typeof(GameSettings), "serverWSUrl", MethodType.Getter)]
    public static class ServerWsUrlGetterPatch
    {
        public static void Postfix(ref string __result)
        {
            string customUrl = Plugin.TargetWsUrl.Value;
            if (!customUrl.EndsWith("/")) customUrl += "/";
            __result = customUrl;
        }
    }

    // Intercept HTTP URL Getter
    [HarmonyPatch(typeof(GameSettings), "serverHttpUrl", MethodType.Getter)]
    public static class ServerHttpUrlGetterPatch
    {
        public static void Postfix(ref string __result)
        {
            string customUrl = Plugin.TargetHttpUrl.Value;
            if (!customUrl.EndsWith("/")) customUrl += "/";
            __result = customUrl;
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
                GameSettings.instance.serverHttpUrl = Plugin.DEFAULT_HTTP_URL;
                GameSettings.instance.serverWSUrl = Plugin.DEFAULT_WS_URL;
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