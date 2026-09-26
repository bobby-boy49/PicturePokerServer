using BepInEx;
using BepInEx.Configuration;
using HarmonyLib;
using System;

namespace PicturePokerRedirector
{
    [BepInPlugin("com.yourname.picturepoker.redirector", "Picture Poker Server Redirector", "1.0.0")]
    public class Plugin : BaseUnityPlugin
    {
        public static ConfigEntry<string> TargetWsUrl;
        public static ConfigEntry<string> TargetHttpUrl;
        public static BepInEx.Logging.ManualLogSource ModLogger;

        private void Awake()
        {
            ModLogger = Logger;

            TargetWsUrl = Config.Bind(
                "Server Settings",
                "ServerWSUrl",
                "ws://127.0.0.1:4444/",
                "The target WebSocket URL for multiplayer traffic."
            );

            TargetHttpUrl = Config.Bind(
                "Server Settings",
                "ServerHttpUrl",
                "http://127.0.0.1:4444/",
                "The target HTTP URL for API traffic."
            );

            Harmony harmony = new Harmony("com.yourname.picturepoker.redirector");
            harmony.PatchAll();

            Logger.LogInfo($"[Redirector Mod] Loaded successfully!");
        }
    }

    // Intercept WebSocket URL
    [HarmonyPatch(typeof(GameSettings), "serverWSUrl", MethodType.Getter)]
    public static class ServerWsUrlGetterPatch
    {
        public static void Postfix(ref string __result)
        {
            // Log the raw, original string defined in GameSettings
            Plugin.ModLogger.LogInfo($"[ORIGINAL WS URI] Intercepted default: {__result}");

            string customUrl = Plugin.TargetWsUrl.Value;
            if (!customUrl.EndsWith("/"))
            {
                customUrl += "/";
            }

            // Overwrite with custom server URL
            __result = customUrl;
        }
    }

    // Intercept HTTP API URL
    [HarmonyPatch(typeof(GameSettings), "serverHttpUrl", MethodType.Getter)]
    public static class ServerHttpUrlGetterPatch
    {
        public static void Postfix(ref string __result)
        {
            // Log the raw, original string defined in GameSettings
            Plugin.ModLogger.LogInfo($"[ORIGINAL HTTP URI] Intercepted default: {__result}");

            string customUrl = Plugin.TargetHttpUrl.Value;
            if (!customUrl.EndsWith("/"))
            {
                customUrl += "/";
            }

            // Overwrite with custom server URL
            __result = customUrl;
        }
    }
}