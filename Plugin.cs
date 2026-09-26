using BepInEx;
using BepInEx.Configuration;
using HarmonyLib;
using WebSocketSharp;

namespace PicturePokerRedirector
{
    [BepInPlugin("com.local.picturepoker.redirector", "Picture Poker Local Redirector", "1.0.0")]
    public class Plugin : BaseUnityPlugin
    {
        // Config entry for the custom server address
        public static ConfigEntry<string> ServerAddress;

        private void Awake()
        {
            // Bind the configuration setting
            ServerAddress = Config.Bind(
                "Network",                                          // Section
                "ServerAddress",                                    // Key
                "ws://127.0.0.1:8080",                              // Default Value
                "The WebSocket server URL to redirect game traffic to." // Description
            );

            Harmony.CreateAndPatchAll(typeof(Plugin).Assembly);
            Logger.LogInfo($"Redirector initialized! Target server: {ServerAddress.Value}");
        }
    }

    [HarmonyPatch(typeof(WebSocket), MethodType.Constructor, new[] { typeof(string), typeof(string[]) })]
    public static class WebSocketPatch
    {
        static void Prefix(ref string url)
        {
            UnityEngine.Debug.Log($"[Redirector] Original URL: {url}");
            
            // Override with the configured address
            url = Plugin.ServerAddress.Value;
            
            UnityEngine.Debug.Log($"[Redirector] Redirected URL: {url}");
        }
    }
}