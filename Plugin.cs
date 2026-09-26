using BepInEx;
using BepInEx.Configuration;
using HarmonyLib;
using System;
using System.Linq;
using System.Reflection;
using WebSocketSharp;

namespace PicturePokerRedirector
{
    [BepInPlugin("com.local.picturepoker.redirector", "Picture Poker Local Redirector", "1.0.0")]
    public class Plugin : BaseUnityPlugin
    {
        public static ConfigEntry<string> ServerAddress;

        private void Awake()
        {
            ServerAddress = Config.Bind(
                "Network",
                "ServerAddress",
                "ws://127.0.0.1:4444",
                "The WebSocket server URL to redirect game traffic to."
            );

            Harmony harmony = new Harmony("com.local.picturepoker.redirector");
            
            MethodInfo prefix = typeof(WebSocketPatch).GetMethod(nameof(WebSocketPatch.Prefix), BindingFlags.Static | BindingFlags.Public);
            
            // Get constructors that accept a parameter named "url"
            var targetConstructors = typeof(WebSocket)
                .GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                .Where(ctor => ctor.GetParameters().Any(p => p.Name.Equals("url", StringComparison.OrdinalIgnoreCase)));

            int patchedCount = 0;
            foreach (var ctor in targetConstructors)
            {
                harmony.Patch(ctor, prefix: new HarmonyMethod(prefix));
                patchedCount++;
            }

            Logger.LogInfo($"Redirector initialized! Patched {patchedCount} WebSocket constructor(s). Target: {ServerAddress.Value}");
        }
    }

    public static class WebSocketPatch
    {
        public static void Prefix(ref string url)
        {
            if (string.IsNullOrEmpty(url)) return;

            UnityEngine.Debug.Log($"[Redirector] >>> ORIGINAL URL DETECTED: {url}");

            try
            {
                string targetBase = Plugin.ServerAddress.Value.Trim();
                if (!targetBase.StartsWith("ws://", StringComparison.OrdinalIgnoreCase) &&
                    !targetBase.StartsWith("wss://", StringComparison.OrdinalIgnoreCase))
                {
                    targetBase = "ws://" + targetBase;
                }

                Uri targetBaseUri = new Uri(targetBase);

                if (Uri.TryCreate(url, UriKind.Absolute, out Uri origAbsolute))
                {
                    UriBuilder builder = new UriBuilder(targetBaseUri)
                    {
                        Path = origAbsolute.AbsolutePath,
                        Query = origAbsolute.Query
                    };
                    url = builder.ToString();
                }
                else if (Uri.TryCreate(url, UriKind.Relative, out Uri origRelative))
                {
                    string relString = origRelative.ToString();
                    string path = relString;
                    string query = "";

                    int qIdx = relString.IndexOf('?');
                    if (qIdx >= 0)
                    {
                        path = relString.Substring(0, qIdx);
                        query = relString.Substring(qIdx + 1);
                    }

                    UriBuilder builder = new UriBuilder(targetBaseUri)
                    {
                        Path = path,
                        Query = query
                    };
                    url = builder.ToString();
                }
                else
                {
                    url = targetBaseUri.ToString();
                }
            }
            catch (Exception ex)
            {
                UnityEngine.Debug.LogError($"[Redirector] Parsing failed: {ex.Message}");
                url = Plugin.ServerAddress.Value;
            }

            UnityEngine.Debug.Log($"[Redirector] >>> REDIRECTED URL (Preserved): {url}");
        }
    }
}