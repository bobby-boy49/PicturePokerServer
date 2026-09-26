using BepInEx;
using HarmonyLib;
using WebSocketSharp;

namespace PicturePokerRedirector
{
    [BepInPlugin("com.local.picturepoker.redirector", "Picture Poker Local Redirector", "1.0.0")]
    public class Plugin : BaseUnityPlugin
    {
        private void Awake()
        {
            Harmony.CreateAndPatchAll(typeof(Plugin).Assembly);
            Logger.LogInfo("Redirector initialized successfully!");
        }
    }

    [HarmonyPatch(typeof(WebSocket), MethodType.Constructor, new[] { typeof(string), typeof(string[]) })]
    public static class WebSocketPatch
    {
        static void Prefix(ref string url)
        {
            UnityEngine.Debug.Log($"[Redirector] Original URL: {url}");
            url = "ws://127.0.0.1:8080";
            UnityEngine.Debug.Log($"[Redirector] Redirected URL: {url}");
        }
    }
}