using BepInEx;
using BepInEx.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
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

            // Display formatted URLs based on current input
            string previewHost = CleanHostInput(inputServerHost);
            GUILayout.Label($"<b>WS:</b> {GetFormattedWsUrl(previewHost)}", GUI.skin.label);
            GUILayout.Label($"<b>HTTP:</b> {GetFormattedHttpUrl(previewHost)}", GUI.skin.label);

            GUILayout.FlexibleSpace();

            GUILayout.BeginHorizontal();

            if (GUILayout.Button("Apply & Redirect", buttonStyle))
            {
                string cleaned = CleanHostInput(inputServerHost);
                inputServerHost = cleaned;
                TargetServerHost.Value = cleaned;

                Config.Save();
                WriteHostToDisk(cleaned);
                Logger.LogInfo($"[Redirector Mod] Server written to settings.json -> Host: {TargetServerHost.Value}");
            }

            if (GUILayout.Button("Reset Default", buttonStyle))
            {
                inputServerHost = DEFAULT_SERVER_HOST;
                TargetServerHost.Value = DEFAULT_SERVER_HOST;

                Config.Save();
                WriteHostToDisk(DEFAULT_SERVER_HOST);
                Logger.LogInfo("[Redirector Mod] Server reset to default address in settings.json.");
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

        /// <summary>
        /// Writes the target host URLs directly into settings.json on disk using Newtonsoft.Json.
        /// </summary>
        private void WriteHostToDisk(string host)
        {
            try
            {
                string settingsPath = GameSettings.settingsJsonPath;

                if (!File.Exists(settingsPath))
                {
                    Logger.LogWarning($"[Redirector Mod] settings.json missing at {settingsPath}. Skipping write.");
                    return;
                }

                string rawJson = File.ReadAllText(settingsPath);
                JObject settings = JObject.Parse(rawJson);

                string formattedWs = GetFormattedWsUrl(host);
                string formattedHttp = GetFormattedHttpUrl(host);

                settings["serverWSUrl"] = formattedWs;
                settings["serverHttpUrl"] = formattedHttp;

                File.WriteAllText(settingsPath, JsonConvert.SerializeObject(settings, Formatting.Indented));

                // Update in-memory GameSettings object so current session picks it up
                if (GameSettings.instance != null)
                {
                    GameSettings.instance.serverWSUrl = formattedWs;
                    GameSettings.instance.serverHttpUrl = formattedHttp;
                }
            }
            catch (Exception ex)
            {
                Logger.LogError($"[Redirector Mod] Failed to write settings.json: {ex.Message}");
            }
        }

        public static string CleanHostInput(string host)
        {
            if (string.IsNullOrWhiteSpace(host)) return DEFAULT_SERVER_HOST;

            host = host.Trim();

            // Strip scheme if present
            if (host.StartsWith("https://", StringComparison.OrdinalIgnoreCase)) host = host.Substring(8);
            else if (host.StartsWith("http://", StringComparison.OrdinalIgnoreCase)) host = host.Substring(7);
            else if (host.StartsWith("wss://", StringComparison.OrdinalIgnoreCase)) host = host.Substring(6);
            else if (host.StartsWith("ws://", StringComparison.OrdinalIgnoreCase)) host = host.Substring(5);

            // Strip trailing slashes or spaces
            return host.TrimEnd('/').Trim();
        }

        public static bool IsLocalHost(string host)
        {
            string cleaned = CleanHostInput(host);

            if (cleaned.StartsWith("localhost", StringComparison.OrdinalIgnoreCase) || 
                cleaned.StartsWith("127.", StringComparison.OrdinalIgnoreCase) || 
                cleaned.StartsWith("192.168.", StringComparison.OrdinalIgnoreCase) || 
                cleaned.StartsWith("10.", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            // Check for 172.16.x.x - 172.31.x.x subnet block
            if (cleaned.StartsWith("172."))
            {
                string[] parts = cleaned.Split('.');
                if (parts.Length >= 2 && int.TryParse(parts[1], out int secondOctet))
                {
                    if (secondOctet >= 16 && secondOctet <= 31)
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public static string GetFormattedWsUrl(string host)
        {
            string cleaned = CleanHostInput(host);
            string scheme = IsLocalHost(cleaned) ? "ws://" : "wss://";
            return $"{scheme}{cleaned}/";
        }

        public static string GetFormattedHttpUrl(string host)
        {
            string cleaned = CleanHostInput(host);
            string scheme = IsLocalHost(cleaned) ? "http://" : "https://";
            return $"{scheme}{cleaned}/";
        }
    }
}