using System;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
// using Microsoft.Azure.Functions.Worker.Extensions.Timer;
using Microsoft.Extensions.Logging;

namespace Functions
{
    public class PriceTxtTimer
    {
        private readonly ILogger<PriceTxtTimer> _log;
        private readonly HttpClient _http;

        public PriceTxtTimer(ILogger<PriceTxtTimer> log, System.Net.Http.HttpClient http)
        {
            _log = log;
            _http = http;
        }

        // 例：1時間おき。必要に応じてCRONは変更してください（UTC）。
        [Function("PriceTxtTimer")]
        public async Task RunAsync(
            // [TimerTrigger("0 0 7 * * *", UseMonitor = true)] TimerInfo _)
            [TimerTrigger("0 */5 * * * *", UseMonitor = true)] TimerInfo _)
        {
            var jst = TimeZoneInfo.FindSystemTimeZoneById("Asia/Tokyo");
            var nowJst = TimeZoneInfo.ConvertTime(DateTimeOffset.UtcNow, jst);

            var connStr = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            var container = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONTAINER") ?? "container";
            var prefix = Environment.GetEnvironmentVariable("TXT_PREFIX") ?? "price";

            var sharedRoot = Environment.GetEnvironmentVariable("SHARED_DATA_DIR") ?? "/shared";

            try
            {
                var authUrl = Environment.GetEnvironmentVariable("AUTH_PRICE_URL");
                var dataUrl = Environment.GetEnvironmentVariable("DATA_PRICE_URL");

                var authPayload = new Dictionary<string, string>
                {
                    ["grant_type"] = "client_credentials",
                    ["client_id"] = Environment.GetEnvironmentVariable("CLIENT_ID") ?? "",
                    ["client_secret"] = Environment.GetEnvironmentVariable("CLIENT_SECRET") ?? ""
                };

                var authContent = new FormUrlEncodedContent(authPayload);
                // var authContent = new StringContent(JsonSerializer.Serialize(authPayload), System.Text.Encoding.UTF8, "application/x-www-form-urlencoded");

                _log.LogInformation("Auth URL: {Url}", authUrl);
                _log.LogDebug("Auth payload keys: {Keys}", string.Join(",", authPayload.GetType().GetProperties().Select(p => p.Name)));

                using var authRes = await _http.PostAsync(authUrl, authContent);
                authRes.EnsureSuccessStatusCode();

                // ステータスとボディを必ずログ
                var authStatus = (int)authRes.StatusCode;
                var authResBody = await authRes.Content.ReadAsStringAsync();
                _log.LogInformation("Auth response status: {Status}", authStatus);
                _log.LogDebug("Auth response body: {Body}", authResBody);

                var authBody = await authRes.Content.ReadFromJsonAsync<JsonElement>();
                string accessToken = null;
                if (authBody.TryGetProperty("access_token", out var atProp)) accessToken = atProp.GetString();

                if (string.IsNullOrEmpty(accessToken))
                {
                    _log.LogError("Access token is null or empty.");
                    return;
                }
                else
                {
                    var getUri = dataUrl + nowJst.ToString("yyyy-M-d");
                    var resBody = await _http.GetStringAsync(getUri);
                    var doc = JsonDocument.Parse(resBody);
                    var text = doc.RootElement.GetRawText();

                    string subdir = Path.Combine(sharedRoot, prefix, nowJst.ToString("yyyy"), nowJst.ToString("MM"));

                    var fileName = $"{nowJst:yyyy-MM-dd}.txt";
                    var blobPath = Path.Combine(subdir, fileName);
                    // test: directory for local save
                    string localSavePath = Path.Combine(sharedRoot, prefix, nowJst.ToString("yyyy"), nowJst.ToString("MM"), fileName);
                    Directory.CreateDirectory(subdir);

                    await File.WriteAllTextAsync(localSavePath, text, System.Text.Encoding.UTF8);
                    await BlobLogWriter.WriteTextAsync(connStr, container, blobPath, text);

                    _log.LogInformation("PriceCsvTimer: TXT file written to {BlobPath}", blobPath);
                }

            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Error occurred while fetching data from external API.");
            }
            
            // Start webhook notifications
            var notifier = new WebhookNotifier(_http, _log);
            await notifier.NotifyDailyPriceUpdateAsync();

            _log.LogInformation("PriceTxtTimer: Daily price update webhook notified.");

            if (nowJst.Day >= 15)
            {
                await notifier.NotifyDeadlinePriceUpdateAsync();
                _log.LogInformation("PriceTxtTimer: Deadline price update webhook notified.");
            }
        }
    }
}