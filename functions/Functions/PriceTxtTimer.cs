using System;
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
            [TimerTrigger("0 */5 0 * * *", UseMonitor = true)] TimerInfo _)
        {
            var jst = TimeZoneInfo.FindSystemTimeZoneById("Asia/Tokyo");
            var nowJst = TimeZoneInfo.ConvertTime(DateTimeOffset.UtcNow, jst);

            var connStr = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            var container = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONTAINER") ?? "container";
            var prefix = Environment.GetEnvironmentVariable("PREFIX") ?? "test";

            var url = "https://pokeapi.co/api/v2/pokemon?limit=10";
            var json = await _http.GetStringAsync(url);

            // ここに処理を実装してください
            var doc = JsonDocument.Parse(json);
            var count = doc.RootElement.GetProperty("results").GetArrayLength();

            var fileName = $"poke-{nowJst:yyyyMMdd-HHmmss}.txt";
            var blobPath = $"{prefix}/{nowJst:yyyy}/{nowJst:MM}/{fileName}";

            var text = $"JST={nowJst:O}\nCount={count}\nRaw={json}\n";
            await BlobLogWriter.WriteTextAsync(connStr, container, blobPath, text);

            await Task.CompletedTask;
        }
    }
}