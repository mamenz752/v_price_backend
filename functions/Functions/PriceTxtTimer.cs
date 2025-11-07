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
            [TimerTrigger("0 */5 * * * *", UseMonitor = true)] TimerInfo _)
        {
            var jst = TimeZoneInfo.FindSystemTimeZoneById("Asia/Tokyo");
            var nowJst = TimeZoneInfo.ConvertTime(DateTimeOffset.UtcNow, jst);

            var connStr = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            var container = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONTAINER") ?? "container";
            var prefix = Environment.GetEnvironmentVariable("PREFIX") ?? "test";

            var url = "https://pokeapi.co/api/v2/pokemon?limit=10";
            var json = await _http.GetStringAsync(url);

            var doc = JsonDocument.Parse(json);
            var count = doc.RootElement.GetProperty("results").GetArrayLength();

            var sharedRoot = Environment.GetEnvironmentVariable("SHARED_DATA_DIR") ?? "/shared";
            string subdir = Path.Combine(sharedRoot, prefix, nowJst.ToString("yyyy"), nowJst.ToString("MM"));
            Directory.CreateDirectory(subdir);

            var fileName = $"poke-{nowJst:yyyyMMdd-HHmmss}.txt";
            var blobPath = Path.Combine(subdir, fileName);
            string localSavePath = Path.Combine(sharedRoot, prefix, nowJst.ToString("yyyy"), nowJst.ToString("MM"), fileName);

            var text = $"JST={nowJst:O}\nCount={count}\nRaw={json}\n";

            await File.WriteAllTextAsync(localSavePath, text);
            await BlobLogWriter.WriteTextAsync(connStr, container, blobPath, text);

            _log.LogInformation("PriceCsvTimer: TXT file written to {BlobPath}", blobPath);
        }
    }
}