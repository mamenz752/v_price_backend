using System;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
// using Microsoft.Azure.Functions.Worker.Extensions.Timer;
using Microsoft.Extensions.Logging;

namespace Functions
{
    public class WeatherCsvTimer
    {
        private readonly ILogger<WeatherCsvTimer> _log;
        private readonly HttpClient _http;

        public WeatherCsvTimer(ILogger<WeatherCsvTimer> log, System.Net.Http.HttpClient http)
        {
            _log = log;
            _http = http;
        }

        // 例：1時間おき。必要に応じてCRONは変更してください（UTC）。
        [Function("WeatherCsvTimer")]
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

            var rows = doc.RootElement.GetProperty("results").EnumerateArray()
                .Select((e, i) => new { idx = i + 1, name = e.GetProperty("name").GetString(), url = e.GetProperty("url").GetString() });

            var sb = new StringBuilder();
            sb.AppendLine("idx,name,url");
            foreach (var r in rows)
            {
                sb.AppendLine($"{r.idx},{r.name},{r.url}");
            }

            var sharedRoot = Environment.GetEnvironmentVariable("SHARED_DATA_DIR") ?? "/shared";
            string subdir = Path.Combine(sharedRoot, prefix, nowJst.ToString("yyyy"), nowJst.ToString("MM"));
            Directory.CreateDirectory(subdir);

            var fileName = $"weather-{nowJst:yyyyMMdd-HHmmss}.csv";
            string blobPath = Path.Combine(subdir, fileName);
            string localSavePath = Path.Combine(sharedRoot, prefix, nowJst.ToString("yyyy"), nowJst.ToString("MM"), fileName);

            File.WriteAllLines(localSavePath, sb.ToString().Split(Environment.NewLine), Encoding.GetEncoding("utf-8"));

            await BlobLogWriter.WriteTextAsync(connStr, container, blobPath, sb.ToString());

            _log.LogInformation("WeatherCsvTimer: CSV file written to {BlobPath}", blobPath);

            var notifier = new WebhookNotifier(_http, _log);
            await notifier.NotifyDeadlineWeatherUpdateAsync();

            _log.LogInformation("WeatherCsvTimer: Deadline weather update webhook notified.");
        }
    }
}