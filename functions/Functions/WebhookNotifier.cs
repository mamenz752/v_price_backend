using System;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Functions
{
    public class WebhookNotifier
    {
        private readonly ILogger _log;
        private readonly HttpClient _http;

        public WebhookNotifier(HttpClient http, ILogger log)
        {
            _http = http;
            _log = log;
        }

        public async Task NotifyDailyPriceUpdateAsync(CancellationToken cancellationToken = default)
        {
            var baseUrl = Environment.GetEnvironmentVariable("WEBHOOK_URL");
            var token = Environment.GetEnvironmentVariable("WEBHOOK_TOKEN");

            string url = $"{baseUrl}/daily";

            var jst = TimeZoneInfo.FindSystemTimeZoneById("Asia/Tokyo");
            var nowJst = TimeZoneInfo.ConvertTime(DateTimeOffset.UtcNow, jst);

            if (string.IsNullOrEmpty(token))
            {
                _log.LogWarning("Webhook token for daily price update is not set.");
                return;
            }

            var payload = new
            {
                eventType = "daily.price.update",
                createdAt = nowJst.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            };

            if (payload == null)
            {
                _log.LogError("Webhook payload cannot be null");
                return;
            }

            using var req = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new StringContent(
                    JsonSerializer.Serialize(payload),
                    System.Text.Encoding.UTF8,
                    "application/json"
                )
            };

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(10));

            if (!string.IsNullOrWhiteSpace(token))
                req.Headers.Add("X-Webhook-Token", token);

            try
            {
                _log.LogDebug("Sending webhook request to {Url} with payload: {Payload}", url, JsonSerializer.Serialize(payload));
                var res = await _http.SendAsync(req, cts.Token);
                if (!res.IsSuccessStatusCode)
                {
                    var body = await res.Content.ReadAsStringAsync();
                    _log.LogWarning("Webhook failed: {Status} {Body}", res.StatusCode, body);
                }
                else
                {
                    _log.LogInformation("Webhook sent to {Url} about daily price update", url);
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Error sending webhook to {Url}", url);
            }
        }

        public async Task NotifyDeadlineUpdateAsync(CancellationToken cancellationToken = default)
        {
            var baseUrl = Environment.GetEnvironmentVariable("WEBHOOK_URL");
            var token = Environment.GetEnvironmentVariable("WEBHOOK_TOKEN");

            string url = $"{baseUrl}/deadline";

            var jst = TimeZoneInfo.FindSystemTimeZoneById("Asia/Tokyo");
            var nowJst = TimeZoneInfo.ConvertTime(DateTimeOffset.UtcNow, jst);

            if (string.IsNullOrEmpty(token))
            {
                _log.LogWarning("Webhook token for daily price update is not set.");
                return;
            }

            var payload = new
            {
                eventType = "deadline.weather.update",
                createdAt = nowJst.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            };


            if (payload == null)
            {
                _log.LogError("Webhook payload cannot be null");
                return;
            }

             using var req = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new StringContent(
                    JsonSerializer.Serialize(payload),
                    System.Text.Encoding.UTF8,
                    "application/json"
                )
            };

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(10));

            if (!string.IsNullOrWhiteSpace(token))
                req.Headers.Add("X-Webhook-Token", token);

            try
            {
                _log.LogDebug("Sending webhook request to {Url} with payload: {Payload}", url, JsonSerializer.Serialize(payload));
                var res = await _http.SendAsync(req, cts.Token);
                if (!res.IsSuccessStatusCode)
                {
                    var body = await res.Content.ReadAsStringAsync();
                    _log.LogWarning("Webhook failed: {Status} {Body}", res.StatusCode, body);
                }
                else
                {
                    _log.LogInformation("Webhook sent to {Url} about deadline weather update", url);
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Error sending webhook to {Url}", url);
            }
        }
    }

}