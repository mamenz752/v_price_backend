using System;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Functions
{
    public class WebhookNotifier
    {
        private readonly ILogger<WebhookNotifier> _log;
        private readonly HttpClient _http;

        public WebhookNotifier(HttpClient http, ILogger<WebhookNotifier> log)
        {
            _http = http;
            _log = log;
            _http.Timeout = TimeSpan.FromSeconds(10);
        }

        public async Task NotifyDailyPriceUpdateAsync(string webhookUrl, string message)
        {
            try
            {
                var payload = new { text = message };
                var response = await _http.PostAsJsonAsync(webhookUrl, payload);
                response.EnsureSuccessStatusCode();
                _log.LogInformation("Webhook notification sent successfully.");
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Failed to send webhook notification.");
            }
        }

        public async Task NotifyDeadlineWeatherUpdateAsync(string webhookUrl, string message)
        {
            try
            {
                var payload = new { text = message };
                var response = await _http.PostAsJsonAsync(webhookUrl, payload);
                response.EnsureSuccessStatusCode();
                _log.LogInformation("Webhook notification sent successfully.");
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Failed to send webhook notification.");
            }
        }
        
        public async Task NotifyDeadlinePriceUpdateAsync(string webhookUrl, string message)
        {
            try
            {
                var payload = new { text = message };
                var response = await _http.PostAsJsonAsync(webhookUrl, payload);
                response.EnsureSuccessStatusCode();
                _log.LogInformation("Webhook notification sent successfully.");
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Failed to send webhook notification.");
            }
        }
    }

}