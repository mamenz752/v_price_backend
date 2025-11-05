using System;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Functions
{
    public class TimerFunction
    {
        private readonly ILogger<TimerFunction> _logger;

        public TimerFunction(ILogger<TimerFunction> logger)
        {
            _logger = logger;
        }

        // 例：5分おき。必要に応じてCRONは変更してください（UTC）。
        [Function("TimerPulse")]
        public async Task RunAsync(
            [TimerTrigger("0 */5 * * * *", RunOnStartup = false, UseMonitor = true)] TimerInfo timerInfo)
        {
            var nowUtc = DateTimeOffset.UtcNow;
            _logger.LogInformation("TimerPulse fired at {Utc} (UTC). IsPastDue={IsPastDue}",
                nowUtc, timerInfo?.IsPastDue ?? false);

            var connStr = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            if (string.IsNullOrWhiteSpace(connStr))
            {
                _logger.LogWarning("AzureWebJobsStorage is empty. Abort counting.");
                return;
            }

            try
            {
                var service = new BlobServiceClient(connStr);

                long grandTxt = 0;
                long grandCsv = 0;
                long grandAll = 0;

                // すべてのコンテナを列挙
                await foreach (BlobContainerItem containerItem in service.GetBlobContainersAsync())
                {
                    var containerClient = service.GetBlobContainerClient(containerItem.Name);

                    long txtCount = 0;
                    long csvCount = 0;
                    long allCount = 0;

                    // ディレクトリ階層は仮想。flat 列挙で全件対象（再帰的に相当）
                    await foreach (BlobItem blob in containerClient.GetBlobsAsync(
                        traits: BlobTraits.None,
                        states: BlobStates.None,
                        prefix: null))
                    {
                        // BLOBのみ列挙される（仮想ディレクトリ自体は列挙されない）
                        var name = blob.Name;

                        // 大文字小文字を無視して拡張子判定
                        if (name.EndsWith(".txt", StringComparison.OrdinalIgnoreCase))
                            txtCount++;
                        else if (name.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
                            csvCount++;

                        allCount++;
                    }

                    grandTxt += txtCount;
                    grandCsv += csvCount;
                    grandAll += allCount;

                    _logger.LogInformation(
                        "[Container:{Container}] total={All}, txt={Txt}, csv={Csv}",
                        containerItem.Name, allCount, txtCount, csvCount);
                }

                _logger.LogInformation(
                    "=== Blob Storage summary (UTC:{Utc}) === total={All}, txt={Txt}, csv={Csv}",
                    nowUtc, grandAll, grandTxt, grandCsv);
            }
            catch (RequestFailedException ex)
            {
                _logger.LogError(ex, "Blob Storage request failed.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error while counting blobs.");
            }
        }
    }
}
