using System;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
// using Microsoft.Azure.Functions.Worker.Extensions.Timer;
using Microsoft.Extensions.Logging;
using HtmlAgilityPack;
using System.IO;
using System.Collections.Generic;
using System.Globalization;

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
        // [TimerTrigger("0 0 21 * * *", UseMonitor = true)] TimerInfo _)
        {
            var jst = TimeZoneInfo.FindSystemTimeZoneById("Asia/Tokyo");
            var nowJst = TimeZoneInfo.ConvertTime(DateTimeOffset.UtcNow, jst);

            var connStr = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            var container = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONTAINER") ?? "container";
            var prefix = Environment.GetEnvironmentVariable("CSV_PREFIX") ?? "weather";

            try
            {
                var placeType = "S";
                var prefNo = 67;
                var blockNo = 47765;
                // 月次データ取得対象日（タイマー起動時の年月を使う）
                var targetDate = new DateTime(nowJst.Year, nowJst.Month, 15);

                var records = await FetchMonthlyWeatherAsync(targetDate, placeType, prefNo, blockNo);

                if (records == null || records.Count == 0)
                {
                    _log.LogWarning("WeatherCsvTimer: No weather records fetched for {Year}/{Month}", targetDate.Year, targetDate.Month);
                }

                var (mid, last) = SplitFirstSecondHalf(records);

                // CSV テキストを作る
                var yearStr = targetDate.Year.ToString("D4");
                var monthStr = targetDate.Month.ToString("D2");

                var sbMid = new StringBuilder();
                sbMid.AppendLine("年,月,日,平均気温,最高気温,最低気温,降水量の合計,日照時間,平均湿度");
                foreach (var r in mid)
                {
                    sbMid.AppendLine(string.Join(",", r.Year, r.Month, r.Day, r.AvgTemp, r.MaxTemp, r.MinTemp, r.Precipitation, r.Sunshine, r.AvgHumidity));
                }

                var sbLast = new StringBuilder();
                sbLast.AppendLine("年,月,日,平均気温,最高気温,最低気温,降水量の合計,日照時間,平均湿度");
                foreach (var r in last)
                {
                    sbLast.AppendLine(string.Join(",", r.Year, r.Month, r.Day, r.AvgTemp, r.MaxTemp, r.MinTemp, r.Precipitation, r.Sunshine, r.AvgHumidity));
                }

                var sharedRoot = Environment.GetEnvironmentVariable("SHARED_DATA_DIR") ?? "/shared";
                string subdir = Path.Combine(sharedRoot, prefix, yearStr, monthStr);
                Directory.CreateDirectory(subdir);

                var fileNameMid = $"weather-{yearStr}-{monthStr}-mid.csv";
                var fileNameLast = $"weather-{yearStr}-{monthStr}-last.csv";

                string localMidPath = Path.Combine(subdir, fileNameMid);
                string localLastPath = Path.Combine(subdir, fileNameLast);

                await File.WriteAllTextAsync(localMidPath, sbMid.ToString(), Encoding.UTF8);
                await File.WriteAllTextAsync(localLastPath, sbLast.ToString(), Encoding.UTF8);

                // Blob にアップロード（パスは仮想ディレクトリを含む形で）
                string blobPathMid = Path.Combine(prefix, yearStr, monthStr, fileNameMid);
                string blobPathLast = Path.Combine(prefix, yearStr, monthStr, fileNameLast);

                await BlobLogWriter.WriteTextAsync(connStr, container, blobPathMid, sbMid.ToString());
                await BlobLogWriter.WriteTextAsync(connStr, container, blobPathLast, sbLast.ToString());

                _log.LogInformation("WeatherCsvTimer: CSV files written to {BlobMid} and {BlobLast}", blobPathMid, blobPathLast);

                var notifier = new WebhookNotifier(_http, _log);
                await notifier.NotifyDeadlineWeatherUpdateAsync();
                _log.LogInformation("WeatherCsvTimer: Deadline weather update webhook notified.");
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "WeatherCsvTimer: failed");
            }
        }


        //             var rows = doc.RootElement.GetProperty("results").EnumerateArray()
        //                 .Select((e, i) => new { idx = i + 1, name = e.GetProperty("name").GetString(), url = e.GetProperty("url").GetString() });

        //             var sb = new StringBuilder();
        //             sb.AppendLine("idx,name,url");
        //             foreach (var r in rows)
        //             {
        //                 sb.AppendLine($"{r.idx},{r.name},{r.url}");
        //             }

        //             var sharedRoot = Environment.GetEnvironmentVariable("SHARED_DATA_DIR") ?? "/shared";
        //             string subdir = Path.Combine(sharedRoot, prefix, nowJst.ToString("yyyy"), nowJst.ToString("MM"));
        //             Directory.CreateDirectory(subdir);

        //             var fileName = $"weather-{nowJst:yyyyMMdd-HHmmss}.csv";
        //             string blobPath = Path.Combine(subdir, fileName);
        //             string localSavePath = Path.Combine(sharedRoot, prefix, nowJst.ToString("yyyy"), nowJst.ToString("MM"), fileName);

        //             File.WriteAllLines(localSavePath, sb.ToString().Split(Environment.NewLine), Encoding.GetEncoding("utf-8"));

        //             await BlobLogWriter.WriteTextAsync(connStr, container, blobPath, sb.ToString());

        //             _log.LogInformation("WeatherCsvTimer: CSV file written to {BlobPath}", blobPath);

        //             var notifier = new WebhookNotifier(_http, _log);
        //             await notifier.NotifyDeadlineWeatherUpdateAsync();

        //             _log.LogInformation("WeatherCsvTimer: Deadline weather update webhook notified.");
        //         }
        // }
        // --- helper types & methods ---
        public class WeatherRecord
        {
            public int Year { get; set; }
            public int Month { get; set; }
            public int Day { get; set; }
            public double AvgTemp { get; set; }
            public double MaxTemp { get; set; }
            public double MinTemp { get; set; }
            public double Precipitation { get; set; }
            public double Sunshine { get; set; }
            public double AvgHumidity { get; set; }
        }

        internal static double ToDouble(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return 0;
            s = s.Replace(" ", "").Replace("\u3000", "");
            if (s == "×" || s == "--" || s == "///") return 0;
            if (double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var v)) return v;
            if (double.TryParse(s, NumberStyles.Any, CultureInfo.CurrentCulture, out v)) return v;
            return 0;
        }

        internal static (List<WeatherRecord> mid, List<WeatherRecord> last) SplitFirstSecondHalf(IEnumerable<WeatherRecord> all)
        {
            var list = all?.ToList() ?? new List<WeatherRecord>();
            var mid = list.Where(r => r.Day >= 1 && r.Day <= 15).ToList();
            var last = list.Where(r => r.Day >= 16).ToList();
            return (mid, last);
        }

        private static async Task<List<WeatherRecord>> FetchMonthlyWeatherAsync(DateTime date, string placeType, int prefNo, int blockNo)
        {
            var url =
                $"https://www.data.jma.go.jp/obd/stats/etrn/view/daily_{placeType.ToLower()}1.php" +
                $"?prec_no={prefNo}&block_no={blockNo:D4}&year={date.Year}&month={date.Month}&day=&view=";

            string html;
            try
            {
                html = await new HttpClient { Timeout = TimeSpan.FromSeconds(10) }.GetStringAsync(url);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching HTML: {ex.Message}");
                return new List<WeatherRecord>();
            }

            var doc = new HtmlAgilityPack.HtmlDocument();
            doc.LoadHtml(html);

            var table = doc.DocumentNode.SelectSingleNode("//table[contains(@class,'data2_s')]");
            if (table == null) return new List<WeatherRecord>();

            var rows = table.SelectNodes("./tr")?.ToList() ?? new List<HtmlAgilityPack.HtmlNode>();
            if (rows.Count == 0) return new List<WeatherRecord>();

            var headerRows = rows.Take(3).ToList();
            var dataRows = rows.Skip(3).ToList();

            var headerCells = headerRows
                .Select(r => r.SelectNodes("./th|./td")?.ToList() ?? new List<HtmlAgilityPack.HtmlNode>())
                .ToList();

            int colCount = headerCells.Any() ? headerCells.Max(r => r.Count) : 0;
            string[] headerKeys = new string[colCount];
            for (int c = 0; c < colCount; c++)
            {
                var parts = new List<string>();
                foreach (var hr in headerCells)
                {
                    if (c < hr.Count)
                    {
                        var text = hr[c].InnerText.Replace("\n", "").Replace("\t", "").Replace("\r", "").Trim();
                        if (!string.IsNullOrEmpty(text)) parts.Add(text);
                    }
                }
                headerKeys[c] = string.Join("|", parts);
            }

            int idxDay = Array.FindIndex(headerKeys, h => h.Contains("日"));
            int idxAvgTemp = Array.FindIndex(headerKeys, h => h.Contains("気温") && h.Contains("平均"));
            int idxMaxTemp = Array.FindIndex(headerKeys, h => h.Contains("気温") && h.Contains("最高"));
            int idxMinTemp = Array.FindIndex(headerKeys, h => h.Contains("気温") && h.Contains("最低"));
            int idxPrecip = Array.FindIndex(headerKeys, h => h.Contains("降水量") && (h.Contains("合計") || h.Contains("総和")));
            int idxSunshine = Array.FindIndex(headerKeys, h => h.Contains("日照") && (h.Contains("時間") || h.Contains("日照時間")));
            int idxAvgHum = Array.FindIndex(headerKeys, h => h.Contains("湿度") && h.Contains("平均"));

            var result = new List<WeatherRecord>();
            foreach (var row in dataRows)
            {
                var tds = row.SelectNodes("./td")?.ToList();
                if (tds == null || tds.Count == 0) continue;
                if (idxDay < 0 || idxDay >= tds.Count) continue;

                var dayText = tds[idxDay].InnerText.Trim();
                if (!int.TryParse(dayText, out var day)) continue;

                var rec = new WeatherRecord
                {
                    Year = date.Year,
                    Month = date.Month,
                    Day = day,
                    AvgTemp = (idxAvgTemp >= 0 && idxAvgTemp < tds.Count) ? ToDouble(tds[idxAvgTemp].InnerText) : 0,
                    MaxTemp = (idxMaxTemp >= 0 && idxMaxTemp < tds.Count) ? ToDouble(tds[idxMaxTemp].InnerText) : 0,
                    MinTemp = (idxMinTemp >= 0 && idxMinTemp < tds.Count) ? ToDouble(tds[idxMinTemp].InnerText) : 0,
                    Precipitation = (idxPrecip >= 0 && idxPrecip < tds.Count) ? ToDouble(tds[idxPrecip].InnerText) : 0,
                    Sunshine = (idxSunshine >= 0 && idxSunshine < tds.Count) ? ToDouble(tds[idxSunshine].InnerText) : 0,
                    AvgHumidity = (idxAvgHum >= 0 && idxAvgHum < tds.Count) ? ToDouble(tds[idxAvgHum].InnerText) : 0
                };

                result.Add(rec);
            }

            return result;
        }
    }
    }