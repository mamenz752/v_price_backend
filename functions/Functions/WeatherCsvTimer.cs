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
using Grpc.Net.Client.Balancer;

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
                DateTime targetDate;
                if (nowJst.Day == 1)
                {
                    // 1日なら前月分を取得
                    targetDate = new DateTime(nowJst.Year, nowJst.Month, 1).AddMonths(-1);
                }
                else
                {
                    // 16日なら当月分を取得
                    targetDate = new DateTime(nowJst.Year, nowJst.Month, 16);
                }
                // TODO: いつもは以下をコメントアウト
                // var targetDate = new DateTime(nowJst.Year, nowJst.Month, nowJst.Day);

                var records = await FetchMonthlyWeatherAsync(targetDate, placeType, prefNo, blockNo, _log);

                if (records == null || records.Count == 0)
                {
                    _log.LogWarning("WeatherCsvTimer: No weather records fetched for {Year}/{Month}", targetDate.Year, targetDate.Month);
                }

                var (mid, last) = SplitFirstSecondHalf(records);

                // CSV テキストを作る
                var yearStr = targetDate.Year.ToString("D4");
                var monthStr = targetDate.Month.ToString("D2");

                // 基本9項目のCSVを作成
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

                // test: directory for local save
                var sharedRoot = Environment.GetEnvironmentVariable("SHARED_DATA_DIR") ?? "/shared";
                string subdir = Path.Combine(sharedRoot, prefix, yearStr, monthStr);
                Directory.CreateDirectory(subdir);

                if (nowJst.Month == targetDate.Month + 1)
                {
                    var fileNameLast = $"{yearStr}_{monthStr}_last.csv";
                    string localLastPath = Path.Combine(subdir, fileNameLast);
                    await File.WriteAllTextAsync(localLastPath, sbLast.ToString(), Encoding.UTF8);

                    string blobPathLast = Path.Combine(prefix, yearStr, monthStr, fileNameLast);
                    await BlobLogWriter.WriteTextAsync(connStr, container, blobPathLast, sbLast.ToString());
                }
                else
                {
                    var fileNameMid = $"{yearStr}_{monthStr}_mid.csv";
                    string localMidPath = Path.Combine(subdir, fileNameMid);
                    await File.WriteAllTextAsync(localMidPath, sbMid.ToString(), Encoding.UTF8);

                    string blobPathMid = Path.Combine(prefix, yearStr, monthStr, fileNameMid);
                    await BlobLogWriter.WriteTextAsync(connStr, container, blobPathMid, sbMid.ToString());
                }

                _log.LogInformation("WeatherCsvTimer: CSV files written");

                var notifier = new WebhookNotifier(_http, _log);
                await notifier.NotifyDeadlineUpdateAsync();
                _log.LogInformation("WeatherCsvTimer: Deadline weather update webhook notified.");
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "WeatherCsvTimer: failed");
            }
        }

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

        private static async Task<List<WeatherRecord>> FetchMonthlyWeatherAsync(DateTime date, string placeType, int prefNo, int blockNo, ILogger<WeatherCsvTimer> _log)
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

            _log.LogInformation("WeatherCsvTimer: Successfully fetched HTML from {Url}", url);
            // TODO: HTML解析ロジックをここに実装

            var table = doc.DocumentNode.SelectSingleNode("//table[contains(@class,'data2_s')]");
            if (table == null) return new List<WeatherRecord>();

            var parsed = ParseTableToGrid(table, headerRowCount: 4); // headerRowCount は調整可
            var headerKeys = parsed.HeaderKeys;
            var dataGrid = parsed.Rows; // List<string[]>

            // デバッグ: ヘッダをログ出力（解析確認用）
            for (int i = 0; i < headerKeys.Length; i++)
            {
                _log.LogInformation("Header[{Index}] = {Key}", i, headerKeys[i]);
            }

            int FindColumn(params string[] parts)
            {
                for (int i = 0; i < headerKeys.Length; i++)
                {
                    var key = headerKeys[i] ?? "";
                    bool ok = true;
                    foreach (var p in parts)
                    {
                        if (string.IsNullOrEmpty(p)) continue;
                        if (!key.Contains(p)) { ok = false; break; }
                    }
                    if (ok) return i;
                }
                return -1;
            }

            // ログ出力結果に基づいた正確なカラム検索
            int idxDay = 0;  // Header[0] = 日
            int idxAvgTemp = FindColumn("気温(℃)", "平均");  // Header[6] = 気温(℃)|平均
            int idxMaxTemp = FindColumn("最高");             // Header[7] = 最高
            int idxMinTemp = FindColumn("最低");             // Header[8] = 最低
            int idxPrecip = FindColumn("降水量(mm)", "合計"); // Header[3] = 降水量(mm)|合計
            int idxSunshine = FindColumn("日照時間(h)");     // Header[16] = 日照時間(h)
            int idxAvgHum = FindColumn("湿度(％)", "平均");   // Header[9] = 湿度(％)|平均

            var result = new List<WeatherRecord>();
            foreach (var cells in dataGrid)
            {
                // cells は string[] 長さはヘッダ幅
                if (idxDay < 0 || idxDay >= cells.Length) continue;
                var dayText = (cells[idxDay] ?? "").Trim();
                if (!int.TryParse(dayText, out var day)) continue;

                double ParseCell(int idx) =>
                    (idx >= 0 && idx < cells.Length) ? ToDouble(cells[idx]) : 0;

                var rec = new WeatherRecord
                {
                    Year = date.Year,
                    Month = date.Month,
                    Day = day,
                    AvgTemp = ParseCell(idxAvgTemp),
                    MaxTemp = ParseCell(idxMaxTemp),
                    MinTemp = ParseCell(idxMinTemp),
                    Precipitation = ParseCell(idxPrecip),
                    Sunshine = ParseCell(idxSunshine),
                    AvgHumidity = ParseCell(idxAvgHum)
                };

                result.Add(rec);
            }

            _log.LogInformation("FetchMonthlyWeatherAsync: parsed {Count} records for {Year}/{Month}", result.Count, date.Year, date.Month);
            return result;
        }
        
        private class TableParsedResult
        {
            public string[] HeaderKeys { get; set; }
            public List<string[]> Rows { get; set; }
        }

        private static TableParsedResult ParseTableToGrid(HtmlNode tableNode, int headerRowCount = 3)
        {
            var allRows = tableNode.SelectNodes(".//tr")?.ToList() ?? new List<HtmlNode>();
            var grid = new List<List<string>>();
            var occupied = new HashSet<(int r, int c)>();
            int maxCols = 0;

            for (int r = 0; r < allRows.Count; r++)
            {
                if (grid.Count <= r) grid.Add(new List<string>());
                var cells = allRows[r].SelectNodes("./th|./td")?.ToList() ?? new List<HtmlNode>();
                int c = 0;
                foreach (var cell in cells)
                {
                    while (occupied.Contains((r, c))) c++;
                    int rowspan = 1, colspan = 1;
                    if (cell.Attributes["rowspan"] != null) int.TryParse(cell.Attributes["rowspan"].Value, out rowspan);
                    if (cell.Attributes["colspan"] != null) int.TryParse(cell.Attributes["colspan"].Value, out colspan);

                    var txt = cell.InnerText.Replace("\r", " ").Replace("\n", " ").Replace("\t", " ").Trim();

                    for (int rr = r; rr < r + rowspan; rr++)
                    {
                        while (grid.Count <= rr) grid.Add(new List<string>());
                        for (int cc = c; cc < c + colspan; cc++)
                        {
                            while (grid[rr].Count <= cc) grid[rr].Add(null);
                            if (rr == r && cc == c) grid[rr][cc] = txt;
                            occupied.Add((rr, cc));
                        }
                    }

                    c += colspan;
                    if (c > maxCols) maxCols = c;
                }
                while (grid[r].Count < maxCols) grid[r].Add(null);
            }

            int useHeaderRows = Math.Min(headerRowCount, grid.Count);
            var headerKeys = new string[maxCols];
            for (int col = 0; col < maxCols; col++)
            {
                var parts = new List<string>();
                for (int hr = 0; hr < useHeaderRows; hr++)
                {
                    if (col < grid[hr].Count && !string.IsNullOrWhiteSpace(grid[hr][col]))
                        parts.Add(grid[hr][col]);
                }
                headerKeys[col] = string.Join("|", parts);
            }

            var dataRows = new List<string[]>();
            for (int r = useHeaderRows; r < grid.Count; r++)
            {
                var arr = new string[maxCols];
                for (int col = 0; col < maxCols; col++)
                {
                    arr[col] = (col < grid[r].Count) ? grid[r][col] ?? "" : "";
                }
                if (arr.All(s => string.IsNullOrEmpty(s))) continue;
                dataRows.Add(arr);
            }

            return new TableParsedResult { HeaderKeys = headerKeys, Rows = dataRows };
        }
    }
    }