/*
 Simple recentlyPriceGraph.js

 Exports:
    - renderRecentlyPriceGraph(canvasId, marketsData)

 Usage (template):
 1) include Chart.js (CDN or local)
 2) set a JS variable with server JSON, e.g.
            <script>window.recentMarketsData = {{ calc_markets_json|safe }};</script>
 3) include this script via {% static %}
 4) call renderRecentlyPriceGraph('price', window.recentMarketsData)

 This file is intentionally small and dependency-free except for Chart.js.
*/

function renderRecentlyPriceGraphConfig(twoWeekData) {
    if (typeof Chart === 'undefined') {
        console.warn('[recentlyPriceGraph] Chart.js not loaded.');
        return null;
    }

    const labels = [];
    const volumes = [];
    const prices = [];

    if (Array.isArray(twoWeekData)) {
        twoWeekData.forEach(item => {
            labels.push(item.target_date || '');
            volumes.push(item.volume != null ? item.volume : null);
            prices.push(item.source_price != null ? item.source_price : null);
        });
    }

    // Ensure chronological order (oldest-first)
    const chartLabels = labels.slice().reverse();
    const volumeData = volumes.slice().reverse();
    const priceData = prices.slice().reverse();

    const cfg = {
        data: {
            labels: chartLabels,
            datasets: [
                {
                    type: 'line',
                    label: '価格（円/kg）',
                    data: priceData,
                    borderColor: '#DEF164',
                    backgroundColor: '#FAFAFA',
                    tension: 0.2,
                    pointRadius: 3,
                    yAxisID: 'y_right',
                    order: 1
                },
                {
                    type: 'bar',
                    label: '入荷量 (t)',
                    data: volumeData,
                    backgroundColor: '#3F8A31AA',
                    yAxisID: 'y_volume',
                    barPercentage: 0.6,
                    order: 2
                }
            ]
        },
        options: {
            // responsive: true,
            // maintainAspectRatio: false,
            scales: {
                x: { title: { display: true, text: '日付' } },
                y_volume: {
                    type: 'linear',
                    position: 'left',
                    title: { display: true, text: '入荷量 (t)' }
                },
                y_right: {
                    type: 'linear',
                    position: 'right',
                    title: { display: true, text: '価格（円/kg）' },
                    grid: { drawOnChartArea: false }
                }
            },
            elements: {
                line: { 
                    lineTension: 0
                }
            },
            plugins: { legend: { position: 'top' } }
        }
    };
    
    return cfg;
}

// 過去2カ月の実際データと9カ月の予測データを表示
function renderPredictPriceGraphConfig(combinedData) {
    if (typeof Chart === 'undefined') {
        console.warn('[predictPriceGraph] Chart.js not loaded.');
        return null;
    }

    const labels = [];
    const allPrices = [];  // 実際価格と予測価格を統合
    const minPrices = [];
    const maxPrices = [];

    if (Array.isArray(combinedData)) {
        combinedData.forEach(item => {
            // ラベルは期間名を使用（例：2025年5月前半）
            labels.push(item.period || '');
            
            if (item.data_type === 'historical') {
                // 過去データ
                allPrices.push(item.actual_price != null ? item.actual_price : null);
                minPrices.push(null);
                maxPrices.push(null);
            } else if (item.data_type === 'prediction') {
                // 予測データ
                allPrices.push(item.prediction_value != null ? item.prediction_value : null);
                minPrices.push(item.min_price != null ? item.min_price : null);
                maxPrices.push(item.max_price != null ? item.max_price : null);
            }
        });
    }

    const cfg = {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: '価格（実際・予測）',
                    data: allPrices,
                    borderColor: '#3F8A31',
                    backgroundColor: 'rgba(63, 138, 49, 0.1)',
                    pointRadius: 5,
                    pointHoverRadius: 6,
                    borderWidth: 4,
                    spanGaps: false
                },
                {
                    label: '予測最安値',
                    data: minPrices,
                    borderColor: '#B7D430',
                    backgroundColor: 'rgba(183, 212, 48, 0.1)',
                    borderDash: [5, 5],
                    pointRadius: 3,
                    spanGaps: false
                },
                {
                    label: '予測最高値',
                    data: maxPrices,
                    borderColor: '#DEF164',
                    backgroundColor: 'rgba(222, 241, 100, 0.1)',
                    borderDash: [5, 5],
                    pointRadius: 3,
                    spanGaps: false
                }
            ]
        },
        options: {
            // responsive: true,
            // maintainAspectRatio: false,
            scales: {
                x: {
                    title: { 
                        display: true, 
                        text: '期間' 
                    },
                    ticks: {
                        maxRotation: 45,
                        minRotation: 0
                    }
                },
                y: {
                    title: { 
                        display: true, 
                        text: '価格（円）' 
                    },
                    grid: { 
                        drawOnChartArea: true 
                    }
                }
            },
            elements: {
                line: { 
                    tension: 0
                }
            },
            plugins: { 
                legend: { 
                    position: 'top',
                    display: true
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        title: function(context) {
                            return context[0].label;
                        },
                        label: function(context) {
                            if (context.parsed.y !== null) {
                                return `${context.dataset.label}: ${context.parsed.y.toLocaleString()}円`;
                            }
                            return null;
                        }
                    }
                }
            },
        }
    };
    
    return cfg;
}


function renderSeasonPriceGraphConfig(seasonData) {
    if (typeof Chart === 'undefined') {
        console.warn('[seasonPriceGraph] Chart.js not loaded.');
        return null;
    }

    const labels = [];
    const currentSeasonPrices = [];
    const lastSeasonPrices = [];
    const fiveYearAvgPrices = [];

    if (Array.isArray(seasonData)) {
        // 最新のデータの日付を取得
        const latestDate = new Date(Math.max(...seasonData.map(item => new Date(item.target_date))));
        // 3ヶ月前の日付を計算
        const threeMonthsAgo = new Date(latestDate);
        threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);

        // 直近3ヶ月のデータのみをフィルタリング
        const filteredData = seasonData.filter(item => {
            const itemDate = new Date(item.target_date);
            return itemDate >= threeMonthsAgo && itemDate <= latestDate;
        });

        // フィルタリングしたデータを使用
        filteredData.forEach(item => {
            labels.push(item.target_date || '');
            currentSeasonPrices.push(item.current_season_price != null ? item.current_season_price : null);
            lastSeasonPrices.push(item.last_season_price != null ? item.last_season_price : null);
            fiveYearAvgPrices.push(item.five_year_avg_price != null ? item.five_year_avg_price : null);
        });
    }

    // データをそのまま使用（既にソート済み）
    const chartLabels = labels;

    const cfg = {
        data: {
            labels: chartLabels,
            datasets: [
                {
                    type: 'line',
                    label: '今期価格（円/kg）',
                    data: currentSeasonPrices,
                    borderColor: '#3F8A31',
                    yAxisID: 'y_right',
                    order: 1,
                    spanGaps: true,
                    tension: 0.2,
                    pointRadius: 0,
                },
                {
                    type: 'line',
                    label: '前年同期（円/kg）',
                    data: lastSeasonPrices,
                    borderColor: '#B7D430',
                    yAxisID: 'y_right',
                    order: 2,
                    spanGaps: true,
                    tension: 0.2,
                    pointRadius: 0,
                },
                {
                    type: 'line',
                    label: '過去5年平均（円/kg）',
                    data: fiveYearAvgPrices,
                    borderColor: '#888888',
                    yAxisID: 'y_right',
                    order: 3,
                    spanGaps: true,
                    tension: 0.2,
                    pointRadius: 0,
                }
            ]
        },
        options: {
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'day',
                        displayFormats: {
                            day: 'M/d'
                        }
                    },
                    title: { display: true, text: '日付' },
                    ticks: {
                        source: 'auto',
                        autoSkip: true,
                        maxTicksLimit: 12, // およそ1週間おきに目盛りを表示
                        maxRotation: 45
                    }
                },
                y_right: {
                    type: 'linear',
                    position: 'left',
                    title: { display: true, text: '価格（円/kg）' },
                    grid: { drawOnChartArea: false }
                }
            },
            elements: {
                line: { 
                    lineTension: 0
                }
            },
            plugins: { legend: { position: 'top' } }
        }
    };
    
    return cfg;
}

function renderYearPriceGraphConfig(yearData) {
    if (typeof Chart === 'undefined') {
        console.warn('[yearPriceGraph] Chart.js not loaded.');
        return null;
    }

    const labels = [];
    const currentSeasonPrices = [];
    const lastSeasonPrices = [];
    const fiveYearAvgPrices = [];

    if (Array.isArray(yearData)) {
        yearData.forEach(item => {
            labels.push(item.target_date || '');
            currentSeasonPrices.push(item.current_season_price != null ? item.current_season_price : null);
            lastSeasonPrices.push(item.last_season_price != null ? item.last_season_price : null);
            fiveYearAvgPrices.push(item.five_year_avg_price != null ? item.five_year_avg_price : null);
        });
    }

    // データをそのまま使用（既にソート済み）
    const chartLabels = labels;

    const cfg = {
        data: {
            labels: chartLabels,
            datasets: [
                {
                    type: 'line',
                    label: '今期価格（円/kg）',
                    data: currentSeasonPrices,
                    borderColor: '#3F8A31',
                    yAxisID: 'y_right',
                    order: 1,
                    spanGaps: true,
                    tension: 0.2,
                    pointRadius: 0,
                },
                {
                    type: 'line',
                    label: '前年同期（円/kg）',
                    data: lastSeasonPrices,
                    borderColor: '#B7D430',
                    yAxisID: 'y_right',
                    order: 2,
                    spanGaps: true,
                    tension: 0.2,
                    pointRadius: 0,
                },
                {
                    type: 'line',
                    label: '過去5年平均（円/kg）',
                    data: fiveYearAvgPrices,
                    borderColor: '#888888',
                    yAxisID: 'y_right',
                    order: 3,
                    spanGaps: true,
                    tension: 0.2,
                    pointRadius: 0,
                }
            ]
        },
        options: {
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'month',
                        displayFormats: {
                            month: 'y/M/d'
                        }
                    },
                    title: { display: true, text: '日付' },
                    ticks: {
                        source: 'auto',
                        autoSkip: false,
                        maxRotation: 45
                    }
                },
                y_right: {
                    type: 'linear',
                    position: 'left',
                    title: { display: true, text: '価格（円/kg）' },
                    grid: { drawOnChartArea: false }
                }
            },
            elements: {
                line: { 
                    lineTension: 0
                }
            },
            plugins: { legend: { position: 'top' } }
        }
    };
    
    return cfg;
}

