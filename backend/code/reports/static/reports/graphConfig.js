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
                    type: 'bar',
                    label: '入荷量 (t)',
                    data: volumeData,
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    yAxisID: 'y_volume',
                    barPercentage: 0.6,
                    order: 1,
                },
                {
                    type: 'line',
                    label: '価格（円/kg）',
                    data: priceData,
                    borderColor: 'rgb(255, 99, 132)',
                    backgroundColor: 'rgba(255,99,132,0.1)',
                    tension: 0.2,
                    pointRadius: 3,
                    yAxisID: 'y_right',
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

// TODO: データつなぎこみ
function renderPredictPriceGraphConfig(predictData) {
    if (typeof Chart === 'undefined') {
        console.warn('[predictPriceGraph] Chart.js not loaded.');
        return null;
    }

    console.log('Predict Data:', predictData);

    const labels = [];
    const currentSeasonPrices = [];
    const lastSeasonPrices = [];
    const fiveYearAvgPrices = [];

    if (Array.isArray(predictData)) {
        predictData.forEach(item => {
            labels.push(item.target_date || '');
            currentSeasonPrices.push(item.current_season_price != null ? item.current_season_price : null);
            lastSeasonPrices.push(item.last_season_price != null ? item.last_season_price : null);
            fiveYearAvgPrices.push(item.five_year_avg_price != null ? item.five_year_avg_price : null);
        });
    }

    // データをそのまま使用（既にソート済み）
    const chartLabels = labels;

    console.log('Processed Data:', {
        labels: chartLabels,
        currentSeason: currentSeasonPrices,
        lastSeason: lastSeasonPrices,
        fiveYearAvg: fiveYearAvgPrices
    });

    const cfg = {
        data: {
            labels: chartLabels,
            datasets: [
                {
                    type: 'line',
                    label: '今期価格（円/kg）',
                    data: currentSeasonPrices,
                    borderColor: 'rgb(255, 99, 132)',
                    backgroundColor: 'rgba(255, 99, 132, 0.1)',
                    yAxisID: 'y_right',
                    order: 1,
                    spanGaps: true,
                    tension: 0.2
                },
                {
                    type: 'line',
                    label: '前年同期（円/kg）',
                    data: lastSeasonPrices,
                    borderColor: 'rgb(54, 162, 235)',
                    backgroundColor: 'rgba(54, 162, 235, 0.1)',
                    yAxisID: 'y_right',
                    order: 2,
                    spanGaps: true,
                    tension: 0.2
                },
                {
                    type: 'line',
                    label: '過去5年平均（円/kg）',
                    data: fiveYearAvgPrices,
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.1)',
                    yAxisID: 'y_right',
                    order: 3,
                    borderDash: [5, 5],
                    spanGaps: true,
                    tension: 0.2
                }
            ]
        },
        options: {
            // responsive: true,
            // maintainAspectRatio: false,
            scales: {
                x: { title: { display: true, text: '日付' } },
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


function renderSeasonPriceGraphConfig(seasonData) {
    if (typeof Chart === 'undefined') {
        console.warn('[seasonPriceGraph] Chart.js not loaded.');
        return null;
    }

    console.log('Season Data:', seasonData);

    const labels = [];
    const currentSeasonPrices = [];
    const lastSeasonPrices = [];
    const fiveYearAvgPrices = [];

    if (Array.isArray(seasonData)) {
        seasonData.forEach(item => {
            labels.push(item.target_date || '');
            currentSeasonPrices.push(item.current_season_price != null ? item.current_season_price : null);
            lastSeasonPrices.push(item.last_season_price != null ? item.last_season_price : null);
            fiveYearAvgPrices.push(item.five_year_avg_price != null ? item.five_year_avg_price : null);
        });
    }

    // データをそのまま使用（既にソート済み）
    const chartLabels = labels;

    console.log('Processed Data:', {
        labels: chartLabels,
        currentSeason: currentSeasonPrices,
        lastSeason: lastSeasonPrices,
        fiveYearAvg: fiveYearAvgPrices
    });

    const cfg = {
        data: {
            labels: chartLabels,
            datasets: [
                {
                    type: 'line',
                    label: '今期価格（円/kg）',
                    data: currentSeasonPrices,
                    borderColor: 'rgb(255, 99, 132)',
                    backgroundColor: 'rgba(255, 99, 132, 0.1)',
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
                    borderColor: 'rgb(54, 162, 235)',
                    backgroundColor: 'rgba(54, 162, 235, 0.1)',
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
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.1)',
                    yAxisID: 'y_right',
                    order: 3,
                    borderDash: [5, 5],
                    spanGaps: true,
                    tension: 0.2,
                    pointRadius: 0,
                }
            ]
        },
        options: {
            scales: {
                x: { title: { display: true, text: '日付' } },
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

