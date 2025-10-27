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

const MAX_DAYS = 15;

function renderTwoWeekComboChartConfig(twoWeekData) {
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
    const chartLabels = labels.slice();
    const volumeData = volumes.slice();
    const priceData = prices.slice();
    
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
