function updateChart(flights) {
    let ctx = document.getElementById('priceChart').getContext('2d');

    let destinations = flights.map(flight => flight.destinationAirport);
    let prices = flights.map(flight => flight.totalFare);

    if (window.flightChart) {
        window.flightChart.destroy();
    }

    window.flightChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: destinations,
            datasets: [{
                label: 'Flight Prices',
                data: prices,
                backgroundColor: 'rgba(0, 123, 255, 0.6)'
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}
