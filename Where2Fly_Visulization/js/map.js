// Set initial map view
let map = L.map('map').setView([20, 0], 3); // Set global view

// Tile layer for map
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

// Airport coordinates (static data)
const airportCoordinates = {
    "BOS": [42.3656, -71.0096, 19], // Boston Logan
    "LGA": [40.7769, -73.8740, 21], // LaGuardia
    "JFK": [40.6413, -73.7781, 13], // John F. Kennedy
    "ORD": [41.9742, -87.9073, 20], // Chicago O'Hare
    "MIA": [25.7959, -80.2870, 10], // Miami International
    "SFO": [37.7749, -122.4194, 17], // San Francisco
    "LAX": [33.9416, -118.4085, 30], // Los Angeles
    "DFW": [32.8998, -97.0403, 22], // Dallas Fort Worth
    "ATL": [33.6407, -84.4277, 31], // Atlanta Hartsfield
    "DEN": [39.8561, -104.6737, 23] // Denver International
};

// Custom markers (Blue for departure, Red for destination)
let blueIcon = new L.Icon({
    iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-blue.png',
    shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
    iconSize: [25, 41],
    iconAnchor: [12, 41],
    popupAnchor: [1, -34],
    shadowSize: [41, 41]
});

let redIcon = new L.Icon({
    iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-red.png',
    shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
    iconSize: [25, 41],
    iconAnchor: [12, 41],
    popupAnchor: [1, -34],
    shadowSize: [41, 41]
});

// Function to search and display flights on the map
function searchFlights() {
    // Get user input for departure and destination airports
    let departureAirport = document.getElementById('departure').value.toUpperCase();
    let destinationAirport = document.getElementById('destination').value.toUpperCase();

    // Ensure both airports are provided
    if (!departureAirport || !destinationAirport) {
        alert('Please enter both departure and destination airports.');
        return;
    }

    // Get coordinates for the departure and destination airports
    let departureCoords = airportCoordinates[departureAirport];
    let destCoords = airportCoordinates[destinationAirport];

    // If any of the airport coordinates are missing, show an error
    if (!departureCoords || !destCoords) {
        alert('Invalid airport code(s) entered. Please try again.');
        return;
    }

    // Update the map with the markers and route
    updateMap(departureCoords, destCoords, departureAirport, destinationAirport);
}

// Function to update the map with markers and route
function updateMap(departureCoords, destCoords, departureAirport, destinationAirport) {
    // Remove existing markers and routes before updating
    map.eachLayer(layer => {
        if (layer instanceof L.Marker || layer instanceof L.Polyline) {
            map.removeLayer(layer);
        }
    });

    let [depLat, depLng, depAlt] = departureCoords;
    let [destLat, destLng, destAlt] = destCoords;

    // Add departure marker
    L.marker([depLat, depLng], { icon: blueIcon })
        .addTo(map)
        .bindPopup(`<b>Departure: ${departureAirport}</b><br>
                    Altitude: ${depAlt}m`)
        .openPopup();

    // Add destination marker
    L.marker([destLat, destLng], { icon: redIcon })
        .addTo(map)
        .bindPopup(`<b>Destination: ${destinationAirport}</b><br>
                    Altitude: ${destAlt}m`)
        .openPopup();

    // Draw route between departure and destination
    L.polyline([[depLat, depLng], [destLat, destLng]], { color: 'blue' }).addTo(map);

    // Adjust map view to fit both markers
    let bounds = new L.LatLngBounds([departureCoords, destCoords]);
    map.fitBounds(bounds);
}
