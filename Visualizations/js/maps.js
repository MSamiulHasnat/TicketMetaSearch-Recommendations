// Initialize the map with a standard and reliable tile layer
let map = L.map('map', {
    zoomControl: false // We'll add it in a better position
}).setView([20, 0], 3);

// Use the standard OpenStreetMap tile layer which is more reliable
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    maxZoom: 19
}).addTo(map);

// Add zoom control to the bottom right
L.control.zoom({
    position: 'bottomright'
}).addTo(map);

// Airport coordinates with enhanced data
const airportCoordinates = {
    "BOS": [42.3656, -71.0096, 19, "Boston Logan International", "USA"], 
    "LGA": [40.7769, -73.8740, 21, "LaGuardia Airport", "USA"], 
    "JFK": [40.6413, -73.7781, 13, "John F. Kennedy International", "USA"], 
    "ORD": [41.9742, -87.9073, 20, "Chicago O'Hare International", "USA"], 
    "MIA": [25.7959, -80.2870, 10, "Miami International", "USA"], 
    "SFO": [37.7749, -122.4194, 17, "San Francisco International", "USA"], 
    "LAX": [33.9416, -118.4085, 30, "Los Angeles International", "USA"], 
    "DFW": [32.8998, -97.0403, 22, "Dallas Fort Worth International", "USA"], 
    "ATL": [33.6407, -84.4277, 31, "Atlanta Hartsfield-Jackson", "USA"], 
    "DEN": [39.8561, -104.6737, 23, "Denver International", "USA"]
};

// Add custom CSS for the markers
const style = document.createElement('style');
style.textContent = `
    .airport-marker {
        background: white;
        border-radius: 50%;
        display: flex;
        justify-content: center;
        align-items: center;
        width: 36px;
        height: 36px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.2);
    }
    .departure-marker i {
        color: #2b5876;
        font-size: 18px;
    }
    .arrival-marker i {
        color: #4e4376;
        font-size: 18px;
    }
    .flight-path {
        stroke-dasharray: 12, 8;
        animation: dash 30s linear infinite;
    }
    @keyframes dash {
        to {
            stroke-dashoffset: -1000; /* Negative value reverses animation direction */
        }
    }
    .airport-label {
        background: rgba(255, 255, 255, 0.9);
        border: none;
        border-radius: 4px;
        padding: 4px 8px;
        font-size: 12px;
        font-weight: bold;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.2);
    }
`;
document.head.appendChild(style);

// Create custom icons for departure and arrival
const departureIcon = L.divIcon({
    html: '<div class="airport-marker departure-marker"><i class="fas fa-plane-departure"></i></div>',
    className: '',
    iconSize: [40, 40],
    iconAnchor: [20, 20],
    popupAnchor: [0, -20]
});

const arrivalIcon = L.divIcon({
    html: '<div class="airport-marker arrival-marker"><i class="fas fa-plane-arrival"></i></div>',
    className: '',
    iconSize: [40, 40],
    iconAnchor: [20, 20],
    popupAnchor: [0, -20]
});

// Function to calculate distance between two points (haversine formula)
function calculateDistance(lat1, lon1, lat2, lon2) {
    const R = 6371; // Radius of Earth in km
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = 
        Math.sin(dLat/2) * Math.sin(dLat/2) +
        Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * 
        Math.sin(dLon/2) * Math.sin(dLon/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c;
}

// Function to calculate flight time based on distance (very rough estimation)
function calculateFlightTime(distance) {
    // Assuming average speed of 800 km/h plus 30 min for takeoff and landing
    const hours = distance / 800 + 0.5;
    const hrs = Math.floor(hours);
    const mins = Math.round((hours - hrs) * 60);
    return `${hrs}h ${mins}m`;
}

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
    // Clear existing layers
    map.eachLayer(layer => {
        if (layer instanceof L.Marker || layer instanceof L.Polyline || layer instanceof L.Tooltip) {
            map.removeLayer(layer);
        }
    });

    // Make sure the base tile layer is still there
    if (map.hasLayer(L.tileLayer) === false) {
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }).addTo(map);
    }

    let [depLat, depLng, depAlt, depName, depCountry] = departureCoords;
    let [destLat, destLng, destAlt, destName, destCountry] = destCoords;

    // Add departure marker with enhanced popup
    const departureMarker = L.marker([depLat, depLng], { icon: departureIcon })
        .addTo(map)
        .bindPopup(`
            <div style="text-align: center;">
                <h6 style="margin-bottom: 5px; color: #2b5876; font-weight: bold;">${departureAirport}</h6>
                <p style="margin: 0; font-size: 12px;">${depName}</p>
                <p style="margin: 0; font-size: 12px;">Altitude: ${depAlt}m</p>
            </div>
        `);

    // Add destination marker with enhanced popup
    const destinationMarker = L.marker([destLat, destLng], { icon: arrivalIcon })
        .addTo(map)
        .bindPopup(`
            <div style="text-align: center;">
                <h6 style="margin-bottom: 5px; color: #4e4376; font-weight: bold;">${destinationAirport}</h6>
                <p style="margin: 0; font-size: 12px;">${destName}</p>
                <p style="margin: 0; font-size: 12px;">Altitude: ${destAlt}m</p>
            </div>
        `);

    // Add permanent labels for airports
    L.marker([depLat, depLng], {
        icon: L.divIcon({
            html: `<div class="airport-label">${departureAirport} - ${depName}</div>`,
            className: '',
            iconSize: [120, 20],
            iconAnchor: [60, -15]
        })
    }).addTo(map);

    L.marker([destLat, destLng], {
        icon: L.divIcon({
            html: `<div class="airport-label">${destinationAirport} - ${destName}</div>`,
            className: '',
            iconSize: [120, 20],
            iconAnchor: [60, -15]
        })
    }).addTo(map);

    // Calculate the great circle path points (for a curved path)
    const latlngs = [];
    const segmentCount = 100;
    for (let i = 0; i <= segmentCount; i++) {
        const fraction = i / segmentCount;
        const lat = depLat + fraction * (destLat - depLat);
        const lng = depLng + fraction * (destLng - depLng);
        // Add a slight arc effect based on distance
        const distance = calculateDistance(depLat, depLng, destLat, destLng);
        let arcHeight = distance / 50; // Adjust divisor to control arc height
        arcHeight = Math.min(arcHeight, 5); // Cap maximum arc height
        
        // Simple arc calculation - add height in the middle, taper to ends
        const heightFactor = Math.sin(Math.PI * fraction);
        const latOffset = arcHeight * heightFactor * (Math.abs(destLng - depLng) / 180);
        
        latlngs.push([lat + latOffset, lng]);
    }

    // Draw curved route between departure and destination with animation
    const flightPath = L.polyline(latlngs, { 
        color: 'rgba(46, 64, 170, 0.8)',
        weight: 3,
        opacity: 0.8,
        className: 'flight-path'
    }).addTo(map);

    // Calculate the flight details
    const distance = calculateDistance(depLat, depLng, destLat, destLng);
    const flightTime = calculateFlightTime(distance);

    // Update flight details section
    document.getElementById('flight-details').innerHTML = `
        <div class="row text-center py-2">
            <div class="col-md-4">
                <small class="text-muted">Distance</small>
                <h5>${Math.round(distance)} km</h5>
            </div>
            <div class="col-md-4">
                <small class="text-muted">Est. Flight Time</small>
                <h5>${flightTime}</h5>
            </div>
            <div class="col-md-4">
                <small class="text-muted">Route</small>
                <h5>${departureAirport} → ${destinationAirport}</h5>
            </div>
        </div>
    `;

    // Fit the map to show both markers
    const bounds = L.latLngBounds([
        [depLat, depLng],
        [destLat, destLng]
    ]);
    map.fitBounds(bounds, { padding: [50, 50] });
}

// Initialize a blank map when the page loads
document.addEventListener('DOMContentLoaded', function() {
    // Make sure map renders correctly by triggering a resize event
    setTimeout(function() {
        window.dispatchEvent(new Event('resize'));
    }, 500);
});