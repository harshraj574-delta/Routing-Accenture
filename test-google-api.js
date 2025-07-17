require('dotenv').config(); // if your file is CommonJS

async function testGoogleDirections() {
  const apiKey = process.env.GOOGLE_MAPS_API_KEY;
  console.log('Using Google Maps API Key:', apiKey);
  const url = `https://maps.googleapis.com/maps/api/directions/json?origin=28.7041,77.1025&destination=28.7141,77.1125&key=${apiKey}&departure_time=now&traffic_model=best_guess`;

  console.log('Testing Google Directions API...');
  console.log('URL:', url.replace(apiKey, 'HIDDEN_KEY'));

  try {
    const response = await fetch(url);
    const data = await response.json();

    console.log('Status:', response.status);
    console.log('Response:', JSON.stringify(data, null, 2));

    if (data.status === 'OK') {
      console.log('✅ Google API is working!');
    } else {
      console.log('❌ Google API error:', data.status, data.error_message);
    }
  } catch (error) {
    console.error('❌ Request failed:', error.message);
  }
}

testGoogleDirections();
