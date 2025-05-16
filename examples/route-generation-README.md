# Route Generation API

## Overview
This API allows you to generate routes based on employee, facility, shift time, date, and profile data provided in the request body. The route generation works independently from the database, making it suitable for integration with other systems.

## API Endpoint
```
POST /api/route-generation/generate
```

## Request Body Structure
The request body should be a JSON object with the following structure:

```json
{
  "employees": [Array of employee objects],
  "facility": {Facility object},
  "shiftTime": "Shift time value",
  "date": "YYYY-MM-DD",
  "profile": {Profile object with routing preferences},
  "saveToDatabase": boolean (optional, defaults to false)
}
```

### Required Fields
- `employees`: Array of employee objects with location data
- `facility`: Object containing facility information
- `shiftTime`: String representing the shift time
- `date`: Date string in YYYY-MM-DD format
- `profile`: Object containing routing profile preferences

### Optional Fields
- `saveToDatabase`: Boolean flag to determine if routes should be saved to database (default: false)

## Example Request
A sample request JSON is provided in the `route-generation-sample.json` file in this directory.

## Response
The API will return a JSON object containing the generated routes with the following structure:

```json
{
  "uuid": "Generated UUID for the route batch",
  "date": "YYYY-MM-DD",
  "shift": "Shift time value",
  "tripType": "PICKUP or DROPOFF",
  "facility": {Facility object},
  "profile": {Profile object},
  "employeeData": [Array of employee objects],
  "totalEmployees": Number of employees,
  "totalRoutes": Number of routes generated,
  "averageOccupancy": Average number of employees per route,
  "routeDetails": {
    "totalDistance": Total distance in meters,
    "totalDuration": Total duration in seconds
  },
  "routeData": [
    {
      "legIndex": Route leg index,
      "encodedPolyline": "Encoded polyline string",
      "distance": Distance in meters,
      "duration": Duration in seconds,
      "employees": [Array of employee objects for this route]
    },
    ...
  ]
}
```

## Usage Example

```javascript
// Example using fetch API
const response = await fetch('http://your-api-url/api/route-generation/generate', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    employees: [...],  // Array of employee objects
    facility: {...},   // Facility object
    shiftTime: "1",    // Shift time
    date: "2023-07-15", // Date
    profile: {...},    // Profile object
    saveToDatabase: false // Don't save to database
  })
});

const generatedRoutes = await response.json();
```

## Notes
- The current implementation uses a simplified route generation algorithm. In a production environment, you would replace this with your actual routing algorithm.
- When `saveToDatabase` is set to `true`, the generated routes will be saved to the database and can be retrieved later using the regular route endpoints.
- When `saveToDatabase` is set to `false`, the routes are generated on-the-fly and returned in the response but not saved to the database.