# Backend Data Directory

This directory contains data files used by the backend services.

## Files

### delhi_ncr_zones.json

This file contains the geographical zone data for the Delhi NCR region. It is used by the route generation service to assign employees to zones based on their location.

The file is a GeoJSON FeatureCollection containing polygon features that represent different zones in the Delhi NCR region. Each feature has the following properties:

- `Name`: The name of the zone
- `fill`: The color used to display the zone on the map

The geometry of each feature is a polygon defined by an array of coordinates.

## Usage

The route generation service automatically loads this file when zones data is not provided in the API request. This allows the frontend to optionally provide zones data, falling back to this file when not provided.

## Updating

To update the zones data, replace this file with a new GeoJSON file containing the updated zone information. The format should match the existing file structure.