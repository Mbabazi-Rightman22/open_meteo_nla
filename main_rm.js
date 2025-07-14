// simplified-weather-to-postgres.js
const { fetchWeatherApi } = require('openmeteo');
const { Client } = require('pg');
const fs = require('fs');
const Papa = require('papaparse');

// PostgreSQL connection config
const pgConfig = {
  host: 'localhost',
  database: 'disa_db',
  user: 'sde',
  password: 'RLP@2025!',
  port: 5432,
  options: '-c search_path=sde'
};

async function setupDatabase() {
  const client = new Client(pgConfig);
  await client.connect();
  
  // Create locations table from CSV data
  await client.query(`
    CREATE TABLE IF NOT EXISTS sde.weather_locations (
      location_id INTEGER PRIMARY KEY,
      latitude FLOAT NOT NULL,
      longitude FLOAT NOT NULL
    );
  `);
  
  // Create weather tables
  await client.query(`
    CREATE TABLE IF NOT EXISTS sde.current_weather (
      id SERIAL PRIMARY KEY,
      location_id INTEGER REFERENCES sde.weather_locations(location_id),
      time TIMESTAMP WITH TIME ZONE,
      temperature_2m FLOAT,
      relative_humidity_2m FLOAT,
      precipitation FLOAT,
      rain FLOAT,
      wind_speed_10m FLOAT,
      UNIQUE(location_id, time)
    );
  `);
  
  await client.query(`
    CREATE TABLE IF NOT EXISTS sde.hourly_weather (
      id SERIAL PRIMARY KEY,
      location_id INTEGER REFERENCES sde.weather_locations(location_id),
      time TIMESTAMP WITH TIME ZONE,
      temperature_2m FLOAT,
      relative_humidity_2m FLOAT,
      wind_speed_10m FLOAT,
      wind_direction_10m FLOAT,
      precipitation FLOAT,
      rain FLOAT,
      UNIQUE(location_id, time)
    );
  `);
  
  return client;
}

async function loadLocationsFromCSV(client, csvFilePath) {
  console.log('Loading locations from CSV...');
  
  // Check if CSV file exists
  if (!fs.existsSync(csvFilePath)) {
    console.log('CSV file not found. Using existing locations from database.');
    return null;
  }
  
  // Read and parse CSV file
  const csvContent = fs.readFileSync(csvFilePath, 'utf8');
  const parsed = Papa.parse(csvContent, {
    header: true,
    dynamicTyping: true,
    skipEmptyLines: true,
    transformHeader: (header) => header.trim(), // Clean headers
    transform: (value) => {
      if (typeof value === 'string') {
        return value.trim(); // Clean values
      }
      return value;
    }
  });
  
  if (parsed.errors.length > 0) {
    console.error('CSV parsing errors:', parsed.errors);
    throw new Error('Failed to parse CSV file');
  }
  
  const locations = parsed.data;
  console.log(`Found ${locations.length} locations in CSV`);
  
  // Validate data before inserting
  const validLocations = locations.filter(location => {
    const isValid = location.LocationID && 
                   !isNaN(parseFloat(location.latitude)) && 
                   !isNaN(parseFloat(location.longitude));
    if (!isValid) {
      console.warn(`Skipping invalid location:`, location);
    }
    return isValid;
  });
  
  console.log(`${validLocations.length} valid locations found`);
  
  // Insert new locations (skip if already exists)
  let insertedCount = 0;
  for (const location of validLocations) {
    try {
      const result = await client.query(
        `INSERT INTO sde.weather_locations (location_id, latitude, longitude) 
         VALUES ($1, $2, $3) 
         ON CONFLICT (location_id) DO NOTHING 
         RETURNING location_id`,
        [
          parseInt(location.LocationID), 
          parseFloat(location.latitude), 
          parseFloat(location.longitude)
        ]
      );
      if (result.rows.length > 0) {
        insertedCount++;
      }
    } catch (err) {
      console.error(`Error inserting location ${location.LocationID}:`, err.message);
    }
  }
  
  console.log(`Inserted ${insertedCount} new locations into database`);
  console.log(`Total valid locations processed: ${validLocations.length}`);
  
  return validLocations;
}

async function getLocationsFromDatabase(client) {
  const result = await client.query(
    'SELECT location_id, latitude, longitude FROM sde.openmeteolocations_wgs84 ORDER BY location_id'
  );
  return result.rows;
}

async function fetchWeatherData(locations) {
  console.log(`Fetching weather data for ${locations.length} locations...`);
  
  // Open-Meteo API limits: reduce batch size and increase delays
  const BATCH_SIZE = 50; // Reduced from 100
  const DELAY_BETWEEN_BATCHES = 15000; // 15 seconds instead of 2
  
  const batches = [];
  
  for (let i = 0; i < locations.length; i += BATCH_SIZE) {
    const batch = locations.slice(i, i + BATCH_SIZE);
    batches.push(batch);
  }
  
  console.log(`Split into ${batches.length} batches of max ${BATCH_SIZE} locations`);
  console.log(`Estimated time: ${Math.round((batches.length * DELAY_BETWEEN_BATCHES) / 1000 / 60)} minutes`);
  
  const allResponses = [];
  
  for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
    const batch = batches[batchIndex];
    console.log(`Fetching batch ${batchIndex + 1}/${batches.length} (${batch.length} locations)...`);
    
    let retryCount = 0;
    const maxRetries = 3;
    
    while (retryCount <= maxRetries) {
      try {
        const params = {
          latitude: batch.map(loc => loc.latitude),
          longitude: batch.map(loc => loc.longitude),
          current: ["temperature_2m", "relative_humidity_2m", "precipitation", "rain", "wind_speed_10m"],
          //hourly: ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m", "precipitation", "rain"],
          timeformat: "unixtime",
          timezone: "Africa/Cairo",
          past_days: 7,
          forecast_days: 3
        };
        
        const url = "https://api.open-meteo.com/v1/forecast";
        const responses = await fetchWeatherApi(url, params);
        
        console.log(`Batch ${batchIndex + 1}: Received ${responses.length} responses`);
        
        // Add batch info to responses for tracking
        responses.forEach((response, index) => {
          response._batchIndex = batchIndex;
          response._locationIndex = index;
          response._originalLocationIndex = (batchIndex * BATCH_SIZE) + index;
        });
        
        allResponses.push(...responses);
        break; // Success, exit retry loop
        
      } catch (error) {
        if (error.message.includes('request limit exceeded')) {
          retryCount++;
          const waitTime = 60 + (retryCount * 30); // 60, 90, 120 seconds
          console.log(`Rate limit hit. Retry ${retryCount}/${maxRetries} - waiting ${waitTime} seconds...`);
          await new Promise(resolve => setTimeout(resolve, waitTime * 1000));
        } else {
          throw error; // Re-throw non-rate-limit errors
        }
      }
    }
    
    if (retryCount > maxRetries) {
      console.error(`Failed batch ${batchIndex + 1} after ${maxRetries} retries. Skipping...`);
      continue;
    }
    
    // Add delay between batches
    if (batchIndex < batches.length - 1) {
      console.log(`Waiting ${DELAY_BETWEEN_BATCHES/1000} seconds before next batch...`);
      await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_BATCHES));
    }
  }
  
  console.log(`Total responses received: ${allResponses.length}`);
  return allResponses;
}

async function insertCurrentWeather(client, locationId, weatherData) {
  if (!weatherData || !weatherData.time) {
    console.log(`No current weather data for location ${locationId}`);
    return;
  }
  
  await client.query(
    `INSERT INTO sde.current_weather 
     (location_id, time, temperature_2m, relative_humidity_2m, precipitation, rain, wind_speed_10m)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (location_id, time) 
     DO UPDATE SET 
       temperature_2m = EXCLUDED.temperature_2m,
       relative_humidity_2m = EXCLUDED.relative_humidity_2m,
       precipitation = EXCLUDED.precipitation,
       rain = EXCLUDED.rain,
       wind_speed_10m = EXCLUDED.wind_speed_10m`,
    [
      locationId,
      weatherData.time,
      weatherData.temperature2m,
      weatherData.relativeHumidity2m,
      weatherData.precipitation,
      weatherData.rain,
      weatherData.windSpeed10m
    ]
  );
}

async function insertHourlyWeather(client, locationId, hourlyData) {
  if (!hourlyData || !hourlyData.time || hourlyData.time.length === 0) {
    console.log(`No hourly weather data for location ${locationId}`);
    return;
  }
  
  console.log(`Inserting ${hourlyData.time.length} hourly records for location ${locationId}`);
  
  for (let i = 0; i < hourlyData.time.length; i++) {
    await client.query(
      `INSERT INTO sde.hourly_weather 
       (location_id, time, temperature_2m, relative_humidity_2m, wind_speed_10m, wind_direction_10m, precipitation, rain)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (location_id, time) 
       DO UPDATE SET 
         temperature_2m = EXCLUDED.temperature_2m,
         relative_humidity_2m = EXCLUDED.relative_humidity_2m,
         wind_speed_10m = EXCLUDED.wind_speed_10m,
         wind_direction_10m = EXCLUDED.wind_direction_10m,
         precipitation = EXCLUDED.precipitation,
         rain = EXCLUDED.rain`,
      [
        locationId,
        hourlyData.time[i],
        hourlyData.temperature2m[i],
        hourlyData.relativeHumidity2m[i],
        hourlyData.windSpeed10m[i],
        hourlyData.windDirection10m[i],
        hourlyData.precipitation[i],
        hourlyData.rain[i]
      ]
    );
  }
}

async function main() {
  let client;
  try {
    // Setup database
    client = await setupDatabase();
    console.log("Database setup complete");
    
    // Try to load locations from CSV first (for initial setup)
    // If CSV doesn't exist, use database locations
    let locations = await loadLocationsFromCSV(client, 'OpenMeteoLocations_WGS84.csv');
    
    if (!locations) {
      // CSV not found, get locations from database
      locations = await getLocationsFromDatabase(client);
      if (locations.length === 0) {
        throw new Error('No locations found in database and CSV file not available. Please ensure OpenMeteoLocations_WGS84.csv exists for initial setup.');
      }
      console.log(`Using ${locations.length} locations from database`);
    }
    
    // Fetch weather data in batches
    const responses = await fetchWeatherData(locations);
    
    // Process each response with proper location mapping
    for (let i = 0; i < responses.length; i++) {
      const response = responses[i];
      const originalIndex = response._originalLocationIndex || i;
      const location = locations[originalIndex];
      
      if (!location) {
        console.log(`No location found for response ${i}, skipping...`);
        continue;
      }
      
      const locationId = location.location_id; // Fixed: use correct column name
      
      console.log(`\nProcessing location ${i + 1}/${responses.length}: ID ${locationId} (${location.latitude}, ${location.longitude})`);
      
      await client.query('BEGIN');
      
      try {
        const utcOffsetSeconds = response.utcOffsetSeconds();
        
        // Process current weather
        if (typeof response.current === 'function') {
          const current = response.current();
          if (current && current.time()) {
            const currentWeatherData = {
              time: new Date((Number(current.time()) + utcOffsetSeconds) * 1000),
              temperature2m: current.variables(0).value(),
              relativeHumidity2m: current.variables(1).value(),
              precipitation: current.variables(2).value(),
              rain: current.variables(3).value(),
              windSpeed10m: current.variables(4).value(),
            };
            
            await insertCurrentWeather(client, locationId, currentWeatherData);
            console.log(`Current weather inserted for location ${locationId}`);
          }
        }
        
        // Process hourly weather
        if (typeof response.hourly === 'function') {
          const hourly = response.hourly();
          if (hourly) {
            const range = (start, stop, step) =>
              Array.from({ length: (stop - start) / step }, (_, i) => start + i * step);
            
            const hourlyData = {
              time: range(Number(hourly.time()), Number(hourly.timeEnd()), hourly.interval())
                .map(t => new Date((t + utcOffsetSeconds) * 1000)),
              temperature2m: hourly.variables(0).valuesArray(),
              relativeHumidity2m: hourly.variables(1).valuesArray(),
              windSpeed10m: hourly.variables(2).valuesArray(),
              windDirection10m: hourly.variables(3).valuesArray(),
              precipitation: hourly.variables(4).valuesArray(),
              rain: hourly.variables(5).valuesArray(),
            };
            
            await insertHourlyWeather(client, locationId, hourlyData);
          }
        }
        
        await client.query('COMMIT');
        
      } catch (error) {
        await client.query('ROLLBACK');
        console.error(`Error processing location ${locationId}: ${error.message}`);
      }
    }
    
    // Print summary
    const currentCount = await client.query('SELECT COUNT(*) FROM sde.current_weather');
    const hourlyCount = await client.query('SELECT COUNT(*) FROM sde.hourly_weather');
    const locationCount = await client.query('SELECT COUNT(*) FROM sde.weather_locations');
    
    console.log("\n=== SUMMARY ===");
    console.log(`Total locations: ${locationCount.rows[0].count}`);
    console.log(`Current weather records: ${currentCount.rows[0].count}`);
    console.log(`Hourly weather records: ${hourlyCount.rows[0].count}`);
    console.log("Weather data successfully stored in PostgreSQL");
    
  } catch (error) {
    console.error("Error:", error);
    process.exit(1);
  } finally {
    if (client) {
      await client.end();
      console.log("Database connection closed");
    }
  }
}

main();