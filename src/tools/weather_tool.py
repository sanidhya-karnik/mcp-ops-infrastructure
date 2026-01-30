"""
Weather Tool
Provides weather data using Open-Meteo API (free, no API key required).
Also includes geocoding for location lookup.
"""

import time
from typing import Any

import httpx
import structlog

from ..schemas.models import (
    WeatherInput, WeatherResponse, WeatherData,
    GeocodingInput, GeocodingResponse, GeocodingResult,
    ToolError
)

logger = structlog.get_logger()

# Weather code descriptions
WEATHER_CODES = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Foggy",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    71: "Slight snow",
    73: "Moderate snow",
    75: "Heavy snow",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail"
}


async def get_weather(
    latitude: float,
    longitude: float,
    days: int = 1
) -> dict[str, Any]:
    """
    Get weather forecast for a location.
    
    Uses Open-Meteo API which is free and requires no API key.
    
    Args:
        latitude: Location latitude (-90 to 90)
        longitude: Location longitude (-180 to 180)
        days: Forecast days (1-7)
        
    Returns:
        WeatherResponse or ToolError as dict
    """
    start_time = time.perf_counter()
    
    # Validate input
    try:
        validated_input = WeatherInput(
            latitude=latitude,
            longitude=longitude,
            days=days
        )
    except Exception as e:
        return ToolError(
            code="VALIDATION_ERROR",
            message=str(e)
        ).model_dump()
    
    # Fetch weather data
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": validated_input.latitude,
                    "longitude": validated_input.longitude,
                    "daily": "temperature_2m_max,temperature_2m_min,precipitation_probability_max,weather_code",
                    "current": "temperature_2m,weather_code",
                    "timezone": "auto",
                    "forecast_days": validated_input.days
                }
            )
            response.raise_for_status()
            data = response.json()
        
        execution_time = (time.perf_counter() - start_time) * 1000
        
        # Parse forecast data
        daily = data.get("daily", {})
        current = data.get("current", {})
        
        forecast = []
        
        # Add current conditions as first entry
        if current:
            current_code = current.get("weather_code")
            forecast.append(WeatherData(
                date="now",
                temperature_current=current.get("temperature_2m"),
                weather_code=current_code,
                weather_description=WEATHER_CODES.get(current_code, "Unknown")
            ))
        
        # Add daily forecast
        dates = daily.get("time", [])
        temps_max = daily.get("temperature_2m_max", [])
        temps_min = daily.get("temperature_2m_min", [])
        precip_prob = daily.get("precipitation_probability_max", [])
        weather_codes = daily.get("weather_code", [])
        
        for i, date in enumerate(dates):
            code = weather_codes[i] if i < len(weather_codes) else None
            forecast.append(WeatherData(
                date=date,
                temperature_max=temps_max[i] if i < len(temps_max) else None,
                temperature_min=temps_min[i] if i < len(temps_min) else None,
                precipitation_probability=precip_prob[i] if i < len(precip_prob) else None,
                weather_code=code,
                weather_description=WEATHER_CODES.get(code, "Unknown") if code else None
            ))
        
        result = WeatherResponse(
            success=True,
            location={
                "latitude": validated_input.latitude,
                "longitude": validated_input.longitude
            },
            timezone=data.get("timezone", "UTC"),
            forecast=forecast
        )
        
        logger.info(
            "Weather fetched",
            lat=latitude,
            lon=longitude,
            time_ms=execution_time
        )
        
        return result.model_dump()
        
    except httpx.HTTPStatusError as e:
        return ToolError(
            code="API_ERROR",
            message=f"Weather API error: {e.response.status_code}"
        ).model_dump()
        
    except Exception as e:
        logger.error("Weather fetch failed", error=str(e))
        return ToolError(
            code="WEATHER_ERROR",
            message=f"Failed to fetch weather: {str(e)}"
        ).model_dump()


async def geocode_location(location: str) -> dict[str, Any]:
    """
    Look up coordinates for a location name.
    
    Uses Open-Meteo Geocoding API (free, no key required).
    
    Args:
        location: City name or address
        
    Returns:
        GeocodingResponse or ToolError as dict
    """
    start_time = time.perf_counter()
    
    # Validate input
    try:
        validated_input = GeocodingInput(location=location)
    except Exception as e:
        return ToolError(
            code="VALIDATION_ERROR",
            message=str(e)
        ).model_dump()
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                "https://geocoding-api.open-meteo.com/v1/search",
                params={
                    "name": validated_input.location,
                    "count": 5,
                    "language": "en",
                    "format": "json"
                }
            )
            response.raise_for_status()
            data = response.json()
        
        execution_time = (time.perf_counter() - start_time) * 1000
        
        results = [
            GeocodingResult(
                name=r.get("name", ""),
                latitude=r.get("latitude", 0),
                longitude=r.get("longitude", 0),
                country=r.get("country", ""),
                admin1=r.get("admin1")
            )
            for r in data.get("results", [])
        ]
        
        result = GeocodingResponse(
            success=True,
            query=validated_input.location,
            results=results
        )
        
        logger.info(
            "Geocoding complete",
            location=location,
            results=len(results),
            time_ms=execution_time
        )
        
        return result.model_dump()
        
    except httpx.HTTPStatusError as e:
        return ToolError(
            code="API_ERROR",
            message=f"Geocoding API error: {e.response.status_code}"
        ).model_dump()
        
    except Exception as e:
        logger.error("Geocoding failed", error=str(e))
        return ToolError(
            code="GEOCODING_ERROR",
            message=f"Failed to geocode location: {str(e)}"
        ).model_dump()
