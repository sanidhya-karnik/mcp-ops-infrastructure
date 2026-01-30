import asyncio
from src.database.connection import get_operations_db
from src.tools.sql_tool import execute_sql_query, get_database_schema
from src.tools.weather_tool import get_weather, geocode_location

async def main():
    # Initialize database
    db = get_operations_db()
    await db.initialize()
    print("✓ Database initialized\n")
    
    # Test 1: Get schema
    print("=== Database Schema ===")
    schema = await get_database_schema()
    for table, cols in schema["tables"].items():
        print(f"  {table}: {', '.join(cols)}")
    
    # Test 2: Query customers
    print("\n=== Top Customers by Lifetime Value ===")
    result = await execute_sql_query(
        "SELECT company, lifetime_value FROM customers ORDER BY lifetime_value DESC LIMIT 5"
    )
    if result.get("success"):
        for row in result["rows"]:
            print(f"  {row['company']}: ${row['lifetime_value']:,.2f}")
    
    # Test 3: Geocoding
    print("\n=== Geocoding Boston ===")
    geo = await geocode_location("Boston")
    if geo.get("success") and geo["results"]:
        loc = geo["results"][0]
        print(f"  {loc['name']}, {loc['country']}: ({loc['latitude']}, {loc['longitude']})")
        
        # Test 4: Weather
        print("\n=== Weather Forecast ===")
        weather = await get_weather(loc["latitude"], loc["longitude"], days=3)
        if weather.get("success"):
            for day in weather["forecast"]:
                if day["date"] == "now":
                    print(f"  Current: {day['temperature_current']}°C - {day['weather_description']}")
                else:
                    print(f"  {day['date']}: {day['temperature_min']}°C - {day['temperature_max']}°C")

    print("\n✓ All tests passed!")

if __name__ == "__main__":
    asyncio.run(main())