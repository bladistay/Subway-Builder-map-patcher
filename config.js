const config = {
    // If you don't know what to put here, check the README.md file for guidance

    /*
    ORIGINAL (not modified) game application location on disk should be specified here.
    On Windows, it might look like "C://Users/YOUR_USER/AppData/Local/Programs/Subway Builder/Subway Builder.exe"
    On Linux, it might look like "/usr/bin/subway-builder"
    On macOS, it might look like "/Applications/Subway Builder.app"
     */
    "location": "C://Users/YOUR_USER/AppData/Local/Programs/Subway Builder/Subway Builder.exe",
    // Platform can be "windows", "macos", or "linux"
    "platform": "windows",
    /*
    List of places (your cities) available in the game.
    Each place should have:
    - code: Unique identifier for the place (3-letter code)
    - name: Human-readable name of the place
    - description: Short description of the place
    - bbox: Bounding box of the place in [minLon, minLat, maxLon, maxLat] format
    - population: Approximate population of the place
     */
    "places": [
        {
            "code": "BER",
            "name": "Berlin",
            "description": "Capital of Germany",
            "bbox": [13.0884, 52.3383, 13.7611, 52.6755],
            "population": 3769000
        }
    ],
    // This should domain that serves vector tiles in PBF/MVT format
    "tileServerDomain": "api.maptiler.com",
    // Tile server URL template (if using maptiler, change the key to your own YOUR_MAPTILER_KEY)
    "tileServerUrl": "https://api.maptiler.com/tiles/v3-openmaptiles/{z}/{x}/{y}.pbf?key=YOUR_MAPTILER_KEY",
};

export default config;