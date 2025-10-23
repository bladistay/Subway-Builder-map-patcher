// javascript
  import fs from 'fs';
  import { Readable } from "stream";
  import config from '../config.js';
  import perfConfig from '../performance_config.js';
  import pLimit from 'p-limit';
  import { createParseStream } from 'big-json';
  import { encode as msgpackEncode } from '@msgpack/msgpack';

  const DEBUG = process.env.DEBUG === '1' || process.env.DEBUG === 'true';

  const convertBbox = (bbox) => [bbox[1], bbox[0], bbox[3], bbox[2]];

  const generateTiles = (bbox, maxTileSize = 0.5) => {
    const [south, west, north, east] = bbox;
    const tiles = [];
    let lat = south;
    while (lat < north) {
      const nextLat = Math.min(lat + maxTileSize, north);
      let lon = west;
      while (lon < east) {
        const nextLon = Math.min(lon + maxTileSize, east);
        tiles.push([lat, lon, nextLat, nextLon]);
        lon = nextLon;
      }
      lat = nextLat;
    }
    return tiles;
  };

  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

  const runQueryWithRetry = async (query, maxRetries = perfConfig.retry.maxAttempts, baseDelay = perfConfig.retry.baseDelay) => {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const res = await fetch("https://overpass-api.de/api/interpreter", {
          credentials: "omit",
          headers: {
            "User-Agent": "SubwayBuilder-Patcher (https://github.com/piemadd/subwaybuilder-patcher)",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5"
          },
          body: `data=${encodeURIComponent(query)}`,
          method: "POST",
          mode: "cors"
        });

        if (!res.ok) {
          if (attempt < maxRetries) {
            const isRateLimit = res.status === 429;
            const delay = isRateLimit
              ? baseDelay * Math.pow(4, attempt - 1)
              : baseDelay * Math.pow(2, attempt - 1);
            await sleep(delay);
            continue;
          }
          throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        }

        const parseStream = createParseStream();
        let parsedData = null;

        parseStream.on('data', (data) => {
          parsedData = data;
        });

        await new Promise((resolve, reject) => {
          const src = Readable.fromWeb(res.body);
          let settled = false;

          const cleanup = () => {
            if (settled) return;
            settled = true;
            src.removeListener('error', onError);
            parseStream.removeListener('error', onError);
            parseStream.removeListener('end', onEnd);
          };

          const onError = (err) => {
            cleanup();
            try { parseStream.destroy(err); } catch (e) {}
            reject(err);
          };

          const onEnd = () => {
            cleanup();
            resolve();
          };

          parseStream.on('end', onEnd);
          parseStream.on('error', onError);
          src.on('error', onError);

          src.pipe(parseStream);
        });

        return parsedData;
      } catch (error) {
        if (attempt < maxRetries) {
          const delay = baseDelay * Math.pow(2, attempt - 1);
          await sleep(delay);
        } else {
          throw error;
        }
      }
    }
  };

  const getStreetName = (tags, preferLocale = 'en') => {
    if (tags.noname === 'yes') return '';
    const localized = tags[`name:${preferLocale}`];
    if (localized && localized.trim()) return localized.trim();
    if (tags.name && tags.name.trim()) return tags.name.trim();
    if (tags.ref && tags.ref.trim()) return tags.ref.trim();
    return '';
  };

  const processRoads = (elements) => {
    const roadTypes = {
      motorway: 'highway',
      trunk: 'major',
      primary: 'major',
      secondary: 'minor',
      tertiary: 'minor',
      residential: 'minor',
    };

    return {
      type: "FeatureCollection",
      features: elements.map((element) => ({
        type: "Feature",
        properties: {
          roadClass: roadTypes[element.tags.highway],
          structure: "normal",
          name: getStreetName(element.tags, (config.locale || 'en')),
        },
        geometry: {
          coordinates: element.geometry.map((coord) => [coord.lon, coord.lat]),
          type: "LineString"
        }
      }))
    };
  };

  const fetchTileRecursiveFactory = (tagQueryBuilder) => {
    return async function fetchTileRecursive(tile, depth = 0, maxDepth = 3) {
      const tileArea = (tile[2] - tile[0]) * (tile[3] - tile[1]);

      const buildQuery = () => tagQueryBuilder(tile);

      if (depth >= maxDepth || tileArea < 0.01) {
        try {
          const data = await runQueryWithRetry(buildQuery());
          return data.elements || [];
        } catch (err) {
          return [];
        }
      }

      try {
        const data = await runQueryWithRetry(buildQuery());
        const count = (data.elements || []).length;
        if (count === 0 && tileArea > 0.1) {
          const midLon = (tile[0] + tile[2]) / 2;
          const midLat = (tile[1] + tile[3]) / 2;
          const subtiles = [
            [tile[0], tile[1], midLon, midLat],
            [midLon, tile[1], tile[2], midLat],
            [tile[0], midLat, midLon, tile[3]],
            [midLon, midLat, tile[2], tile[3]],
          ];
          const results = [];
          for (const subtile of subtiles) {
            const subtileResults = await fetchTileRecursive(subtile, depth + 1, maxDepth);
            for (let i = 0; i < subtileResults.length; i++) results.push(subtileResults[i]);
            await sleep(perfConfig.requestDelay);
          }
          return results;
        }
        return data.elements || [];
      } catch (err) {
        if (tileArea > 0.1) {
          const midLon = (tile[0] + tile[2]) / 2;
          const midLat = (tile[1] + tile[3]) / 2;
          const subtiles = [
            [tile[0], tile[1], midLon, midLat],
            [midLon, tile[1], tile[2], midLat],
            [tile[0], midLat, midLon, tile[3]],
            [midLon, midLat, tile[2], tile[3]],
          ];
          const results = [];
          for (const subtile of subtiles) {
            const subtileResults = await fetchTileRecursive(subtile, depth + 1, maxDepth);
            for (let i = 0; i < subtileResults.length; i++) results.push(subtileResults[i]);
            await sleep(perfConfig.requestDelay);
          }
          return results;
        }
        return [];
      }
    };
  };

  const roadQueryBuilder = (tile) => `
  [out:json][timeout:180];
  (
    way["highway"="motorway"](${tile.join(',')});
    way["highway"="trunk"](${tile.join(',')});
    way["highway"="primary"](${tile.join(',')});
    way["highway"="secondary"](${tile.join(',')});
    way["highway"="tertiary"](${tile.join(',')});
    way["highway"="residential"](${tile.join(',')});
  );
  out geom;`;

  const buildingQueryBuilder = (tile) => `
  [out:json][timeout:180];
  (
    way["building"](${tile.join(',')});
  );
  out geom;`;

  const placesQueryBuilder = (tile) => `
  [out:json][timeout:180];
  (
    nwr["place"="neighbourhood"](${tile.join(',')});
    nwr["place"="quarter"](${tile.join(',')});
    nwr["place"="suburb"](${tile.join(',')});
    nwr["place"="hamlet"](${tile.join(',')});
    nwr["place"="village"](${tile.join(',')});
    nwr["aeroway"="terminal"](${tile.join(',')});
  );
  out geom;`;

  const fetchRoadTileRecursive = fetchTileRecursiveFactory(roadQueryBuilder);
  const fetchBuildingTileRecursive = fetchTileRecursiveFactory(buildingQueryBuilder);
  const fetchPlaceTileRecursive = fetchTileRecursiveFactory(placesQueryBuilder);

  const fetchDataTiled = async (bbox, tileSize, fetchTileRecursive) => {
    const tiles = generateTiles(bbox, tileSize);
    const all = [];
    for (let i = 0; i < tiles.length; i++) {
      const tile = tiles[i];
      const tileResults = await fetchTileRecursive(tile);
      for (let j = 0; j < tileResults.length; j++) all.push(tileResults[j]);
      if (i < tiles.length - 1) {
        const jitter = Math.random() * 500;
        await sleep(perfConfig.requestDelay + jitter);
      }
    }
    return all;
  };

  const fetchRoadData = async (bbox) => {
    const bboxArea = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]);
    const skipFullDownload = bboxArea > 1.5;

    if (perfConfig.tryFullBboxFirst && !skipFullDownload) {
      try {
        const data = await runQueryWithRetry(roadQueryBuilder(bbox), 1);
        if (data.elements.length === 0) {
          return processRoads(await fetchDataTiled(bbox, perfConfig.overpassTileSize.roads, fetchRoadTileRecursive));
        }
        return processRoads(data.elements);
      } catch (err) {
        return processRoads(await fetchDataTiled(bbox, perfConfig.overpassTileSize.roads, fetchRoadTileRecursive));
      }
    } else {
      return processRoads(await fetchDataTiled(bbox, perfConfig.overpassTileSize.roads, fetchRoadTileRecursive));
    }
  };

  const fetchBuildingsData = async (bbox) => {
    const bboxArea = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]);
    const skipFullDownload = bboxArea > 1.5;

    if (perfConfig.tryFullBboxFirst && !skipFullDownload) {
      try {
        const data = await runQueryWithRetry(buildingQueryBuilder(bbox), 1);
        if (data.elements.length === 0) {
          return fetchDataTiled(bbox, perfConfig.overpassTileSize.buildings, fetchBuildingTileRecursive);
        }
        return data.elements;
      } catch (err) {
        return fetchDataTiled(bbox, perfConfig.overpassTileSize.buildings, fetchBuildingTileRecursive);
      }
    } else {
      return fetchDataTiled(bbox, perfConfig.overpassTileSize.buildings, fetchBuildingTileRecursive);
    }
  };

  const fetchPlacesData = async (bbox) => {
    const bboxArea = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]);
    const skipFullDownload = bboxArea > 1.5;

    if (perfConfig.tryFullBboxFirst && !skipFullDownload) {
      try {
        const data = await runQueryWithRetry(placesQueryBuilder(bbox), 1);
        if (data.elements.length === 0) {
          return fetchDataTiled(bbox, perfConfig.overpassTileSize.places, fetchPlaceTileRecursive);
        }
        return data.elements;
      } catch (err) {
        return fetchDataTiled(bbox, perfConfig.overpassTileSize.places, fetchPlaceTileRecursive);
      }
    } else {
      return fetchDataTiled(bbox, perfConfig.overpassTileSize.places, fetchPlaceTileRecursive);
    }
  };

  const writeMsgpackBinary = async (filePath, data) => {
    return new Promise((resolve, reject) => {
      try {
        const binary = msgpackEncode(data);
        fs.writeFileSync(filePath, binary);
        resolve();
      } catch (error) {
        reject(error);
      }
    });
  };

  const writeJsonStream = async (filePath, data) => {
    return new Promise((resolve, reject) => {
      const writeStream = fs.createWriteStream(filePath, { encoding: 'utf8', highWaterMark: 1024 * 1024 });
      writeStream.on('error', (err) => {
        reject(err);
      });
      writeStream.on('finish', () => {
        resolve();
      });

      const writeWithBackpressure = (chunk) => {
        return new Promise((resolveWrite) => {
          if (!writeStream.write(chunk)) {
            writeStream.once('drain', resolveWrite);
          } else {
            resolveWrite();
          }
        });
      };

      if (Array.isArray(data)) {
        (async () => {
          try {
            await writeWithBackpressure('[');
            for (let i = 0; i < data.length; i++) {
              if (i > 0) await writeWithBackpressure(',');
              await writeWithBackpressure(JSON.stringify(data[i]));
            }
            writeStream.end(']');
          } catch (err) {
            reject(err);
          }
        })();
      } else if (data && typeof data === 'object' && data.features && Array.isArray(data.features)) {
        (async () => {
          try {
            await writeWithBackpressure('{"type":"FeatureCollection","features":[');
            for (let i = 0; i < data.features.length; i++) {
              if (i > 0) await writeWithBackpressure(',');
              await writeWithBackpressure(JSON.stringify(data.features[i]));
            }
            writeStream.end(']}');
          } catch (err) {
            reject(err);
          }
        })();
      } else {
        try {
          writeStream.end(JSON.stringify(data));
        } catch (error) {
          reject(error);
        }
      }
    });
  };

  const fetchAllData = async (place) => {
    if (!fs.existsSync(`./raw-data/${place.code}`)) {
      fs.mkdirSync(`./raw-data/${place.code}`, { recursive: true });
    }

    const convertedBoundingBox = convertBbox(place.bbox);

    const startTime = Date.now();
    const roadData = await fetchRoadData(convertedBoundingBox);
    if (perfConfig.datasetDelay) await sleep(perfConfig.datasetDelay);

    const startBuildings = Date.now();
    const buildingData = await fetchBuildingsData(convertedBoundingBox);
    if (perfConfig.datasetDelay) await sleep(perfConfig.datasetDelay);

    const startPlaces = Date.now();
    const placesData = await fetchPlacesData(convertedBoundingBox);

    await writeJsonStream(`./raw-data/${place.code}/roads.geojson`, roadData);
    await writeMsgpackBinary(`./raw-data/${place.code}/buildings.msgpack`, buildingData);
    await writeMsgpackBinary(`./raw-data/${place.code}/places.msgpack`, placesData);
  };

  if (!fs.existsSync('./raw-data')) {
    fs.mkdirSync('./raw-data');
  }

  const limit = pLimit(perfConfig.maxConcurrentDownloads);

  const tasks = config.places.map(place => limit(async () => {
    try {
      await fetchAllData(place);
    } catch (err) {
      throw err;
    }
  }));

  Promise.all(tasks)
    .then(() => {
      process.exit(0);
    })
    .catch((err) => {
      process.exit(1);
    });