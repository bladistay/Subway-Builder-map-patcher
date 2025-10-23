import fs from 'fs';
import config from '../config.js';
import perfConfig from '../performance_config.js';
import { createParseStream } from 'big-json';
import RBush from 'rbush';
import StreamArray from 'stream-json/streamers/StreamArray.js';
import pkg from 'stream-chain';
const { chain } = pkg;
import Piscina from 'piscina';
import os from 'os';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { decode as msgpackDecode } from '@msgpack/msgpack';
import zlib from 'zlib';
import { pipeline } from 'stream/promises';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const calculateArea = (coords) => {
  let area = 0;
  const n = coords.length;
  for (let i = 0; i < n - 1; i++) {
    const [lon1, lat1] = coords[i];
    const [lon2, lat2] = coords[i + 1];
    area += lon1 * lat2 - lon2 * lat1;
  }
  return Math.abs(area / 2) * 111320 * 111320 * Math.cos(coords[0][1] * Math.PI / 180);
};

const calculateDistance = (coord1, coord2) => {
  const [lon1, lat1] = coord1;
  const [lon2, lat2] = coord2;
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
      Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
};

const optimizeBuilding = (unOptimizedBuilding) => {
  const simplifiedPolygon = [[
    [unOptimizedBuilding.minX, unOptimizedBuilding.minY],
    [unOptimizedBuilding.maxX, unOptimizedBuilding.minY],
    [unOptimizedBuilding.maxX, unOptimizedBuilding.maxY],
    [unOptimizedBuilding.minX, unOptimizedBuilding.maxY],
    [unOptimizedBuilding.minX, unOptimizedBuilding.minY],
  ]];

  return {
    b: [unOptimizedBuilding.minX, unOptimizedBuilding.minY, unOptimizedBuilding.maxX, unOptimizedBuilding.maxY],
    f: unOptimizedBuilding.foundationDepth,
    p: simplifiedPolygon,
  }
};

const optimizeIndex = async (unOptimizedIndex) => {
  const optimizedCells = Object.keys(unOptimizedIndex.cells).map((key) =>
      [...key.split(',').map((n) => Number(n)), ...unOptimizedIndex.cells[key]]
  );

  const optimizedBuildings = [];
  const batchSize = 50000;
  const totalBatches = Math.ceil(unOptimizedIndex.buildings.length / batchSize);

  for (let batchNum = 0; batchNum < totalBatches; batchNum++) {
    const startIdx = batchNum * batchSize;
    const endIdx = Math.min(startIdx + batchSize, unOptimizedIndex.buildings.length);

    for (let i = startIdx; i < endIdx; i++) {
      optimizedBuildings.push(optimizeBuilding(unOptimizedIndex.buildings[i]));
    }

    await new Promise(resolve => setImmediate(resolve));
  }

  return {
    cs: unOptimizedIndex.cellHeightCoords,
    bbox: [unOptimizedIndex.minLon, unOptimizedIndex.minLat, unOptimizedIndex.maxLon, unOptimizedIndex.maxLat],
    grid: [unOptimizedIndex.cols, unOptimizedIndex.rows],
    cells: optimizedCells,
    buildings: optimizedBuildings,
    stats: {
      count: unOptimizedIndex.buildings.length,
      maxDepth: unOptimizedIndex.maxDepth,
    }
  }
};

const squareFeetPerPopulation = {
  yes: 600, apartments: 240, barracks: 100, bungalow: 600, cabin: 600,
  detached: 600, annexe: 240, dormitory: 125, farm: 600, ger: 240,
  hotel: 240, house: 600, houseboat: 600, residential: 600, semidetached_house: 400,
  static_caravan: 500, stilt_house: 600, terrace: 500, tree_house: 240, trullo: 240,
};

const squareFeetPerJob = {
  commercial: 150, industrial: 500, kiosk: 50, office: 150, retail: 300,
  supermarket: 300, warehouse: 500, religious: 100, cathedral: 100, chapel: 100,
  church: 100, kingdom_hall: 100, monastery: 100, mosque: 100, presbytery: 100,
  shrine: 100, synagogue: 100, temple: 100, bakehouse: 300, college: 250,
  fire_station: 500, government: 150, gatehouse: 150, hospital: 150, kindergarten: 100,
  museum: 300, public: 300, school: 100, train_station: 1000, transportation: 1000,
  university: 250, grandstand: 150, pavilion: 150, riding_hall: 150, sports_hall: 150,
  sports_centre: 150, stadium: 150,
};

const validPlaces = ['quarter', 'neighbourhood', 'suburb', 'hamlet', 'village'];

let terminalTicker = 0;

const processPlaceConnections = async (place, rawBuildings, rawPlaces) => {
  let neighborhoods = {};
  let centersOfNeighborhoods = {};
  let calculatedBuildings = {};

  rawPlaces.forEach((place) => {
    if (place.tags.place && (validPlaces.includes(place.tags.place)) || (place.tags.aeroway && place.tags.aeroway == 'terminal')) {
      neighborhoods[place.id] = place;
      if (place.type == 'node') {
        centersOfNeighborhoods[place.id] = [place.lon, place.lat];
      } else if (place.type == 'way' || place.type == 'relation') {
        const center = [(place.bounds.minlon + place.bounds.maxlon) / 2, (place.bounds.minlat + place.bounds.maxlat) / 2];
        centersOfNeighborhoods[place.id] = center;
      }
    }
  });

  const neighborhoodIndex = new RBush();
  const neighborhoodList = Object.keys(centersOfNeighborhoods).map(id => {
    const [lon, lat] = centersOfNeighborhoods[id];
    return {
      minX: lon, minY: lat,
      maxX: lon, maxY: lat,
      id,
      center: [lon, lat]
    };
  });
  neighborhoodIndex.load(neighborhoodList);

  const [minLon, minLat, maxLon, maxLat] = place.bbox;
  const gridResolution = 0.002;
  const gridCols = Math.ceil((maxLon - minLon) / gridResolution);
  const gridRows = Math.ceil((maxLat - minLat) / gridResolution);
  const totalGridCells = gridCols * gridRows;

  const neighborhoodGrid = new Map();
  const searchRadius = 0.05;

  const gridBatchSize = 5000;
  const totalGridBatches = Math.ceil(totalGridCells / gridBatchSize);

  for (let batchNum = 0; batchNum < totalGridBatches; batchNum++) {
    const batchStart = batchNum * gridBatchSize;
    const batchEnd = Math.min(batchStart + gridBatchSize, totalGridCells);

    for (let cellIdx = batchStart; cellIdx < batchEnd; cellIdx++) {
      const row = Math.floor(cellIdx / gridCols);
      const col = cellIdx % gridCols;
      const lon = minLon + col * gridResolution;
      const lat = minLat + row * gridResolution;

      const candidates = neighborhoodIndex.search({
        minX: lon - searchRadius,
        minY: lat - searchRadius,
        maxX: lon + searchRadius,
        maxY: lat + searchRadius
      });

      let nearestId = null;
      let minDistSq = Infinity;

      for (const neighborhood of candidates) {
        const [nLon, nLat] = neighborhood.center;
        const dLon = lon - nLon;
        const dLat = lat - nLat;
        const distSq = dLon * dLon + dLat * dLat;

        if (distSq < minDistSq) {
          minDistSq = distSq;
          nearestId = neighborhood.id;
        }
      }

      if (nearestId) {
        neighborhoodGrid.set(`${col},${row}`, nearestId);
      }
    }

    if (batchNum % 50 === 0) {
      await new Promise(resolve => setImmediate(resolve));
    }
  }

  const buildingBatchSize = perfConfig.batchSizes.buildings;
  let processedCount = 0;

  for (let i = 0; i < rawBuildings.length; i += buildingBatchSize) {
    const batch = rawBuildings.slice(i, i + buildingBatchSize);

    batch.forEach((building) => {
      if (!building.tags.building) return;

      const __coords = building.geometry.map((point) => [point.lon, point.lat]);
      if (__coords.length < 3) return;
      if (__coords[0][0] !== __coords[__coords.length - 1][0] || __coords[0][1] !== __coords[__coords.length - 1][1]) {
        __coords.push(__coords[0]);
      }

      const buildingAreaSqMeters = calculateArea(__coords);
      let buildingAreaMultiplier = Math.max(Number(building.tags['building:levels']), 1);
      if (isNaN(buildingAreaMultiplier)) buildingAreaMultiplier = 1;
      const buildingArea = buildingAreaSqMeters * buildingAreaMultiplier * 10.7639;

      let minLonB = Infinity, maxLonB = -Infinity, minLatB = Infinity, maxLatB = -Infinity;
      for (const [lon, lat] of __coords) {
        if (lon < minLonB) minLonB = lon;
        if (lon > maxLonB) maxLonB = lon;
        if (lat < minLatB) minLatB = lat;
        if (lat > maxLatB) maxLatB = lat;
      }
      const buildingCenter = [(minLonB + maxLonB) / 2, (minLatB + maxLatB) / 2];

      if (squareFeetPerPopulation[building.tags.building]) {
        const approxPop = Math.floor(buildingArea / squareFeetPerPopulation[building.tags.building]);
        calculatedBuildings[building.id] = {
          ...building,
          approxPop,
          buildingCenter,
        };
      } else if (squareFeetPerJob[building.tags.building]) {
        let approxJobs = Math.floor(buildingArea / squareFeetPerJob[building.tags.building]);
        if (building.tags.aeroway && building.tags.aeroway == 'terminal') {
          approxJobs *= 20;
        }
        calculatedBuildings[building.id] = {
          ...building,
          approxJobs,
          buildingCenter,
        };
      }
    });

    processedCount += batch.length;
  }

  let finalVoronoiMembers = {};
  let finalVoronoiMetadata = {};

  Object.keys(neighborhoods).forEach((placeID) => {
    finalVoronoiMembers[placeID] = [];
    finalVoronoiMetadata[placeID] = {
      placeID,
      name: neighborhoods[placeID].tags.name,
      totalPopulation: 0,
      totalJobs: 0,
      percentOfTotalPopulation: null,
      percentOfTotalJobs: null,
    };
  });

  let assignedCount = 0;
  const buildingList = Object.values(calculatedBuildings);

  for (let i = 0; i < buildingList.length; i++) {
    const building = buildingList[i];
    const [lon, lat] = building.buildingCenter;

    const col = Math.floor((lon - minLon) / gridResolution);
    const row = Math.floor((lat - minLat) / gridResolution);
    const placeID = neighborhoodGrid.get(`${col},${row}`);

    if (placeID && finalVoronoiMembers[placeID]) {
      finalVoronoiMembers[placeID].push(building);
      finalVoronoiMetadata[placeID].totalPopulation += (building.approxPop ?? 0);
      finalVoronoiMetadata[placeID].totalJobs += (building.approxJobs ?? 0);
    }

    assignedCount++;

    if (assignedCount % 50000 === 0) {
      await new Promise(resolve => setImmediate(resolve));
    }
  }

  let totalPopulation = 0;
  let totalJobs = 0;
  Object.values(finalVoronoiMetadata).forEach((meta) => {
    totalPopulation += meta.totalPopulation;
    totalJobs += meta.totalJobs;
  });

  Object.values(finalVoronoiMetadata).forEach((place) => {
    finalVoronoiMetadata[place.placeID].percentOfTotalPopulation = place.totalPopulation / totalPopulation || 0;
    finalVoronoiMetadata[place.placeID].percentOfTotalJobs = place.totalJobs / totalJobs || 0;
  });

  let finalNeighborhoods = {};
  Object.values(finalVoronoiMetadata).forEach((place) => {
    let id = place.placeID;

    if (neighborhoods[id] && neighborhoods[id].tags && neighborhoods[id].tags.aeroway && neighborhoods[id].tags.aeroway == 'terminal') {
      id = "AIR_Terminal_" + terminalTicker;
      terminalTicker++;
    }

    finalNeighborhoods[place.placeID] = {
      id: id,
      location: centersOfNeighborhoods[place.placeID],
      jobs: place.totalJobs,
      residents: place.totalPopulation,
      popIds: [],
    };
  });

  const places = Object.values(finalVoronoiMetadata);

  const workerCount = perfConfig.workerThreads > 0 ? perfConfig.workerThreads :
      perfConfig.workerThreads === -1 ? os.cpus().length :
          Math.max(1, os.cpus().length - 1);
  const demandPool = new Piscina({
    filename: join(__dirname, 'demand_worker.js'),
    minThreads: workerCount,
    maxThreads: workerCount,
  });

  const demandBatchSize = 100;
  const totalDemandBatches = Math.ceil(places.length / demandBatchSize);
  let neighborhoodConnections = [];

  for (let batchNum = 0; batchNum < totalDemandBatches; batchNum++) {
    const startIdx = batchNum * demandBatchSize;
    const batch = places.slice(startIdx, startIdx + demandBatchSize);

    const workerBatchSize = Math.ceil(batch.length / workerCount);
    const workerTasks = [];

    for (let i = 0; i < batch.length; i += workerBatchSize) {
      const workerBatch = batch.slice(i, i + workerBatchSize);
      workerTasks.push(
          demandPool.run({
            originPlaces: workerBatch,
            allPlaces: places,
            centersOfNeighborhoods,
          })
      );
    }

    const results = await Promise.all(workerTasks);

    results.forEach(batchConnections => {
      for (let j = 0; j < batchConnections.length; j++) {
        neighborhoodConnections.push(batchConnections[j]);
      }
    });
  }

  await demandPool.destroy();

  const filteredConnections = [];
  const filterBatchSize = 100000;
  const totalFilterBatches = Math.ceil(neighborhoodConnections.length / filterBatchSize);

  let idCounter = 0;
  for (let batchNum = 0; batchNum < totalFilterBatches; batchNum++) {
    const startIdx = batchNum * filterBatchSize;
    const batch = neighborhoodConnections.slice(startIdx, startIdx + filterBatchSize);

    batch.forEach((connection) => {
      if (connection.size > 0) {
        const id = idCounter.toString();
        finalNeighborhoods[connection.jobId].popIds.push(id);
        finalNeighborhoods[connection.residenceId].popIds.push(id);
        filteredConnections.push({
          ...connection,
          id,
        });
        idCounter++;
      }
    });

    await new Promise(resolve => setImmediate(resolve));
  }

  neighborhoodConnections = filteredConnections;

  const terminalBatchSize = 50000;
  const totalTerminalBatches = Math.ceil(neighborhoodConnections.length / terminalBatchSize);

  for (let batchNum = 0; batchNum < totalTerminalBatches; batchNum++) {
    const startIdx = batchNum * terminalBatchSize;
    const endIdx = Math.min(startIdx + terminalBatchSize, neighborhoodConnections.length);

    for (let i = startIdx; i < endIdx; i++) {
      const connection = neighborhoodConnections[i];
      connection.residenceId = finalNeighborhoods[connection.residenceId].id;
      connection.jobId = finalNeighborhoods[connection.jobId].id;
    }

    await new Promise(resolve => setImmediate(resolve));
  }

  const stats = {
    totalPopulation,
    totalJobs,
    neighborhoods: Object.keys(finalNeighborhoods).length,
    connections: neighborhoodConnections.length,
    avgConnectionSize: Math.round(neighborhoodConnections.reduce((sum, c) => sum + c.size, 0) / (neighborhoodConnections.length || 1)),
    totalMovement: neighborhoodConnections.reduce((sum, c) => sum + c.size, 0),
  };

  return {
    points: Object.values(finalNeighborhoods),
    pops: neighborhoodConnections,
    stats,
  };
};

const processBuildings = async (place, rawBuildings) => {
  let minLon = 9999, minLat = 9999, maxLon = -999, maxLat = -999;

  const updateInterval = Math.max(1, Math.floor(rawBuildings.length / 100));
  rawBuildings.forEach((building, idx) => {
    building.geometry.forEach((coord) => {
      if (coord.lon < minLon) minLon = coord.lon;
      if (coord.lat < minLat) minLat = coord.lat;
      if (coord.lon > maxLon) maxLon = coord.lon;
      if (coord.lat > maxLat) maxLat = coord.lat;
    });

    if (idx % updateInterval === 0) { }
  });

  const horizontalDistance = calculateDistance([minLon, minLat], [maxLon, minLat]);
  const verticalDistance = calculateDistance([minLon, minLat], [minLon, maxLat]);

  const cellSizeMeters = 100;
  const cols = Math.ceil(horizontalDistance / cellSizeMeters);
  const rows = Math.ceil(verticalDistance / cellSizeMeters);

  const cellWidth = (maxLon - minLon) / cols;
  const cellHeight = (maxLat - minLat) / rows;

  const workerCount = perfConfig.workerThreads > 0 ? perfConfig.workerThreads :
      perfConfig.workerThreads === -1 ? os.cpus().length :
          Math.max(1, os.cpus().length - 1);
  const pool = new Piscina({
    filename: join(__dirname, 'building_worker.js'),
    minThreads: workerCount,
    maxThreads: workerCount,
  });

  const batchSize = 50000;
  const totalBatches = Math.ceil(rawBuildings.length / batchSize);
  let processedBuildings = {};

  const startTime = Date.now();

  for (let batchNum = 0; batchNum < totalBatches; batchNum++) {
    const batchStartTime = Date.now();
    const startIdx = batchNum * batchSize;
    const batch = rawBuildings.slice(startIdx, startIdx + batchSize);

    const workerBatchSize = Math.ceil(batch.length / workerCount);
    const workerTasks = [];

    for (let i = 0; i < batch.length; i += workerBatchSize) {
      const workerBatch = batch.slice(i, i + workerBatchSize);
      workerTasks.push(
          pool.run({
            buildings: workerBatch,
            startIdx: startIdx + i,
            minLon, maxLon, minLat, maxLat,
            cellWidth, cellHeight, cols, rows,
          })
      );
    }

    const results = await Promise.all(workerTasks);

    results.forEach(batchResult => {
      batchResult.forEach(building => {
        processedBuildings[building.id] = building;
      });
    });
  }

  await pool.destroy();

  let cellsDict = {};
  const buildingIdsForCells = Object.keys(processedBuildings);
  const cellBatchSize = 25000;
  const totalCellBatches = Math.ceil(buildingIdsForCells.length / cellBatchSize);

  for (let batchNum = 0; batchNum < totalCellBatches; batchNum++) {
    const startIdx = batchNum * cellBatchSize;
    const endIdx = Math.min(startIdx + cellBatchSize, buildingIdsForCells.length);

    for (let i = startIdx; i < endIdx; i++) {
      const id = buildingIdsForCells[i];
      const building = processedBuildings[id];
      const buildingCoord = `${building.xCellCoord},${building.yCellCoord}`;
      if (!cellsDict[buildingCoord]) cellsDict[buildingCoord] = [];
      cellsDict[buildingCoord].push(building.id);
    }

    await new Promise(resolve => setImmediate(resolve));
  }

  let maxDepth = 1;
  const optimizedBuildings = [];
  const buildingIdsForOptimize = Object.keys(processedBuildings);
  const optimizeBatchSize = 100000;
  const totalOptimizeBatches = Math.ceil(buildingIdsForOptimize.length / optimizeBatchSize);

  for (let batchNum = 0; batchNum < totalOptimizeBatches; batchNum++) {
    const startIdx = batchNum * optimizeBatchSize;
    const batchIds = buildingIdsForOptimize.slice(startIdx, startIdx + optimizeBatchSize);

    batchIds.forEach(id => {
      const building = processedBuildings[id];

      if (building.tags['building:levels:underground'] && Number(building.tags['building:levels:underground']) > maxDepth) {
        maxDepth = Number(building.tags['building:levels:underground']);
      }

      const simplePolygon = [[
        [building.bbox.minLon, building.bbox.minLat],
        [building.bbox.maxLon, building.bbox.minLat],
        [building.bbox.maxLon, building.bbox.maxLat],
        [building.bbox.minLon, building.bbox.maxLat],
        [building.bbox.minLon, building.bbox.minLat],
      ]];

      optimizedBuildings.push({
        minX: building.bbox.minLon,
        minY: building.bbox.minLat,
        maxX: building.bbox.maxLon,
        maxY: building.bbox.maxLat,
        foundationDepth: building.tags['building:levels:underground'] ? Number(building.tags['building:levels:underground']) : 1,
        polygon: simplePolygon,
      });

      delete processedBuildings[id];
    });

    await new Promise(resolve => setImmediate(resolve));
  }

  const optimizedIndex = await optimizeIndex({
    cellHeightCoords: cellHeight,
    minLon, minLat, maxLon, maxLat,
    cols, rows,
    cells: cellsDict,
    buildings: optimizedBuildings,
    maxDepth,
  });

  return optimizedIndex;
};

const readMsgpackBinary = async (filePath) => {
  const binary = fs.readFileSync(filePath);
  return msgpackDecode(binary);
};

const readJsonFileStreaming = (filePath) => {
  return new Promise((resolve, reject) => {
    const parseStream = createParseStream();
    let jsonData;

    parseStream.on('data', (data) => {
      jsonData = data;
    });

    parseStream.on('end', () => {
      resolve(jsonData);
    });

    parseStream.on('error', (err) => {
      reject(err);
    });

    fs.createReadStream(filePath).pipe(parseStream);
  });
};

const streamJsonArray = (filePath, onChunk, chunkSize = perfConfig.batchSizes.buildings) => {
  return new Promise((resolve, reject) => {
    const pipeline = chain([
      fs.createReadStream(filePath),
      StreamArray.withParser(),
    ]);

    let buffer = [];
    let totalProcessed = 0;

    pipeline.on('data', (data) => {
      buffer.push(data.value);

      if (buffer.length >= chunkSize) {
        const chunk = buffer;
        buffer = [];
        totalProcessed += chunk.length;
        onChunk(chunk, totalProcessed);
      }
    });

    pipeline.on('end', () => {
      if (buffer.length > 0) {
        totalProcessed += buffer.length;
        onChunk(buffer, totalProcessed);
      }
      resolve(totalProcessed);
    });

    pipeline.on('error', reject);
  });
};

const writeJsonFileStreaming = (filePath, data, progressCallback) => {
  return new Promise((resolve, reject) => {
    const writeStream = fs.createWriteStream(filePath, { encoding: 'utf8', highWaterMark: 1024 * 1024 });

    writeStream.on('error', reject);
    writeStream.on('finish', resolve);

    const writeWithBackpressure = (chunk) => {
      return new Promise((resolveWrite) => {
        if (!writeStream.write(chunk)) {
          writeStream.once('drain', resolveWrite);
        } else {
          resolveWrite();
        }
      });
    };

    if (data && typeof data === 'object') {
      (async () => {
        try {
          await writeWithBackpressure('{');
          const keys = Object.keys(data);

          for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            if (i > 0) await writeWithBackpressure(',');

            await writeWithBackpressure(`"${key}":`);

            if (Array.isArray(data[key])) {
              await writeWithBackpressure('[');

              for (let j = 0; j < data[key].length; j++) {
                if (j > 0) await writeWithBackpressure(',');
                await writeWithBackpressure(JSON.stringify(data[key][j]));

                if (progressCallback && j % 5000 === 0 && data[key].length > 10000) {
                  progressCallback(key, j, data[key].length);
                }
              }

              await writeWithBackpressure(']');
              if (progressCallback && data[key].length > 10000) {
                progressCallback(key, data[key].length, data[key].length);
              }
            } else {
              await writeWithBackpressure(JSON.stringify(data[key]));
            }
          }

          writeStream.end('}');
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

const gzipFile = async (srcPath, destPath) => {
  await pipeline(
    fs.createReadStream(srcPath),
    zlib.createGzip({ level: zlib.constants.Z_BEST_COMPRESSION }),
    fs.createWriteStream(destPath)
  );
};

const processAllData = async (place) => {
  const rawBuildings = await readMsgpackBinary(`./raw-data/${place.code}/buildings.msgpack`);
  const rawPlaces = await readMsgpackBinary(`./raw-data/${place.code}/places.msgpack`);

  const processedBuildings = await processBuildings(place, rawBuildings);
  const processedConnections = await processPlaceConnections(place, rawBuildings, rawPlaces);

  const buildingsJsonPath = `./processed-data/${place.code}/buildings_index.json`;
  const buildingsGzPath = `./processed-data/${place.code}/buildings_index.json.gz`;
  await writeJsonFileStreaming(
      buildingsJsonPath,
      processedBuildings,
      () => {}
  );
  await gzipFile(buildingsJsonPath, buildingsGzPath);
  fs.unlinkSync(buildingsJsonPath);

  const rawRoadsGz = `./raw-data/${place.code}/roads.geojson.gz`;
  const rawRoads = `./raw-data/${place.code}/roads.geojson`;
  const procRoadsGz = `./processed-data/${place.code}/roads.geojson.gz`;
  const procRoadsTmp = `./processed-data/${place.code}/roads.geojson`;
  if (fs.existsSync(rawRoadsGz)) {
    fs.cpSync(rawRoadsGz, procRoadsGz);
  } else if (fs.existsSync(rawRoads)) {
    fs.cpSync(rawRoads, procRoadsTmp);
    await gzipFile(procRoadsTmp, procRoadsGz);
    fs.unlinkSync(procRoadsTmp);
  } else {
    const emptyTmp = `./processed-data/${place.code}/roads.geojson`;
    fs.writeFileSync(emptyTmp, JSON.stringify({ type: "FeatureCollection", features: [] }), 'utf8');
    await gzipFile(emptyTmp, procRoadsGz);
    fs.unlinkSync(emptyTmp);
  }

  const demandJsonPath = `./processed-data/${place.code}/demand_data.json`;
  const demandGzPath = `./processed-data/${place.code}/demand_data.json.gz`;
  await writeJsonFileStreaming(
      demandJsonPath,
      processedConnections,
      () => {}
  );
  await gzipFile(demandJsonPath, demandGzPath);
  fs.unlinkSync(demandJsonPath);

  return {
    ...processedConnections.stats,
    buildings: processedBuildings.stats.count
  };
};

if (!fs.existsSync('./processed-data')) fs.mkdirSync('./processed-data');

const processPlaces = async () => {
  const allStats = [];

  for (const place of config.places) {
    if (fs.existsSync(`./processed-data/${place.code}`)) {
      fs.rmSync(`./processed-data/${place.code}`, { recursive: true, force: true });
    }
    fs.mkdirSync(`./processed-data/${place.code}`);
    const stats = await processAllData(place);
    if (stats) {
      allStats.push({ place: place.name, ...stats });
    }
  }

  if (allStats.length > 0) {
    const totals = allStats.reduce((acc, stat) => ({
      population: acc.population + (stat.totalPopulation || 0),
      jobs: acc.jobs + (stat.totalJobs || 0),
      neighborhoods: acc.neighborhoods + (stat.neighborhoods || 0),
      connections: acc.connections + (stat.connections || 0),
      buildings: acc.buildings + (stat.buildings || 0),
      movement: acc.movement + (stat.totalMovement || 0),
    }), { population: 0, jobs: 0, neighborhoods: 0, connections: 0, buildings: 0, movement: 0 });

    return totals;
  }
};

processPlaces().catch(() => {
  process.exit(1);
});