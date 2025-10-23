// Worker for parallel building processing
import { parentPort } from 'worker_threads';

// Lightweight geometry functions (duplicated from main for worker isolation)
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

// Centroid removed - we now use fast bbox center

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

// Process a batch of buildings
export default ({ buildings, startIdx, minLon, maxLon, minLat, maxLat, cellWidth, cellHeight, cols, rows }) => {
  const processedBuildings = [];

  buildings.forEach((building, localIdx) => {
    const i = startIdx + localIdx;
    let minBuildingLon = 9999, minBuildingLat = 9999;
    let maxBuildingLon = -999, maxBuildingLat = -999;

    building.geometry.forEach((coord) => {
      if (coord.lon < minBuildingLon) minBuildingLon = coord.lon;
      if (coord.lat < minBuildingLat) minBuildingLat = coord.lat;
      if (coord.lon > maxBuildingLon) maxBuildingLon = coord.lon;
      if (coord.lat > maxBuildingLat) maxBuildingLat = coord.lat;
    });

    // Fast bbox center (good enough for grid cell assignment!)
    const buildingCenter = [
      (minBuildingLon + maxBuildingLon) / 2,
      (minBuildingLat + maxBuildingLat) / 2
    ];

    const [lon, lat] = buildingCenter;
    const xCell = Math.min(cols - 1, Math.max(0, Math.floor((lon - minLon) / cellWidth)));
    const yCell = Math.min(rows - 1, Math.max(0, Math.floor((lat - minLat) / cellHeight)));

    processedBuildings.push({
      bbox: { minLon: minBuildingLon, minLat: minBuildingLat, maxLon: maxBuildingLon, maxLat: maxBuildingLat },
      center: buildingCenter,
      tags: building.tags || {},
      id: i,
      xCellCoord: xCell,
      yCellCoord: yCell,
    });
  });

  return processedBuildings;
};

// Worker for calculating building populations/jobs
export const calculateBuildingStats = ({ buildings }) => {
  return buildings.map(building => {
    if (!building.tags.building) return null;

    const __coords = building.geometry.map((point) => [point.lon, point.lat]);
    if (__coords.length < 3) return null;
    if (__coords[0][0] !== __coords[__coords.length - 1][0] || __coords[0][1] !== __coords[__coords.length - 1][1]) {
      __coords.push(__coords[0]);
    }

    // Exact area for accurate population/jobs
    const buildingAreaSqMeters = calculateArea(__coords);
    let buildingAreaMultiplier = Math.max(Number(building.tags['building:levels']), 1);
    if (isNaN(buildingAreaMultiplier)) buildingAreaMultiplier = 1;
    const buildingArea = buildingAreaSqMeters * buildingAreaMultiplier * 10.7639;

    // Fast bbox center (good enough for neighborhood assignment!)
    let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity;
    for (const [lon, lat] of __coords) {
      if (lon < minLon) minLon = lon;
      if (lon > maxLon) maxLon = lon;
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
    }
    const buildingCenter = [(minLon + maxLon) / 2, (minLat + maxLat) / 2];

    if (squareFeetPerPopulation[building.tags.building]) {
      const approxPop = Math.floor(buildingArea / squareFeetPerPopulation[building.tags.building]);
      return {
        ...building,
        approxPop,
        buildingCenter,
      };
    } else if (squareFeetPerJob[building.tags.building]) {
      let approxJobs = Math.floor(buildingArea / squareFeetPerJob[building.tags.building]);
      if (building.tags.aeroway && building.tags.aeroway == 'terminal') {
        approxJobs *= 20;
      }
      return {
        ...building,
        approxJobs,
        buildingCenter,
      };
    }

    return null;
  }).filter(b => b !== null);
};

