// Worker for parallel demand calculation
const calculateDistance = (coord1, coord2) => {
  const [lon1, lat1] = coord1;
  const [lon2, lat2] = coord2;
  const R = 6371000; // Earth radius in meters
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
};

// Calculate connections for a batch of origin places
export default ({ originPlaces, allPlaces, centersOfNeighborhoods }) => {
  const connections = [];

  originPlaces.forEach((outerPlace) => {
    let totalAssigned = 0;
    const tempConnections = [];
    
    // First pass: calculate all connections
    allPlaces.forEach((innerPlace) => {
      let connectionSizeBasedOnJobsPercent = innerPlace.percentOfTotalJobs * outerPlace.totalPopulation;

      // Lower threshold to preserve more population (was 50)
      if (connectionSizeBasedOnJobsPercent <= 1) {
        return; // Skip only very small connections
      }

      const connectionDistance = calculateDistance(
        centersOfNeighborhoods[outerPlace.placeID],
        centersOfNeighborhoods[innerPlace.placeID]
      );
      const connectionSeconds = connectionDistance * 0.12;

      let totalSize = Math.round(connectionSizeBasedOnJobsPercent);
      totalAssigned += totalSize;
      let splits = Math.ceil(totalSize / 400);

      for (let k = 0; k < splits; k++) {
        tempConnections.push({
          residenceId: outerPlace.placeID,
          jobId: innerPlace.placeID,
          size: Math.round(totalSize / splits),
          drivingDistance: Math.round(connectionDistance),
          drivingSeconds: Math.round(connectionSeconds),
        });
      }
    });
    
    // Redistribute any lost population to preserve totals
    const lostPopulation = outerPlace.totalPopulation - totalAssigned;
    if (lostPopulation > 5 && tempConnections.length > 0) {
      // Distribute lost population proportionally across connections
      const perConnection = Math.floor(lostPopulation / tempConnections.length);
      const remainder = lostPopulation % tempConnections.length;
      
      tempConnections.forEach((conn, idx) => {
        conn.size += perConnection + (idx < remainder ? 1 : 0);
      });
    }
    
    // Use for loop for very large arrays (safer than spread or apply)
    for (let j = 0; j < tempConnections.length; j++) {
      connections.push(tempConnections[j]);
    }
  });

  return connections;
};

