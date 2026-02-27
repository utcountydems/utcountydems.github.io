const https = require('https');

function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { timeout: 30000 }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { reject(new Error('JSON parse error: ' + e.message + '\nData start: ' + data.slice(0, 200))); }
      });
    }).on('error', reject).on('timeout', () => reject(new Error('Request timeout')));
  });
}

// Ray-casting point-in-polygon
// ring is array of [lng, lat] pairs (closed ring)
// pt is [lng, lat]
function pointInPolygon(pt, ring) {
  const x = pt[0], y = pt[1];
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    const intersect = ((yi > y) !== (yj > y)) &&
                      (x < (xj - xi) * (y - yi) / (yj - yi) + xi);
    if (intersect) inside = !inside;
  }
  return inside;
}

async function main() {
  // --- Step 1: Fetch county boundary ---
  console.log('Fetching Utah County boundary...');
  const countyUrl = "https://services1.arcgis.com/99lidPhWCzftIe9K/ArcGIS/rest/services/UtahCountyBoundaries/FeatureServer/0/query?where=NAME%3D'UTAH'&outFields=NAME&returnGeometry=true&outSR=4326&geometryPrecision=4&f=geojson";
  const countyData = await fetchJSON(countyUrl);

  const feature = countyData.features[0];
  const geomType = feature.geometry.type;
  console.log('County geometry type:', geomType);

  // Get outer ring of first polygon
  let outerRing;
  if (geomType === 'Polygon') {
    outerRing = feature.geometry.coordinates[0];
  } else if (geomType === 'MultiPolygon') {
    // Use the largest polygon (most vertices) as the outer ring
    let maxLen = 0;
    for (const poly of feature.geometry.coordinates) {
      if (poly[0].length > maxLen) {
        maxLen = poly[0].length;
        outerRing = poly[0];
      }
    }
    console.log('MultiPolygon: using largest ring with', outerRing.length, 'vertices');
  }
  console.log('Outer ring vertex count:', outerRing.length);
  console.log('First vertex of county ring:', outerRing[0]);

  // --- Step 2: Fetch all precincts in batches of 100 ---
  const baseUrl = 'https://services1.arcgis.com/9DapJHuwsEakbYuW/arcgis/rest/services/Utah_County_-_Current_Precincts/FeatureServer/4/query?where=1%3D1&outFields=PRECINCTID&returnGeometry=true&outSR=4326&geometryPrecision=4&f=json&resultRecordCount=100&resultOffset=';

  const allFeatures = [];
  const offsets = [0, 100, 200, 300, 400, 500];

  for (const offset of offsets) {
    console.log(`Fetching precincts offset=${offset}...`);
    const data = await fetchJSON(baseUrl + offset);
    const features = data.features || [];
    console.log(`  Got ${features.length} features (exceededTransferLimit: ${data.exceededTransferLimit})`);
    allFeatures.push(...features);
  }

  console.log(`Total precincts fetched: ${allFeatures.length}`);

  // --- Step 3: For each precinct, get first vertex of first ring ---
  let insideCount = 0;
  let outsideCount = 0;
  const outsideExamples = [];
  const insideExamples = [];

  for (const f of allFeatures) {
    const geom = f.geometry;
    if (!geom || !geom.rings || geom.rings.length === 0) {
      console.log('WARNING: feature missing geometry:', f.attributes && f.attributes.PRECINCTID);
      outsideCount++;
      continue;
    }
    // First vertex of first ring
    const firstVertex = geom.rings[0][0];
    const inside = pointInPolygon(firstVertex, outerRing);
    if (inside) {
      insideCount++;
      if (insideExamples.length < 3) insideExamples.push({ id: f.attributes.PRECINCTID, vertex: firstVertex });
    } else {
      outsideCount++;
      if (outsideExamples.length < 5) outsideExamples.push({ id: f.attributes.PRECINCTID, vertex: firstVertex });
    }
  }

  console.log('\n=== RESULTS ===');
  console.log('Inside county polygon:', insideCount);
  console.log('Outside county polygon:', outsideCount);
  console.log('Total tested:', insideCount + outsideCount);

  if (outsideExamples.length > 0) {
    console.log('\nFirst few OUTSIDE examples:');
    outsideExamples.forEach(e => console.log(' ', e.id, e.vertex));
  }
  if (insideExamples.length > 0) {
    console.log('\nFirst few INSIDE examples:');
    insideExamples.forEach(e => console.log(' ', e.id, e.vertex));
  }
}

main().catch(err => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
