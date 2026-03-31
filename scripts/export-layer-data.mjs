#!/usr/bin/env node

import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import process from 'node:process';

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, '..');
const defaultOutDir = resolve(repoRoot, 'data', 'layers');
const RDKB_BOOTSTRAP_URL = 'https://mapping.rdkb.com/portal/home/gallery.html?view=grid&sortOrder=desc&sortField=relevance&focus=layers';

const layerConfigs = {
  'rdck-address-points': {
    label: 'RDCK Address Points',
    sourceType: 'arcgis',
    transport: 'direct',
    url: 'https://gis.rdck.bc.ca/server/rest/services/RDCK_Public_Web_Map_MIL/MapServer/0',
    chunkSize: 500
  },
  'rdck-cadastre': {
    label: 'RDCK Cadastre - Property Lines',
    sourceType: 'arcgis',
    transport: 'direct',
    url: 'https://gis.rdck.bc.ca/server/rest/services/RDCK_Public_Web_Map_MIL/MapServer/8',
    chunkSize: 100
  },
  'rdkb-property-search': {
    label: 'RDKB Property Search',
    sourceType: 'arcgis',
    transport: 'browser',
    bootstrapUrl: RDKB_BOOTSTRAP_URL,
    url: 'https://mapping.rdkb.com/server/rest/services/Planning/Property_Search/FeatureServer/150',
    chunkSize: 100
  },
  'rdkb-parcels': {
    label: 'RDKB PMBC Parcels',
    sourceType: 'arcgis',
    transport: 'browser',
    bootstrapUrl: RDKB_BOOTSTRAP_URL,
    url: 'https://mapping.rdkb.com/server/rest/services/Planning/BaseLayersExternal/FeatureServer/253',
    chunkSize: 100
  },
  'rdkb-address-points': {
    label: 'RDKB Address Points',
    sourceType: 'arcgis',
    transport: 'browser',
    bootstrapUrl: RDKB_BOOTSTRAP_URL,
    url: 'https://mapping.rdkb.com/server/rest/services/Planning/BaseLayersExternal/FeatureServer/261',
    chunkSize: 500
  },
  'bc-parcelmap': {
    label: 'BC ParcelMap BC Parcel Fabric',
    sourceType: 'wfs',
    transport: 'direct',
    url: 'https://openmaps.gov.bc.ca/geo/pub/ows',
    typeName: 'pub:WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_SVW',
    chunkSize: 250
  }
};

function printUsage() {
  console.log(`Usage:
  npm run export:layers -- --list
  npm run export:layers -- --layers rdck-cadastre
  npm run export:layers -- --layers rdck-cadastre,rdkb-parcels --headed
  npm run export:layers -- --all --max-records 500

Options:
  --list               List supported layer keys.
  --layers a,b,c       Export specific layer keys.
  --all                Export every configured layer.
  --out-dir path       Output directory. Default: data/layers
  --chunk-size n       Override per-layer chunk size.
  --max-records n      Stop after n records per layer.
  --precision n        Geometry decimal precision. Default: 6
  --headed             Use a visible browser for protected sources.
  --headless           Force headless browser mode.
  --help               Show this help.
`);
}

function parseArgs(argv) {
  const options = {
    all: false,
    list: false,
    layers: [],
    outDir: defaultOutDir,
    chunkSize: null,
    maxRecords: null,
    precision: 6,
    headed: null
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    switch (arg) {
      case '--list':
        options.list = true;
        break;
      case '--all':
        options.all = true;
        break;
      case '--layers':
        options.layers.push(...String(argv[i + 1] || '').split(',').map(part => part.trim()).filter(Boolean));
        i += 1;
        break;
      case '--out-dir':
        options.outDir = resolve(repoRoot, argv[i + 1] || '');
        i += 1;
        break;
      case '--chunk-size':
        options.chunkSize = Number(argv[i + 1]);
        i += 1;
        break;
      case '--max-records':
        options.maxRecords = Number(argv[i + 1]);
        i += 1;
        break;
      case '--precision':
        options.precision = Number(argv[i + 1]);
        i += 1;
        break;
      case '--headed':
        options.headed = true;
        break;
      case '--headless':
        options.headed = false;
        break;
      case '--help':
      case '-h':
        printUsage();
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  if (!Number.isInteger(options.precision) || options.precision < 0 || options.precision > 12) {
    throw new Error('--precision must be an integer between 0 and 12.');
  }
  if (options.chunkSize !== null && (!Number.isInteger(options.chunkSize) || options.chunkSize <= 0)) {
    throw new Error('--chunk-size must be a positive integer.');
  }
  if (options.maxRecords !== null && (!Number.isInteger(options.maxRecords) || options.maxRecords <= 0)) {
    throw new Error('--max-records must be a positive integer.');
  }

  return options;
}

function listLayers() {
  const lines = Object.entries(layerConfigs).map(([key, layer]) => `${key.padEnd(22)} ${layer.label}`);
  console.log(lines.join('\n'));
}

function pickLayerKeys(options) {
  if (options.all) return Object.keys(layerConfigs);
  if (options.layers.length) {
    const invalid = options.layers.filter(key => !layerConfigs[key]);
    if (invalid.length) {
      throw new Error(`Unknown layer key(s): ${invalid.join(', ')}`);
    }
    return options.layers;
  }
  throw new Error('No layers selected. Use --layers or --all.');
}

function batchValues(values, size) {
  const batches = [];
  for (let index = 0; index < values.length; index += size) {
    batches.push(values.slice(index, index + size));
  }
  return batches;
}

function roundNumber(value, precision) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return value;
  const factor = 10 ** precision;
  return Math.round(value * factor) / factor;
}

function roundCoordinates(value, precision) {
  if (!Array.isArray(value)) return value;
  if (typeof value[0] === 'number') {
    return value.map(number => roundNumber(number, precision));
  }
  return value.map(item => roundCoordinates(item, precision));
}

function roundGeometry(geometry, precision) {
  if (!geometry) return null;
  return {
    ...geometry,
    coordinates: roundCoordinates(geometry.coordinates, precision)
  };
}

function signedRingArea(ring) {
  let sum = 0;
  for (let index = 0; index < ring.length - 1; index += 1) {
    const [x1, y1] = ring[index];
    const [x2, y2] = ring[index + 1];
    sum += (x1 * y2) - (x2 * y1);
  }
  return sum / 2;
}

function pointInRing(point, ring) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i, i += 1) {
    const xi = ring[i][0];
    const yi = ring[i][1];
    const xj = ring[j][0];
    const yj = ring[j][1];
    const intersects = ((yi > point[1]) !== (yj > point[1]))
      && (point[0] < ((xj - xi) * (point[1] - yi)) / ((yj - yi) || Number.EPSILON) + xi);
    if (intersects) inside = !inside;
  }
  return inside;
}

function arcGisPolygonToGeoJson(rings) {
  if (!rings?.length) return null;
  const outers = [];
  const holes = [];

  for (const ring of rings) {
    if (!ring?.length) continue;
    if (signedRingArea(ring) < 0) {
      outers.push([ring]);
    } else {
      holes.push(ring);
    }
  }

  if (!outers.length) {
    return {
      type: 'Polygon',
      coordinates: rings
    };
  }

  for (const hole of holes) {
    const samplePoint = hole[0];
    const target = outers.find(polygon => pointInRing(samplePoint, polygon[0]));
    if (target) {
      target.push(hole);
    } else {
      outers.push([hole]);
    }
  }

  if (outers.length === 1) {
    return {
      type: 'Polygon',
      coordinates: outers[0]
    };
  }

  return {
    type: 'MultiPolygon',
    coordinates: outers.map(polygon => [polygon[0], ...polygon.slice(1)])
  };
}

function arcGisGeometryToGeoJson(geometry, geometryType) {
  if (!geometry) return null;
  if (geometryType === 'esriGeometryPoint') {
    return {
      type: 'Point',
      coordinates: [geometry.x, geometry.y]
    };
  }
  if (geometryType === 'esriGeometryPolyline') {
    if (!geometry.paths?.length) return null;
    return geometry.paths.length === 1
      ? { type: 'LineString', coordinates: geometry.paths[0] }
      : { type: 'MultiLineString', coordinates: geometry.paths };
  }
  if (geometryType === 'esriGeometryPolygon') {
    return arcGisPolygonToGeoJson(geometry.rings);
  }
  if (geometryType === 'esriGeometryMultipoint') {
    return {
      type: 'MultiPoint',
      coordinates: geometry.points || []
    };
  }
  throw new Error(`Unsupported geometry type: ${geometryType}`);
}

function normalizeArcGisAttributes(attributes, fieldMap) {
  const result = {};
  for (const [key, value] of Object.entries(attributes || {})) {
    const field = fieldMap.get(key);
    if (field?.type === 'esriFieldTypeDate' && typeof value === 'number') {
      result[key] = Number.isFinite(value) ? new Date(value).toISOString() : value;
      continue;
    }
    result[key] = value;
  }
  return result;
}

function arcGisFeatureToGeoJson(feature, metadata, precision) {
  return {
    type: 'Feature',
    geometry: roundGeometry(arcGisGeometryToGeoJson(feature.geometry, metadata.geometryType), precision),
    properties: normalizeArcGisAttributes(feature.attributes || {}, metadata.fieldMap)
  };
}

function featureCollection(features) {
  return {
    type: 'FeatureCollection',
    features
  };
}

function partFileName(index) {
  return `part-${String(index + 1).padStart(4, '0')}.geojson`;
}

const directTransport = {
  kind: 'direct',
  async requestText(url, init = {}) {
    const response = await fetch(url, init);
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`HTTP ${response.status} ${response.statusText} for ${url}`);
    }
    return text;
  },
  async close() {}
};

async function getPlaywrightModule() {
  try {
    return await import('playwright');
  } catch (error) {
    throw new Error(`Playwright is required for browser-backed exports. Run "npm install" first. (${error.message})`);
  }
}

async function launchBrowser(browserModule, headless) {
  const { chromium } = browserModule;
  const attempts = [
    { channel: 'msedge' },
    { channel: 'chrome' },
    {}
  ];
  const errors = [];

  for (const attempt of attempts) {
    try {
      return await chromium.launch({
        headless,
        ...attempt
      });
    } catch (error) {
      const label = attempt.channel || 'bundled Chromium';
      errors.push(`${label}: ${error.message}`);
    }
  }

  throw new Error(`Unable to launch a browser for protected exports.\n${errors.join('\n')}`);
}

async function waitForChallenge(page) {
  await page.waitForLoadState('domcontentloaded', { timeout: 120000 }).catch(() => {});
  await page.waitForTimeout(3000);
  await page.waitForFunction(
    () => !/Just a moment/i.test(document.title),
    { timeout: 60000 }
  ).catch(() => {});
}

function buildWarmupUrl(url) {
  if (!url) return url;
  if (url.includes('/query')) return `${url.split('/query')[0]}?f=json`;
  return url;
}

async function createBrowserTransport({ bootstrapUrl, headless }) {
  const browserModule = await getPlaywrightModule();
  const browser = await launchBrowser(browserModule, headless);
  const context = await browser.newContext({
    viewport: { width: 1400, height: 1000 }
  });
  const page = await context.newPage();
  await page.goto(bootstrapUrl, { waitUntil: 'domcontentloaded', timeout: 120000 });
  await waitForChallenge(page);

  return {
    kind: 'browser',
    async requestText(url, init = {}) {
      const payload = {
        url,
        init: {
          method: init.method || 'GET',
          headers: init.headers || {},
          body: typeof init.body === 'string' ? init.body : (init.body ? String(init.body) : undefined)
        }
      };

      for (let attempt = 0; attempt < 2; attempt += 1) {
        const result = await page.evaluate(async ({ url: innerUrl, init: innerInit }) => {
          const response = await fetch(innerUrl, innerInit);
          const text = await response.text();
          return {
            ok: response.ok,
            status: response.status,
            statusText: response.statusText,
            text
          };
        }, payload);

        if (result.ok) return result.text;
        if (attempt === 0 && result.status === 403) {
          const warmupUrl = buildWarmupUrl(url);
          await page.goto(warmupUrl, { waitUntil: 'domcontentloaded', timeout: 120000 });
          await waitForChallenge(page);
          if ((payload.init.method || 'GET').toUpperCase() === 'GET' && warmupUrl === url) {
            const directText = await page.evaluate(() => document.body ? document.body.innerText : '');
            if (directText && directText.trim().startsWith('{')) return directText;
          }
          await page.goto(bootstrapUrl, { waitUntil: 'domcontentloaded', timeout: 120000 }).catch(() => {});
          await waitForChallenge(page);
          continue;
        }
        throw new Error(`HTTP ${result.status} ${result.statusText} for ${url}`);
      }

      throw new Error(`Unable to fetch ${url}`);
    },
    async close() {
      await context.close();
      await browser.close();
    }
  };
}

async function fetchJsonWithTransport(transport, url, init = {}) {
  const headers = {
    Accept: 'application/json',
    ...(init.headers || {})
  };
  const text = await transport.requestText(url, { ...init, headers });
  let data;
  try {
    data = JSON.parse(text);
  } catch (error) {
    throw new Error(`Expected JSON from ${url} but got: ${text.slice(0, 180)}...`);
  }
  if (data?.error) {
    throw new Error(data.error.message || `ArcGIS error for ${url}`);
  }
  return data;
}

async function postArcGisQuery(transport, layerUrl, params) {
  const body = new URLSearchParams(params).toString();
  return fetchJsonWithTransport(transport, `${layerUrl}/query`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
    },
    body
  });
}

async function fetchArcGisMetadata(transport, layerUrl) {
  const metadata = await fetchJsonWithTransport(transport, `${layerUrl}?f=json`);
  return {
    ...metadata,
    fieldMap: new Map((metadata.fields || []).map(field => [field.name, field]))
  };
}

async function fetchArcGisObjectIds(transport, layerUrl) {
  const data = await postArcGisQuery(transport, layerUrl, {
    where: '1=1',
    returnIdsOnly: 'true',
    f: 'json'
  });
  return (data.objectIds || []).slice().sort((left, right) => left - right);
}

async function fetchArcGisFeatureBatch(transport, layerUrl, objectIds) {
  const data = await postArcGisQuery(transport, layerUrl, {
    objectIds: objectIds.join(','),
    outFields: '*',
    returnGeometry: 'true',
    outSR: '4326',
    f: 'json'
  });
  return data.features || [];
}

async function exportArcGisLayer(layerKey, layer, transport, layerOutDir, options) {
  const metadata = await fetchArcGisMetadata(transport, layer.url);
  const objectIds = await fetchArcGisObjectIds(transport, layer.url);
  const selectedIds = options.maxRecords ? objectIds.slice(0, options.maxRecords) : objectIds;
  const chunkSize = options.chunkSize || layer.chunkSize || metadata.maxRecordCount || 100;
  const batches = batchValues(selectedIds, chunkSize);
  const parts = [];
  let exportedCount = 0;

  for (let index = 0; index < batches.length; index += 1) {
    const batchIds = batches[index];
    const features = await fetchArcGisFeatureBatch(transport, layer.url, batchIds);
    const geoJson = featureCollection(features.map(feature => arcGisFeatureToGeoJson(feature, metadata, options.precision)));
    const file = partFileName(index);
    await writeFile(resolve(layerOutDir, file), JSON.stringify(geoJson) + '\n');
    exportedCount += geoJson.features.length;
    parts.push({
      file,
      count: geoJson.features.length
    });
    console.log(`[${layerKey}] wrote ${file} (${geoJson.features.length} features, ${exportedCount}/${selectedIds.length})`);
  }

  const layerManifest = {
    exportedAt: new Date().toISOString(),
    layerKey,
    label: layer.label,
    sourceType: layer.sourceType,
    transport: layer.transport,
    url: layer.url,
    availableCount: objectIds.length,
    exportedCount,
    geometryType: metadata.geometryType,
    objectIdField: metadata.objectIdField || metadata.objectIdFieldName || null,
    displayField: metadata.displayField || metadata.displayFieldName || null,
    precision: options.precision,
    chunkSize,
    fields: (metadata.fields || []).map(field => ({
      name: field.name,
      alias: field.alias || field.name,
      type: field.type
    })),
    parts
  };

  await writeFile(resolve(layerOutDir, 'metadata.json'), JSON.stringify(layerManifest, null, 2) + '\n');
  return layerManifest;
}

function parseNumberMatched(xml) {
  const match = xml.match(/numberMatched="([^"]+)"/i);
  if (!match) return null;
  return match[1] === 'unknown' ? null : Number(match[1]);
}

async function fetchWfsHits(transport, layer) {
  const url = `${layer.url}?service=WFS&version=2.0.0&request=GetFeature&typeNames=${encodeURIComponent(layer.typeName)}&resultType=hits`;
  const xml = await transport.requestText(url);
  return {
    count: parseNumberMatched(xml),
    xml
  };
}

async function fetchWfsFeatureBatch(transport, layer, startIndex, count) {
  const url = `${layer.url}?service=WFS&version=2.0.0&request=GetFeature&typeNames=${encodeURIComponent(layer.typeName)}&outputFormat=application/json&srsName=EPSG:4326&startIndex=${startIndex}&count=${count}`;
  return fetchJsonWithTransport(transport, url);
}

async function exportWfsLayer(layerKey, layer, transport, layerOutDir, options) {
  const { count: totalAvailable } = await fetchWfsHits(transport, layer);
  const chunkSize = options.chunkSize || layer.chunkSize || 250;
  const targetCount = options.maxRecords || totalAvailable;
  const parts = [];
  let exportedCount = 0;
  let batchIndex = 0;
  let propertyNames = null;

  for (let offset = 0; targetCount === null || offset < targetCount; offset += chunkSize) {
    const limit = targetCount === null ? chunkSize : Math.min(chunkSize, targetCount - offset);
    if (limit <= 0) break;
    const data = await fetchWfsFeatureBatch(transport, layer, offset, limit);
    if (!data.features?.length) break;
    const roundedFeatures = data.features.map(feature => ({
      ...feature,
      geometry: roundGeometry(feature.geometry, options.precision)
    }));
    propertyNames ||= Object.keys(roundedFeatures[0]?.properties || {});
    const file = partFileName(batchIndex);
    await writeFile(resolve(layerOutDir, file), JSON.stringify(featureCollection(roundedFeatures)) + '\n');
    exportedCount += roundedFeatures.length;
    parts.push({
      file,
      count: roundedFeatures.length
    });
    batchIndex += 1;
    console.log(`[${layerKey}] wrote ${file} (${roundedFeatures.length} features, ${exportedCount}${targetCount ? `/${targetCount}` : ''})`);
    if (roundedFeatures.length < limit) break;
  }

  const layerManifest = {
    exportedAt: new Date().toISOString(),
    layerKey,
    label: layer.label,
    sourceType: layer.sourceType,
    transport: layer.transport,
    url: layer.url,
    typeName: layer.typeName,
    availableCount: totalAvailable,
    exportedCount,
    precision: options.precision,
    chunkSize,
    propertyNames: propertyNames || [],
    parts
  };

  await writeFile(resolve(layerOutDir, 'metadata.json'), JSON.stringify(layerManifest, null, 2) + '\n');
  return layerManifest;
}

async function exportLayer(layerKey, layer, transport, options) {
  const layerOutDir = resolve(options.outDir, layerKey);
  await mkdir(layerOutDir, { recursive: true });
  if (layer.sourceType === 'arcgis') {
    return exportArcGisLayer(layerKey, layer, transport, layerOutDir, options);
  }
  if (layer.sourceType === 'wfs') {
    return exportWfsLayer(layerKey, layer, transport, layerOutDir, options);
  }
  throw new Error(`Unsupported source type: ${layer.sourceType}`);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.list) {
    listLayers();
    return;
  }

  const layerKeys = pickLayerKeys(options);
  await mkdir(options.outDir, { recursive: true });

  let browserTransport = null;
  const results = [];
  const failures = [];

  try {
    for (const layerKey of layerKeys) {
      const layer = layerConfigs[layerKey];
      const transport = layer.transport === 'browser'
        ? (browserTransport ||= await createBrowserTransport({
            bootstrapUrl: layer.bootstrapUrl || RDKB_BOOTSTRAP_URL,
            headless: options.headed === null ? false : !options.headed
          }))
        : directTransport;

      try {
        results.push(await exportLayer(layerKey, layer, transport, options));
      } catch (error) {
        failures.push({
          layerKey,
          message: error.message
        });
        console.error(`[${layerKey}] ${error.message}`);
      }
    }
  } finally {
    await browserTransport?.close();
  }

  const manifest = {
    exportedAt: new Date().toISOString(),
    outDir: options.outDir,
    requestedLayers: layerKeys,
    maxRecords: options.maxRecords,
    precision: options.precision,
    layers: results,
    failures
  };

  await writeFile(resolve(options.outDir, 'layer-export-manifest.json'), JSON.stringify(manifest, null, 2) + '\n');

  if (failures.length) {
    process.exitCode = 1;
  }
}

main().catch(error => {
  console.error(error.message);
  process.exitCode = 1;
});
