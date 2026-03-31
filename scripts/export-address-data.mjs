#!/usr/bin/env node

import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, '..');
const outDir = resolve(repoRoot, 'data');

const RDCK_ADDR = 'https://gis.rdck.bc.ca/server/rest/services/RDCK_Public_Web_Map_MIL/MapServer/0';
const RDKB_ADDRESS_TABLE = 'https://mapping.rdkb.com/server/rest/services/Planning/BaseLayersExternal/FeatureServer/260';
const RDKB_ADDRESS_POINTS = 'https://mapping.rdkb.com/server/rest/services/Planning/BaseLayersExternal/FeatureServer/261';
const CHUNK_SIZE = 200;

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      Accept: 'application/json'
    }
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for ${url}`);
  }
  const data = await response.json();
  if (data && data.error) {
    throw new Error(data.error.message || `ArcGIS error for ${url}`);
  }
  return data;
}

async function fetchObjectIds(layerUrl) {
  const url = `${layerUrl}/query?where=1%3D1&returnIdsOnly=true&f=json`;
  const data = await fetchJson(url);
  return (data.objectIds || []).slice().sort((a, b) => a - b);
}

async function fetchFeaturesByIds(layerUrl, objectIds, fields, returnGeometry = false) {
  const batches = [];
  for (let i = 0; i < objectIds.length; i += CHUNK_SIZE) {
    batches.push(objectIds.slice(i, i + CHUNK_SIZE));
  }

  const features = [];
  for (const batch of batches) {
    const url = `${layerUrl}/query?objectIds=${encodeURIComponent(batch.join(','))}&outFields=${encodeURIComponent(fields)}&returnGeometry=${returnGeometry ? 'true' : 'false'}&outSR=4326&f=json`;
    const data = await fetchJson(url);
    features.push(...(data.features || []));
    process.stdout.write(`Fetched ${features.length}/${objectIds.length} rows from ${layerUrl}\r`);
  }
  process.stdout.write('\n');
  return features;
}

function normalizePoint(feature, fieldName = null) {
  const geometry = feature.geometry || {};
  const x = geometry.x;
  const y = geometry.y;
  const value = fieldName ? feature.attributes?.[fieldName] : null;
  return {
    pointValue: value ?? null,
    lat: typeof y === 'number' ? y : null,
    lon: typeof x === 'number' ? x : null
  };
}

function normalizeRdck(feature) {
  const attrs = feature.attributes || {};
  const point = normalizePoint(feature);
  return {
    objectId: attrs.OBJECTID ?? null,
    address: attrs.ADDRESS || '',
    suite: attrs.SUITE || '',
    streetNo: attrs.STREETNO || '',
    streetName: attrs.STRNAME || '',
    streetType: attrs.STRTYPE || '',
    community: attrs.COMM || '',
    folio: attrs.FOLIO || '',
    lat: point.lat,
    lon: point.lon
  };
}

function normalizeRdkbTable(feature) {
  const attrs = feature.attributes || {};
  return {
    objectId: attrs.OBJECTID ?? null,
    addressId: attrs.AddressID ?? null,
    folio: attrs.folio || '',
    suite: attrs.apt_no || '',
    streetNo: attrs.street_no || '',
    streetName: attrs.street_name || '',
    city: attrs.city || '',
    postalCode: attrs.postal_code || ''
  };
}

function normalizeRdkbPoint(feature) {
  const attrs = feature.attributes || {};
  const point = normalizePoint(feature, 'OBJECTID_1');
  return {
    objectId: attrs.OBJECTID ?? null,
    addressObjectId: point.pointValue,
    fullAddress: attrs.FULL_ADDRESS || '',
    unitCount: attrs.Unit_Count ?? null,
    buildingAddress: attrs.Building_Address || '',
    buildingName: attrs.BUILDING_NAME || '',
    locality: attrs.LOCALITY || '',
    addressType: attrs.ADDRESS_POINT_TYPE || '',
    lat: point.lat,
    lon: point.lon
  };
}

function joinRdkb(tableRows, pointRows) {
  const pointByAddressId = new Map(pointRows.map(row => [String(row.addressObjectId), row]));
  return tableRows.map(row => {
    const point = pointByAddressId.get(String(row.objectId));
    const baseAddress = [row.streetNo, row.streetName].filter(Boolean).join(' ').trim();
    return {
      objectId: row.objectId,
      addressId: row.addressId,
      folio: row.folio,
      suite: row.suite,
      streetNo: row.streetNo,
      streetName: row.streetName,
      city: row.city,
      postalCode: row.postalCode,
      fullAddress: point?.fullAddress || [row.suite ? `${row.suite}-${baseAddress}` : baseAddress, row.city].filter(Boolean).join(', '),
      locality: point?.locality || row.city || '',
      unitCount: point?.unitCount ?? null,
      buildingAddress: point?.buildingAddress || '',
      buildingName: point?.buildingName || '',
      addressType: point?.addressType || '',
      lat: point?.lat ?? null,
      lon: point?.lon ?? null
    };
  });
}

async function main() {
  await mkdir(outDir, { recursive: true });

  const rdckIds = await fetchObjectIds(RDCK_ADDR);
  const rdckFeatures = await fetchFeaturesByIds(RDCK_ADDR, rdckIds, 'OBJECTID,ADDRESS,SUITE,STREETNO,STRNAME,STRTYPE,COMM,FOLIO', true);

  const rdckRows = rdckFeatures.map(normalizeRdck);
  let rdkbTableRows = [];
  let rdkbPointRows = [];
  let rdkbJoinedRows = [];
  const warnings = [];

  try {
    const [rdkbTableIds, rdkbPointIds] = await Promise.all([
      fetchObjectIds(RDKB_ADDRESS_TABLE),
      fetchObjectIds(RDKB_ADDRESS_POINTS)
    ]);
    const [rdkbTableFeatures, rdkbPointFeatures] = await Promise.all([
      fetchFeaturesByIds(RDKB_ADDRESS_TABLE, rdkbTableIds, 'OBJECTID,AddressID,folio,apt_no,street_no,street_name,city,postal_code', false),
      fetchFeaturesByIds(RDKB_ADDRESS_POINTS, rdkbPointIds, 'OBJECTID,OBJECTID_1,FULL_ADDRESS,Unit_Count,Building_Address,BUILDING_NAME,LOCALITY,ADDRESS_POINT_TYPE', true)
    ]);
    rdkbTableRows = rdkbTableFeatures.map(normalizeRdkbTable);
    rdkbPointRows = rdkbPointFeatures.map(normalizeRdkbPoint);
    rdkbJoinedRows = joinRdkb(rdkbTableRows, rdkbPointRows);
  } catch (error) {
    warnings.push(`RDKB export skipped: ${error.message}. Their bulk FeatureServer requests are blocked for this non-browser client.`);
  }

  const manifest = {
    exportedAt: new Date().toISOString(),
    counts: {
      rdck: rdckRows.length,
      rdkbTable: rdkbTableRows.length,
      rdkbPoints: rdkbPointRows.length,
      rdkbJoined: rdkbJoinedRows.length
    },
    warnings
  };

  const writes = [
    writeFile(resolve(outDir, 'rdck-addresses.json'), JSON.stringify(rdckRows, null, 2) + '\n'),
    writeFile(resolve(outDir, 'address-export-manifest.json'), JSON.stringify(manifest, null, 2) + '\n')
  ];
  if (rdkbTableRows.length) {
    writes.push(writeFile(resolve(outDir, 'rdkb-address-table.json'), JSON.stringify(rdkbTableRows, null, 2) + '\n'));
    writes.push(writeFile(resolve(outDir, 'rdkb-address-points.json'), JSON.stringify(rdkbPointRows, null, 2) + '\n'));
    writes.push(writeFile(resolve(outDir, 'rdkb-addresses.json'), JSON.stringify(rdkbJoinedRows, null, 2) + '\n'));
  }
  await Promise.all(writes);

  console.log(`Wrote ${rdckRows.length} RDCK rows to ${outDir}`);
  if (warnings.length) console.warn(warnings.join('\n'));
}

main().catch(error => {
  console.error(error.message);
  process.exitCode = 1;
});
