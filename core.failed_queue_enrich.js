/*************************************************
 * core.failed_queue_enrich.js
 * - Enriquecimiento de FAILED_QUEUE usando Airtable.
 * - Diseñado para ejecutarse después de run_build_failed_queue().
 **************************************************/

if (typeof CONFIG === 'undefined') {
  throw new Error('CONFIG no está definido. Revisa config.js');
}

function run_enrich_failed_queue_from_airtable() {
  _logInfo('core.failed_queue_enrich > inicio');

  var globalCfg = CONFIG.AIRTABLE || {};
  var fqCfg = globalCfg.FAILED_QUEUE || {};
  var isStubMode = Boolean(globalCfg.USE_STUB);

  if (!globalCfg.BASE_ID && !isStubMode) {
    _logWarn('run_enrich_failed_queue_from_airtable omitido: falta CONFIG.AIRTABLE.BASE_ID');
    return { ok: false, skipped: true, reason: 'missing_base_id' };
  }

  if (!fqCfg.TABLE && !isStubMode) {
    _logWarn('run_enrich_failed_queue_from_airtable omitido: falta CONFIG.AIRTABLE.FAILED_QUEUE.TABLE');
    return { ok: false, skipped: true, reason: 'missing_table' };
  }

  if (typeof fetchAirtableRecordsByIds !== 'function' || typeof resolveFailedQueueSelectFields !== 'function') {
    throw new Error('Helpers de io.airtable.js no disponibles. ¿Importaste io.airtable.js?');
  }

  if (typeof _getSpreadsheet_ !== 'function' || typeof _getSheetByKey_ !== 'function' ||
      typeof _readAsObjects_ !== 'function' || typeof _upsertByKey_ !== 'function') {
    throw new Error('Helpers de core.failed_queue no disponibles. Confirma que core.failed_queue.js está cargado.');
  }

  var ss = _getSpreadsheet_();
  var shFQ = _getSheetByKey_(ss, 'FAILED_QUEUE', CONFIG.FAILED_QUEUE_HEADERS);
  var rows = _readAsObjects_(shFQ);
  if (!rows.length) {
    _logInfo('core.failed_queue_enrich: FAILED_QUEUE vacío. Nada por hacer.');
    return { ok: true, processed: 0, enriched: 0 };
  }

  var lookupColumn = fqCfg.SHEET_LOOKUP_COLUMN || 'reference_id';
  var lookupField = fqCfg.AIRTABLE_LOOKUP_FIELD || lookupColumn;
  var tableName = fqCfg.TABLE || 'FAILED_QUEUE';
  var ids = [];
  rows.forEach(function(row){
    var val = String(row[lookupColumn] || '').trim();
    if (val) ids.push(val);
  });
  ids = (typeof _uniqueValues === 'function') ? _uniqueValues(ids) : _uniqueValues_local(ids);
  if (!ids.length) {
    _logWarn('core.failed_queue_enrich: no se encontraron valores para lookup en FAILED_QUEUE', { lookupColumn: lookupColumn });
    return { ok: false, processed: rows.length, enriched: 0, reason: 'no_lookup_values' };
  }

  var selectFields = resolveFailedQueueSelectFields();
  var fetchRes = fetchAirtableRecordsByIds(ids, {
    table: tableName,
    view: fqCfg.VIEW,
    lookupField: lookupField,
    fields: selectFields,
    chunkSize: fqCfg.MAX_IDS_PER_BATCH || globalCfg.MAX_IDS_PER_BATCH,
    baseId: globalCfg.BASE_ID,
    apiKey: globalCfg.API_KEY,
    useStub: isStubMode,
    stubFixture: fqCfg.STUB_FIXTURE
  });

  if (!fetchRes.ok) {
    _logWarn('core.failed_queue_enrich: error al consultar Airtable', { reason: fetchRes.reason || fetchRes.error });
    return fetchRes;
  }

  var records = fetchRes.records || [];
  if (!records.length) {
    _logInfo('core.failed_queue_enrich: no hubo coincidencias en Airtable.');
    return { ok: true, processed: rows.length, enriched: 0 };
  }

  var fieldMap = fqCfg.FIELD_MAP || {};
  var byLookup = {};
  records.forEach(function(rec){
    var fields = rec.fields || {};
    var key = String(fields[lookupField] || '').trim();
    if (!key) return;
    byLookup[key] = { record: rec, fields: fields };
  });

  var tz = (typeof Session !== 'undefined' && Session.getScriptTimeZone) ? Session.getScriptTimeZone() : 'UTC';
  var now = new Date();
  var isoNow;
  if (typeof Utilities !== 'undefined' && Utilities.formatDate) {
    isoNow = Utilities.formatDate(now, tz, "yyyy-MM-dd'T'HH:mm:ss'Z'");
  } else if (typeof now.toISOString === 'function') {
    isoNow = now.toISOString();
  } else {
    isoNow = String(now);
  }

  var updates = [];
  rows.forEach(function(row){
    var lookupValue = String(row[lookupColumn] || '').trim();
    if (!lookupValue) return;
    var payload = byLookup[lookupValue];
    if (!payload) return;

    var updated = {};
    for (var key in row) {
      if (Object.prototype.hasOwnProperty.call(row, key)) {
        updated[key] = row[key];
      }
    }

    for (var sheetColumn in fieldMap) {
      if (!Object.prototype.hasOwnProperty.call(fieldMap, sheetColumn)) continue;
      var airtableField = fieldMap[sheetColumn];
      if (!airtableField) continue;
      if (airtableField === '__recordId') {
        updated[sheetColumn] = (payload.record && payload.record.id) ? payload.record.id : '';
        continue;
      }
      if (payload.fields.hasOwnProperty(airtableField)) {
        updated[sheetColumn] = payload.fields[airtableField];
      }
    }

    if (CONFIG.FAILED_QUEUE_HEADERS.indexOf('airtable_payload_json') !== -1) {
      try {
        updated.airtable_payload_json = JSON.stringify(payload.fields || {});
      } catch (e) {
        updated.airtable_payload_json = '';
      }
    }

    if (CONFIG.FAILED_QUEUE_HEADERS.indexOf('airtable_last_sync') !== -1) {
      updated.airtable_last_sync = isoNow;
    }

    updates.push(updated);
  });

  if (!updates.length) {
    _logInfo('core.failed_queue_enrich: no hubo filas que coincidan para actualizar.');
    return { ok: true, processed: rows.length, enriched: 0 };
  }

  var upsertRes = _upsertByKey_(shFQ, CONFIG.FAILED_QUEUE_HEADERS, 'id', updates);
  var out = {
    ok: true,
    processed: rows.length,
    matched: updates.length,
    inserted: upsertRes.inserted,
    updated: upsertRes.updated
  };
  _logInfo('core.failed_queue_enrich > fin ' + JSON.stringify(out));
  return out;
}

function _uniqueValues_local(arr) {
  var seen = {};
  var out = [];
  for (var i = 0; i < arr.length; i++) {
    var val = arr[i];
    if (!val || seen[val]) continue;
    seen[val] = true;
    out.push(val);
  }
  return out;
}

function _logInfo(msg, meta) {
  if (typeof logInfo === 'function') return logInfo(msg, meta);
  if (console && console.log) console.log(msg, meta || '');
}

function _logWarn(msg, meta) {
  if (typeof logWarn === 'function') return logWarn(msg, meta);
  if (console && console.warn) console.warn(msg, meta || '');
}

function _logError(msg, meta) {
  if (typeof logError === 'function') return logError(msg, meta);
  if (console && console.error) console.error(msg, meta || '');
}
