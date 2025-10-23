/*************************************************
 * io.airtable.js — Wrapper ligero para Airtable
 *
 * Objetivo:
 *  - Centralizar GET requests a Airtable (paginación, retries básicos).
 *  - Exponer helpers reutilizables por otros módulos.
 *
 * NOTA: Este módulo asume ejecución en Google Apps Script.
 **************************************************/

if (typeof CONFIG === 'undefined') {
  throw new Error('CONFIG no está definido. Revisa config.js');
}

var _AIRTABLE_GLOBAL_CFG = CONFIG.AIRTABLE || {};
var _AIRTABLE_ENDPOINT = (_AIRTABLE_GLOBAL_CFG && _AIRTABLE_GLOBAL_CFG.ENDPOINT) || 'https://api.airtable.com/v0/';

function _airLogInfo(msg, meta) {
  if (typeof logInfo === 'function') return logInfo(msg, meta);
  if (console && console.log) console.log(msg, meta || '');
}

function _airLogWarn(msg, meta) {
  if (typeof logWarn === 'function') return logWarn(msg, meta);
  if (console && console.warn) console.warn(msg, meta || '');
}

function _airLogError(msg, meta) {
  if (typeof logError === 'function') return logError(msg, meta);
  if (console && console.error) console.error(msg, meta || '');
}

function _isStubEnabled(options) {
  if (options && Object.prototype.hasOwnProperty.call(options, 'useStub')) {
    return Boolean(options.useStub);
  }
  return Boolean(_AIRTABLE_GLOBAL_CFG && _AIRTABLE_GLOBAL_CFG.USE_STUB);
}

function _resolveStubFixtureKey(table, options) {
  if (options && options.stubFixture) return options.stubFixture;
  if (options && options.fixtureKey) return options.fixtureKey;

  var cfg = _AIRTABLE_GLOBAL_CFG || {};
  var fq = cfg.FAILED_QUEUE || {};

  // Permite usar alias → si el valor coincide con la tabla configurada para FAILED_QUEUE
  if (fq && fq.TABLE) {
    if (table === fq.TABLE) return fq.STUB_FIXTURE || 'airtable_failed_queue.sample';
  }

  // Si pasan directamente el nombre canónico (FAILED_QUEUE)
  if (table === 'FAILED_QUEUE' || table === 'failed_queue') {
    return fq.STUB_FIXTURE || 'airtable_failed_queue.sample';
  }

  return (cfg.DEFAULT_STUB_FIXTURE) || fq.STUB_FIXTURE || 'airtable_failed_queue.sample';
}

function _getStubPayload(fixtureKey) {
  if (!fixtureKey) return null;
  if (typeof AIRTABLE_STUB_RESPONSES === 'undefined') {
    _airLogWarn('Modo stub activo pero AIRTABLE_STUB_RESPONSES no está definido.', { fixtureKey: fixtureKey });
    return null;
  }
  var payload = AIRTABLE_STUB_RESPONSES[fixtureKey];
  if (!payload) {
    _airLogWarn('No se encontró fixture para Airtable stub.', { fixtureKey: fixtureKey });
    return null;
  }
  return payload;
}

function _filterStubRecordsByValues(records, fieldName, values) {
  if (!records || !records.length) return [];
  if (!values || !values.length) return records.slice();
  var lookup = {};
  for (var i = 0; i < values.length; i++) {
    var key = String(values[i] || '').trim();
    if (key) lookup[key] = true;
  }
  var out = [];
  for (var r = 0; r < records.length; r++) {
    var rec = records[r];
    var fields = rec && rec.fields ? rec.fields : {};
    var rawVal = fields[fieldName];
    if (rawVal === undefined || rawVal === null) continue;
    var norm = String(rawVal).trim();
    if (!norm) continue;
    if (lookup[norm]) out.push(rec);
  }
  return out;
}

function _resolveAirtableApiKey_(cfg) {
  cfg = cfg || _AIRTABLE_GLOBAL_CFG || {};
  if (cfg.API_KEY) return cfg.API_KEY;
  if (typeof PropertiesService !== 'undefined') {
    var key = PropertiesService.getScriptProperties().getProperty('AIRTABLE_API_KEY');
    if (key) return key;
  }
  return '';
}

function _cloneParams(obj) {
  var out = {};
  for (var k in obj) if (Object.prototype.hasOwnProperty.call(obj, k)) out[k] = obj[k];
  return out;
}

function _buildQueryString(params) {
  if (!params) return '';
  var parts = [];
  for (var key in params) {
    if (!Object.prototype.hasOwnProperty.call(params, key)) continue;
    var value = params[key];
    if (value === null || value === undefined || value === '') continue;
    if (key === 'fields' && value && value.length) {
      for (var i = 0; i < value.length; i++) {
        parts.push('fields[]=' + encodeURIComponent(value[i]));
      }
      continue;
    }
    parts.push(encodeURIComponent(key) + '=' + encodeURIComponent(value));
  }
  return parts.join('&');
}

function _uniqueValues(arr) {
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

function _airtableSleep(ms) {
  if (!ms || ms <= 0) return;
  if (typeof Utilities !== 'undefined' && typeof Utilities.sleep === 'function') {
    Utilities.sleep(ms);
  }
}

/**
 * fetchAirtableRecords({ table, params, baseId, apiKey, maxPages, maxRecords, pageSize })
 * Retorna { ok, records, offset, total, capped, status, error }
 */
function fetchAirtableRecords(options) {
  options = options || {};
  var table = options.table;
  if (!table) throw new Error('fetchAirtableRecords: options.table es requerido');

  if (_isStubEnabled(options)) {
    var stubKey = _resolveStubFixtureKey(table, options);
    var stubPayload = _getStubPayload(stubKey);
    if (stubPayload) {
      var stubRecords = stubPayload.records || [];
      var limit = options.maxRecords || 0;
      if (limit > 0 && stubRecords.length > limit) {
        stubRecords = stubRecords.slice(0, limit);
      }
      return {
        ok: true,
        records: stubRecords,
        total: stubRecords.length,
        stub: true,
        fixture: stubKey
      };
    }
    _airLogWarn('Modo stub activo pero sin payload; se intentará request real', { table: table, fixture: stubKey });
  }

  var cfg = _AIRTABLE_GLOBAL_CFG;
  var baseId = options.baseId || (cfg && cfg.BASE_ID);
  if (!baseId) {
    _airLogWarn('Airtable BASE_ID no configurado; se omite request', { table: table });
    return { ok: false, records: [], skipped: true, reason: 'missing_base_id' };
  }

  var apiKey = options.apiKey || _resolveAirtableApiKey_(cfg);
  if (!apiKey) {
    _airLogWarn('Airtable API_KEY no configurado; se omite request', { table: table });
    return { ok: false, records: [], skipped: true, reason: 'missing_api_key' };
  }

  if (typeof UrlFetchApp === 'undefined') {
    throw new Error('UrlFetchApp no está disponible. Ejecuta este código dentro de Apps Script.');
  }

  var params = _cloneParams(options.params || {});
  var pageSize = options.pageSize || params.pageSize || 100;
  params.pageSize = pageSize;

  var maxPages = options.maxPages || cfg.MAX_PAGES || 10;
  var maxRecords = options.maxRecords || 0;
  var rateMs = (typeof options.rateLimitMs === 'number') ? options.rateLimitMs : (cfg && cfg.RATE_LIMIT_MS) || 0;

  var baseUrl = _AIRTABLE_ENDPOINT.replace(/\/$/, '') + '/' +
    encodeURIComponent(baseId) + '/' + encodeURIComponent(table);

  var allRecords = [];
  var page = 0;
  var offset = params.offset || '';

  do {
    params.offset = offset || '';
    var query = _buildQueryString(params);
    var url = query ? baseUrl + '?' + query : baseUrl;

    var response = UrlFetchApp.fetch(url, {
      method: 'get',
      headers: {
        Authorization: 'Bearer ' + apiKey,
        Accept: 'application/json'
      },
      muteHttpExceptions: true
    });

    var status = response.getResponseCode();
    var text = response.getContentText();
    if (status >= 400) {
      _airLogWarn('Airtable respondió error', { status: status, body: text });
      return { ok: false, records: allRecords, status: status, error: text };
    }

    var json;
    try {
      json = JSON.parse(text);
    } catch (e) {
      _airLogError('No se pudo parsear respuesta de Airtable', { err: String(e), body: text });
      return { ok: false, records: allRecords, error: 'invalid_json' };
    }

    if (json.records && json.records.length) {
      allRecords = allRecords.concat(json.records);
    }

    offset = json.offset || '';
    page++;
    if (maxRecords && allRecords.length >= maxRecords) break;
    if (offset && rateMs) _airtableSleep(rateMs);
  } while (offset && page < maxPages);

  var capped = Boolean(offset);
  if (capped) {
    _airLogWarn('fetchAirtableRecords alcanzó límite de páginas', {
      table: table,
      maxPages: maxPages,
      fetched: allRecords.length
    });
  }

  return {
    ok: true,
    records: allRecords,
    offset: offset || null,
    total: allRecords.length,
    capped: capped
  };
}

/**
 * fetchAirtableRecordsByIds(ids, { table, lookupField, fields, view, chunkSize })
 *  - ids: array de valores a buscar (se limpia/normaliza).
 *  - lookupField: campo en Airtable usado para el OR().
 */
function fetchAirtableRecordsByIds(ids, options) {
  options = options || {};
  var table = options.table;
  if (!table) throw new Error('fetchAirtableRecordsByIds: options.table es requerido');

  var stubEnabled = _isStubEnabled(options);

  var lookupField = options.lookupField || 'reference_id';
  var fields = options.fields || [];
  if (!fields.length) fields = [lookupField];

  var cfg = _AIRTABLE_GLOBAL_CFG || {};
  var chunkSize = options.chunkSize || (cfg.FAILED_QUEUE && cfg.FAILED_QUEUE.MAX_IDS_PER_BATCH) || 10;
  var rateMs = (cfg && cfg.RATE_LIMIT_MS) || 0;

  var cleaned = [];
  if (ids && ids.length) {
    for (var i = 0; i < ids.length; i++) {
      var val = String(ids[i] || '').trim();
      if (val) cleaned.push(val);
    }
  }
  cleaned = _uniqueValues(cleaned);
  if (!cleaned.length) {
    return { ok: true, records: [], total: 0 };
  }

  if (stubEnabled) {
    var stubKey = _resolveStubFixtureKey(table, options);
    var stubPayload = _getStubPayload(stubKey);
    if (stubPayload) {
      var stubRecords = stubPayload.records || [];
      var filtered = _filterStubRecordsByValues(stubRecords, lookupField, cleaned);
      var projected = [];
      if (fields && fields.length) {
        for (var i = 0; i < filtered.length; i++) {
          var rec = filtered[i];
          var newFields = {};
          for (var f = 0; f < fields.length; f++) {
            var fieldName = fields[f];
            if (fieldName === '__recordId') continue;
            if (rec.fields && Object.prototype.hasOwnProperty.call(rec.fields, fieldName)) {
              newFields[fieldName] = rec.fields[fieldName];
            }
          }
          var clone = {
            id: rec.id,
            createdTime: rec.createdTime,
            fields: newFields
          };
          projected.push(clone);
        }
      } else {
        projected = filtered.slice();
      }
      return {
        ok: true,
        records: projected,
        total: projected.length,
        stub: true,
        fixture: stubKey
      };
    }
    _airLogWarn('Modo stub activo pero fixture no disponible; se usará llamada real', { table: table, fixture: stubKey });
  }

  var allRecords = [];
  for (var index = 0; index < cleaned.length; index += chunkSize) {
    var chunk = cleaned.slice(index, index + chunkSize);
    var filter = _buildFilterByFormula(chunk, lookupField);
    var params = {
      filterByFormula: filter,
      fields: fields.slice()
    };
    if (options.view) params.view = options.view;

    var res = fetchAirtableRecords({
      table: table,
      params: params,
      baseId: options.baseId,
      apiKey: options.apiKey,
      maxPages: options.maxPages || 1, // cada chunk debería caber en una página
      pageSize: options.pageSize || chunkSize
    });

    if (!res.ok) {
      return res;
    }

    if (res.records && res.records.length) {
      allRecords = allRecords.concat(res.records);
    }

    if (rateMs) _airtableSleep(rateMs);
  }

  return { ok: true, records: allRecords, total: allRecords.length };
}

function _buildFilterByFormula(values, fieldName) {
  if (!values.length) return '';
  var parts = [];
  for (var i = 0; i < values.length; i++) {
    parts.push('{' + fieldName + "}='" + _escapeFormulaValue(values[i]) + "'");
  }
  if (parts.length === 1) return parts[0];
  return 'OR(' + parts.join(',') + ')';
}

function _escapeFormulaValue(value) {
  return String(value).replace(/'/g, "\\'");
}

/**
 * Helper para obtener listado de campos solicitados para FAILED_QUEUE.
 */
function resolveFailedQueueSelectFields() {
  var cfg = (_AIRTABLE_GLOBAL_CFG && _AIRTABLE_GLOBAL_CFG.FAILED_QUEUE) || {};
  var fields = [];
  if (cfg.SELECT_FIELDS && cfg.SELECT_FIELDS.length) {
    fields = fields.concat(cfg.SELECT_FIELDS);
  }
  var map = cfg.FIELD_MAP || {};
  for (var key in map) {
    if (!Object.prototype.hasOwnProperty.call(map, key)) continue;
    var fieldName = map[key];
    if (!fieldName || fieldName === '__recordId') continue;
    if (fields.indexOf(fieldName) === -1) fields.push(fieldName);
  }
  var lookup = cfg.AIRTABLE_LOOKUP_FIELD;
  if (lookup && fields.indexOf(lookup) === -1) fields.push(lookup);
  return fields;
}
