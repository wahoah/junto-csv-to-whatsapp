/*************************************************
 * core.failed_queue.gs  —  sin dependencias rígidas
 * - id = reference_id (Pay ID)
 * - 1 fila por cobro (upsert), streak de días consecutivos
 * - Headers y nombres de hoja desde CONFIG
 **************************************************/

if (typeof CONFIG === 'undefined') {
  throw new Error('CONFIG no está definido. Revisa config.gs');
}

/** ========= Entrypoint público ========= **/
function run_build_failed_queue() {
  _logInfo('core.failed_queue > run_build_failed_queue: inicio');

  var ss = _getSpreadsheet_();

  var shMaster = _getSheetByKey_(ss, 'MASTER', CONFIG.MASTER_HEADERS);
  var shFQ     = _getSheetByKey_(ss, 'FAILED_QUEUE', CONFIG.FAILED_QUEUE_HEADERS);

  // Asegura headers exactos
  _ensureHeaders_(shFQ, CONFIG.FAILED_QUEUE_HEADERS);

  var masterRows = _readAsObjects_(shMaster);
  if (!masterRows.length) {
    _logInfo('core.failed_queue: MASTER vacío. Nada por hacer.');
    return { processed: 0, inserted: 0, updated: 0 };
  }

  // 1) Todos los FAILED (de cualquier banco)
  var failedRows = masterRows.filter(function(r){
    return String(r.status || '').toUpperCase() === String(CONFIG.STATUSES.FAILED);
  });
  if (!failedRows.length) {
    _logInfo('core.failed_queue: no hay registros FAILED en MASTER.');
    return { processed: 0, inserted: 0, updated: 0 };
  }

  // 2) Índice actual por id para preservar metacampos
  var fqRows = _readAsObjects_(shFQ);
  var idxFQ  = new Map(fqRows.map(function(r){ return [String(r.id || '').trim(), r]; }));

  // 3) Utils
  var tz = Session.getScriptTimeZone();
  var today = new Date();
  var todayStr = Utilities.formatDate(today, tz, 'yyyy-MM-dd');
  var toNum = function(v){
    if (v === null || v === undefined) return '';
    var n = Number(String(v).replace(/[^\d.-]/g, '').replace(',', '.'));
    return Number.isFinite(n) ? n : v;
  };

  // 4) Build payloads (DEDUP por id) + streak
  var byId = new Map();

  failedRows.forEach(function(src){
    // Clave estable = Pay ID mapeado a MASTER.reference_id
    var keyId = String(src.reference_id || src.lookup_key || src.composite_ref || '').trim();
    if (!keyId) return;

    var old = idxFQ.get(keyId) || {};

    // Streak por días consecutivos
    var lastFail = old.last_failed_at ? new Date(old.last_failed_at) : null;
    var streak = 1;
    if (lastFail) {
      var lastStr = Utilities.formatDate(lastFail, tz, 'yyyy-MM-dd');
      if (lastStr === todayStr) {
        streak = Number(old.consecutive_failed_days || 1); // mismo día: no sumes
      } else {
        var diffDays = Math.floor((today - lastFail) / (1000 * 60 * 60 * 24));
        streak = (diffDays === 1)
          ? Number(old.consecutive_failed_days || 1) + 1
          : 1;
      }
    }

    var row = {
      id: keyId,
      source_bank: src.source_bank || src.bank || '',
      reference_id: src.reference_id || '',
      amount: toNum(src.amount),
      currency: src.currency || (CONFIG.DEFAULTS_BY_BANK && CONFIG.DEFAULTS_BY_BANK._FALLBACK && CONFIG.DEFAULTS_BY_BANK._FALLBACK.currency) || 'USD',
      due_date: src.due_date || src.txn_date || src.date || src.operation_date || src.tx_date || '',
      customer_name: src.customer_name || '',
      concept: src.concept || src.description || '',
      lookup_key: src.lookup_key || '',
      status: CONFIG.STATUSES.FAILED,

      first_seen_at: old.first_seen_at || todayStr,
      days_overdue: old.days_overdue || '',
      retry_count: Number.isFinite(Number(old.retry_count)) ? Number(old.retry_count) : 0,
      wa_status: old.wa_status || 'PENDING',
      wa_sent_at: old.wa_sent_at || '',

      first_failed_at: old.first_failed_at || todayStr,
      last_failed_at: todayStr,
      consecutive_failed_days: streak,
      error_desc: src.error_desc || ''
    };

    byId.set(keyId, row); // dedupe en el mismo batch
  });

  var toUpsert = Array.from(byId.values());
  var res = _upsertByKey_(shFQ, CONFIG.FAILED_QUEUE_HEADERS, 'id', toUpsert);

  var out = { processed: toUpsert.length, inserted: res.inserted, updated: res.updated };
  _logInfo('core.failed_queue > run_build_failed_queue: fin ' + JSON.stringify(out));
  return out;
}

/** ========= Helpers internos ========= **/

function _getSpreadsheet_() {
  if (CONFIG && CONFIG.SPREADSHEET_ID) {
    return SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID);
  }
  var ss = SpreadsheetApp.getActiveSpreadsheet() || SpreadsheetApp.getActive();
  if (!ss) throw new Error('No hay Spreadsheet activo y falta CONFIG.SPREADSHEET_ID');
  return ss;
}

/** Obtiene hoja por KEY usando, si existe, tus helpers; si no, fallback local */
function _getSheetByKey_(ss, key, headersOpt) {
  // 1) Si tienes helpers globales, úsalos
  if (typeof getOrCreateSheetByKey_ === 'function') {
    return getOrCreateSheetByKey_(key, headersOpt);
  }
  // 2) Fallback local por nombre
  var name = (CONFIG.SHEETS && CONFIG.SHEETS[key]) || key;
  var sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    _logInfo('Hoja creada (fallback)', { key: key, name: name, gid: sh.getSheetId() });
  }
  if (headersOpt && headersOpt.length) {
    _ensureHeaders_(sh, headersOpt);
  }
  return sh;
}

function _ensureHeaders_(sh, headers) {
  var lc = sh.getLastColumn();
  var row = lc ? sh.getRange(1,1,1,lc).getValues()[0] : [];
  var same = (row.length === headers.length) && headers.every(function(h,i){ return String(row[i]||'') === String(h); });
  if (!same) {
    // Si ya hay datos y headers distintos, sobre-escribiría: preferimos reset si está vacío
    if (sh.getLastRow() <= 1) {
      sh.clear();
      sh.getRange(1,1,1,headers.length).setValues([headers]);
    } else {
      _logInfo('Headers distintos en ' + sh.getName() + ' (no se cambian automáticamente).', { existing: row, expected: headers });
    }
  }
}

function _readAsObjects_(sh) {
  var lr = sh.getLastRow(), lc = sh.getLastColumn();
  if (lr < 2 || lc < 1) return [];
  var values = sh.getRange(1,1,lr,lc).getValues();
  var headers = values[0].map(function(h){ return String(h || '').trim(); });
  var out = [];
  for (var r=1; r<values.length; r++) {
    var obj = {};
    for (var c=0; c<headers.length; c++) obj[headers[c]] = values[r][c];
    out.push(obj);
  }
  return out;
}

/** Upsert por clave única */
function _upsertByKey_(sh, headers, keyName, objects) {
  var existing = _readAsObjects_(sh);
  var keyToRow = new Map();
  existing.forEach(function(obj, i){
    var k = String(obj[keyName] || '').trim();
    if (k) keyToRow.set(k, i + 2); // +2 por encabezado
  });

  var inserted = 0, updated = 0;
  var toAppend = [];

  objects.forEach(function(obj){
    var k = String(obj[keyName] || '').trim();
    if (!k) return;
    var rowIndex = keyToRow.get(k);
    var vals = headers.map(function(h){ return (obj[h] !== undefined) ? obj[h] : ''; });
    if (rowIndex) {
      sh.getRange(rowIndex, 1, 1, headers.length).setValues([vals]);
      updated++;
    } else {
      toAppend.push(vals);
      inserted++;
    }
  });

  if (toAppend.length) {
    var start = sh.getLastRow() + 1;
    sh.getRange(start, 1, toAppend.length, headers.length).setValues(toAppend);
  }

  return { inserted: inserted, updated: updated };
}

function _logInfo(msg, meta) {
  if (typeof logInfo === 'function') return logInfo(msg, meta);
  console && console.log && console.log(msg, meta || '');
}