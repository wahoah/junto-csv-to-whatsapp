/** io.sheets.gs — baseline estable por NOMBRE (usa CONFIG.SHEETS) **/

function getSS_() {
  if (!CONFIG || !CONFIG.SPREADSHEET_ID) {
    throw new Error("CONFIG.SPREADSHEET_ID no está definido en config.gs");
  }
  try {
    return SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID);
  } catch (e) {
    // Fallback (bound). Si tampoco hay, explota con mensaje claro.
    var ss = SpreadsheetApp.getActiveSpreadsheet() || SpreadsheetApp.getActive();
    if (ss) return ss;
    throw new Error("No pude abrir el Spreadsheet. Revisa CONFIG.SPREADSHEET_ID o ejecuta como bound script.");
  }
}

/** Obtiene/crea por NOMBRE de CONFIG.SHEETS[key]. Si headers se dan y la hoja está vacía, los escribe. */
function getOrCreateSheetByKey_(keyName, headersOpt) {
  var ss = getSS_();
  var name = (CONFIG.SHEETS && CONFIG.SHEETS[keyName]) || keyName;
  var sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    if (typeof logInfo === 'function') logInfo("Hoja creada", { key: keyName, name: name, gid: sh.getSheetId() });
  }

  if (headersOpt && headersOpt.length) {
    var lr = sh.getLastRow(), lc = sh.getLastColumn();
    if (lr === 0) {
      sh.appendRow(headersOpt);
    } else {
      var existing = lc > 0 ? sh.getRange(1,1,1,lc).getValues()[0] : [];
      var same = (existing.length === headersOpt.length) &&
                 headersOpt.every(function(h,i){ return String(existing[i]||'') === String(h); });
      if (!same && typeof logWarn === 'function') {
        logWarn("Headers diferentes en " + name + " (no se cambiarán automáticamente).",
                { existing: existing, expected: headersOpt });
      }
    }
  }
  return sh;
}

function ensureSheet(keyName, headers) {
  return getOrCreateSheetByKey_(keyName, headers);
}

function appendRows(keyName, rows2D) {
  if (!rows2D || !rows2D.length) return;
  var sh = getOrCreateSheetByKey_(keyName);
  sh.getRange(sh.getLastRow()+1, 1, rows2D.length, rows2D[0].length).setValues(rows2D);
}

function readAsObjects(sheet) {
  var lr = sheet.getLastRow(), lc = sheet.getLastColumn();
  if (lr < 2 || lc < 1) return [];
  var values = sheet.getRange(1,1,lr,lc).getValues();
  var headers = values[0].map(function(h){ return String(h||'').trim(); });
  var out = [];
  for (var r=1; r<values.length; r++) {
    var obj = {};
    for (var c=0; c<headers.length; c++) obj[headers[c]] = values[r][c];
    out.push(obj);
  }
  return out;
}

function upsertByKey(sheet, headers, keyName, objects) {
  var existing = readAsObjects(sheet);
  var keyToRow = new Map();
  existing.forEach(function(obj, i){
    var k = String(obj[keyName]||'').trim();
    if (k) keyToRow.set(k, i+2); // +2 por encabezado
  });

  var inserted=0, updated=0, toAppend=[];
  objects.forEach(function(obj){
    var k = String(obj[keyName]||'').trim();
    if (!k) return;
    var vals = headers.map(function(h){ return (obj[h]!==undefined) ? obj[h] : ''; });
    var rowIndex = keyToRow.get(k);
    if (rowIndex) {
      sheet.getRange(rowIndex,1,1,headers.length).setValues([vals]);
      updated++;
    } else {
      toAppend.push(vals);
      inserted++;
    }
  });

  if (toAppend.length) {
    var start = sheet.getLastRow()+1;
    sheet.getRange(start,1,toAppend.length,headers.length).setValues(toAppend);
  }
  return { inserted: inserted, updated: updated };
}


function writeIngestLog(entry) {
  var headers = CONFIG.INGEST_LOG_HEADERS ||
    ["file_name","source_bank","processed_at","rows_total","rows_ok","rows_err","rows_duplicate","sizeBytes","success_count","failed_count","pending_count","lastUpdated"];

  var sh = getOrCreateSheetByKey_("INGEST_LOG", headers);

  // Mapea dinámicamente según el header
  var row = headers.map(function(h){
    if (h === "processed_at") return new Date();
    return (entry && entry[h] !== undefined) ? entry[h] : (h === "sizeBytes" || h === "lastUpdated" ? "" : 0);
  });

  sh.appendRow(row);
}