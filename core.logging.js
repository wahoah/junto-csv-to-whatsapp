// === API pública (deja estos nombres iguales) ===
function logInfo(message, meta)  { _logWrite_("INFO",  message, meta); }
function logWarn(message, meta)  { _logWrite_("WARN",  message, meta); }
function logError(message, meta) { _logWrite_("ERROR", message, meta); }

// === Impl segura ===
function _logWrite_(level, message, meta) {
  var payload = { ts: new Date(), level: level, message: String(message || ""), meta: meta || {} };

  // siempre consola
  var pfx = level === "ERROR" ? "[ERROR]" : (level === "WARN" ? "[WARN]" : "[INFO]");
  try { console.log(pfx, payload.message, payload.meta || ""); } catch (_) {}

  // ¿tienes desactivado el log en hoja?
  if (!CONFIG || !CONFIG.FLAGS || CONFIG.FLAGS.WRITE_LOG_SHEET !== true) return;

  try {
    var sh = _getLogSheet_();           // <— abre por getSS_ y crea si no existe
    if (!sh) return;                    // fallback silencioso

    // asegurar headers si vacía
    var HEADERS = (CONFIG.LOG_HEADERS && CONFIG.LOG_HEADERS.length) ? CONFIG.LOG_HEADERS : ["ts","level","message","meta"];
    if (sh.getLastRow() === 0) sh.appendRow(HEADERS);

    // escribir fila
    var row = [
      new Date(),
      level,
      payload.message,
      (typeof payload.meta === "object" ? JSON.stringify(payload.meta) : String(payload.meta || ""))
    ];
    sh.appendRow(row);
  } catch (e) {
    try { console.warn("[LOGSHEET ERR]", e && e.message ? e.message : String(e)); } catch(_) {}
    // nunca lanzar — el pipeline debe continuar
  }
}

function _getLogSheet_() {
  try {
    // intenta usar tu helper central
    if (typeof getOrCreateSheetByKey_ === "function") {
      return getOrCreateSheetByKey_("LOGS", (CONFIG.LOG_HEADERS || ["ts","level","message","meta"]));
    }
    // fallback local: abrir por ID y crear por nombre
    var ss = (typeof getSS_ === "function") ? getSS_() : SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID);
    if (!ss) return null;
    var name = (CONFIG.SHEETS && CONFIG.SHEETS.LOGS) ? CONFIG.SHEETS.LOGS : "LOGS";
    var sh = ss.getSheetByName(name);
    if (!sh) {
      sh = ss.insertSheet(name);
      var HEADERS = (CONFIG.LOG_HEADERS && CONFIG.LOG_HEADERS.length) ? CONFIG.LOG_HEADERS : ["ts","level","message","meta"];
      sh.appendRow(HEADERS);
    }
    return sh;
  } catch (e) {
    try { console.warn("[LOGSHEET ERR]", e && e.message ? e.message : String(e)); } catch(_) {}
    return null;
  }
}