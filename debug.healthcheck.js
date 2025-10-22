/** debug.healthcheck.gs */

function run_healthcheck() {
  try {
    logInfo("HC: inicio");
    // 1) Abrir SS
    var ss = getSS_();
    logInfo("HC: SS abierto", { url: ss.getUrl() });

    // 2) Asegurar hojas base con headers desde CONFIG
    ensureSheet('MASTER', CONFIG.MASTER_HEADERS);
    ensureSheet('FAILED_QUEUE', CONFIG.FAILED_QUEUE_HEADERS);
    ensureSheet('LOGS', (CONFIG.LOG_HEADERS || ["ts","level","message","meta"]));
    logInfo("HC: hojas aseguradas");

    // 3) Escribir una fila dummy en MASTER
    var dummy = {};
    CONFIG.MASTER_HEADERS.forEach(function(h){ dummy[h] = ""; });
    dummy.id = "HC-"+Date.now();
    dummy.source_bank = "DEBUG";
    dummy.file_name = "hc.csv";
    dummy.file_date = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
    dummy.row_number = 1;
    dummy.reference_id = "PAYID-HC-001";
    dummy.amount = 1.23;
    dummy.currency = "USD";
    dummy.status = "PENDIENTE";
    dummy.processed_at = new Date();
    dummy.processed_by = "healthcheck";
    var rowMaster = CONFIG.MASTER_HEADERS.map(function(h){ return (dummy[h]!==undefined) ? dummy[h] : ""; });
    appendRows('MASTER', [rowMaster]);
    logInfo("HC: escribió dummy en MASTER");

    // 4) Leer MASTER como objetos
    var shMaster = getOrCreateSheetByKey_('MASTER');
    var objs = readAsObjects(shMaster);
    logInfo("HC: readAsObjects(MASTER) OK", { rows: objs.length });

    // 5) Probar upsertByKey en FAILED_QUEUE
    var shFQ = getOrCreateSheetByKey_('FAILED_QUEUE', CONFIG.FAILED_QUEUE_HEADERS);
    var up = upsertByKey(shFQ, CONFIG.FAILED_QUEUE_HEADERS, 'id', [{
      id: "PAYID-HC-001",
      source_bank: "DEBUG",
      reference_id: "PAYID-HC-001",
      amount: 1.23,
      currency: "USD",
      due_date: "",
      customer_name: "PRUEBA",
      concept: "HC",
      lookup_key: "",
      status: "FAILED",
      first_seen_at: Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd"),
      days_overdue: "",
      retry_count: 0,
      wa_status: "PENDING",
      wa_sent_at: "",
      first_failed_at: Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd"),
      last_failed_at: Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd"),
      consecutive_failed_days: 1
    }]);
    logInfo("HC: upsertByKey(FAILED_QUEUE) OK", up);

    logInfo("HC: fin OK");
  } catch (e) {
    logError("HC: ERROR", { err: e && e.message ? e.message : String(e) });
    throw e;
  }
}


function debug_ingest_log_write() {
  writeIngestLog({
    file_name: "banistmo_ejemplo_RECH.csv",
    source_bank: "BANISTMO",
    rows_total: 10,
    rows_ok: 9,
    rows_err: 1,
    rows_duplicate: 0,
    sizeBytes: 12345,
    lastUpdated: new Date()
  });
  logInfo("debug_ingest_log_write: OK");
}

function debug_test_logs() {
  logInfo("logger ok", { probe: true });
}