/** ============================================================================
 * MAIN · ORCHESTRATOR
 * ----------------------------------------------------------------------------
 * Punto de entrada del pipeline.
 *  - setupSheetsOnce(): crea/verifica las hojas base y sus headers.
 *  - scanRawFiles(): lista archivos en la carpeta RAW por ID (solo log).
 *  - smokeTest_WriteToMASTER(): escribe 1 fila de prueba en MASTER.
 *  - run_setup_and_smoke(): corre los tres pasos anteriores.
 *
 * Requisitos:
 *  - CONFIG.SPREADSHEET_ID y CONFIG.RAW_FOLDER_ID definidos en config.gs
 *  - io.sheets.gs: ensureSheet(), appendRows()
 *  - io.drive.gs: listFilesInFolderById()
 *  - core.validate.gs: validateRowBasic()
 *  - core.consolidate.gs: consolidateToMasterRow()
 *  - core.logging.gs: logInfo(), logWarn(), logError()
 *  - core.idempotency.gs: buildInternalId()
 * ============================================================================ */

/** Crea/verifica hojas y headers base */
function setupSheetsOnce() {
  ensureSheet("MASTER", CONFIG.MASTER_HEADERS);
  ensureSheet("SCHEMA_MAP", ["bank","source_column","target_field","transform","required","notes"]);
  ensureSheet("CONFIG", ["key","value"]);
  ensureSheet("FAILED_QUEUE", CONFIG.FAILED_QUEUE_HEADERS);
  ensureSheet("INGEST_LOG", CONFIG.INGEST_LOG_HEADERS);
  ensureSheet("LOGS", ["ts","level","message","meta"]);
  logInfo("Sheets verificados/creados (headers listos).");
}
//  End of setupSheetsOnce()
/** Lista los archivos crudos en la carpeta RAW (por ID) y escribe logs con metadatos */
function scanRawFiles() {
  try {
    var files = listFilesInFolderById(CONFIG.RAW_FOLDER_ID);
    if (!files || files.length === 0) {
      logWarn("No se encontraron archivos en carpeta RAW.", { folderId: CONFIG.RAW_FOLDER_ID });
      return;
    }
    files.forEach(function(f){
      logInfo("Archivo detectado", {
        name: f.getName(),
        id: f.getId(),
        sizeBytes: f.getSize(),
        updated: f.getLastUpdated()
      });
    });
  } catch (e) {
    logError("scanRawFiles error", { err: e && e.message ? e.message : String(e) });
  }
}

/** Inserta 1 fila de prueba en MASTER para validar escritura/orden de columnas */
function smokeTest_WriteToMASTER() {
  var meta = {
    source_bank: "SMOKETEST",
    file_name: "dummy.csv",
    file_date: Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd"),
    row_number: 1
  };

  var mappedRow = {
    reference_id: "ST-REF-0001",
    amount: 100.50,
    currency: "USD",            // MVP: USD por defecto según CONFIG
    concept: "Smoke test row",
    customer_name: "JUAN PRUEBA",
    status: "PENDIENTE"
  };

  var validation = validateRowBasic(mappedRow);
  var row = consolidateToMasterRow(mappedRow, meta, validation);
  appendRows("MASTER", [row]);
  logInfo("Smoke test: 1 fila añadida a MASTER.", { id_preview: row[0] });
}

/** Orquestador mínimo para probar setup + lectura de carpeta + escritura a MASTER */
function run_setup_and_smoke() {
  logInfo("Inicio run_setup_and_smoke()");
  setupSheetsOnce();
  scanRawFiles();
  smokeTest_WriteToMASTER();
  logInfo("Fin run_setup_and_smoke()");
}

/** Corre todas las ingestas disponibles (adapters/*) de manera segura */
function run_ingest_all_adapters() {
  logInfo("run_ingest_all_adapters: inicio");
  const results = [];

  // === BANISTMO ===
  if (typeof run_ingest_banistmo_all === 'function') {
    try {
      const r = run_ingest_banistmo_all(); // <-- adapter.banistmo
      results.push({ adapter: 'BANISTMO', ok: true, res: r });
      logInfo("Adapter BANISTMO OK", r || {});
    } catch (e) {
      results.push({ adapter: 'BANISTMO', ok: false, err: String(e) });
      logError("Adapter BANISTMO ERROR", { err: e && e.message ? e.message : String(e) });
    }
  } else {
    logWarn("Adapter BANISTMO no disponible: run_ingest_banistmo_all() no está definido.");
  }

  // === BANCO GENERAL ===
  if (typeof run_ingest_banco_general_all === 'function') {
    try {
      const r2 = run_ingest_banco_general_all(); // <-- adapter.banco_general
      results.push({ adapter: 'BANCO_GENERAL', ok: true, res: r2 });
      logInfo("Adapter BANCO_GENERAL OK", r2 || {});
    } catch (e) {
      results.push({ adapter: 'BANCO_GENERAL', ok: false, err: String(e) });
      logError("Adapter BANCO_GENERAL ERROR", { err: e && e.message ? e.message : String(e) });
    }
  } else {
    logWarn("Adapter BANCO_GENERAL no disponible: run_ingest_banco_general_all() no está definido.");
  }

  logInfo("run_ingest_all_adapters: fin", results);
  return results;
}

/** Orquestador diario “real”: setup → escaneo → ingestas → failed_queue */
function run_daily_pipeline() {
  logInfo("run_daily_pipeline: inicio");
  // 1) Setup básico
  setupSheetsOnce();

  // 2) Escaneo de carpeta RAW (solo logging/visibilidad)
  scanRawFiles();

  // 3) Ingestas (adapters/*)
  run_ingest_all_adapters();

  // 4) Construir/actualizar FAILED_QUEUE desde MASTER (todos los bancos)
  if (typeof run_build_failed_queue === 'function') {
    run_build_failed_queue();
  } else {
    logWarn("run_build_failed_queue() no está disponible. ¿Agregaste core.failed_queue.gs?");
  }

  // 5) Enriquecer FAILED_QUEUE con info externa (ej. Airtable)
  if (typeof run_enrich_failed_queue_from_airtable === 'function') {
    try {
      run_enrich_failed_queue_from_airtable();
    } catch (e) {
      logError("run_enrich_failed_queue_from_airtable error", { err: e && e.message ? e.message : String(e) });
    }
  } else {
    logInfo("run_enrich_failed_queue_from_airtable() no está disponible. Importa core.failed_queue_enrich.js si deseas habilitarlo.");
  }

  logInfo("run_daily_pipeline: fin");
}

/* ============================================================================
 * PREPARADO PARA LA SIGUIENTE ETAPA (referencia, aún no implementado aquí):
 * ----------------------------------------------------------------------------
 * function ingestBanistmoOneFile_(file) {
 *   // 1) Leer CSV → rows
 *   // 2) Por cada fila: adapterBanistmo_mapRawRow(rawArray) → rawObj
 *   // 3) applyMapSimple("BANISTMO", rawObj, schemaMap) → mappedRow
 *   // 4) normalize/validate → consolidate → buffer OK/ERR
 *   // 5) appendRows("MASTER", okRows)
 *   // 6) writeIngestLog({...})
 * }
 *
 * function run_ingest_banistmo() {
 *   setupSheetsOnce();
 *   var schemaMap = loadSchemaMap();
 *   var files = listFilesInFolderById(CONFIG.RAW_FOLDER_ID, ".csv"); // opcional filtro
 *   // seleccionar 1 archivo banistmo por nombre/heurística y llamar ingestBanistmoOneFile_
 * }
 * ============================================================================ */
