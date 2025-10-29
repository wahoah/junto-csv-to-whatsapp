/*************************************************
 * adapters/adapter.banco_general.gs
 * Ingesta CSV Banco General → MASTER + INGEST_LOG
 * Reglas:
 * - composite_ref = Observaciones
 * - status = SUCCESS si "descripción de error" vacío; si no, FAILED
 **************************************************/

function run_ingest_banco_general_all() {
  var files = listFilesInFolderById(CONFIG.RAW_FOLDER_ID, '.csv') || [];
  var totals = { files: 0, rows_total: 0, rows_ok: 0, rows_err: 0, rows_duplicate: 0, success_count: 0, failed_count: 0, pending_count: 0 };

  files.forEach(function (file) {
    // procesa solo si parece de Banco General (ajusta si lo prefieres estricto)
    if (!/general/i.test(file.getName())) return;

    try {
      totals.files++;
      logInfo("Procesando archivo Banco General", { name: file.getName(), id: file.getId() });

      var rows = _bg_parseCsvFile_(file);
      if (!rows.length) {
        writeIngestLog({
          file_name: file.getName(), source_bank: 'BANCO_GENERAL',
          rows_total: 0, rows_ok: 0, rows_err: 0, rows_duplicate: 0,
          sizeBytes: file.getSize(), lastUpdated: file.getLastUpdated()
        });
        return;
      }

      var header = rows[0].map(function (h) { return String(h || '').trim(); });
      var dataRows = rows.slice(1);
      totals.rows_total += dataRows.length;

      var buffer = [];

      dataRows.forEach(function (arr, idx) {
        try {
          var raw = _bg_rowToObj_(header, arr);
          var mapped = _bg_mapToMaster_(raw);  // ← aplica reglas de status/observaciones

          var s = (mapped.status || "").toUpperCase();
          if (s === "SUCCESS") totals.success_count++;
          else if (s === "FAILED") totals.failed_count++;
          else totals.pending_count++;


          var meta = {
            source_bank: "BANCO_GENERAL",
            file_name: file.getName(),
            file_date: Utilities.formatDate(file.getLastUpdated(), Session.getScriptTimeZone(), "yyyy-MM-dd"),
            row_number: idx + 2
          };

          var v = (typeof validateRowBasic === 'function') ? validateRowBasic(mapped) : {};
          var row = consolidateToMasterRow(mapped, meta, v);

          buffer.push(row);
          totals.rows_ok++;
        } catch (e) {
          totals.rows_err++;
          logWarn("Banco General fila inválida", { row: idx + 2, err: String(e) });
        }
      });

      if (buffer.length) appendRows('MASTER', buffer);

      writeIngestLog({
        file_name: file.getName(),
        source_bank: 'BANCO_GENERAL',
        rows_total: totals.rows_total,
        rows_ok: totals.rows_ok,
        rows_err: totals.rows_err,
        rows_duplicate: totals.rows_duplicate,
        sizeBytes: file.getSize(),
        lastUpdated: file.getLastUpdated(),
        success_count: totals.success_count,
        failed_count: totals.failed_count,
        pending_count: totals.pending_count
      });
    } catch (e) {
      if (typeof logError === 'function') {
        logError("run_ingest_banco_general_all: error procesando archivo", { name: file.getName(), err: String(e) });
      } else if (typeof Logger !== 'undefined' && Logger.log) {
        Logger.log("run_ingest_banco_general_all error procesamiento " + file.getName() + ": " + String(e));
      }
    } finally {
      moveFileToProcessed_(file, CONFIG.RAW_FOLDER_ID);
    }
  });

  logInfo("run_ingest_banco_general_all: resumen", totals);
  return totals;
}

/** ===== Helpers específicos Banco General ===== **/

function _bg_parseCsvFile_(file) {
  var text = file.getBlob().getDataAsString(); // cámbialo a 'ISO-8859-1' si tu CSV no es UTF-8
  var rows = Utilities.parseCsv(text);
  return rows.filter(function (r) { return r && r.length && r.join('').trim() !== ''; });
}

function _bg_rowToObj_(header, arr) {
  var obj = {};
  for (var i = 0; i < header.length; i++) obj[header[i]] = arr[i];
  return obj;
}

/** normaliza string */
function _norm_(s) {
  return String(s || '')
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/** extrae status bruto desde posibles columnas */
function _bg_extractRawStatus_(raw) {
  var keys = [
    'Descripción de error', 'Descripcion de error', 'Descripcion Error',
    'Status', 'Estado', 'Resultado'
  ];
  for (var i = 0; i < keys.length; i++) {
    if (raw.hasOwnProperty(keys[i])) return String(raw[keys[i]] || '');
  }
  return '';
}

/** Regla: si descripción de error está vacía → SUCCESS; si trae algo → FAILED */
function _bg_statusRule_(raw) {
  var err = String(raw['Descripción de error'] || raw['Descripcion de error'] || raw['Descripcion Error'] || '').trim();
  if (err === '') return CONFIG.STATUSES.SUCCESS;
  return CONFIG.STATUSES.FAILED;
}

/** intenta derivar reference_id desde Observaciones (composite_ref) si no viene explícito */
function _bg_extractRefFromObserv_(observ) {
  var s = String(observ || '').toUpperCase();
  var m = s.match(/CAPA[A-Z0-9]{5,}/);
  if (!m) return '';
  var token = m[0];
  if (token.length > 11) return token.slice(0, 11);
  return token;
}

function _bg_toNumber_(v) {
  var s = String(v || '').replace(/[^\d.,-]/g, '').replace(/\./g, '').replace(',', '.');
  var n = Number(s);
  return Number.isFinite(n) ? n : '';
}

/** Mapeo Banco General → esquema MASTER */
function _bg_mapToMaster_(raw) {
  var compositeRef = raw['OBSERVACIONES'] || raw['Observaciones'] || '';
  var errText = String(
    raw['DESCRIPCION DE ERROR'] || raw['Descripción de error'] || raw['Descripcion de error'] || ''
  ).trim();

  // reference_id SIEMPRE desde OBSERVACIONES en tu formato actual
  var ref = _bg_extractRefFromObserv_(compositeRef);

  var m = {
    reference_id: ref,                                                     // ← clave
    amount: _bg_toNumber_(raw['MONTO'] || raw['Monto']),
    currency: CONFIG.DEFAULTS_BY_BANK.BANCO_GENERAL.currency || 'USD',
    txn_date: (raw['FECHA'] || raw['Fecha'] || ''),                    // si tu CSV trae fecha
    due_date: '',
    status: (String(raw['DESCRIPCION DE ERROR'] || '').trim() === ''
      ? CONFIG.STATUSES.SUCCESS
      : CONFIG.STATUSES.FAILED),                              // ← regla que pediste
    concept: (raw['ADDENDA'] || raw['Concepto'] || ''),
    customer_name: (raw['NOMBRE DEL BENEFICIARIO'] || raw['Cliente'] || ''),
    account_number: (raw['CUENTA'] || ''),
    bank_code: 'BANCO_GENERAL',
    product_type: '',
    email: '',
    lookup_key: '',
    composite_ref: compositeRef,
    error_desc: errText
  };

  if (!m.reference_id) throw new Error('reference_id vacío (no se pudo derivar de OBSERVACIONES)');
  if (m.amount === '' || isNaN(Number(m.amount))) throw new Error('Monto inválido');

  return m;
}

/** 1) Lista columnas exactas del primer CSV de Banco General */
function bg_audit_columns() {
  const files = listFilesInFolderById(CONFIG.RAW_FOLDER_ID, '.csv') || [];
  const file = files.find(f => /general/i.test(f.getName()));
  if (!file) return logWarn('bg_audit_columns: no encontré CSV de Banco General en RAW');

  const rows = _bg_parseCsvFile_(file);
  if (!rows.length) return logWarn('bg_audit_columns: CSV vacío');
  logInfo('BG columnas detectadas:', { header: rows[0] });
}

/** 2) Muestra N filas “mapeadas” (sin escribir) para revisar mapeo */
function bg_audit_samples(n) {
  const files = listFilesInFolderById(CONFIG.RAW_FOLDER_ID, '.csv') || [];
  const file = files.find(f => /general/i.test(f.getName()));
  if (!file) return logWarn('bg_audit_samples: no encontré CSV de Banco General');

  const rows = _bg_parseCsvFile_(file);
  if (rows.length < 2) return logWarn('bg_audit_samples: solo encabezado');
  const header = rows[0].map(h => String(h || '').trim());
  const data = rows.slice(1).slice(0, n || 10);

  const out = data.map((arr, i) => {
    const raw = _bg_rowToObj_(header, arr);
    const statusTxt = _bg_extractRawStatus_(raw);
    const mapped = (function () {
      try { return _bg_mapToMaster_(raw); } catch (e) { return { __error: String(e) }; }
    })();
    return { row: i + 2, status_text: statusTxt, mapped: mapped };
  });

  logInfo('BG audit samples', out);
}

/** 3) Métrica de extracción de reference_id desde Observaciones */
function bg_audit_reference_extraction() {
  const files = listFilesInFolderById(CONFIG.RAW_FOLDER_ID, '.csv') || [];
  const file = files.find(f => /general/i.test(f.getName()));
  if (!file) return logWarn('bg_audit_reference_extraction: no encontré CSV de Banco General');

  const rows = _bg_parseCsvFile_(file);
  if (rows.length < 2) return logWarn('bg_audit_reference_extraction: solo encabezado');
  const header = rows[0].map(h => String(h || '').trim());
  const data = rows.slice(1);

  let ok = 0, fromObs = 0, fail = 0;
  data.forEach(arr => {
    const raw = _bg_rowToObj_(header, arr);
    const hasRef = String(raw['Referencia'] || raw['Ref'] || raw['Id'] || '').trim() !== '';
    const comp = raw['Observaciones'] || raw['observaciones'] || '';
    const ref2 = _bg_extractRefFromObserv_(comp);
    if (hasRef) ok++;
    else if (ref2) fromObs++;
    else fail++;
  });
  logInfo('BG reference_id audit', { explicit: ok, derived_from_observaciones: fromObs, missing: fail });
}

function _bg_toNumber_(v) {
  var s = String(v || '').replace(/[^\d.,-]/g, '').replace(/\./g, '').replace(',', '.');
  var n = Number(s);
  return Number.isFinite(n) ? n : '';
}

function _bg_extractRefFromObserv_(observ) {
  var s = String(observ || '').toUpperCase();
  var m = s.match(/CAPA[A-Z0-9]{5,}/);
  if (!m) return '';
  var token = m[0];
  if (token.length > 11) return token.slice(0, 11);
  return token;
}
