/*************************************************
 * adapters/adapter.banistmo.gs — versión mínima estable
 * Lee CSVs en RAW_FOLDER_ID, mapea Pay ID → reference_id,
 * escribe en MASTER y en INGEST_LOG.
 **************************************************/

function run_ingest_banistmo_all() {
  var files = listFilesInFolderById(CONFIG.RAW_FOLDER_ID, '.csv') || [];
  var totals = { files: 0, rows_total: 0, rows_ok: 0, rows_err: 0, rows_duplicate: 0, success_count: 0, failed_count: 0, pending_count: 0 };


  files.forEach(function (file) {
    // Heurística simple: procesa todo .csv (si quieres, filtra por nombre “banistmo”)
    if (!/banistmo/i.test(file.getName())) return;

    totals.files++;
    logInfo("Procesando archivo Banistmo", { name: file.getName(), id: file.getId() });

    // Parse CSV
    var rows = _bnm_parseCsvFile_(file);          // Array<Array<string>>
    if (!rows.length) {
      writeIngestLog({
        file_name: file.getName(), source_bank: 'BANISTMO',
        rows_total: 0, rows_ok: 0, rows_err: 0, rows_duplicate: 0,
        sizeBytes: file.getSize(), lastUpdated: file.getLastUpdated()
      });
      return;
    }

    // Detect header row & build header map
    var header = rows[0].map(function (h) { return String(h || '').trim(); });
    var dataRows = rows.slice(1);                 // salta encabezado
    totals.rows_total += dataRows.length;

    var buffer = [];

    dataRows.forEach(function (arr, idx) {
      try {
        var raw = _bnm_rowToObj_(header, arr);    // objeto por nombre de columna
        var mapped = _bnm_mapToMaster_(raw);      // normaliza campos clave

        // meta de consolidación
        var meta = {
          source_bank: "BANISTMO",
          file_name: file.getName(),
          file_date: Utilities.formatDate(file.getLastUpdated(), Session.getScriptTimeZone(), "yyyy-MM-dd"),
          row_number: idx + 2 // +2 por header base-1
        };

        // contabiliza por estatus ANTES de consolidar
        var s = (mapped.status || "").toUpperCase();
        if (s === "SUCCESS") totals.success_count++;
        else if (s === "FAILED") totals.failed_count++;
        else totals.pending_count++;

        // validación/consolidación
        var v = (typeof validateRowBasic === 'function') ? validateRowBasic(mapped) : {};
        var row = consolidateToMasterRow(mapped, meta, v);

        buffer.push(row);
        totals.rows_ok++;
      } catch (e) {
        totals.rows_err++;
        logWarn("Banistmo fila inválida", { row: idx + 2, err: String(e) });
      }
    });

    if (buffer.length) appendRows('MASTER', buffer);

    writeIngestLog({
      file_name: file.getName(),
      source_bank: 'BANISTMO',
      rows_total: totals.rows_total,
      rows_ok: totals.rows_ok,
      rows_err: totals.rows_err,
      rows_duplicate: totals.rows_duplicate,
      sizeBytes: file.getSize(),
      lastUpdated: file.getLastUpdated(),
      success_count: totals.success_count,   // ← NUEVO
      failed_count: totals.failed_count,     // ← NUEVO
      pending_count: totals.pending_count    // ← NUEVO
    });
  });

  logInfo("run_ingest_banistmo_all: resumen", totals);
  return totals;
}

/** ---- Helpers Banistmo ---- **/

function _bnm_parseCsvFile_(file) {
  var blob = file.getBlob();
  // Si el CSV es UTF-8 con BOM, Utilities.parseCsv lo maneja bien
  var text = blob.getDataAsString(); // si necesitas, especifica 'UTF-8'
  var rows = Utilities.parseCsv(text);
  // Limpia filas vacías
  return rows.filter(function (r) { return r && r.length && r.join('').trim() !== ''; });
}

/** Convierte una fila array a objeto por nombre de columna */
function _bnm_rowToObj_(header, arr) {
  var obj = {};
  for (var i = 0; i < header.length; i++) obj[header[i]] = arr[i];
  return obj;
}

/** Mapeo específico Banistmo → esquema maestro */
function _bnm_mapToMaster_(raw) {
  var toNumber = function (v) {
    var s = String(v || '').replace(/[^\d.,-]/g, '').replace(/\./g, '').replace(',', '.');
    var n = Number(s);
    return Number.isFinite(n) ? n : '';
  };

  var m = {
    reference_id: (raw['Pay ID'] || raw['Referencia de Pago'] || '').toString().trim(),
    amount: toNumber(raw['Monto de Transacción'] || raw['Amount']),
    currency: (raw['Moneda'] || CONFIG.DEFAULTS_BY_BANK.BANISTMO.currency || 'USD'),
    txn_date: (raw['Fecha'] || raw['Fecha de Transacción'] || ''),
    due_date: '',
    // 👇 aquí el cambio
    status: _bnm_statusMap_(_bnm_extractRawStatus_(raw)),
    // 👆 antes usaba directamente raw['Status']
    concept: (raw['Descripción de la Transacción'] || raw['Concepto'] || ''),
    customer_name: (raw['Nombre del Beneficiario'] || raw['Cliente'] || ''),
    account_number: (raw['Número de Cuenta Beneficiario'] || ''),
    bank_code: (raw['Banco Beneficiario'] || ''),
    product_type: (raw['Producto Beneficiario'] || ''),
    email: (raw['Email del Beneficiario'] || ''),
    lookup_key: ''
  };

  if (!m.reference_id) throw new Error('Pay ID vacío');
  if (m.amount === '' || isNaN(Number(m.amount))) throw new Error('Monto inválido');
  if (!m.status) m.status = CONFIG.STATUSES.PENDIENTE;

  return m;
}

/** Normaliza string (lowercase, sin acentos, sin espacios repetidos) */
function _norm_(s) {
  return String(s || '')
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // quita acentos
    .replace(/\s+/g, ' ')
    .trim();
}

/** Extrae el texto de estado desde varias posibles columnas del CSV */
function _bnm_extractRawStatus_(raw) {
  var keys = [
    'Status',
    'Resultado de la Transacción',
    'Resultado de la Transaccion',
    'Estado',
    'Estatus',
    'Resultado',
    'Transaction Result'
  ];
  for (var i = 0; i < keys.length; i++) {
    if (raw.hasOwnProperty(keys[i]) && String(raw[keys[i]]).trim() !== '') {
      return String(raw[keys[i]]);
    }
  }
  return '';
}

/** Mapea el texto del banco → CONFIG.STATUSES */
function _bnm_statusMap_(s) {
  var t = _norm_((s || ''));

  // sinónimos de éxito
  if (/aprob|aplicad|ejecut|realiz|pagad|ok|exitos/.test(t))
    return CONFIG.STATUSES.SUCCESS;

  // sinónimos de fallo
  if (/rechaz|fall|negad|error|anul|devuelt|reversa|cancelad/.test(t))
    return CONFIG.STATUSES.FAILED;

  // pendientes / en proceso
  if (/pend|proceso|en curso|verific|await|espera/.test(t))
    return CONFIG.STATUSES.PENDIENTE;

  // fallback
  return CONFIG.STATUSES.PENDIENTE;
}

