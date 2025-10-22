/** =============================================================================
 * CORE · CONSOLIDATE
 * -----------------------------------------------------------------------------
 * Arma la fila final (orden de columnas MASTER) a partir de:
 *  - mappedRow: objeto canónico ya mapeado (applyMapSimple)
 *  - meta: { source_bank, file_name, file_date, row_number }
 *  - validation: { validation_status, validation_errors }
 *
 * Reglas clave:
 *  - currency: default por banco (CONFIG.DEFAULTS_BY_BANK) → fallback USD.
 *  - status: si viene status_source (nativo del banco) se normaliza a canónico.
 *  - lookup_key: hoy = reference_id (futuro: composite_ref || reference_id).
 *  - processed_at/processed_by: audit fields.
 *  - Mantiene el ORDEN de columnas de CONFIG.MASTER_HEADERS.
 * ============================================================================= */


/** Normaliza un estado "nativo de banco" a nuestros estados canónicos.
 *  Entradas toleradas (case-insensitive, con/ sin acentos):
 *    - Éxito:   "ejecutada", "ok", "procesada", "aplicada", "pagada", "pagado", "abonada", "aprobada"
 *    - Fallo:   "rechazada", "rechazo", "fallida", "error", "no procesada", "devuelta", "reversada"
 *    - Otros:   "pendiente", "en proceso", "", null → "PENDIENTE"
 *  Devuelve uno de: "SUCCESS" | "FAILED" | "PENDIENTE" | "REVERSED" | "CANCELLED"
 */
function normalizeBankStatus_(rawStatus) {
  if (rawStatus === null || rawStatus === undefined) return "PENDIENTE";
  var s = String(rawStatus).toLowerCase().trim();

  // Mapa directo para los casos comunes
  var direct = {
    // ÉXITO
    "ejecutada": "SUCCESS",
    "ejecutado": "SUCCESS",
    "ok": "SUCCESS",
    "procesada": "SUCCESS",
    "procesado": "SUCCESS",
    "aplicada": "SUCCESS",
    "aplicado": "SUCCESS",
    "pagada": "SUCCESS",
    "pagado": "SUCCESS",
    "abonada": "SUCCESS",
    "abonado": "SUCCESS",
    "aprobada": "SUCCESS",
    "aprobado": "SUCCESS",

    // FALLO
    "rechazada": "FAILED",
    "rechazo": "FAILED",
    "fallida": "FAILED",
    "fallido": "FAILED",
    "error": "FAILED",
    "no procesada": "FAILED",
    "no procesado": "FAILED",
    "devuelta": "FAILED",
    "devuelto": "FAILED",

    // REVERSO / CANCELACIÓN (si algún banco lo trae así)
    "reversada": "REVERSED",
    "reversado": "REVERSED",
    "reverso": "REVERSED",
    "cancelada": "CANCELLED",
    "cancelado": "CANCELLED",

    // PENDIENTE
    "pendiente": "PENDIENTE",
    "en proceso": "PENDIENTE"
  };

  if (direct[s]) return direct[s];

  // Heurísticas: por si vienen frases más largas del banco
  // Éxito: contiene palabras clave
  var successHints = ["ejecut", "proces", "aplic", "pagad", "abonad", "aprob"];
  for (var i = 0; i < successHints.length; i++) {
    if (s.indexOf(successHints[i]) >= 0) return "SUCCESS";
  }

  // Fallo: contiene palabras clave
  var failHints = ["rechaz", "fallid", "error", "no proces", "devuelt"];
  for (var j = 0; j < failHints.length; j++) {
    if (s.indexOf(failHints[j]) >= 0) return "FAILED";
  }

  // Reverso/cancelación
  if (s.indexOf("revers") >= 0) return "REVERSED";
  if (s.indexOf("cancel") >= 0) return "CANCELLED";

  // Si no se reconoce, considerar pendiente (conservador)
  return "PENDIENTE";
}


/** Devuelve la fila final lista para append en MASTER, en el orden de CONFIG.MASTER_HEADERS. */
function consolidateToMasterRow(mappedRow, meta, validation) {
  var now = new Date();

  // Defaults por banco → fallback global
  var defaultsBank = (CONFIG.DEFAULTS_BY_BANK[meta.source_bank] ||
                      CONFIG.DEFAULTS_BY_BANK._FALLBACK || { currency: "USD", status: "PENDIENTE" });

  // status: si viene status_source, normalízalo; si no, usa mapped/status/default
  var statusSource = mappedRow.status_source;
  var normalizedStatus = (statusSource !== undefined && String(statusSource).trim() !== "")
    ? normalizeBankStatus_(statusSource)
    : (mappedRow.status || defaultsBank.status || "PENDIENTE");

  // currency con default
  var currency = mappedRow.currency || defaultsBank.currency || "USD";

  // id interno
  var id = buildInternalId(meta);

  // lookup_key: hoy composite_ref || reference_id
  var lookup_key = (mappedRow.composite_ref && String(mappedRow.composite_ref).trim() !== "")
    ? mappedRow.composite_ref
    : (mappedRow.reference_id || "");

  // Armar objeto de salida con TODOS los posibles campos
  var out = Object.assign({}, mappedRow, meta, {
    id: id,
    currency: currency,
    status: normalizedStatus,
    lookup_key: lookup_key,
    processed_at: now,
    processed_by: "orchestrator.gs"
  });

  // Validación a columnas estándar si viene
  if (validation) {
    out.validation_status = validation.validation_status || validation.status || "";
    out.validation_errors = validation.validation_errors || validation.errors || "";
  }

  // IMPORTANTÍSIMO: devolver ordenado según headers de CONFIG
  return CONFIG.MASTER_HEADERS.map(function (h) {
    return (out[h] !== undefined) ? out[h] : "";
  });
}