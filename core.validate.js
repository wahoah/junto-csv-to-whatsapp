/** VALIDACIONES Y ESTADOS BÁSICOS */

function validateRowBasic(row) {
  var errors = [];

  // Requeridos
  if (!row.reference_id || trimSpaces(row.reference_id) === "") {
    errors.push("MISSING_REFERENCE_ID");
  }
  if (row.amount === "" || row.amount === null || isNaN(row.amount) || Number(row.amount) <= 0) {
    errors.push("INVALID_AMOUNT");
  }

  var status = errors.length === 0 ? "OK" : (errors.indexOf("INVALID_AMOUNT") >= 0 ? "INVALID_AMOUNT" : "MISSING_FIELDS");

  return {
    validation_status: status,
    validation_errors: errors.join("|")
  };
}