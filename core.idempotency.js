/** IDS INTERNOS */

function buildInternalId(meta) {
  // Hash corto del trío: bank + file + row_number (suficiente para evitar duplicados simples)
  var raw = [meta.source_bank, meta.file_name, meta.row_number].join("|");
  var hash = Utilities.base64Encode(Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, raw))
    .replace(/[^A-Za-z0-9]/g,"")
    .substring(0,10);
  return hash;
}