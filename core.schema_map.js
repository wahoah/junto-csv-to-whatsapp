/** SCHEMA MAP: Carga de la hoja SCHEMA_MAP y aplicación básica (STUB) */

function loadSchemaMap() {
  var sheet = ensureSheet(CONFIG.SHEETS.SCHEMA_MAP, ["bank","source_column","target_field","transform","required","notes"]);
  var rng = sheet.getDataRange().getValues();
  var headers = rng[0];
  var rows = rng.slice(1);
  var map = {}; // { bank: [ {source_column, target_field, transform, required} ] }
  rows.forEach(function(r){
    var obj = {};
    for (var i=0; i<headers.length; i++) obj[headers[i]] = r[i];
    var bank = String(obj.bank || "").toUpperCase();
    if (!map[bank]) map[bank] = [];
    map[bank].push({
      source_column: obj.source_column,
      target_field: obj.target_field,
      transform: obj.transform,
      required: String(obj.required).toLowerCase() === "true"
    });
  });
  return map;
}

/**
 * Aplica un mapeo directo nombreColumnaOrigen -> campoCanónico.
 * Para esta primera fase, asumimos que rawRow ya es un objeto {colName: valor} o por posición que el adapter traducirá.
 * Aquí solo dejamos la firma y el patrón.
 */
function applyMapSimple(bank, rawObj, schemaMap) {
  var bankKey = String(bank || "").toUpperCase();
  var rules = schemaMap[bankKey] || [];
  var out = {};
  rules.forEach(function(rule){
    var src = rule.source_column;
    var dst = rule.target_field;
    var val = rawObj[src];
    // Transforms mínimos (solo ejemplos):
    if (rule.transform === "trim") val = trimSpaces(val);
    if (rule.transform === "upper_normalize") val = upperNormalize(val);
    if (rule.transform === "to_decimal") val = toDecimal(val);
    if (rule.transform && rule.transform.indexOf("const:") === 0) {
      val = rule.transform.split("const:")[1];
    }
    out[dst] = val;
  });
  return out;
}