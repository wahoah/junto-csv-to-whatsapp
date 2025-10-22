/** TRANSFORMACIONES PURAS (PLACEHOLDERS) */

function trimSpaces(v) {
  if (v === null || v === undefined) return "";
  return String(v).replace(/\s+/g, " ").trim();
}

function upperNormalize(v) {
  v = trimSpaces(v);
  // Normaliza acentos si necesitas; por ahora, solo a MAYÚSCULAS
  return v.toUpperCase();
}

function toDecimal(v) {
  v = trimSpaces(v);
  if (v === "") return "";
  // Quita separadores de miles si existieran (no deberían)
  v = v.replace(/,/g, "");
  var num = Number(v);
  return isNaN(num) ? "" : num;
}