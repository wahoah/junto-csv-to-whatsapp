/** UTILIDADES PARA DRIVE Y CSV (por ID) */

function getFolderById_(folderId) {
  if (!folderId) throw new Error("RAW_FOLDER_ID");
  return DriveApp.getFolderById(folderId);
}

function listFilesInFolderById(folderId, extensionFilter) {
  var folder = getFolderById_(folderId);
  var files = [];
  var it = folder.getFiles();
  while (it.hasNext()) {
    var f = it.next();
    if (!extensionFilter || f.getName().toLowerCase().endsWith(extensionFilter.toLowerCase())) {
      files.push(f);
    }
  }
  return files;
}

function readCsvFileFlexible(file) {
  var content = file.getBlob().getDataAsString();
  var delimiters = [",",";","|"];
  for (var i=0; i<delimiters.length; i++) {
    try {
      var rows = Utilities.parseCsv(content, delimiters[i]);
      if (rows && rows.length > 0 && rows[0].length > 1) return rows;
    } catch(e) {}
  }
  return Utilities.parseCsv(content);
}