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

function getOrCreateProcessedFolder_(parentFolderId) {
  var processedFolderId = CONFIG && CONFIG.PROCESSED_FOLDER_ID;
  if (processedFolderId) {
    try {
      return DriveApp.getFolderById(processedFolderId);
    } catch (e) {
      if (typeof logWarn === 'function') {
        logWarn('No se pudo abrir PROCESSED_FOLDER_ID, se intentará crear subcarpeta', { err: String(e) });
      }
      // cae a la lógica de subcarpeta
    }
  }

  var parent = getFolderById_(parentFolderId);
  var name = (CONFIG && CONFIG.PROCESSED_SUBFOLDER_NAME) || 'processed';
  var folders = parent.getFoldersByName(name);
  if (folders.hasNext()) return folders.next();
  return parent.createFolder(name);
}

function moveFileToProcessed_(file, parentFolderId) {
  if (!file) return false;
  if (CONFIG && CONFIG.MOVE_PROCESSED_FILES === false) return false;

  var targetFolderId = parentFolderId || (CONFIG && CONFIG.RAW_FOLDER_ID);
  if (!targetFolderId) return false;

  try {
    var destination = getOrCreateProcessedFolder_(targetFolderId);
    file.moveTo(destination);
    if (typeof logInfo === 'function') {
      logInfo('Archivo movido a carpeta de procesados', { name: file.getName(), target: destination.getName() });
    }
    return true;
  } catch (err) {
    if (typeof logWarn === 'function') {
      logWarn('No se pudo mover archivo procesado', { name: file && file.getName ? file.getName() : '', err: String(err) });
    }
    return false;
  }
}
