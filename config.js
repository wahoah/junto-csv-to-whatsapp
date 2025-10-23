/** CONFIGURACIÓN Y CONSTANTES GLOBALES */

//Rellena estos IDs antes de correr:
var CONFIG = {

  // === IDs ===
  SPREADSHEET_ID: "1B8nzqm9RbEPJj7HBIPb-Ypivsje5j2BZXqae_Q8-qeU",   // requerido
  RAW_FOLDER_ID: "1WUlPUcDcRe7OsnNKrwwJ0E7CMV3hjDDQ",  // requerido (carpeta /csv_files/raw)

  // IDs del las hojas, el código intentará encontrarlas por nombre; y si no existen, las crea.
  SHEET_IDS: {
    MASTER: null, // p.ej. 0
    SCHEMA_MAP: null,
    CONFIG: null,
    FAILED_QUEUE: null,
    INGEST_LOG: null,
    LOGS: null
  },

  STATUSES: {
    SUCCESS: 'SUCCESS',
    FAILED: 'FAILED',
    PENDIENTE: 'PENDIENTE'
  },

  // Nombres canónicos para creación (fallback si no hay GID configurado)
  SHEETS: {
    MASTER: "MASTER",
    SCHEMA_MAP: "SCHEMA_MAP",
    CONFIG: "CONFIG",
    FAILED_QUEUE: "FAILED_QUEUE",
    INGEST_LOG: "INGEST_LOG",
    LOGS: "LOGS"
  },

  MASTER_HEADERS: [
    "id", "source_bank", "file_name", "file_date", "row_number",
    "reference_id", "amount", "currency", "txn_date", "due_date", "status",
    "concept", "error_desc", "composite_ref", "user_ref", "txn_ref",
    "customer_name", "account_number", "bank_code", "product_type", "email",
    "validation_status", "validation_errors", "processed_at", "processed_by",
    "lookup_key", "phone_e164", "payment_link", "lang", "matched_in_db",
    "status_source", "status_ts"
  ],

  FAILED_QUEUE_HEADERS: [
    "id", "source_bank", "reference_id", "amount", "currency", "due_date",
    "customer_name", "concept", "error_desc", "lookup_key", "status",
    "first_seen_at", "days_overdue", "retry_count", "wa_status", "wa_sent_at",
    "first_failed_at", "last_failed_at", "consecutive_failed_days",
    "airtable_record_id", "airtable_phone_e164", "airtable_segment",
    "airtable_wa_template", "airtable_notes", "airtable_last_sync",
    "airtable_payload_json"
  ],

  INGEST_LOG_HEADERS: [
    "file_name", "source_bank", "processed_at",
    "rows_total", "rows_ok", "rows_err", "rows_duplicate",
    "sizeBytes",
    "success_count", "failed_count", "pending_count", 
    "lastUpdated"                                  
  ],


  // Feature flags (para habilitar cosas gradualmente)
  FLAGS: {
    PARSE_COMPOSITE: false,  // luego true cuando tengamos ejemplos reales
    WRITE_LOG_SHEET: true    // escribe en hoja LOGS además de Logger.log
  },

  //Moneda por defecto en USD para el MVP
  DEFAULTS_BY_BANK: {
    BANISTMO: { currency: "USD", status: "PENDIENTE" },
    BANCO_GENERAL: { currency: "USD", status: "PENDIENTE" },
    _FALLBACK: { currency: "USD", status: "PENDIENTE" }
  },

  AIRTABLE: {
    USE_STUB: true,       // activa modo stub mientras no tienes credenciales reales
    BASE_ID: "",           // p.ej. "appXXXXXXXXXXXXXX"
    API_KEY: "",           // recomendación: guarda el valor real en Script Properties
    ENDPOINT: "https://api.airtable.com/v0/",
    RATE_LIMIT_MS: 220,    // Airtable permite ~5 req/s. Ajusta si hay throttling.

    FAILED_QUEUE: {
      TABLE: "",           // ID o nombre de la tabla con info complementaria
      VIEW: "",            // opcional: vista filtrada que contenga sólo fallidos
      SHEET_LOOKUP_COLUMN: "reference_id",   // columna en FAILED_QUEUE (Sheets) para hacer match
      AIRTABLE_LOOKUP_FIELD: "reference_id", // campo en Airtable para buscar coincidencias
      MAX_IDS_PER_BATCH: 10,                 // Nº de IDs por fórmula OR() (evita límites de Airtable)
      FIELD_MAP: {
        airtable_record_id: "__recordId",    // se guarda el Record ID de Airtable
        airtable_phone_e164: "phone_e164",
        airtable_segment: "segment",
        airtable_wa_template: "wa_template",
        airtable_notes: "notes"
        // agrega más campos según necesites → clave = columna en Sheets, valor = nombre del campo en Airtable
      },
      // Si defines campos adicionales arriba, agrégales alias aquí para que se pidan en el GET
      SELECT_FIELDS: [
        "reference_id",
        "phone_e164",
        "segment",
        "wa_template",
        "notes"
      ],
      STUB_FIXTURE: "airtable_failed_queue.sample" // nombre de la respuesta mock en fixtures/rest_responses
    }
  }
};
