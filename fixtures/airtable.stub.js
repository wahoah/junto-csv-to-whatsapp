/*************************************************
 * fixtures/airtable.stub.js
 * - Registra payloads mock para modo stub de io.airtable.js
 * - El contenido proviene de fixtures/rest_responses/*.json
 **************************************************/

var AIRTABLE_STUB_RESPONSES = (typeof AIRTABLE_STUB_RESPONSES !== 'undefined')
  ? AIRTABLE_STUB_RESPONSES
  : {};

AIRTABLE_STUB_RESPONSES['airtable_failed_queue.sample'] = {
  "records": [
    {
      "id": "recFAILED001",
      "createdTime": "2024-06-15T12:00:00.000Z",
      "fields": {
        "reference_id": "CAPARE00008008013",
        "PhoneNumber": "+50760000001",
        "segment": "VIP",
        "wa_template": "failed_payment_vip",
        "notes": "Cliente con ticket abierto",
        "ID": "CAPARE00001"
      }
    },
    {
      "id": "recFAILED002",
      "createdTime": "2024-06-16T09:30:00.000Z",
      "fields": {
        "reference_id": "CAPARE00008008005",
        "PhoneNumber": "+50760000002",
        "segment": "STANDARD",
        "wa_template": "failed_payment_standard",
        "notes": "Primer recordatorio pendiente",
        "ID": "CAPARE00002"
      }
    },
    {
      "id": "recFAILED003",
      "createdTime": "2024-06-17T15:45:00.000Z",
      "fields": {
        "reference_id": "CAPARE00123",
        "PhoneNumber": "+50760000003",
        "segment": "STANDARD",
        "wa_template": "failed_payment_standard",
        "notes": "Cliente solicitó reintento mañana",
        "ID": "CAPADR00001"
      }
    }
  ]
};
