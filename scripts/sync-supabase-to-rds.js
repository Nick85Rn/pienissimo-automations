// Sync Supabase → RDS Archive
// Strategia: upsert idempotente, basato su modified_time/created_time
// Esegue le 4 tabelle Zoho in sequenza, log finale con totali

import { createClient } from '@supabase/supabase-js';
import pg from 'pg';

const { Client } = pg;

// ============ CONFIG ============
const TABLES = [
  {
    name: 'zoho_raw_chats',
    pk: 'chat_id',
    timestampCol: 'created_time', // usiamo created_time perché non c'è modified_time
    columns: [
      'chat_id', 'visitor_name', 'operator', 'department',
      'created_time', 'closed_time', 'waiting_time_seconds',
      'duration_seconds', 'message_count', 'rating', 'device',
      'visitor_email', 'question', 'raw_data', 'transcript',
      'transcript_synced_at', 'category', 'subcategory', 'sentiment',
      'resolved', 'categorized_at', 'categorized_by_model', 'categorization_error'
    ],
    jsonbCols: ['raw_data', 'transcript'],
  },
  {
    name: 'zoho_raw_assistenza',
    pk: 'ticket_id',
    timestampCol: 'modified_time',
    columns: [
      'ticket_id', 'ticket_number', 'subject', 'status', 'priority',
      'category', 'assignee', 'created_time', 'closed_time',
      'thread_count', 'channel', 'first_response_sec', 'avg_response_sec',
      'resolution_sec', 'sla_synced_at', 'modified_time'
    ],
    jsonbCols: [],
  },
  {
    name: 'zoho_raw_sviluppo',
    pk: 'ticket_id',
    timestampCol: 'modified_time',
    columns: [
      'ticket_id', 'ticket_number', 'subject', 'status', 'priority',
      'category', 'assignee', 'created_time', 'closed_time',
      'thread_count', 'channel', 'first_response_sec', 'avg_response_sec',
      'resolution_sec', 'sla_synced_at', 'modified_time'
    ],
    jsonbCols: [],
  },
  {
    name: 'zoho_raw_formazione',
    pk: 'id',
    timestampCol: 'created_at',
    columns: [
      'id', 'created_at', 'topic', 'original_title', 'company',
      'operator', 'description', 'duration_minutes', 'created_time',
      'unique_id'
    ],
    jsonbCols: [],
  },
];

const BATCH_SIZE = 500;

// ============ CONNESSIONI ============
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { auth: { persistSession: false } }
);

const rds = new Client({
  host: process.env.RDS_HOST,
  database: process.env.RDS_DATABASE,
  user: process.env.RDS_USER,
  password: process.env.RDS_PASSWORD,
  port: 5432,
  ssl: { rejectUnauthorized: false },
  connectionTimeoutMillis: 30000,
});

// ============ HELPERS ============
async function getLastTimestampOnRDS(tableName, timestampCol) {
  const result = await rds.query(
    `SELECT MAX(${timestampCol}) AS last_ts FROM public.${tableName}`
  );
  return result.rows[0].last_ts; // null se tabella vuota
}

async function fetchFromSupabase(table, sinceTimestamp) {
  // Paginato per evitare timeout / memory issues
  let allRows = [];
  let from = 0;
  const pageSize = 1000;

  while (true) {
    let query = supabase
      .from(table.name)
      .select(table.columns.join(','))
      .order(table.timestampCol, { ascending: true })
      .range(from, from + pageSize - 1);

    if (sinceTimestamp) {
      query = query.gt(table.timestampCol, sinceTimestamp);
    }

    const { data, error } = await query;
    if (error) throw new Error(`Supabase fetch error: ${error.message}`);
    if (!data || data.length === 0) break;

    allRows = allRows.concat(data);
    if (data.length < pageSize) break;
    from += pageSize;
  }

  return allRows;
}

function buildUpsertQuery(table, rows) {
  const cols = table.columns;
  const placeholders = [];
  const values = [];
  let paramIndex = 1;

  for (const row of rows) {
    const rowPlaceholders = cols.map(c => {
      const val = row[c];
      // JSONB columns: stringify se è oggetto
      if (table.jsonbCols.includes(c) && val !== null && typeof val === 'object') {
        values.push(JSON.stringify(val));
      } else {
        values.push(val);
      }
      return `$${paramIndex++}`;
    });
    placeholders.push(`(${rowPlaceholders.join(',')})`);
  }

  const updateCols = cols
    .filter(c => c !== table.pk)
    .map(c => `${c} = EXCLUDED.${c}`)
    .join(', ');

  const sql = `
    INSERT INTO public.${table.name} (${cols.join(',')})
    VALUES ${placeholders.join(',')}
    ON CONFLICT (${table.pk}) DO UPDATE SET ${updateCols}
  `;

  return { sql, values };
}

async function upsertBatch(table, rows) {
  if (rows.length === 0) return 0;
  const { sql, values } = buildUpsertQuery(table, rows);
  await rds.query(sql, values);
  return rows.length;
}

async function syncTable(table) {
  console.log(`\n=== ${table.name} ===`);

  const lastTs = await getLastTimestampOnRDS(table.name, table.timestampCol);
  console.log(`  Ultimo ${table.timestampCol} su RDS: ${lastTs || '(tabella vuota)'}`);

  const rows = await fetchFromSupabase(table, lastTs);
  console.log(`  Righe da Supabase più recenti: ${rows.length}`);

  if (rows.length === 0) {
    console.log('  ✅ Nessuna nuova riga da sincronizzare');
    return { table: table.name, synced: 0 };
  }

  let totalSynced = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const count = await upsertBatch(table, batch);
    totalSynced += count;
    console.log(`  Batch ${i / BATCH_SIZE + 1}: ${count} righe upsertate (totale: ${totalSynced}/${rows.length})`);
  }

  console.log(`  ✅ ${table.name}: ${totalSynced} righe sincronizzate`);
  return { table: table.name, synced: totalSynced };
}

// ============ MAIN ============
async function main() {
  console.log('==========================================');
  console.log('Sync Supabase → RDS Archive');
  console.log(`Avviato alle ${new Date().toISOString()}`);
  console.log('==========================================');

  await rds.connect();
  console.log('✅ Connesso a RDS');

  const results = [];
  for (const table of TABLES) {
    try {
      const res = await syncTable(table);
      results.push(res);
    } catch (err) {
      console.error(`❌ Errore su ${table.name}: ${err.message}`);
      results.push({ table: table.name, synced: 0, error: err.message });
    }
  }

  await rds.end();

  console.log('\n==========================================');
  console.log('RIEPILOGO FINALE');
  console.log('==========================================');
  for (const r of results) {
    const status = r.error ? `❌ ERRORE: ${r.error}` : `✅ ${r.synced} righe`;
    console.log(`  ${r.table}: ${status}`);
  }
  console.log(`Completato alle ${new Date().toISOString()}`);

  const hasErrors = results.some(r => r.error);
  process.exit(hasErrors ? 1 : 0);
}

main().catch(err => {
  console.error('💥 ERRORE FATALE:', err);
  process.exit(1);
});
