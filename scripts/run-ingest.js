import 'dotenv/config';
import fetch from 'node-fetch';
import pkg from 'pg';
const { Client } = pkg;

const MESES = [
  'JANEIRO','FEVEREIRO','MARÇO','ABRIL','MAIO','JUNHO',
  'JULHO','AGOSTO','SETEMBRO','OUTUBRO','NOVEMBRO','DEZEMBRO'
];

const BATCH_SIZE = 900; // Limite de linhas por batch

function logEnvPresence() {
  const has = k => (process.env[k] ? '✅' : '❌');
  console.log('ENV CHECK:',
    `DATABASE_URL ${has('DATABASE_URL')}`,
    `EXPORTER_URL ${has('EXPORTER_URL')}`,
    `EXPORTER_TOKEN ${has('EXPORTER_TOKEN')}`
  );
}

async function fetchSheet(sheetId, tab, tipo) {
  const url = `${process.env.EXPORTER_URL}?sheetId=${sheetId}&tab=${encodeURIComponent(tab)}&tipo=${tipo}&token=${process.env.EXPORTER_TOKEN}`;
  console.log('→ GET', url.replace(process.env.EXPORTER_TOKEN, '***')); // não vaza token
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const json = await res.json();
  return json;
}

async function main() {
  console.log('🔰 Iniciando ingestor...');
  logEnvPresence();

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });

  await client.connect();
  console.log('🔗 Conectado ao Supabase');

  const { rows: planilhas } = await client.query(`
    select ano, tipo, sheet_id
    from t_planilhas
    where ativo = true
    order by ano, tipo
  `);
  console.log('📄 Planilhas ativas:', planilhas);

  let totalInserted = 0;
  for (const { ano, tipo, sheet_id } of planilhas) {
    for (const mes of MESES) {
      try {
        const data = await fetchSheet(sheet_id, mes, tipo);
        if (!data.ok) {
          console.warn(`⚠️ ${tipo}/${ano}/${mes}: exportador respondeu erro: ${data.error}`);
          continue;
        }
        console.log(`➡️ ${tipo}/${ano}/${mes}: ${data.rowsCount} linhas x ${data.colCount} colunas`);

        const rows = data.rows || [];
        const validRows = [];

        for (let i = 1; i < rows.length; i++) {
          const row = rows[i];
          if (!row || !row.some(v => v && String(v).trim() !== '')) continue;

          // ignora cabeçalho conhecido de DISTRIBUICAO
          const headerDistrib = [
            'CLIENTE','TIPO DE PROCESSO','RESP. PROCESSO','RESP. PETIÇÃO',
            'RESP. CORREÇÃO','RESP. DISTRIBUIÇÃO','COMPETÊNCIA','VALOR DA CAUSA',
            'DISTRIBUÍDO','UNIDADE'
          ];
          const isDistribHeader = Array.isArray(row) &&
            row.map(String).map(s => s.trim().toUpperCase()).join('|') === headerDistrib.join('|');
          if (isDistribHeader) continue;

          // Filtro de linhas válidas
          const nome = row[0]; // B
          const cpf = row[1]; // C
          const status = tipo === 'REQUERIMENTOS' ? row[7] : row[8]; // I ou J

          if (tipo === 'REQUERIMENTOS' && (!nome || !cpf)) continue;
          if (tipo === 'DISTRIBUICAO' && !nome) continue;

          const abaMesRetornada = data.tab; // Ex: "MARCO"
          validRows.push([tipo, sheet_id, abaMesRetornada, i, JSON.stringify(row)]);
        }

        // Dividir em batches menores (até 900 linhas por batch)
        for (let i = 0; i < validRows.length; i += BATCH_SIZE) {
          const batch = validRows.slice(i, i + BATCH_SIZE);

          const placeholders = batch.map((_, idx) => `($${idx * 5 + 1}, $${idx * 5 + 2}, $${idx * 5 + 3}, $${idx * 5 + 4}, $${idx * 5 + 5})`).join(', ');
          const query = `INSERT INTO stg_sheets_raw (source, sheet_id, aba_mes, row_idx, payload) VALUES ${placeholders} ON CONFLICT (source, sheet_id, aba_mes, row_idx) DO UPDATE SET payload = excluded.payload, ingested_at = now()`;
          const values = batch.flat();
          await client.query(query, values);
          totalInserted += batch.length;
        }

        console.log(`✅ Upserts (batched): ${validRows.length} linhas`);

      } catch (err) {
        console.warn(`❗ ${tipo}/${ano}/${mes}: ${err.message}`);
      }
    }
  }

  console.log(`🎯 Total inserido/atualizado no staging: ${totalInserted}`);
  await client.end();

  // se nada entrou, força exit code ≠ 0 para ficar evidente no Actions
  if (totalInserted === 0) {
    console.error('⚠️ Nenhuma linha inserida. Verifique token/URL/t_planilhas/abas.');
    process.exit(2);
  }
}

main().catch(err => {
  console.error('💥 Falha fatal no ingestor:', err);
  process.exit(1);
});