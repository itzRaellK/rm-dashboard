import 'dotenv/config';
import fetch from 'node-fetch';
import pkg from 'pg';
const { Client } = pkg;

const MESES = [
  'JANEIRO','FEVEREIRO','MARÇO','ABRIL','MAIO','JUNHO',
  'JULHO','AGOSTO','SETEMBRO','OUTUBRO','NOVEMBRO','DEZEMBRO'
];

const BATCH_SIZE = 900; // Limite de linhas por batch
const REQUEST_DELAY_MS = 1000; // 1 segundo entre requisições
const PLANILHA_TIMEOUT_MS = 30000; // 30 segundos por planilha

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

// Nova função: verificar se a aba existe
async function checkSheetTabExists(sheetId, tab, tipo) {
  const url = `${process.env.EXPORTER_URL}?sheetId=${sheetId}&tab=${encodeURIComponent(tab)}&tipo=${tipo}&token=${process.env.EXPORTER_TOKEN}&checkOnly=true`;
  try {
    const res = await fetch(url);
    if (!res.ok) return false;
    const json = await res.json();
    return json.ok === true;
  } catch (err) {
    return false;
  }
}

// Nova função: detectar cabeçalho fixo de Distribuição com tolerância
function isDistribHeaderRow(row) {
  if (!Array.isArray(row)) return false;

  // Verifica se a primeira coluna é "CLIENTE" (ignorando case, espaços e acentos)
  const firstCol = row[0];
  if (!firstCol) return false;

  const normalizedFirst = String(firstCol).trim().normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();
  if (normalizedFirst !== 'CLIENTE') return false;

  // Verifica se pelo menos as primeiras colunas correspondem ao cabeçalho esperado
  const expectedHeaders = [
    'CLIENTE','TIPO DE PROCESSO','RESP. PROCESSO','RESP. PETIÇÃO',
    'RESP. CORREÇÃO','RESP. DISTRIBUIÇÃO','COMPETÊNCIA','VALOR DA CAUSA',
    'DISTRIBUÍDO','UNIDADE'
  ];

  let matchCount = 0;
  for (let i = 0; i < Math.min(expectedHeaders.length, row.length); i++) {
    const actual = String(row[i]).trim().normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();
    const expected = expectedHeaders[i];
    if (actual === expected) matchCount++;
  }

  // Se pelo menos 5 das 10 colunas forem iguais, consideramos cabeçalho
  return matchCount >= 5;
}

// Nova função: detectar se é uma linha de cabeçalho semelhante
function isLikelyHeaderRow(row) {
  if (!Array.isArray(row)) return false;

  const firstCol = row[0];
  if (!firstCol) return false;

  const normalizedFirst = String(firstCol).trim().normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();

  // Lista de palavras que indicam cabeçalho
  const headerKeywords = [
    'CLIENTE', 'TIPO', 'RESP', 'COMPETÊNCIA', 'VALOR', 'DISTRIBUÍDO', 'UNIDADE',
    'PROCESSO', 'PETIÇÃO', 'CORREÇÃO', 'DISTRIBUIÇÃO', 'PROTOCOLO', 'BENEFÍCIO'
  ];

  return headerKeywords.some(k => normalizedFirst.includes(k));
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
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
    console.log(`📁 Processando planilha: ${tipo} (${sheet_id})`);

    // Timeout por planilha
    const planilhaTimeout = setTimeout(() => {
      console.warn(`⏰ Timeout de 30s atingido para ${tipo}/${ano}. Pulando...`);
    }, PLANILHA_TIMEOUT_MS);

    try {
      for (const mes of MESES) {
        // Verificar se a aba existe
        const abaExiste = await checkSheetTabExists(sheet_id, mes, tipo);
        if (!abaExiste) {
          console.warn(`⚠️ Aba ${mes} não existe em ${tipo}/${ano}. Pulando...`);
          continue;
        }

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

            // Ignorar cabeçalho fixo de DISTRIBUICAO
            if (tipo === 'DISTRIBUICAO' && isDistribHeaderRow(row)) continue;

            // Nova regra: pular linhas com '-' na coluna A (cabeçalho fixo em Distribuição)
            if (tipo === 'DISTRIBUICAO' && row[0] && String(row[0]).trim() === '-') continue;

            // Nova regra: pular linhas que parecem cabeçalhos (genérico)
            if (isLikelyHeaderRow(row)) continue;

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
          if (err.message.includes('maximum redirect')) {
            console.log(`⏰ Esperando 5 segundos para evitar mais limites...`);
            await sleep(5000);
          }
        }

        // Delay entre requisições para evitar limites do Google
        await sleep(REQUEST_DELAY_MS);
      }

      // Limpar timeout
      clearTimeout(planilhaTimeout);
      console.log(`✅ Planilha ${tipo} concluída.`);

    } catch (err) {
      console.error(`💥 Erro fatal na planilha ${tipo}/${ano}:`, err);
      clearTimeout(planilhaTimeout);
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