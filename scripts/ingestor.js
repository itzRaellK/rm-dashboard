// api/ingestor.js
import fetch from 'node-fetch'
import pkg from 'pg'
const { Client } = pkg

const MESES = [
  'JANEIRO','FEVEREIRO','MARÇO','ABRIL','MAIO','JUNHO',
  'JULHO','AGOSTO','SETEMBRO','OUTUBRO','NOVEMBRO','DEZEMBRO'
]

function isAuthorized(req) {
  const fromCron = !!req.headers['x-vercel-cron']
  const tok = req.query?.token || req.query?.EXPORTER_TOKEN
  return fromCron || (tok && process.env.EXPORTER_TOKEN && tok === process.env.EXPORTER_TOKEN)
}

export default async function handler(req, res) {
  try {
    if (!isAuthorized(req)) {
      return res.status(401).json({ ok:false, error:'unauthorized' })
    }

    // ENVs mínimos
    const missing = []
    if (!process.env.DATABASE_URL)   missing.push('DATABASE_URL')
    if (!process.env.EXPORTER_URL)   missing.push('EXPORTER_URL')
    if (!process.env.EXPORTER_TOKEN) missing.push('EXPORTER_TOKEN')
    if (missing.length) {
      return res.status(500).json({ ok:false, error:`Missing env: ${missing.join(', ')}` })
    }

    const client = new Client({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false }
    })
    await client.connect()

    const { rows: planilhas } = await client.query(`
      select ano, tipo, sheet_id
      from t_planilhas
      where ativo = true
      order by ano, tipo
    `)

    if (!planilhas.length) {
      await client.end()
      return res.status(200).json({ ok:true, note:'Sem planilhas ativas em t_planilhas', inserted:0 })
    }

    const headerDistrib = [
      'CLIENTE','TIPO DE PROCESSO','RESP. PROCESSO','RESP. PETIÇÃO',
      'RESP. CORREÇÃO','RESP. DISTRIBUIÇÃO','COMPETÊNCIA','VALOR DA CAUSA',
      'DISTRIBUÍDO','UNIDADE'
    ].join('|')

    const mesUnico = req.query?.mes && String(req.query.mes).toUpperCase()
    const meses = mesUnico ? [mesUnico] : MESES

    let totalInseridos = 0
    const debug = []

    for (const { ano, tipo, sheet_id } of planilhas) {
      for (const mes of meses) {
        const url = `${process.env.EXPORTER_URL}?sheetId=${sheet_id}&tab=${encodeURIComponent(mes)}&tipo=${encodeURIComponent(tipo)}&token=${process.env.EXPORTER_TOKEN}`

        try {
          const resp = await fetch(url)
          const ct = resp.headers.get('content-type') || ''
          const bodyText = await resp.text()
          let data = null
          if (ct.includes('application/json')) {
            try { data = JSON.parse(bodyText) } catch {}
          }

          if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}; body=${bodyText.slice(0,300)}`)
          }
          if (!data || data.ok !== true) {
            throw new Error(`Exportador retornou erro: ${data?.error || bodyText.slice(0,300)}`)
          }

          const { rows = [], rowsCount = 0, colCount = 0 } = data
          debug.push({ tipo, ano, mes, rowsCount, colCount })

          let inserted = 0
          for (let i = 1; i < rows.length; i++) {
            const row = rows[i]
            if (!row || !row.some(v => v && String(v).trim() !== '')) continue

            if (tipo === 'DISTRIBUICAO') {
              const joined = row.map(v => (v == null ? '' : String(v).trim().toUpperCase())).join('|')
              if (joined === headerDistrib) continue
            }

            await client.query(`
              insert into stg_sheets_raw (source, sheet_id, aba_mes, row_idx, payload)
              values ($1,$2,$3,$4,$5)
              on conflict (source, sheet_id, aba_mes, row_idx) do update
              set payload = excluded.payload,
                  ingested_at = now()
            `, [tipo, sheet_id, mes, i, JSON.stringify(row)])

            inserted++
          }
          totalInseridos += inserted
        } catch (e) {
          debug.push({ tipo, ano, mes, error: e.message })
        }
      }
    }

    await client.end()
    return res.status(200).json({ ok:true, inserted: totalInseridos, debug })
  } catch (err) {
    // se algo estourar fora do fluxo, devolve JSON
    return res.status(500).json({ ok:false, error: String(err) })
  }
}
