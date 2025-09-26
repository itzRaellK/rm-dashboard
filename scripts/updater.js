// api/updater.js
import pkg from 'pg'
const { Client } = pkg

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
    if (!process.env.DATABASE_URL) {
      return res.status(500).json({ ok:false, error:'Missing env: DATABASE_URL' })
    }

    const client = new Client({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false }
    })
    await client.connect()

    // executa suas procedures
    const procs = [
      'update_dim_unidade()',
      'update_dim_responsavel()',
      'update_fato_requerimento()',
      'update_fato_distribuicao()'
    ]

    const results = []
    for (const p of procs) {
      try {
        await client.query(`select ${p}`)
        results.push({ proc:p, ok:true })
      } catch (e) {
        results.push({ proc:p, ok:false, error:e.message })
      }
    }

    await client.end()
    return res.status(200).json({ ok:true, results })
  } catch (err) {
    return res.status(500).json({ ok:false, error: String(err) })
  }
}
