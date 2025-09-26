// scripts/run-update.js
import 'dotenv/config'
import pkg from 'pg'
const { Client } = pkg

const procs = [ "refresh_all()" ]; // <— troque a lista toda por isso

async function main(){
  const client = new Client({ connectionString: process.env.DATABASE_URL, ssl:{rejectUnauthorized:false} })
  await client.connect()
  console.log("✅ Conectado ao Supabase")
  for (const p of procs){
    console.log(`▶️ Executando ${p}...`)
    await client.query(`call ${p}`)   // se refresh_all for PROCEDURE use CALL; se FUNCTION, use SELECT
  }
  await client.end()
  console.log("🏁 Updates concluídos.")
}
main().catch(e=>{ console.error(e); process.exit(1) })
