require("dotenv").config();
const express = require("express");
const Database = require("better-sqlite3");
const AWS = require("aws-sdk");

const app = express();
const PORT = 3000;

const db = new Database("ventasnacionales.db");
const sqs = new AWS.SQS({ region: process.env.AWS_REGION });


app.use(express.json());

// Crear tabla si no existe
db.prepare(`
  CREATE TABLE IF NOT EXISTS ventas_nacionales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    modelo TEXT,
    precio INTEGER,
    comprador TEXT,
    fecha TEXT
  )
`).run();

async function pollSQS() {
    const params = {
      QueueUrl: process.env.SQS_QUEUE_URL,
      MaxNumberOfMessages: 5,
      WaitTimeSeconds: 10,
    };
  
    try {
      const data = await sqs.receiveMessage(params).promise();
  
      if (data.Messages) {
        for (const msg of data.Messages) {
          const venta = JSON.parse(msg.Body);
  
          // Guardar en DB
          db.prepare(`
            INSERT INTO ventas_nacionales (modelo, precio, comprador, fecha)
            VALUES (?, ?, ?, ?)
          `).run(venta.modelo, venta.precio, venta.comprador, venta.fecha);
  
          // Borrar mensaje de la cola
          await sqs.deleteMessage({
            QueueUrl: process.env.SQS_QUEUE_URL,
            ReceiptHandle: msg.ReceiptHandle,
          }).promise();
  
          console.log("✔️ Venta registrada desde SQS:", venta);
        }
      }
    } catch (err) {
      console.error("Error leyendo de SQS:", err.message);
    }
  
    setTimeout(pollSQS, 5000); // Vuelve a leer en 5s
  }
  
  pollSQS();

  app.get("/", (req, res) => {
    res.status(200).json({ mensaje: "Bienvenido a la API de las ventas nacionales" });
  });
  

app.get("/ventas", (req, res) => {
  const ventas = db.prepare("SELECT * FROM ventas_nacionales").all();
  res.json(ventas);
});

app.listen(PORT, () => {
  console.log(`API Ventas Nacionales corriendo en http://localhost:${PORT}`);
});