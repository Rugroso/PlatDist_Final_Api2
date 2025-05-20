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
  

app.get("/nacional/ventas", (req, res) => {
  const ventas = db.prepare("SELECT * FROM ventas_nacionales").all();
  res.json(ventas);
});

// Crear una nueva venta
app.post("/nacional/ventas", (req, res) => {
  const { modelo, precio, comprador, fecha } = req.body;

  if (!modelo || !precio || !comprador || !fecha) {
    return res.status(400).json({ error: "Todos los campos son obligatorios" });
  }

  const stmt = db.prepare(`
    INSERT INTO ventas_nacionales (modelo, precio, comprador, fecha)
    VALUES (?, ?, ?, ?)
  `);

  const result = stmt.run(modelo, precio, comprador, fecha);
  res.status(201).json({ mensaje: "Venta creada", id: result.lastInsertRowid });
});

// Actualizar una venta por ID
app.put("/nacional/ventas/:id", (req, res) => {
  const { id } = req.params;
  const { modelo, precio, comprador, fecha } = req.body;

  const ventaExistente = db.prepare("SELECT * FROM ventas_nacionales WHERE id = ?").get(id);
  if (!ventaExistente) {
    return res.status(404).json({ error: "Venta no encontrada" });
  }

  db.prepare(`
    UPDATE ventas_nacionales
    SET modelo = ?, precio = ?, comprador = ?, fecha = ?
    WHERE id = ?
  `).run(modelo || ventaExistente.modelo, precio || ventaExistente.precio, comprador || ventaExistente.comprador, fecha || ventaExistente.fecha, id);

  res.json({ mensaje: "Venta actualizada correctamente" });
});

// Eliminar una venta por ID
app.delete("/nacional/ventas/:id", (req, res) => {
  const { id } = req.params;

  const ventaExistente = db.prepare("SELECT * FROM ventas_nacionales WHERE id = ?").get(id);
  if (!ventaExistente) {
    return res.status(404).json({ error: "Venta no encontrada" });
  }

  db.prepare("DELETE FROM ventas_nacionales WHERE id = ?").run(id);
  res.json({ mensaje: "Venta eliminada correctamente" });
});


app.listen(PORT, () => {
  console.log(`API Ventas Nacionales corriendo en http://localhost:${PORT}`);
});