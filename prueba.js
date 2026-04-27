require('dotenv').config();
const express = require('express');
const admin = require('firebase-admin');
const sql = require('mssql');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// Azure SQL
const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_DATABASE,
  port: 1433,
  options: {
    encrypt: true,
    trustServerCertificate: false
  }
};

sql.connect(config)
  .then(() => console.log('Azure: Conectado'))
  .catch(err => console.error('Azure: Error de conexion:', err));

// Firebase Admin
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();
console.log('Firebase: Admin inicializado');

// --- Rutas Azure ---
app.get('/datos', async (req, res) => {
  try {
    const result = await sql.query('SELECT * FROM usuarios');
    res.json(result.recordset);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message, code: err.code }); // <-- cambia esto
  }
});

app.post('/datos', async (req, res) => {
  const { nombre } = req.body;
  if (!nombre) return res.status(400).send('El nombre es requerido');
  try {
    await sql.query`INSERT INTO usuarios (nombre) VALUES (${nombre})`;
    res.send('Insertado');
  } catch (err) {
    res.status(500).send(err.message);
  }
});

app.put('/datos/:id', async (req, res) => {
  const { id } = req.params;
  const { nombre } = req.body;
  try {
    await sql.query`UPDATE usuarios SET nombre = ${nombre} WHERE id = ${id}`;
    res.send('Actualizado');
  } catch (err) {
    res.status(500).send(err.message);
  }
});

app.delete('/datos/:id', async (req, res) => {
  const { id } = req.params;
  try {
    await sql.query`DELETE FROM usuarios WHERE id = ${id}`;
    res.send('Eliminado');
  } catch (err) {
    res.status(500).send(err.message);
  }
});

// --- Rutas Firebase ---
app.post('/firebase/verify-token', async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).send('Token requerido');
  try {
    const decoded = await admin.auth().verifyIdToken(token);
    res.json({ uid: decoded.uid, email: decoded.email });
  } catch (err) {
    res.status(401).json({ error: 'Token invalido' });
  }
});

app.get('/firebase/:coleccion', async (req, res) => {
  try {
    const snapshot = await db.collection(req.params.coleccion).get();
    const docs = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    res.json(docs);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/firebase/:coleccion', async (req, res) => {
  try {
    const ref = await db.collection(req.params.coleccion).add(req.body);
    res.json({ id: ref.id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Insertar multiples documentos en batch
app.post('/firebase/:coleccion/batch', async (req, res) => {
  const { docs } = req.body;
  if (!Array.isArray(docs) || docs.length === 0) {
    return res.status(400).json({ error: 'Se requiere array "docs"' });
  }
  try {
    const collection = db.collection(req.params.coleccion);
    const batch = db.batch();
    docs.forEach(doc => {
      const ref = collection.doc();
      batch.set(ref, { ...doc, creado: admin.firestore.FieldValue.serverTimestamp() });
    });
    await batch.commit();
    res.json({ insertados: docs.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Puerto: ${PORT}`);
});