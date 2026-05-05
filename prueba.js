require('dotenv').config();
const express = require('express');
const admin = require('firebase-admin');
const cors = require('cors');
const multer = require('multer');
const { S3Client, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const app = express();
app.use(cors());
app.use(express.json());

// Firebase Admin
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();
console.log('Firebase: Admin inicializado');

// Backblaze B2
const s3 = new S3Client({
  endpoint: process.env.B2_ENDPOINT,
  region: process.env.B2_REGION,
  credentials: {
    accessKeyId: process.env.B2_KEY_ID,
    secretAccessKey: process.env.B2_APPLICATION_KEY,
  },
});

const B2_BUCKET = process.env.B2_BUCKET_NAME;
const B2_PUBLIC_BASE_URL = process.env.B2_PUBLIC_BASE_URL?.replace(/\/$/, '') ?? '';
const upload = multer({ storage: multer.memoryStorage() });

// Verificar token de Firebase Auth
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

// Leer coleccion
app.get('/firebase/:coleccion', async (req, res) => {
  try {
    const snapshot = await db.collection(req.params.coleccion).get();
    const docs = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    res.json(docs);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Insertar documento
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

// Estado del backend
app.get('/status', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Obtener imagenes del laboratorio
app.get('/firebase/laboratorio', async (req, res) => {
  try {
    const snapshot = await db
      .collection('laboratorio_uploads')
      .orderBy('createdAt', 'desc')
      .get();

    const entries = snapshot.docs.map(doc => {
      const data = doc.data();
      const createdAt = data.createdAt;
      return {
        id: doc.id,
        key: data.key,
        url: data.url,
        originalName: data.originalName,
        createdAt: createdAt && typeof createdAt.toDate === 'function'
          ? createdAt.toDate().toISOString()
          : null,
      };
    });

    res.json(entries);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Rutas Backblaze B2 ---

// Subir archivo
app.post('/storage/upload', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se recibio archivo' });
  const key = `${Date.now()}-${req.file.originalname}`;
  try {
    await s3.send(new PutObjectCommand({
      Bucket: B2_BUCKET,
      Key: key,
      Body: req.file.buffer,
      ContentType: req.file.mimetype,
    }));

    const url = `${B2_PUBLIC_BASE_URL}/${B2_BUCKET}/${key}`;
    const docRef = await db.collection('laboratorio_uploads').add({
      key,
      url,
      originalName: req.file.originalname,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    res.json({ key, url, docId: docRef.id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Listar archivos del bucket
app.get('/storage', async (req, res) => {
  try {
    const data = await s3.send(new ListObjectsV2Command({ Bucket: B2_BUCKET }));
    const files = (data.Contents || []).map(f => ({ key: f.Key, size: f.Size, modified: f.LastModified }));
    res.json(files);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// URL firmada para descargar un archivo (expira en 1 hora)
app.get('/storage/url/:key', async (req, res) => {
  try {
    const url = await getSignedUrl(
      s3,
      new GetObjectCommand({ Bucket: B2_BUCKET, Key: req.params.key }),
      { expiresIn: 3600 }
    );
    res.json({ url });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Eliminar archivo
app.delete('/storage/:key', async (req, res) => {
  try {
    await s3.send(new DeleteObjectCommand({ Bucket: B2_BUCKET, Key: req.params.key }));
    res.json({ message: 'Archivo eliminado' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Puerto: ${PORT}`);
});
