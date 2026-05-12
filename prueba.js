require('dotenv').config();
const crypto = require('crypto');
const express = require('express');
const admin = require('firebase-admin');
const cors = require('cors');
const multer = require('multer');
const { S3Client, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { RtcTokenBuilder, RtcRole } = require('agora-access-token');

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
  forcePathStyle: true,
});

const B2_BUCKET = process.env.B2_BUCKET_NAME;
const B2_PUBLIC_BASE_URL = process.env.B2_PUBLIC_BASE_URL?.replace(/\/$/, '') ?? '';
const upload = multer({ storage: multer.memoryStorage() });

console.log('B2 config:', {
  endpoint: process.env.B2_ENDPOINT,
  region: process.env.B2_REGION,
  bucket: process.env.B2_BUCKET_NAME,
  publicBaseUrl: process.env.B2_PUBLIC_BASE_URL,
});

const FIREBASE_API_KEY = process.env.FIREBASE_API_KEY;

const firebaseAuthUrl = (path) => {
  if (!FIREBASE_API_KEY) return '';
  return `https://identitytoolkit.googleapis.com/v1/${path}?key=${FIREBASE_API_KEY}`;
};

function ensureApiKey(req, res, next) {
  if (!FIREBASE_API_KEY) {
    return res.status(500).json({ error: 'Falta FIREBASE_API_KEY' });
  }
  next();
}

async function authenticateToken(req, res, next) {
  const auth = req.headers['authorization'];
  if (!auth || !auth.toString().startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Token requerido' });
  }
  const token = auth.toString().replace('Bearer ', '');
  try {
    const decoded = await admin.auth().verifyIdToken(token);
    req.firebaseUid = decoded.uid;
    next();
  } catch (err) {
    return res.status(401).json({ error: 'Token invalido' });
  }
}

function nombreVisibleUsuario(doc) {
  if (!doc) return null;
  const n = doc.nombre != null ? String(doc.nombre).trim() : '';
  if (n) return n;
  if (doc.email) return String(doc.email);
  return null;
}

// =============================================================================
// LLAMADAS DE VOZ (Agora token + Firestore + FCM): toda la logica de negocio aqui.
// El cliente Flutter solo: motor RTC, permisos, UI y lectura Firestore.
// =============================================================================
const { FieldValue } = require('firebase-admin/firestore');

const AGORA_APP_ID = process.env.AGORA_APP_ID || '';
const AGORA_APP_CERTIFICATE = process.env.AGORA_APP_CERTIFICATE || '';

const EST_LLAMADA = {
  RINGING: 'ringing',
  ACCEPTED: 'accepted',
  REJECTED: 'rejected',
  ENDED: 'ended',
  MISSED: 'missed',
  BUSY: 'busy',
};

/** UID Agora uint32 estable: primeros 4 bytes de SHA-256(UID Firebase). Misma logica en Flutter. */
function uidAgoraDesdeFirebaseUid(firebaseUid) {
  const buf = crypto.createHash('sha256').update(String(firebaseUid), 'utf8').digest();
  const u = buf.readUInt32BE(0);
  return u === 0 ? 1 : u;
}

/** Nombre de canal Agora (reglas Agora: longitud y caracteres). */
function validarNombreCanal(canal) {
  const c = String(canal ?? '').trim();
  if (!c || c.length > 63) {
    const err = new Error('canal invalido o vacio (max 63 caracteres)');
    err.code = 'BAD_CANAL';
    throw err;
  }
  return c;
}

/**
 * Segundos hasta expiracion del privilegio en token (60..86400).
 * undefined/null -> 3600. Valores no finitos o <= 0 -> error (sin sustituir por 3600 en silencio).
 */
function normalizarExpiraSegundos(val) {
  if (val === undefined || val === null) return 3600;
  const n = Number(val);
  if (!Number.isFinite(n)) {
    const err = new Error('expiraEnSegundos debe ser un numero finito');
    err.code = 'BAD_EXP';
    throw err;
  }
  if (n <= 0) {
    const err = new Error('expiraEnSegundos debe ser > 0 (minimo aplicado en token: 60 s)');
    err.code = 'BAD_EXP';
    throw err;
  }
  return Math.min(Math.max(Math.floor(n), 60), 86400);
}

function construirTokenRtc(canal, firebaseUid, expiraEnSegundos) {
  if (!AGORA_APP_ID || !AGORA_APP_CERTIFICATE) {
    const err = new Error('Faltan AGORA_APP_ID o AGORA_APP_CERTIFICATE en el servidor');
    err.code = 'AGORA_ENV';
    throw err;
  }
  const c = validarNombreCanal(canal);
  const uidAgora = uidAgoraDesdeFirebaseUid(firebaseUid);
  const expSeg =
    expiraEnSegundos === undefined || expiraEnSegundos === null
      ? 3600
      : normalizarExpiraSegundos(expiraEnSegundos);
  const privilegeExpiredTs = Math.floor(Date.now() / 1000) + expSeg;
  const token = RtcTokenBuilder.buildTokenWithUid(
    AGORA_APP_ID,
    AGORA_APP_CERTIFICATE,
    c,
    uidAgora,
    RtcRole.PUBLISHER,
    privilegeExpiredTs,
  );
  return { token, uidAgora, privilegeExpiredTs, canal: c };
}

/** Quita idLlamadaActiva en presencia de ambos participantes (Admin SDK). */
async function limpiarPresenciaParticipantes(idLlamada) {
  const ref = db.collection('llamadas').doc(idLlamada);
  const snap = await ref.get();
  if (!snap.exists) return;
  const { idEmisor, idReceptor } = snap.data();
  const batch = db.batch();
  const del = { idLlamadaActiva: FieldValue.delete() };
  if (idEmisor) batch.set(db.collection('llamadas_presencia').doc(idEmisor), del, { merge: true });
  if (idReceptor) batch.set(db.collection('llamadas_presencia').doc(idReceptor), del, { merge: true });
  await batch.commit();
}

async function enviarFcmLlamadaEntrante(idReceptor, { idLlamada, canal, idEmisor }) {
  const snap = await db.collection('tokens_llamadas').doc(idReceptor).get();
  const dataTok = snap.exists ? snap.data() : null;
  const fcmToken = dataTok && dataTok.token ? String(dataTok.token) : null;
  if (!fcmToken) {
    return { enviado: false, motivo: 'sin_token_fcm' };
  }
  await admin.messaging().send({
    token: fcmToken,
    data: {
      tipo: 'llamada_entrante',
      idLlamada,
      canal,
      idEmisor,
    },
    android: { priority: 'high' },
    apns: { headers: { 'apns-priority': '10' } },
  });
  return { enviado: true };
}

/** Token RTC (renovacion desde el cliente). */
app.post('/llamadas/agora-token', authenticateToken, (req, res) => {
  try {
    const canal = req.body?.canal != null ? String(req.body.canal).trim() : '';
    validarNombreCanal(canal);
    const expRaw = req.body?.expiraEnSegundos;
    const exp = expRaw === undefined || expRaw === null ? 3600 : normalizarExpiraSegundos(expRaw);
    const out = construirTokenRtc(canal, req.firebaseUid, exp);
    res.json({ ...out, agoraAppId: AGORA_APP_ID });
  } catch (err) {
    console.error('agora-token', err);
    if (err.code === 'BAD_EXP' || err.code === 'BAD_CANAL') {
      return res.status(400).json({ error: err.message });
    }
    const code = err.code === 'AGORA_ENV' ? 503 : 500;
    res.status(code).json({ error: err.message });
  }
});

/** Crea la llamada en Firestore con transaccion (presencia + anti carrera), FCM y token emisor. */
app.post('/llamadas/iniciar', authenticateToken, async (req, res) => {
  try {
    const idEmisor = req.firebaseUid;
    const idReceptor = req.body?.idReceptor != null ? String(req.body.idReceptor).trim() : '';
    if (!idReceptor) {
      return res.status(400).json({ error: 'idReceptor obligatorio' });
    }
    if (idEmisor === idReceptor) {
      return res.status(400).json({ error: 'No puedes llamarte a ti mismo' });
    }

    const nombreEmisor = req.body?.nombreEmisor != null ? String(req.body.nombreEmisor).trim() : '';
    const nombreReceptor = req.body?.nombreReceptor != null ? String(req.body.nombreReceptor).trim() : '';

    let idLlamada;
    let canal;
    try {
      const txResult = await db.runTransaction(async (t) => {
        const refE = db.collection('llamadas_presencia').doc(idEmisor);
        const refR = db.collection('llamadas_presencia').doc(idReceptor);
        const [snapE, snapR] = await Promise.all([t.get(refE), t.get(refR)]);

        const asegurarLibre = async (snap, rol) => {
          if (!snap.exists) return;
          const activaId = snap.data()?.idLlamadaActiva;
          if (!activaId) return;
          const cSnap = await t.get(db.collection('llamadas').doc(String(activaId)));
          if (!cSnap.exists) {
            t.set(snap.ref, { idLlamadaActiva: FieldValue.delete() }, { merge: true });
            return;
          }
          const cd = cSnap.data();
          if (cd.activa === true && (cd.estado === EST_LLAMADA.RINGING || cd.estado === EST_LLAMADA.ACCEPTED)) {
            const err = new Error(rol === 'emisor' ? 'emisor_ocupado' : 'receptor_ocupado');
            err.statusCode = 409;
            if (rol === 'receptor') err.codigo = EST_LLAMADA.BUSY;
            if (rol === 'emisor') err.idLlamadaActiva = String(activaId);
            throw err;
          }
          t.set(snap.ref, { idLlamadaActiva: FieldValue.delete() }, { merge: true });
        };

        await asegurarLibre(snapE, 'emisor');
        await asegurarLibre(snapR, 'receptor');

        const refCall = db.collection('llamadas').doc();
        const newId = refCall.id;
        const newCanal = `voz_${newId}`;
        t.set(refCall, {
          idLlamada: newId,
          idEmisor,
          idReceptor,
          canal: newCanal,
          estado: EST_LLAMADA.RINGING,
          fecha: FieldValue.serverTimestamp(),
          activa: true,
          ...(nombreEmisor ? { nombreEmisor } : {}),
          ...(nombreReceptor ? { nombreReceptor } : {}),
        });
        t.set(refE, { idLlamadaActiva: newId }, { merge: true });
        t.set(refR, { idLlamadaActiva: newId }, { merge: true });
        return { idLlamada: newId, canal: newCanal };
      });
      idLlamada = txResult.idLlamada;
      canal = txResult.canal;
    } catch (e) {
      if (e.statusCode === 409) {
        return res.status(409).json({
          error: e.message,
          ...(e.codigo ? { codigo: e.codigo } : {}),
          ...(e.idLlamadaActiva ? { idLlamadaActiva: e.idLlamadaActiva } : {}),
        });
      }
      throw e;
    }

    const cred = construirTokenRtc(canal, idEmisor, undefined);

    let fcmRes = { enviado: false, motivo: 'no_intentado' };
    try {
      fcmRes = await enviarFcmLlamadaEntrante(idReceptor, { idLlamada, canal, idEmisor });
    } catch (e) {
      console.error('FCM iniciar', e);
      fcmRes = { enviado: false, motivo: String(e.message || e) };
    }

    res.status(201).json({
      idLlamada,
      canal,
      agoraAppId: AGORA_APP_ID,
      token: cred.token,
      uidAgora: cred.uidAgora,
      privilegeExpiredTs: cred.privilegeExpiredTs,
      fcm: fcmRes,
    });
  } catch (err) {
    console.error('llamadas/iniciar', err);
    const code = err.code === 'AGORA_ENV' ? 503 : 500;
    res.status(code).json({ error: err.message });
  }
});

/** Receptor acepta: valida estado, actualiza Firestore y devuelve token RTC. */
app.post('/llamadas/aceptar', authenticateToken, async (req, res) => {
  try {
    const uid = req.firebaseUid;
    const idLlamada = req.body?.idLlamada != null ? String(req.body.idLlamada).trim() : '';
    if (!idLlamada) {
      return res.status(400).json({ error: 'idLlamada obligatorio' });
    }
    const ref = db.collection('llamadas').doc(idLlamada);
    const snap = await ref.get();
    if (!snap.exists) {
      return res.status(404).json({ error: 'llamada_no_encontrada' });
    }
    const data = snap.data();
    if (data.idReceptor !== uid) {
      return res.status(403).json({ error: 'solo_el_receptor_puede_aceptar' });
    }
    if (data.estado !== EST_LLAMADA.RINGING) {
      return res.status(409).json({ error: 'estado_invalido', estado: data.estado });
    }

    let cred;
    try {
      const canalOk = validarNombreCanal(data.canal);
      cred = construirTokenRtc(canalOk, uid, undefined);
    } catch (e) {
      return res.status(500).json({ error: 'llamada_sin_canal_valido', detalle: e.message });
    }

    await ref.update({
      estado: EST_LLAMADA.ACCEPTED,
    });

    res.json({
      idLlamada,
      canal: cred.canal,
      agoraAppId: AGORA_APP_ID,
      token: cred.token,
      uidAgora: cred.uidAgora,
      privilegeExpiredTs: cred.privilegeExpiredTs,
    });
  } catch (err) {
    console.error('llamadas/aceptar', err);
    const code = err.code === 'AGORA_ENV' ? 503 : 500;
    res.status(code).json({ error: err.message });
  }
});

/** Receptor rechaza timbre. */
app.post('/llamadas/rechazar', authenticateToken, async (req, res) => {
  try {
    const uid = req.firebaseUid;
    const idLlamada = req.body?.idLlamada != null ? String(req.body.idLlamada).trim() : '';
    if (!idLlamada) {
      return res.status(400).json({ error: 'idLlamada obligatorio' });
    }
    const ref = db.collection('llamadas').doc(idLlamada);
    const snap = await ref.get();
    if (!snap.exists) {
      return res.status(404).json({ error: 'llamada_no_encontrada' });
    }
    const data = snap.data();
    if (data.idReceptor !== uid) {
      return res.status(403).json({ error: 'solo_el_receptor_puede_rechazar' });
    }
    if (data.estado !== EST_LLAMADA.RINGING) {
      return res.status(409).json({ error: 'estado_invalido', estado: data.estado });
    }
    await ref.update({
      estado: EST_LLAMADA.REJECTED,
      activa: false,
    });
    await limpiarPresenciaParticipantes(idLlamada);
    res.json({ ok: true, idLlamada });
  } catch (err) {
    console.error('llamadas/rechazar', err);
    res.status(500).json({ error: err.message });
  }
});

/** Emisor cuelga antes de que contesten (sigue en ringing). */
app.post('/llamadas/cancelar-emisor', authenticateToken, async (req, res) => {
  try {
    const uid = req.firebaseUid;
    const idLlamada = req.body?.idLlamada != null ? String(req.body.idLlamada).trim() : '';
    if (!idLlamada) {
      return res.status(400).json({ error: 'idLlamada obligatorio' });
    }
    const ref = db.collection('llamadas').doc(idLlamada);
    const snap = await ref.get();
    if (!snap.exists) {
      return res.status(404).json({ error: 'llamada_no_encontrada' });
    }
    const data = snap.data();
    if (data.idEmisor !== uid) {
      return res.status(403).json({ error: 'solo_el_emisor_puede_cancelar' });
    }
    if (data.estado !== EST_LLAMADA.RINGING) {
      return res.status(409).json({ error: 'estado_invalido', estado: data.estado });
    }
    await ref.update({
      estado: EST_LLAMADA.ENDED,
      activa: false,
    });
    await limpiarPresenciaParticipantes(idLlamada);
    res.json({ ok: true, idLlamada });
  } catch (err) {
    console.error('llamadas/cancelar-emisor', err);
    res.status(500).json({ error: err.message });
  }
});

/** Cualquier participante cuelga llamada en curso o finaliza sesion. */
app.post('/llamadas/finalizar', authenticateToken, async (req, res) => {
  try {
    const uid = req.firebaseUid;
    const idLlamada = req.body?.idLlamada != null ? String(req.body.idLlamada).trim() : '';
    if (!idLlamada) {
      return res.status(400).json({ error: 'idLlamada obligatorio' });
    }
    const ref = db.collection('llamadas').doc(idLlamada);
    const snap = await ref.get();
    if (!snap.exists) {
      return res.status(404).json({ error: 'llamada_no_encontrada' });
    }
    const data = snap.data();
    if (data.idEmisor !== uid && data.idReceptor !== uid) {
      return res.status(403).json({ error: 'no_participante' });
    }
    await ref.update({
      estado: EST_LLAMADA.ENDED,
      activa: false,
    });
    await limpiarPresenciaParticipantes(idLlamada);
    res.json({ ok: true, idLlamada });
  } catch (err) {
    console.error('llamadas/finalizar', err);
    res.status(500).json({ error: err.message });
  }
});

/** Timeout de timbre: solo el emisor y solo si sigue en ringing (sustituye logica en cliente). */
app.post('/llamadas/marcar-perdida', authenticateToken, async (req, res) => {
  try {
    const uid = req.firebaseUid;
    const idLlamada = req.body?.idLlamada != null ? String(req.body.idLlamada).trim() : '';
    if (!idLlamada) {
      return res.status(400).json({ error: 'idLlamada obligatorio' });
    }
    const ref = db.collection('llamadas').doc(idLlamada);
    const snap = await ref.get();
    if (!snap.exists) {
      return res.status(404).json({ error: 'llamada_no_encontrada' });
    }
    const data = snap.data();
    if (data.idEmisor !== uid) {
      return res.status(403).json({ error: 'solo_el_emisor_puede_marcar_perdida' });
    }
    if (data.estado !== EST_LLAMADA.RINGING) {
      return res.json({ ok: true, sin_cambios: true, estado: data.estado });
    }
    await ref.update({
      estado: EST_LLAMADA.MISSED,
      activa: false,
    });
    await limpiarPresenciaParticipantes(idLlamada);
    res.json({ ok: true, idLlamada });
  } catch (err) {
    console.error('llamadas/marcar-perdida', err);
    res.status(500).json({ error: err.message });
  }
});

/** Compatibilidad: reenvia FCM (la ruta preferida es POST /llamadas/iniciar). */
app.post('/llamadas/notificar-entrante', authenticateToken, async (req, res) => {
  try {
    const idReceptor = req.body?.idReceptor != null ? String(req.body.idReceptor).trim() : '';
    const idLlamada = req.body?.idLlamada != null ? String(req.body.idLlamada).trim() : '';
    const canal = req.body?.canal != null ? String(req.body.canal).trim() : '';
    if (!idReceptor || !idLlamada || !canal) {
      return res.status(400).json({ error: 'idReceptor, idLlamada y canal son obligatorios' });
    }
    validarNombreCanal(canal);
    const fcmRes = await enviarFcmLlamadaEntrante(idReceptor, {
      idLlamada,
      canal,
      idEmisor: req.firebaseUid,
    });
    res.json(fcmRes);
  } catch (err) {
    console.error('notificar-entrante', err);
    if (err.code === 'BAD_CANAL') {
      return res.status(400).json({ error: err.message });
    }
    res.status(500).json({ error: err.message });
  }
});

// Custom token para Firebase Client SDK (Firestore snapshots en Flutter)
app.post('/auth/custom-token', authenticateToken, async (req, res) => {
  try {
    const customToken = await admin.auth().createCustomToken(req.firebaseUid);
    res.json({ customToken });
  } catch (err) {
    console.error('custom-token', err);
    res.status(500).json({ error: err.message });
  }
});

// --- Mensajes (cliente / trabajador) ---

function makeConversationId(uidA, uidB) {
  return [uidA, uidB].sort().join('_');
}

function normalizeRol(rol) {
  return String(rol ?? 'cliente').toLowerCase();
}

async function fetchUsuarioDoc(uid) {
  const doc = await db.collection('usuarios').doc(uid).get();
  if (!doc.exists) return null;
  return { id: doc.id, ...doc.data() };
}

function assertParticipant(convData, uid) {
  const parts = convData?.participantes;
  if (!Array.isArray(parts) || !parts.includes(uid)) {
    return false;
  }
  return true;
}

function tsToIso(v) {
  if (v && typeof v.toDate === 'function') return v.toDate().toISOString();
  return null;
}

// Crear o asegurar conversacion cliente-trabajador (mismo id para ambos lados)
app.post('/mensajes/conversaciones', authenticateToken, async (req, res) => {
  const otroUid = req.body?.otroUid;
  if (!otroUid || typeof otroUid !== 'string') {
    return res.status(400).json({ error: 'Body requiere otroUid (string)' });
  }
  const meUid = req.firebaseUid;
  if (otroUid === meUid) {
    return res.status(400).json({ error: 'No puedes conversar contigo mismo' });
  }

  try {
    const [yo, otro] = await Promise.all([fetchUsuarioDoc(meUid), fetchUsuarioDoc(otroUid)]);
    if (!yo) return res.status(404).json({ error: 'Tu usuario no existe en usuarios' });
    if (!otro) return res.status(404).json({ error: 'Usuario destino no encontrado' });

    const rolYo = normalizeRol(yo.rol);
    const rolOtro = normalizeRol(otro.rol);
    const okPair =
      (rolYo === 'cliente' && rolOtro === 'trabajador') ||
      (rolYo === 'trabajador' && rolOtro === 'cliente');
    if (!okPair) {
      return res.status(403).json({ error: 'Solo se permiten conversaciones entre cliente y trabajador' });
    }

    const conversationId = makeConversationId(meUid, otroUid);
    const clienteUid = rolYo === 'cliente' ? meUid : otroUid;
    const trabajadorUid = rolYo === 'trabajador' ? meUid : otroUid;

    const ref = db.collection('conversaciones').doc(conversationId);
    const existente = await ref.get();
    const clienteDoc = rolYo === 'cliente' ? yo : otro;
    const trabajadorDoc = rolYo === 'trabajador' ? yo : otro;
    const payload = {
      participantes: [meUid, otroUid],
      clienteUid,
      trabajadorUid,
      clienteNombre: nombreVisibleUsuario(clienteDoc),
      trabajadorNombre: nombreVisibleUsuario(trabajadorDoc),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    if (!existente.exists) {
      payload.creado = admin.firestore.FieldValue.serverTimestamp();
    }
    await ref.set(payload, { merge: true });

    res.status(200).json({ conversationId });
  } catch (err) {
    console.error('mensajes/conversaciones POST', err);
    res.status(500).json({ error: err.message });
  }
});

// Listar conversaciones del usuario actual
app.get('/mensajes/conversaciones', authenticateToken, async (req, res) => {
  const meUid = req.firebaseUid;
  try {
    const snap = await db
      .collection('conversaciones')
      .where('participantes', 'array-contains', meUid)
      .orderBy('updatedAt', 'desc')
      .limit(50)
      .get();

    const items = snap.docs.map((doc) => {
      const data = doc.data();
      return {
        id: doc.id,
        participantes: data.participantes,
        clienteUid: data.clienteUid,
        trabajadorUid: data.trabajadorUid,
        ultimoMensaje: data.ultimoMensaje ?? null,
        ultimoSenderUid: data.ultimoSenderUid ?? null,
        updatedAt: tsToIso(data.updatedAt),
        creado: tsToIso(data.creado),
      };
    });

    const uidSet = new Set();
    for (const it of items) {
      if (it.clienteUid) uidSet.add(it.clienteUid);
      if (it.trabajadorUid) uidSet.add(it.trabajadorUid);
    }
    const nombrePorUid = {};
    await Promise.all(
      [...uidSet].map(async (uid) => {
        const udoc = await db.collection('usuarios').doc(uid).get();
        if (udoc.exists) {
          const d = udoc.data();
          nombrePorUid[uid] = (d.nombre && String(d.nombre).trim()) || d.email || uid;
        }
      }),
    );

    const enriched = items.map((it) => ({
      ...it,
      clienteNombre: it.clienteUid ? (nombrePorUid[it.clienteUid] ?? null) : null,
      trabajadorNombre: it.trabajadorUid ? (nombrePorUid[it.trabajadorUid] ?? null) : null,
    }));
    res.json(enriched);
  } catch (err) {
    console.error('mensajes/conversaciones GET', err);
    res.status(500).json({ error: err.message });
  }
});

// Listar mensajes de una conversacion
app.get('/mensajes/conversaciones/:conversationId/mensajes', authenticateToken, async (req, res) => {
  const meUid = req.firebaseUid;
  const { conversationId } = req.params;
  const limitNum = Math.min(parseInt(req.query.limit, 10) || 50, 100);
  const antesDe = req.query.antesDe;

  try {
    const convRef = db.collection('conversaciones').doc(conversationId);
    const convSnap = await convRef.get();
    if (!convSnap.exists) {
      return res.status(404).json({ error: 'Conversacion no encontrada' });
    }
    const convData = convSnap.data();
    if (!assertParticipant(convData, meUid)) {
      return res.status(403).json({ error: 'No participas en esta conversacion' });
    }

    let q = convRef.collection('mensajes').orderBy('createdAt', 'desc').limit(limitNum);
    if (antesDe) {
      const cursor = await convRef.collection('mensajes').doc(antesDe).get();
      if (cursor.exists) {
        q = q.startAfter(cursor);
      }
    }
    const snap = await q.get();
    const mensajes = snap.docs.map((doc) => {
      const d = doc.data();
      return {
        id: doc.id,
        conversationId,
        senderUid: d.senderUid,
        texto: d.texto,
        createdAt: tsToIso(d.createdAt),
      };
    });
    res.json(mensajes);
  } catch (err) {
    console.error('mensajes GET lista', err);
    res.status(500).json({ error: err.message });
  }
});

// Enviar mensaje
app.post('/mensajes/conversaciones/:conversationId', authenticateToken, async (req, res) => {
  const meUid = req.firebaseUid;
  const { conversationId } = req.params;
  const texto = (req.body?.texto ?? '').toString().trim();
  if (!texto) {
    return res.status(400).json({ error: 'texto es obligatorio' });
  }

  try {
    const convRef = db.collection('conversaciones').doc(conversationId);
    const convSnap = await convRef.get();
    if (!convSnap.exists) {
      return res.status(404).json({ error: 'Conversacion no encontrada' });
    }
    const convData = convSnap.data();
    if (!assertParticipant(convData, meUid)) {
      return res.status(403).json({ error: 'No participas en esta conversacion' });
    }

    const msgRef = await convRef.collection('mensajes').add({
      senderUid: meUid,
      texto,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    await convRef.update({
      ultimoMensaje: texto,
      ultimoSenderUid: meUid,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    res.status(201).json({ id: msgRef.id, conversationId });
  } catch (err) {
    console.error('mensajes POST enviar', err);
    res.status(500).json({ error: err.message });
  }
});

// Registro de usuario por correo y password
app.post('/auth/register', async (req, res) => {
  const { email, password, rol = 'cliente', categoria, subcategoria, nombre } = req.body;
  if (!email || !password) {
    return res.status(400).json({ error: 'Email y password son obligatorios' });
  }

  try {
    const userRecord = await admin.auth().createUser({ email, password });
    await db.collection('usuarios').doc(userRecord.uid).set({
      uid: userRecord.uid,
      email,
      nombre: nombre ?? null,
      rol,
      categoria: categoria ?? null,
      subcategoria: subcategoria ?? null,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });
    res.status(201).json({ uid: userRecord.uid });
  } catch (err) {
    console.error('Auth register error', err);
    if (err.code === 'auth/email-already-exists') {
      return res.status(409).json({ error: 'Email ya registrado' });
    }
    res.status(500).json({ error: err.message });
  }
});

// Login con Firebase REST API
app.post('/auth/login', ensureApiKey, async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) {
    return res.status(400).json({ error: 'Email y password son obligatorios' });
  }

  try {
    const response = await fetch(firebaseAuthUrl('accounts:signInWithPassword'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email,
        password,
        returnSecureToken: true,
      }),
    });
    const data = await response.json();
    if (!response.ok) {
      return res.status(response.status).json(data);
    }
    res.json(data);
  } catch (err) {
    console.error('Auth login error', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/usuarios/me', authenticateToken, async (req, res) => {
  try {
    const uid = req.firebaseUid;
    if (!uid) {
      return res.status(401).json({ error: 'UID no encontrado' });
    }
    const doc = await db.collection('usuarios').doc(uid).get();
    if (!doc.exists) {
      return res.status(404).json({ error: 'Usuario no encontrado' });
    }
    res.json({ id: doc.id, ...doc.data() });
  } catch (err) {
    console.error('Perfil error', err);
    res.status(500).json({ error: err.message });
  }
});

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

// Obtener imagenes del laboratorio
app.get('/firebase/laboratorio', async (req, res) => {
  try {
    const snapshot = await db
      .collection('laboratorio_uploads')
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

// Leer coleccion
app.get('/firebase/:coleccion', async (req, res) => {
  try {
    let query = db.collection(req.params.coleccion);
    // Filtros opcionales: ?campo=valor (excluye parametros internos)
    const skip = new Set(['limit', 'offset']);
    for (const [key, value] of Object.entries(req.query)) {
      if (!skip.has(key)) query = query.where(key, '==', value);
    }
    const snapshot = await query.get();
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

// Actualizar documento por ID
app.patch('/firebase/:coleccion/:id', async (req, res) => {
  try {
    await db.collection(req.params.coleccion).doc(req.params.id).update(req.body);
    res.json({ ok: true });
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

// --- Rutas Backblaze B2 ---

// Subir archivo
app.post('/storage/upload', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se recibio archivo' });
  const key = `${Date.now()}-${req.file.originalname}`;
  console.log('Upload request start:', {
    name: req.file.originalname,
    size: req.file.size,
    fieldname: req.file.fieldname,
  });
  try {
    console.log('Uploading to b2', key);
    await s3.send(new PutObjectCommand({
      Bucket: B2_BUCKET,
      Key: key,
      Body: req.file.buffer,
      ContentType: req.file.mimetype,
    }));

    const url = `${B2_PUBLIC_BASE_URL}/${key}`;
    const docRef = await db.collection('laboratorio_uploads').add({
      key,
      url,
      originalName: req.file.originalname,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    console.log('Upload saved to Firestore', {
      id: docRef.id,
      url,
      bucket: B2_BUCKET,
      key,
    });
    console.log('Upload response ready', {
      docId: docRef.id,
      status: 'success',
    });
    res.json({ key, url, docId: docRef.id });
  } catch (err) {
    console.error('Upload failed', err);
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
