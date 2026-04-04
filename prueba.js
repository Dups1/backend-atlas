require('dotenv').config();
const express = require('express');
const sql = require('mssql');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

//config
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
  .then(() => console.log('Conectado'))
  .catch(err => console.error('Error de conexión:', err));

//CRUD a bd
app.get('/datos', async (req, res) => {
  try {
    const result = await sql.query('SELECT * FROM usuarios');
    res.json(result.recordset);
  } catch (err) {
    console.error(err);
    res.status(500).send('Error en consulta');
  }
});

app.post('/datos', async (req, res) => {
  const { nombre } = req.body;

  if (!nombre) {
    return res.status(400).send('El nombre es requerido');
  }

  try {
    await sql.query`
      INSERT INTO usuarios (nombre)
      VALUES (${nombre})
    `;
    res.send('Insertado');
  } catch (err) {
    console.error(err);
    res.status(500).send(err.message);
  }
});

app.put('/datos/:id', async (req, res) => {
  const { id } = req.params;
  const { nombre } = req.body;

  try {
    await sql.query`
      UPDATE usuarios
      SET nombre = ${nombre}
      WHERE id = ${id}
    `;
    res.send('Actualizado');
  } catch (err) {
    console.error(err);
    res.status(500).send(err.message);
  }
});

app.delete('/datos/:id', async (req, res) => {
  const { id } = req.params;

  try {
    await sql.query`
      DELETE FROM usuarios WHERE id = ${id}
    `;
    res.send('Eliminado');
  } catch (err) {
    console.error(err);
    res.status(500).send(err.message);
  }
});


const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Puerto: ${PORT}`);
});