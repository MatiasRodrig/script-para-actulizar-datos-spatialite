import axios from 'axios';
import sqlite3 from 'sqlite3';
import dotenv from 'dotenv'

dotenv.config()

const obtenerDataDesdeEpicollect = async () => {
    try {
        const response = await axios.get(process.env.baseURL);
        if (response.status === 200) {
            console.log('Datos obtenidos correctamente desde Epicollect5');
            return response.data.data.entries; 
        } else {
            console.error(`Error: ${response.status}`);
            return null;
        }
    } catch (error) {
        console.error('Error al obtener datos desde Epicollect5:', error);
        return null;
    }
};


const iniciarBaseDeDatos = (db) => {
    db.serialize(() => {
        db.run(`CREATE TABLE IF NOT EXISTS entries (
            title TEXT,
            partida INTEGER,
            titular TEXT,
            fecha_visita TEXT,
            latitud REAL,
            longitud REAL,
            numero_acta INTEGER,
            monto_notificado TEXT
        )`, (err) => {
            if (err) {
                console.error('Error al crear la tabla:', err.message);
            } else {
                console.log('Tabla creada o verificada correctamente');
            }
        });
    });
};


const actualizarBaseDeDatos = async (db, nuevaData) => {
    const insertarEstado = db.prepare(`INSERT OR IGNORE INTO entries (title, partida, titular, fecha_visita, latitud, longitud, numero_acta, monto_notificado) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);

    let exitoso = false;

    try {
        await db.run('BEGIN TRANSACTION');
        for (const entry of nuevaData) {
            const { title, partida, titular, fecha_visita, latitud, longitud, numero_acta, monto_notificado } = entry;
            const row = await new Promise((resolve, reject) => {
                db.get('SELECT * FROM entries WHERE title = ? AND partida = ? AND titular = ? AND fecha_visita = ? AND latitud = ? AND longitud = ? AND numero_acta = ? AND monto_notificado = ?', [title, partida, titular, fecha_visita, latitud, longitud, numero_acta, monto_notificado], (err, row) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(row);
                    }
                });
            });
            if (!row) {
                await new Promise((resolve, reject) => {
                    insertarEstado.run(title, partida, titular, fecha_visita, latitud, longitud, numero_acta, monto_notificado, function (err) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve();
                        }
                    });
                });
            } else {
                console.log('La entrada ya existe en la base de datos, ignorándola o actualizándola según sea necesario');
            }
        }
        await db.run('COMMIT');
        console.log('Transacción realizada correctamente');
        exitoso = true;
    } catch (error) {
        console.error('Error en la transacción:', error.message);
    } finally {
        insertarEstado.finalize();
        console.log('Inserción finalizada correctamente');
    }

    return exitoso;
};


const actualizarBaseDeDatosSpatialite = async () => {
    const nuevaData = await obtenerDataDesdeEpicollect();
    if (nuevaData) {
        const db = new sqlite3.Database(process.env.dbFilePath, async (err) => {
            if (err) {
                console.error('Error al abrir la base de datos:', err.message);
                return;
            }
            console.log('Base de datos abierta correctamente');
            await new Promise((resolve, reject) => {
                db.serialize(() => {
                    db.run(`CREATE TABLE IF NOT EXISTS entries (
                        title TEXT,
                        partida INTEGER,
                        titular TEXT,
                        fecha_visita TEXT,
                        latitud REAL,
                        longitud REAL,
                        numero_acta INTEGER,
                        monto_notificado TEXT
                    )`, (err) => {
                        if (err) {
                            console.error('Error al crear la tabla:', err.message);
                            reject(err);
                        } else {
                            console.log('Tabla creada o verificada correctamente');
                            resolve();
                        }
                    });
                });
            });

            await actualizarBaseDeDatos(db, nuevaData);
            db.close((err) => {
                if (err) {
                    console.error('Error al cerrar la base de datos:', err.message);
                } else {
                    console.log('Base de datos cerrada correctamente');
                }
            });
        });

        console.log('Datos actualizados en prueba.db');
    } else {
        console.log('No se recibieron datos para actualizar');
    }
};

actualizarBaseDeDatosSpatialite();


