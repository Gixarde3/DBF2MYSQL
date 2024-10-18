const fs = require('fs');
const path = require('path');
const iconv = require('iconv-lite'); // Para manejar codificación de caracteres

// Función principal que procesa un directorio
function procesarDirectorio(directorio) {
    try {
        // Array para almacenar la estructura de las tablas
        const tablas = [];
        let contenidoSQL = '';

        // Leer el contenido del directorio
        const archivos = fs.readdirSync(directorio);

        //Obtener nombre de directorio
        const nombreDirectorio = path.basename(directorio);
        
        // Filtrar solo archivos DBF
        const archivosDBF = archivos.filter(archivo => 
            path.extname(archivo).toLowerCase() === '.dbf'
        );

        // Generar SQL para la estructura
        contenidoSQL += `-- Archivo: ${nombreDirectorio}\n
        DROP DATABASE IF EXISTS ${nombreDirectorio};
        CREATE DATABASE ${nombreDirectorio};
        USE ${nombreDirectorio};\n\n
        `;

        // Procesar cada archivo DBF
        archivosDBF.forEach(archivo => {
            const nombreTabla = archivo.toLowerCase().replace('.dbf', '');
            const rutaCompleta = path.join(directorio, archivo);
            
            // Leer el archivo DBF
            const buffer = fs.readFileSync(rutaCompleta);
            
            // Procesar el encabezado DBF
            const estructura = procesarEncabezadoDBF(buffer);
            
            // Solo procesar si hay campos válidos
            if (estructura.length > 0) {
                tablas.push({ nombre: nombreTabla, campos: estructura });
                
                // Procesar los registros
                const registros = procesarRegistrosDBF(buffer, estructura);
                
                
                contenidoSQL += generarSQL(nombreTabla, estructura, registros);
                
                // Buscar archivo CDX asociado
                const archivoCDX = path.join(directorio, `${nombreTabla}.cdx`);
                if (fs.existsSync(archivoCDX)) {
                    const indices = procesarCDX(archivoCDX);
                    contenidoSQL += generarIndicesSQL(nombreTabla, indices);
                }
            } else {
                console.warn(`Advertencia: La tabla ${nombreTabla} no tiene campos válidos y será omitida.`);
            }
        });

        // Guardar el SQL generado si hay contenido
        if (contenidoSQL) {
            const archivoSalida = path.join(directorio, 'conversion.sql');
            fs.writeFileSync(archivoSalida, contenidoSQL, 'utf8');
        }
        
        return tablas;
    } catch (error) {
        console.error('Error al procesar el directorio:', error);
        throw error;
    }
}

// Función para procesar el encabezado DBF
function procesarEncabezadoDBF(buffer) {
    const campos = [];
    const numCampos = Math.floor((buffer.readUInt16LE(8) - 32) / 32);
    
    for (let i = 0; i < numCampos; i++) {
        const offset = 32 + (i * 32);
        const nombreCampoRaw = buffer.slice(offset, offset + 11).toString('utf8').replace(/\u0000/g, '').trim();
        
        // Validar que el nombre del campo no esté vacío
        if (nombreCampoRaw && nombreCampoRaw.length > 0) {
            const tipoCampo = buffer.slice(offset + 11, offset + 12).toString();
            const longitud = buffer.readUInt8(offset + 16);
            const decimales = buffer.readUInt8(offset + 17);
            
            campos.push({
                nombre: nombreCampoRaw,
                tipo: tipoCampo,
                longitud: longitud,
                decimales: decimales
            });
        } else {
            console.warn(`Advertencia: Campo sin nombre encontrado en el índice ${i} - será omitido`);
        }
    }
    
    return campos;
}

// Función para procesar registros DBF
function procesarRegistrosDBF(buffer, estructura) {
    const registros = [];
    const tamañoEncabezado = buffer.readUInt16LE(8);
    const longitudRegistro = buffer.readUInt16LE(10);
    const numRegistros = buffer.readUInt32LE(4);
    
    for (let i = 0; i < numRegistros; i++) {
        const offset = tamañoEncabezado + (i * longitudRegistro);
        const registro = {};
        
        let posicionCampo = 1; // Skip deleted flag
        estructura.forEach(campo => {
            const valor = buffer.slice(
                offset + posicionCampo,
                offset + posicionCampo + campo.longitud
            );
            registro[campo.nombre] = procesarValorCampo(valor, campo);
            posicionCampo += campo.longitud;
        });
        
        registros.push(registro);
    }
    
    return registros;
}

// Función para procesar un archivo CDX
function procesarCDX(rutaArchivo) {
    const indices = [];
    const buffer = fs.readFileSync(rutaArchivo);
    
    // Leer el encabezado CDX
    const numIndices = buffer.readUInt16LE(4);
    
    // Procesar cada índice
    let offset = 512; // Tamaño típico del encabezado CDX
    for (let i = 0; i < numIndices; i++) {
        const nombreIndice = buffer.slice(offset, offset + 11).toString('utf8').trim();
        const expresion = buffer.slice(offset + 11, offset + 220).toString('utf8').trim();
        
        // Solo agregar índices con nombres válidos
        if (nombreIndice && nombreIndice.length > 0) {
            indices.push({
                nombre: nombreIndice,
                expresion: expresion
            });
        }
        
        offset += 512; // Cada entrada de índice típicamente ocupa 512 bytes
    }
    
    return indices;
}

// Función para generar SQL
function generarSQL(nombreTabla, estructura, registros) {
    let sql = `-- Tabla: ${nombreTabla}\n`;
    
    // Crear tabla solo si hay campos válidos
    if (estructura.length > 0) {
        sql += `CREATE TABLE ${nombreTabla} (\n`;
        sql += estructura.map(campo => {
            const tipo = mapearTipoSQL(campo);
            return `    ${campo.nombre} ${tipo}`;
        }).join(',\n');
        sql += '\n);\n\n';
        
        // Insertar datos
        if (registros.length > 0){
            sql += `INSERT INTO ${nombreTabla} (${estructura.map(c => c.nombre).join(', ')}) VALUES `;
        }
        registros.forEach(registro => {
            sql += '(';
            sql += estructura.map(campo => {
                const valor = registro[campo.nombre];
                return formatearValorSQL(valor, campo);
            }).join(', ');
            sql += '),\n';
        });
        sql = sql.slice(0, -2) + ';\n\n';
    }
    
    return sql + '\n';
}

// Función para generar SQL de índices
function generarIndicesSQL(nombreTabla, indices) {
    let sql = '';
    
    indices.forEach((indice, i) => {
        // Validar que la expresión del índice no esté vacía
        if (indice.expresion && indice.expresion.length > 0) {
            sql += `CREATE INDEX idx_${nombreTabla}_${i} ON ${nombreTabla} (${indice.expresion});\n`;
        }
    });
    
    return sql + '\n';
}

// Función auxiliar para mapear tipos de datos
function mapearTipoSQL(campo) {
    switch (campo.tipo) {
        case 'C': return `VARCHAR(${campo.longitud})`;
        case 'N': return campo.decimales > 0 ? 
            `DECIMAL(${campo.longitud},${campo.decimales})` : 
            `BIGINT`;
        case 'L': return 'BOOLEAN';
        case 'D': return 'DATE';
        case 'M': return 'TEXT';
        default: return `VARCHAR(${campo.longitud})`;
    }
}

// Función para procesar valores de campos
function procesarValorCampo(buffer, campo) {
    const valor = buffer.toString('utf8').trim();
    
    switch (campo.tipo) {
        case 'N':
            return valor === '' ? null : 
                   campo.decimales > 0 ? parseFloat(valor) : parseInt(valor);
        case 'L':
            return ['y', 't'].includes(valor.toLowerCase());
        case 'D':
            if (valor === '') return null;
            return `${valor.slice(0,4)}-${valor.slice(4,6)}-${valor.slice(6,8)}`;
        default:
            return valor === '' ? null : valor;
    }
}

// Función para formatear valores SQL
function formatearValorSQL(valor, campo) {
    if (valor === null) return 'NULL';
    
    switch (campo.tipo) {
        case 'C':
        case 'M':
        case 'D':
            return `'${valor.replace(/'/g, "''")}'`;
        case 'L':
            return valor ? '1' : '0';
        default:
            return valor.toString();
    }
}

// Exportar funciones principales
module.exports = {
    procesarDirectorio,
    procesarEncabezadoDBF,
    procesarRegistrosDBF,
    procesarCDX
};