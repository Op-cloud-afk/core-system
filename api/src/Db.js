const sql = require('mssql');

let pool = null;

async function getPool() {
    if (pool) return pool;

    const config = {
        server: process.env.SQL_SERVER || 'njco-sql-server.database.windows.net',
        database: process.env.SQL_DATABASE || 'njco-operations',
        options: {
            encrypt: true,
            trustServerCertificate: false
        }
    };

    // Use Azure AD (Managed Identity) or SQL auth
    if (process.env.SQL_USE_AAD === 'true') {
        config.authentication = {
            type: 'azure-active-directory-default'
        };
    } else {
        config.user = process.env.SQL_USER;
        config.password = process.env.SQL_PASSWORD;
    }

    pool = await sql.connect(config);
    return pool;
}

async function query(sqlText, params) {
    const p = await getPool();
    const req = p.request();
    if (params) {
        for (const [key, val] of Object.entries(params)) {
            req.input(key, val);
        }
    }
    return req.query(sqlText);
}

module.exports = { getPool, query };