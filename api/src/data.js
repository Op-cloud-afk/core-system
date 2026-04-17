const { app } = require('@azure/functions');
const { query } = require('./db');

// Table name whitelist and their SQL mappings
const TABLE_MAP = {
    containers: {
        table: 'Containers',
        columns: 'Id, UnitId, Prefix, SerialNr, ContainerTypeId, OwnerCode, PurchaseDate, PurchasePrice, TransportCost, PurchaseInvoice, SupplierId, SaleDate, SalePrice, SaleInvoiceNr, RentalDate, ReturnNr, CurrentLocation, Status, CreatedAt, UpdatedAt',
        orderBy: 'Id ASC'
    },
    movements: {
        table: 'Movements',
        columns: '*',
        orderBy: 'MovementDate DESC'
    },
    certificates: {
        table: 'Certificates',
        columns: '*',
        orderBy: 'Id ASC'
    },
    jobs: {
        table: 'Jobs',
        columns: '*',
        orderBy: 'Id ASC'
    },
    customers: {
        table: 'Customers',
        columns: '*',
        orderBy: 'Name ASC'
    },
    suppliers: {
        table: 'Suppliers',
        columns: '*',
        orderBy: 'Name ASC'
    },
    carriers: {
        table: 'Carriers',
        columns: '*',
        orderBy: 'Name ASC'
    },
    services: {
        table: 'Services',
        columns: '*',
        orderBy: 'Name ASC'
    },
    employees: {
        table: 'Employees',
        columns: '*',
        orderBy: 'Name ASC'
    },
    containertypes: {
        table: 'ContainerTypes',
        columns: '*',
        orderBy: 'Id ASC'
    },
    loosegear: {
        table: 'LooseGearCerts',
        columns: '*',
        orderBy: 'Id ASC'
    },
    images: {
        table: 'Images',
        columns: '*',
        orderBy: 'Id DESC'
    }
};

// GET /api/data?table=containers
app.http('getData', {
    methods: ['GET'],
    authLevel: 'anonymous',
    route: 'data',
    handler: async (request, context) => {
        try {
            const tableName = request.query.get('table');
            if (!tableName || !TABLE_MAP[tableName]) {
                return {
                    status: 400,
                    jsonBody: { error: 'Invalid table. Valid tables: ' + Object.keys(TABLE_MAP).join(', ') }
                };
            }

            const cfg = TABLE_MAP[tableName];
            const result = await query(`SELECT ${cfg.columns} FROM ${cfg.table} ORDER BY ${cfg.orderBy}`);

            return {
                jsonBody: {
                    table: tableName,
                    count: result.recordset.length,
                    data: result.recordset
                }
            };
        } catch (err) {
            context.error('getData error:', err);
            return {
                status: 500,
                jsonBody: { error: err.message }
            };
        }
    }
});

// POST /api/data?table=containers  (body = JSON object with column values)
app.http('addData', {
    methods: ['POST'],
    authLevel: 'anonymous',
    route: 'data',
    handler: async (request, context) => {
        try {
            const tableName = request.query.get('table');
            if (!tableName || !TABLE_MAP[tableName]) {
                return {
                    status: 400,
                    jsonBody: { error: 'Invalid table.' }
                };
            }

            const body = await request.json();
            const cfg = TABLE_MAP[tableName];
            const cols = Object.keys(body);
            const placeholders = cols.map((c, i) => `@p${i}`);

            const sqlText = `INSERT INTO ${cfg.table} (${cols.join(', ')}) VALUES (${placeholders.join(', ')})`;

            const { getPool } = require('./db');
            const pool = await getPool();
            const req = pool.request();
            cols.forEach((c, i) => req.input(`p${i}`, body[c]));
            await req.query(sqlText);

            return {
                status: 201,
                jsonBody: { success: true, message: `Row added to ${cfg.table}` }
            };
        } catch (err) {
            context.error('addData error:', err);
            return {
                status: 500,
                jsonBody: { error: err.message }
            };
        }
    }
});

// GET /api/schema?table=containers  (get column info for a table)
app.http('getSchema', {
    methods: ['GET'],
    authLevel: 'anonymous',
    route: 'schema',
    handler: async (request, context) => {
        try {
            const tableName = request.query.get('table');
            if (tableName && !TABLE_MAP[tableName]) {
                return { status: 400, jsonBody: { error: 'Invalid table.' } };
            }

            let sqlText;
            if (tableName) {
                sqlText = `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                           FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE TABLE_NAME = '${TABLE_MAP[tableName].table}'
                           ORDER BY ORDINAL_POSITION`;
            } else {
                sqlText = `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                           FROM INFORMATION_SCHEMA.COLUMNS
                           ORDER BY TABLE_NAME, ORDINAL_POSITION`;
            }

            const result = await query(sqlText);
            return { jsonBody: { data: result.recordset } };
        } catch (err) {
            context.error('getSchema error:', err);
            return { status: 500, jsonBody: { error: err.message } };
        }
    }
});
