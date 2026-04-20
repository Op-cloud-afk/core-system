const { app } = require('@azure/functions');
const { query } = require('./db');

const TABLE_MAP = {
    containers: {
        sql: `SELECT c.Id, c.UnitId, c.Prefix, c.SerialNr,
                ct.Code AS ContainerTypeCode, ct.Description AS ContainerTypeName,
                c.OwnerCode, c.PurchaseDate, c.PurchasePrice, c.TransportCost,
                c.PurchaseInvoice, c.SupplierId,
                sup.Name AS SupplierName,
                c.SaleDate, c.SalePrice, c.SaleInvoiceNr,
                c.RentalDate, c.ReturnNr, c.CurrentLocation, c.Status,
                c.CreatedAt, c.UpdatedAt
              FROM Containers c
              LEFT JOIN ContainerTypes ct ON c.ContainerTypeId = ct.Id
              LEFT JOIN Suppliers sup ON c.SupplierId = sup.Id
              ORDER BY c.Id ASC`
    },
    movements: {
        sql: `SELECT m.Id, m.ContainerId,
                con.UnitId, con.Prefix, con.SerialNr,
                m.Dato, m.Direction,
                m.CarrierId, car.Name AS CarrierName,
                m.CustomerId, cust.Name AS CustomerName,
                m.EmployeeId, emp.Name AS EmployeeName,
                m.Comment, m.AccountingRef, m.Status, m.SPItemId,
                m.CreatedAt,
                (SELECT STRING_AGG(st.Name, ', ')
                 FROM MovementServices ms
                 JOIN ServiceTypes st ON ms.ServiceTypeId = st.Id
                 WHERE ms.MovementId = m.Id) AS ServiceNames
              FROM Movements m
              LEFT JOIN Containers con ON m.ContainerId = con.Id
              LEFT JOIN Carriers car ON m.CarrierId = car.Id
              LEFT JOIN Customers cust ON m.CustomerId = cust.Id
              LEFT JOIN Employees emp ON m.EmployeeId = emp.Id
              ORDER BY m.Dato DESC`
    },
    certificates: {
        sql: `SELECT cert.Id, cert.ContainerId,
                con.UnitId, con.Prefix, con.SerialNr,
                cert.CertNr, cert.InspectionTypeId,
                it.Name AS InspectionTypeName,
                cert.EmployeeId, emp.Name AS EmployeeName,
                cert.CustomerId, cust.Name AS CustomerName,
                cert.TestDate, cert.CSC_Months, cert.ExpiryDate,
                cert.Approved, cert.SentStatus, cert.InvoiceNr,
                cert.OrgCertUrl, cert.Comment, cert.CreatedAt
              FROM Certificates cert
              LEFT JOIN Containers con ON cert.ContainerId = con.Id
              LEFT JOIN InspectionTypes it ON cert.InspectionTypeId = it.Id
              LEFT JOIN Employees emp ON cert.EmployeeId = emp.Id
              LEFT JOIN Customers cust ON cert.CustomerId = cust.Id
              ORDER BY cert.Id ASC`
    },
    jobs: {
        sql: `SELECT j.Id, j.JobNr, j.ReceivedDate, j.Description,
                j.CustomerId, cust.Name AS CustomerName,
                j.Status, j.Category, j.CreatedAt
              FROM Jobs j
              LEFT JOIN Customers cust ON j.CustomerId = cust.Id
              ORDER BY j.Id ASC`
    },
    customers: {
        sql: `SELECT Id, REPLACE(REPLACE(Name, ';#', ''), '#;', '') AS Name, IsActive FROM Customers ORDER BY Name ASC`
    },
    suppliers: { sql: `SELECT * FROM Suppliers ORDER BY Name ASC` },
    carriers: { sql: `SELECT * FROM Carriers ORDER BY Name ASC` },
    servicetypes: { sql: `SELECT * FROM ServiceTypes ORDER BY Name ASC` },
    employees: { sql: `SELECT * FROM Employees ORDER BY Name ASC` },
    containertypes: { sql: `SELECT * FROM ContainerTypes ORDER BY Id ASC` },
    loosegear: {
        sql: `SELECT lg.Id, lg.ContainerId,
                con.UnitId, con.Prefix, con.SerialNr,
                lg.ItemType, lg.CertNr, lg.TestDate, lg.ExpiryDate,
                lg.Approved, lg.Comment, lg.CreatedAt
              FROM LooseGearCerts lg
              LEFT JOIN Containers con ON lg.ContainerId = con.Id
              ORDER BY lg.Id ASC`
    },
    images: { sql: `SELECT * FROM Images ORDER BY Id DESC` },
    inspectiontypes: { sql: `SELECT * FROM InspectionTypes ORDER BY Id ASC` }
};

app.http('getData', {
    methods: ['GET'],
    authLevel: 'anonymous',
    route: 'data',
    handler: async (request, context) => {
        try {
            const tableName = request.query.get('table');
            if (!tableName || !TABLE_MAP[tableName]) {
                return { status: 400, jsonBody: { error: 'Invalid table. Valid: ' + Object.keys(TABLE_MAP).join(', ') } };
            }
            const result = await query(TABLE_MAP[tableName].sql);
            return { jsonBody: { table: tableName, count: result.recordset.length, data: result.recordset } };
        } catch (err) {
            context.error('getData error:', err);
            return { status: 500, jsonBody: { error: err.message } };
        }
    }
});

const WRITE_TABLES = {
    containers: 'Containers', movements: 'Movements', certificates: 'Certificates',
    jobs: 'Jobs', customers: 'Customers', suppliers: 'Suppliers', carriers: 'Carriers',
    employees: 'Employees', loosegear: 'LooseGearCerts', images: 'Images'
};

app.http('addData', {
    methods: ['POST'],
    authLevel: 'anonymous',
    route: 'data',
    handler: async (request, context) => {
        try {
            const tableName = request.query.get('table');
            if (!tableName || !WRITE_TABLES[tableName]) {
                return { status: 400, jsonBody: { error: 'Invalid table for write.' } };
            }
            const body = await request.json();
            const realTable = WRITE_TABLES[tableName];
            const cols = Object.keys(body);
            const placeholders = cols.map((c, i) => `@p${i}`);
            const sqlText = `INSERT INTO ${realTable} (${cols.join(', ')}) VALUES (${placeholders.join(', ')})`;
            const { getPool } = require('./db');
            const pool = await getPool();
            const req = pool.request();
            cols.forEach((c, i) => req.input(`p${i}`, body[c]));
            await req.query(sqlText);
            return { status: 201, jsonBody: { success: true, message: `Row added to ${realTable}` } };
        } catch (err) {
            context.error('addData error:', err);
            return { status: 500, jsonBody: { error: err.message } };
        }
    }
});

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
                const baseTable = WRITE_TABLES[tableName] || tableName;
                sqlText = `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '${baseTable}' ORDER BY ORDINAL_POSITION`;
            } else {
                sqlText = `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS ORDER BY TABLE_NAME, ORDINAL_POSITION`;
            }
            const result = await query(sqlText);
            return { jsonBody: { data: result.recordset } };
        } catch (err) {
            context.error('getSchema error:', err);
            return { status: 500, jsonBody: { error: err.message } };
        }
    }
});
