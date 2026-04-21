const { app } = require('@azure/functions');
const { query } = require('../db');

// Smart queries with JOINs that resolve foreign keys to actual names
// This eliminates the need for client-side lookups
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
    suppliers: {
        sql: `SELECT * FROM Suppliers ORDER BY Name ASC`
    },
    carriers: {
        sql: `SELECT * FROM Carriers ORDER BY Name ASC`
    },
    servicetypes: {
        sql: `SELECT * FROM ServiceTypes ORDER BY Name ASC`
    },
    employees: {
        sql: `SELECT * FROM Employees ORDER BY Name ASC`
    },
    containertypes: {
        sql: `SELECT * FROM ContainerTypes ORDER BY Id ASC`
    },
    loosegear: {
        sql: `SELECT lg.Id, lg.ContainerId,
                con.UnitId, con.Prefix, con.SerialNr,
                lg.ItemType, lg.CertNr, lg.TestDate, lg.ExpiryDate,
                lg.Approved, lg.Comment, lg.CreatedAt
              FROM LooseGearCerts lg
              LEFT JOIN Containers con ON lg.ContainerId = con.Id
              ORDER BY lg.Id ASC`
    },
    images: {
        sql: `SELECT i.Id, i.EntityType, i.EntityId, i.BlobUrl, i.FileName, i.SortOrder,
                CASE
                    WHEN i.EntityType = 'Truckliste' THEN m.ContainerId
                    WHEN i.EntityType = 'Certifikatnumre' THEN cert.ContainerId
                    ELSE NULL
                END AS ContainerId
              FROM Images i
              LEFT JOIN Movements m ON i.EntityType = 'Truckliste'
                AND CAST(i.EntityId AS VARCHAR) = CAST(m.SPItemId AS VARCHAR)
              LEFT JOIN Certificates cert ON i.EntityType = 'Certifikatnumre'
                AND CAST(i.EntityId AS VARCHAR) = CAST(cert.Id AS VARCHAR)
              ORDER BY i.Id DESC`
    },
    containerimages: {
        sql: `SELECT
                m.ContainerId,
                i.Id AS ImageId, i.BlobUrl, i.FileName, i.SortOrder,
                m.Dato AS MovementDate, m.Id AS MovementId
              FROM Images i
              INNER JOIN Movements m ON CAST(i.EntityId AS VARCHAR) = CAST(m.SPItemId AS VARCHAR)
              WHERE i.EntityType = 'Truckliste'
              ORDER BY m.ContainerId ASC, m.Dato DESC`
    },
    inspectiontypes: {
        sql: `SELECT * FROM InspectionTypes ORDER BY Id ASC`
    },
    containerstats: {
        sql: `SELECT
                c.Id AS ContainerId,
                COUNT(m.Id) AS MovementCount,
                MAX(m.Dato) AS LastMovementDate,
                (SELECT TOP 1 cust.Name FROM Movements m2
                 LEFT JOIN Customers cust ON m2.CustomerId = cust.Id
                 WHERE m2.ContainerId = c.Id AND cust.Name IS NOT NULL
                 ORDER BY m2.Dato DESC) AS LastCustomer,
                (SELECT TOP 1 car.Name FROM Movements m3
                 LEFT JOIN Carriers car ON m3.CarrierId = car.Id
                 WHERE m3.ContainerId = c.Id AND car.Name IS NOT NULL
                 ORDER BY m3.Dato DESC) AS LastCarrier,
                (SELECT TOP 1 m4.Direction FROM Movements m4
                 WHERE m4.ContainerId = c.Id
                 ORDER BY m4.Dato DESC) AS LastDirection,
                (SELECT COUNT(DISTINCT i.Id) FROM Images i
                 INNER JOIN Movements m5 ON CAST(i.EntityId AS VARCHAR) = CAST(m5.SPItemId AS VARCHAR)
                 WHERE i.EntityType = 'Truckliste' AND m5.ContainerId = c.Id) AS PhotoCount
              FROM Containers c
              LEFT JOIN Movements m ON m.ContainerId = c.Id
              GROUP BY c.Id
              ORDER BY c.Id ASC`
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
            const result = await query(cfg.sql);

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
// Maps to base tables only (no JOINed views)
const WRITE_TABLES = {
    containers: 'Containers',
    movements: 'Movements',
    certificates: 'Certificates',
    jobs: 'Jobs',
    customers: 'Customers',
    suppliers: 'Suppliers',
    carriers: 'Carriers',
    employees: 'Employees',
    loosegear: 'LooseGearCerts',
    images: 'Images'
};

app.http('addData', {
    methods: ['POST'],
    authLevel: 'anonymous',
    route: 'data',
    handler: async (request, context) => {
        try {
            const tableName = request.query.get('table');
            if (!tableName || !WRITE_TABLES[tableName]) {
                return {
                    status: 400,
                    jsonBody: { error: 'Invalid table for write.' }
                };
            }

            const body = await request.json();
            const realTable = WRITE_TABLES[tableName];
            const cols = Object.keys(body);
            const placeholders = cols.map((c, i) => `@p${i}`);

            const sqlText = `INSERT INTO ${realTable} (${cols.join(', ')}) VALUES (${placeholders.join(', ')})`;

            const { getPool } = require('../db');
            const pool = await getPool();
            const req = pool.request();
            cols.forEach((c, i) => req.input(`p${i}`, body[c]));
            await req.query(sqlText);

            return {
                status: 201,
                jsonBody: { success: true, message: `Row added to ${realTable}` }
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
                // For schema, we want the base table columns
                const baseTable = WRITE_TABLES[tableName] || tableName;
                sqlText = `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                           FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE TABLE_NAME = '${baseTable}'
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

// GET /api/debug-images — diagnostic: show image-container mapping status
app.http('debugImages', {
    methods: ['GET'],
    authLevel: 'anonymous',
    route: 'debug-images',
    handler: async (request, context) => {
        try {
            // Check if SPItemId column exists in Containers
            const colCheck = await query(`
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'Containers' AND COLUMN_NAME = 'SPItemId'
            `);
            const hasSPItemId = colCheck.recordset.length > 0;

            // Get sample images with their EntityType/EntityId
            const imgSample = await query(`
                SELECT TOP 20 Id, EntityType, EntityId, BlobUrl, FileName
                FROM Images ORDER BY Id ASC
            `);

            // Get distinct EntityType values and counts
            const imgStats = await query(`
                SELECT EntityType, COUNT(*) as cnt,
                       MIN(CAST(EntityId AS INT)) as minId,
                       MAX(CAST(EntityId AS INT)) as maxId
                FROM Images
                GROUP BY EntityType
            `);

            // Get container ID range
            const containerRange = await query(`
                SELECT MIN(Id) as minId, MAX(Id) as maxId, COUNT(*) as cnt FROM Containers
            `);

            // If SPItemId exists, check overlap
            let spItemOverlap = null;
            if (hasSPItemId) {
                spItemOverlap = await query(`
                    SELECT TOP 10 c.Id, c.SPItemId, c.UnitId,
                           i.EntityId as ImgEntityId, i.BlobUrl
                    FROM Containers c
                    LEFT JOIN Images i ON i.EntityType = 'Truckliste'
                        AND (i.EntityId = c.SPItemId OR CAST(i.EntityId AS VARCHAR) = CAST(c.SPItemId AS VARCHAR))
                    WHERE c.SPItemId IS NOT NULL
                    ORDER BY c.Id ASC
                `);
            }

            // Also check direct Id match
            const directMatch = await query(`
                SELECT TOP 10 c.Id, c.UnitId,
                       i.EntityId as ImgEntityId, i.BlobUrl
                FROM Containers c
                INNER JOIN Images i ON i.EntityType = 'Truckliste'
                    AND CAST(i.EntityId AS VARCHAR) = CAST(c.Id AS VARCHAR)
                ORDER BY c.Id ASC
            `);

            return {
                jsonBody: {
                    hasSPItemId,
                    containerRange: containerRange.recordset[0],
                    imageStats: imgStats.recordset,
                    imageSamples: imgSample.recordset,
                    directIdMatches: directMatch.recordset.length,
                    directMatchSamples: directMatch.recordset,
                    spItemIdMatches: spItemOverlap ? spItemOverlap.recordset.length : 'N/A (no SPItemId column)',
                    spItemIdSamples: spItemOverlap ? spItemOverlap.recordset : null
                }
            };
        } catch (err) {
            context.error('debugImages error:', err);
            return { status: 500, jsonBody: { error: err.message } };
        }
    }
});
