const sql = require('mssql');

const dbConfig = {
    server: process.env.DB_SERVER,
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    port: parseInt(process.env.DB_PORT || '1433', 10),
    options: {
        encrypt: process.env.DB_ENCRYPT !== 'false',
        trustServerCertificate: process.env.DB_TRUST_CERT === 'true',
        enableArithAbort: true,
    },
    pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },
    connectionTimeout: 15000,
    requestTimeout: 30000,
};

// A job not updated within this window is considered dead (process crashed)
const STALE_MINUTES = 15;

let pool = null;

async function getPool() {
    if (!pool) {
        pool = await sql.connect(dbConfig);
        pool.on('error', (err) => {
            console.error('[JobStore] Pool error:', err.message);
            pool = null;
        });
    }
    return pool;
}

const JobStore = {
    /**
     * Atomic check-or-insert.
     * If a non-stale pending/running job already exists for this facility+date+triptype,
     * return its jobId without inserting a new one (prevents duplicate jobs from race conditions).
     * Returns { jobId, created: boolean }
     */
    async createOrGetJob(newJobId, params) {
        const p = await getPool();
        const result = await p.request()
            .input('newJobId', sql.VarChar(36), newJobId)
            .input('facilityid', sql.VarChar(50), String(params.facilityid || ''))
            .input('sDate', sql.VarChar(20), String(params.sDate || ''))
            .input('triptype', sql.VarChar(10), String(params.triptype || ''))
            .input('shifttime', sql.NVarChar(sql.MAX), JSON.stringify(params.shifttime || []))
            .input('updatedBy', sql.VarChar(100), String(params.updatedBy || ''))
            .input('staleMinutes', sql.Int, STALE_MINUTES)
            .query(`
                DECLARE @existingJobId VARCHAR(36);

                SELECT TOP 1 @existingJobId = jobId
                FROM RouteGenerationJobs
                WHERE facilityid = @facilityid
                  AND sDate      = @sDate
                  AND triptype   = @triptype
                  AND status     IN ('pending', 'running')
                  AND updatedAt  > DATEADD(MINUTE, -@staleMinutes, GETDATE());

                IF @existingJobId IS NOT NULL
                BEGIN
                    SELECT @existingJobId AS jobId, CAST(0 AS BIT) AS created;
                END
                ELSE
                BEGIN
                    INSERT INTO RouteGenerationJobs
                        (jobId, status, facilityid, sDate, triptype, shifttime, updatedBy,
                         progressMessage, progressPercent, createdAt, updatedAt)
                    VALUES
                        (@newJobId, 'pending', @facilityid, @sDate, @triptype, @shifttime, @updatedBy,
                         'Job queued', 0, GETDATE(), GETDATE());

                    SELECT @newJobId AS jobId, CAST(1 AS BIT) AS created;
                END
            `);

        const row = result.recordset[0];
        return { jobId: row.jobId, created: row.created === true || row.created === 1 };
    },

    /**
     * Partial update — only sets columns that are present in `updates`.
     * Always touches updatedAt, which keeps the heartbeat alive.
     */
    async updateJob(jobId, updates = {}) {
        const p = await getPool();
        const req = p.request().input('jobId', sql.VarChar(36), jobId);
        const sets = ['updatedAt = GETDATE()'];

        if (updates.status !== undefined) {
            req.input('status', sql.VarChar(20), updates.status);
            sets.push('status = @status');
        }
        if (updates.progressMessage !== undefined) {
            req.input('progressMessage', sql.NVarChar(500), updates.progressMessage);
            sets.push('progressMessage = @progressMessage');
        }
        if (updates.progressPercent !== undefined) {
            req.input('progressPercent', sql.Int, updates.progressPercent);
            sets.push('progressPercent = @progressPercent');
        }
        if (updates.errorMessage !== undefined) {
            req.input('errorMessage', sql.NVarChar(1000), String(updates.errorMessage || '').substring(0, 900));
            sets.push('errorMessage = @errorMessage');
        }

        await req.query(`UPDATE RouteGenerationJobs SET ${sets.join(', ')} WHERE jobId = @jobId`);
    },

    async getJob(jobId) {
        const p = await getPool();
        const result = await p.request()
            .input('jobId', sql.VarChar(36), jobId)
            .query(`
                SELECT jobId, status, facilityid, sDate, triptype, updatedBy,
                       progressMessage, progressPercent, errorMessage, createdAt, updatedAt
                FROM RouteGenerationJobs
                WHERE jobId = @jobId
            `);
        return result.recordset[0] || null;
    },

    /**
     * Returns the most recent non-stale pending/running job for a shift, or null.
     * Used by the /in-progress endpoint so any admin can see an active generation.
     */
    async getInProgressJob(facilityid, sDate, triptype) {
        const p = await getPool();
        const result = await p.request()
            .input('facilityid', sql.VarChar(50), String(facilityid || ''))
            .input('sDate', sql.VarChar(20), String(sDate || ''))
            .input('triptype', sql.VarChar(10), String(triptype || ''))
            .input('staleMinutes', sql.Int, STALE_MINUTES)
            .query(`
                SELECT TOP 1 jobId, status, progressMessage, progressPercent, updatedBy, createdAt
                FROM RouteGenerationJobs
                WHERE facilityid = @facilityid
                  AND sDate      = @sDate
                  AND triptype   = @triptype
                  AND status     IN ('pending', 'running')
                  AND updatedAt  > DATEADD(MINUTE, -@staleMinutes, GETDATE())
                ORDER BY createdAt DESC
            `);
        return result.recordset[0] || null;
    },

    /**
     * Run on server startup.
     * Any job still marked running/pending but not updated in STALE_MINUTES
     * died with the previous process — mark it failed so admins aren't blocked.
     */
    async markStuckJobsFailed() {
        try {
            const p = await getPool();
            const result = await p.request()
                .input('staleMinutes', sql.Int, STALE_MINUTES)
                .query(`
                    UPDATE RouteGenerationJobs
                    SET status          = 'failed',
                        errorMessage    = 'Server restarted while job was running',
                        progressMessage = 'Route generation failed (server restart)',
                        updatedAt       = GETDATE()
                    WHERE status   IN ('pending', 'running')
                      AND updatedAt < DATEADD(MINUTE, -@staleMinutes, GETDATE())
                `);
            const fixed = result.rowsAffected?.[0] ?? 0;
            if (fixed > 0) console.log(`[JobStore] Marked ${fixed} stuck job(s) as failed on startup`);
        } catch (err) {
            console.error('[JobStore] markStuckJobsFailed error:', err.message);
        }
    },

    async purgeOldJobs() {
        try {
            const p = await getPool();
            const result = await p.request().query(`
                DELETE FROM RouteGenerationJobs
                WHERE createdAt < DATEADD(hour, -24, GETDATE())
            `);
            const deleted = result.rowsAffected?.[0] ?? 0;
            if (deleted > 0) console.log(`[JobStore] Purged ${deleted} expired job(s)`);
        } catch (err) {
            console.error('[JobStore] purgeOldJobs error:', err.message);
        }
    }
};

module.exports = JobStore;
