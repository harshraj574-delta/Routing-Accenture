-- Run this once on your MSSQL database to enable async route generation jobs
CREATE TABLE RouteGenerationJobs (
    jobId         VARCHAR(36)      NOT NULL PRIMARY KEY,
    status        VARCHAR(20)      NOT NULL DEFAULT 'pending',  -- pending | running | completed | failed
    facilityid    VARCHAR(50)      NULL,
    sDate         VARCHAR(20)      NULL,
    triptype      VARCHAR(10)      NULL,
    shifttime     NVARCHAR(MAX)    NULL,   -- JSON array stored as string
    updatedBy     VARCHAR(100)     NULL,
    progressMessage NVARCHAR(500)  NULL,
    progressPercent INT            NULL DEFAULT 0,
    errorMessage  NVARCHAR(1000)   NULL,
    createdAt     DATETIME         NOT NULL DEFAULT GETDATE(),
    updatedAt     DATETIME         NOT NULL DEFAULT GETDATE()
);

CREATE INDEX IX_RouteGenerationJobs_createdAt ON RouteGenerationJobs (createdAt);
