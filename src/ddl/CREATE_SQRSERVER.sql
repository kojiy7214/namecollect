-- Table ncs.columnmap

-- DROP TABLE IF EXISTS ncs.columnmap;

CREATE TABLE ncs.columnmap
(
    id BIGINT NOT NULL IDENTITY(1, 1),
    provider NVARCHAR(255) NOT NULL,
    target NVARCHAR(255) NOT NULL,
    pname NVARCHAR(255) NOT NULL,
    tname NVARCHAR(255) NOT NULL,
    popt NVARCHAR(MAX), 
    sync BIT NOT NULL,
    tenant NVARCHAR(MAX),
    CONSTRAINT columnmap_pkey PRIMARY KEY (id)
);

-- Table ncs.idmap

-- DROP TABLE IF EXISTS ncs.idmap;

CREATE TABLE ncs.idmap
(
    id BIGINT NOT NULL IDENTITY(1, 1),
    provider NVARCHAR(255) NOT NULL,
    target NVARCHAR(255) NOT NULL,
    pid NVARCHAR(255),
    tid NVARCHAR(255),
    targetlist NVARCHAR(MAX),
    targetstatus NVARCHAR(MAX),
    targetvalue NVARCHAR(MAX),
    tenant NVARCHAR(255),
    mergedvalue NVARCHAR(MAX),
    [option] NVARCHAR(MAX),
    CONSTRAINT idx_id PRIMARY KEY (id),
    CONSTRAINT idmap_provider_target_pid_key UNIQUE (provider, target, pid),
    CONSTRAINT idmap_provider_target_pid_tenant_key UNIQUE (provider, target, pid, tenant)
);

-- Table ncs.session

-- DROP TABLE IF EXISTS ncs.session;

CREATE TABLE ncs.session
(
    id BIGINT NOT NULL IDENTITY(1, 1),
    apisignature NVARCHAR(MAX) NOT NULL,
    sessionid NVARCHAR(MAX) NOT NULL,
    accessdate DATETIME2 NOT NULL,
    tenant NVARCHAR(MAX),
    [from] BIGINT,
    CONSTRAINT session_pkey PRIMARY KEY (id)
);