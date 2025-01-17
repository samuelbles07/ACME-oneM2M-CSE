import psycopg2

"""
Installation
- Install postgresql (brew install postgresql) to get 
- Install psycopg2 using pip

ref: https://www.psycopg.org/docs/install.html#psycopg-vs-psycopg-binary

Create database on postgres server with the same name as DB_NAME
"""

DB_NAME = "acme-cse"
DB_PORT = 5432
DB_HOSTNAME = "localhost"
DB_USERNAME = "postgres"
DB_PASSWORD = "musang"

class Tables:
    @staticmethod
    def queryResources() -> str:
        return """
                CREATE TABLE IF NOT EXISTS RESOURCES
                (
                    index SERIAL PRIMARY KEY,
                    ty INTEGER NOT NULL,
                    ri VARCHAR(255) UNIQUE NOT NULL,
                    rn VARCHAR(255) NOT NULL,
                    pi VARCHAR(255),
                    ct TIMESTAMP NOT NULL,
                    lt TIMESTAMP NOT NULL,
                    acpi JSONB,
                    et TIMESTAMP,
                    st INTEGER,
                    at JSONB,
                    aa JSONB,
                    lbl JSONB,
                    esi VARCHAR(255),
                    daci JSONB,
                    cr VARCHAR(255),
                    cstn VARCHAR(255),
                    __rtype__ VARCHAR NOT NULL,
                    __originator__ VARCHAR,
                    __srn__ VARCHAR UNIQUE NOT NULL,
                    __announcedto__ JSONB,
                    __rvi__ VARCHAR(255),
                    __node__ VARCHAR(255),
                    __imported__ BOOLEAN,
                    __isinstantiated__ BOOLEAN,
                    __remoteid__ VARCHAR,
                    __modified__ JSONB,
                    __createdinternally__ VARCHAR,
                    __isvirtual__ BOOLEAN
                );
                """

    @staticmethod
    def queryACP() -> str:
        return """
                CREATE TABLE IF NOT EXISTS ACP
                (
                    index SERIAL PRIMARY KEY,
                    resource_index INTEGER,
                    pv JSONB NOT NULL,
                    pvs JSONB NOT NULL,
                    adri JSONB,
                    apri JSONB,
                    airi JSONB,
                    CONSTRAINT fk_resources FOREIGN KEY(resource_index) REFERENCES resources(index) ON DELETE CASCADE
                );
                """
    
    @staticmethod
    def queryAE() -> str:
        return """
                CREATE TABLE IF NOT EXISTS AE
                (
                    index SERIAL PRIMARY KEY,
                    resource_index INTEGER,
                    apn VARCHAR(255),
                    api VARCHAR(255) NOT NULL,
                    aei VARCHAR(255) UNIQUE NOT NULL,
                    mei VARCHAR(255),
                    tri VARCHAR(255),
                    trn VARCHAR(255),
                    poa JSONB,
                    regs VARCHAR(255),
                    trps BOOLEAN,
                    ontologyRef VARCHAR(255),
                    rr BOOLEAN NOT NULL,
                    nl VARCHAR(255),
                    csz JSONB,
                    scp JSONB,
                    srv JSONB,
                    CONSTRAINT fk_resources FOREIGN KEY(resource_index) REFERENCES resources(index) ON DELETE CASCADE
                );
                """

    @staticmethod
    def queryCNT() -> str:
        return """
                CREATE TABLE IF NOT EXISTS CNT
                (
                    index SERIAL PRIMARY KEY,
                    resource_index INTEGER,
                    mni INTEGER,
                    mbs INTEGER,
                    mia INTEGER,
                    cni INTEGER NOT NULL,
                    cbs INTEGER NOT NULL,
                    li VARCHAR(255),
                    ontologyRef VARCHAR(255),
                    disr BOOLEAN,
                    CONSTRAINT fk_resources FOREIGN KEY(resource_index) REFERENCES resources(index) ON DELETE CASCADE
                );
                """

    @staticmethod
    def queryCIN() -> str:
        return """
                CREATE TABLE IF NOT EXISTS CIN
                (
                    index SERIAL PRIMARY KEY,
                    resource_index INTEGER,
                    cnf VARCHAR(255),
                    cs INTEGER,
                    conr VARCHAR(255),
                    ontologyRef VARCHAR(255),
                    con VARCHAR(255) NOT NULL,
                    dcnt INTEGER,
			        dgt TIMESTAMP,
                    CONSTRAINT fk_resources FOREIGN KEY(resource_index) REFERENCES resources(index) ON DELETE CASCADE
                );
                """
    
    @staticmethod
    def queryCB() -> str:
        return """
                CREATE TABLE IF NOT EXISTS CB
                (
                    index SERIAL PRIMARY KEY,
                    resource_index INTEGER,
                    cst SMALLINT NOT NULL,
                    csi VARCHAR(255) UNIQUE NOT NULL,
                    poa JSONB NOT NULL,
                    nl VARCHAR(255),
                    ncp VARCHAR(255),
                    csz JSONB,
                    srv JSONB NOT NULL,
                    srt JSONB,
                    rr BOOLEAN,
                    CONSTRAINT fk_resources FOREIGN KEY(resource_index) REFERENCES resources(index) ON DELETE CASCADE
                );
                """

    @staticmethod
    def queryCSR() -> str:
        return """
                CREATE TABLE IF NOT EXISTS CSR
                (
                    index SERIAL PRIMARY KEY,
                    resource_index INTEGER,
                    cst SMALLINT NOT NULL,
                    poa JSONB,
                    cb VARCHAR(255) NOT NULL,
                    csi VARCHAR(255) UNIQUE NOT NULL,
                    mei VARCHAR(255),
                    tri VARCHAR(255),
                    rr BOOLEAN NOT NULL,
                    nl VARCHAR(255),
                    csz JSONB,
                    trn VARCHAR(255),
                    dcse JSONB,
                    mtcc VARCHAR(255),
                    egid VARCHAR(255),
                    tren BOOLEAN,
                    ape JSONB,
                    srv JSONB NOT NULL,
                    CONSTRAINT fk_resources FOREIGN KEY(resource_index) REFERENCES resources(index) ON DELETE CASCADE
                );
                """

    @staticmethod
    def queryGRP() -> str:
        return """
                CREATE TABLE IF NOT EXISTS GRP
                (
                    index SERIAL PRIMARY KEY,
                    resource_index INTEGER,
                    mt SMALLINT NOT NULL,
                    spty VARCHAR(255),
                    cnm INTEGER NOT NULL,
                    mnm INTEGER NOT NULL,
                    mid JSONB NOT NULL,
                    macp JSONB,
                    mtv BOOLEAN,
                    csy SMALLINT NOT NULL,
                    gn VARCHAR(255),
                    ssi BOOLEAN,
                    nar INTEGER,
                    CONSTRAINT fk_resources FOREIGN KEY(resource_index) REFERENCES resources(index) ON DELETE CASCADE
                );
                """   
    
    @staticmethod
    def querySUB() -> str:
        return """
                CREATE TABLE IF NOT EXISTS SUB
                (
                    index SERIAL PRIMARY KEY,
                    resource_index INTEGER,
                    enc JSONB,
                    exc INTEGER,
                    nu JSONB,
                    gpi VARCHAR(255),
                    nfu JSONB,
                    bn VARCHAR(255),
                    rl INTEGER,
                    psn INTEGER,
                    pn VARCHAR(255),
                    nsp INTEGER,
                    ln BOOLEAN,
                    nct SMALLINT,
                    nec INTEGER,
                    su VARCHAR(255),
                    acrs VARCHAR(255),
                    nse BOOLEAN,
                    nsi JSONB,
                    ma TIMESTAMP,
                    CONSTRAINT fk_resources FOREIGN KEY(resource_index) REFERENCES resources(index) ON DELETE CASCADE
                );
                """   
    
    @staticmethod
    def queryNOD() -> str:
        return """
                CREATE TABLE IF NOT EXISTS NOD
                (
                    index SERIAL PRIMARY KEY,
                    resource_index INTEGER,
                    ni VARCHAR(255) NOT NULL,
                    hcl VARCHAR(255),
                    hael JSONB,
                    hsl JSONB,
                    mgca VARCHAR(255),
                    rms VARCHAR(255),
                    nid VARCHAR(255),
                    CONSTRAINT fk_resources FOREIGN KEY(resource_index) REFERENCES resources(index) ON DELETE CASCADE
                );
                """   

    @staticmethod
    def queryFWR() -> str:
        return """
                CREATE TABLE IF NOT EXISTS FWR
                (
                    index SERIAL PRIMARY KEY,
                    resource_index INTEGER,
                    mgd INTEGER NOT NULL,
                    obis JSONB,
                    obps JSONB,
                    dc VARCHAR(255),
                    mgs VARCHAR(255),
                    cmlk JSONB,
                    vr VARCHAR(255) NOT NULL,
                    fwn VARCHAR(255) NOT NULL,
                    url VARCHAR(255) NOT NULL,
                    ud BOOLEAN NOT NULL,
                    uds JSONB NOT NULL,
                    CONSTRAINT fk_resources FOREIGN KEY(resource_index) REFERENCES resources(index) ON DELETE CASCADE
                );
                """
    
    @staticmethod
    def queryDVI() -> str:
        return """
                CREATE TABLE IF NOT EXISTS DVI
                (
                    index SERIAL PRIMARY KEY,
                    resource_index INTEGER,
                    mgd INTEGER NOT NULL,
                    obis JSONB,
                    obps JSONB,
                    dc VARCHAR(255),
                    mgs VARCHAR(255),
                    cmlk JSONB,
                    dlb VARCHAR NOT NULL,
                    man VARCHAR(255) NOT NULL,
                    mfdl VARCHAR(255),
                    mfd VARCHAR(255),
                    mod VARCHAR(255) NOT NULL,
                    smod VARCHAR(255),
                    dty VARCHAR(255) NOT NULL,
                    dvnm VARCHAR(255),
                    fwv VARCHAR(255),
                    swv VARCHAR(255),
                    hwv VARCHAR(255),
                    osv VARCHAR(255),
                    cnty VARCHAR(255),
                    loc VARCHAR(255),
                    syst VARCHAR(255),
                    spur VARCHAR(255),
                    purl VARCHAR(255),
                    ptl JSONB,
                    CONSTRAINT fk_resources FOREIGN KEY(resource_index) REFERENCES resources(index) ON DELETE CASCADE
                );
                """

    @staticmethod
    def queryREQ() -> str:
        return """
                CREATE TABLE IF NOT EXISTS REQ
                (
                    index SERIAL PRIMARY KEY,
                    resource_index INTEGER,
                    op SMALLINT NOT NULL,
                    tg VARCHAR(255) NOT NULL,
                    org VARCHAR(255) NOT NULL,
                    rid VARCHAR(255) NOT NULL,
                    mi VARCHAR(255) NOT NULL,
                    pc TEXT,
                    rs SMALLINT NOT NULL,
                    ors TEXT NOT NULL,
                    CONSTRAINT fk_resources FOREIGN KEY(resource_index) REFERENCES resources(index) ON DELETE CASCADE
                );
                """
                
    @staticmethod
    def queryBatchNotif() -> str:
        return """
                CREATE TABLE IF NOT EXISTS batch_notif
                (
                    index SERIAL PRIMARY KEY,
                    ri VARCHAR(255) NOT NULL,
                    nu VARCHAR(255),
                    tstamp TIMESTAMP,
                    request JSONB,
                    CONSTRAINT fk_ri FOREIGN KEY(ri) REFERENCES resources(ri) ON DELETE CASCADE
                );
                """
                

if __name__ == "__main__":

    # Connect to your postgres DB
    conn = psycopg2.connect(database=DB_NAME, port = DB_PORT, host=DB_HOSTNAME, user=DB_USERNAME, password=DB_PASSWORD)
    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Execute queries and commit
    cur.execute(Tables.queryResources())
    conn.commit()

    cur.execute(Tables.queryACP())
    conn.commit()

    cur.execute(Tables.queryAE())
    conn.commit()

    cur.execute(Tables.queryCNT())
    conn.commit()
    
    cur.execute(Tables.queryCIN())
    conn.commit()

    cur.execute(Tables.queryCB())
    conn.commit()

    cur.execute(Tables.queryCSR())
    conn.commit()
    
    cur.execute(Tables.queryGRP())
    conn.commit()

    cur.execute(Tables.querySUB())
    conn.commit()

    cur.execute(Tables.queryNOD())
    conn.commit()

    cur.execute(Tables.queryFWR())
    conn.commit()
    
    cur.execute(Tables.queryDVI())
    conn.commit()

    cur.execute(Tables.queryREQ())
    conn.commit()
    
    cur.execute(Tables.queryBatchNotif())
    conn.commit()

    # Close cursor and connection to databse
    cur.close()
    conn.close()
    print("Postgres connection closed")
