CREATE TABLE bahnhof (
    haltestellennummer integer PRIMARY KEY,
    name varchar(30),
    longname varchar(50),
    abkurzung varchar(30),
    synonym varchar(50)
);

CREATE TABLE bfkoord (
    haltestellennummer integer PRIMARY KEY,
    x float,
    y float,
    z smallint
);

CREATE TABLE gleis (
    haltestellennummer integer,
    fahrtnummer integer,
    verwaltung varchar(6),
    gleisinformation varchar(8),
    zeit varchar(10),
    verkehrstageschlussel integer
);

CREATE TABLE kminfo (
    haltestellennummer integer PRIMARY KEY,
    wert integer
);

CREATE TABLE bfkoord_geo (
    haltestellennummer integer PRIMARY KEY,
    x float,
    y float,
    z smallint
);

CREATE TABLE umsteigv (
    haltestellennummer integer,
    verwaltungsbezeichnung1 varchar(10),
    verwaltungsbezeichnung2 varchar(10),
    mindestumsteigezeit smallint
);

CREATE TABLE umsteigl (
    haltestellennummer integer,
    verwaltung1 varchar(10),
    gattung1 varchar(10),
    linie1 varchar(10),
    richtung1 varchar(10),
    verwaltung2 varchar(10),
    gattung2 varchar(10),
    linie2 varchar(10),
    richtung2 varchar(10),
    umsteigezeit smallint,
    garantiert boolean
);

CREATE TABLE umsteigz (
    haltestellennummer integer,
    fahrtnummer1 integer,
    verwaltung1 varchar(10),
    fahrtnummer2 integer,
    verwaltung2 varchar(10),
    umsteigezeit smallint,
    garantiert boolean
);

CREATE TABLE vereinig (
    haltestellennummer1 integer,
    haltestellennummer2 integer,
    fahrtnummer1 integer,
    verwaltung1 varchar(10),
    fahrtnummer2 integer,
    verwaltung2 varchar(10),
    kommentar varchar(255)
);

CREATE TABLE bitfeld (
    bitfeldnummer integer,
    servicedate DATE
);

CREATE TABLE richtung (
    richtingschlussel varchar(8) PRIMARY KEY,
    text varchar(50)
);

CREATE TABLE eckdaten (
    fahrplan_start DATE,
    fahrplan_end DATE,
    bezeichnung VARCHAR(25),
    fahrplan_periode VARCHAR(10),
    land VARCHAR(10),
    exportdatum VARCHAR(25),
    hrdf_version VARCHAR(10),
    lieferant VARCHAR(10)
);

CREATE TABLE metabhf_ubergangbeziehung (
    haltestellennummer1 integer,
    haltestellennummer2 integer,
    dauer integer
);

CREATE TABLE metabhf_ubergangbeziehung_a (
    haltestellennummer1 integer,
    haltestellennummer2 integer,
    attributscode varchar(3)
);

CREATE TABLE bfprios (
    haltestellennummer integer PRIMARY KEY,
    umsteigeprioritat integer
);

CREATE TABLE metabhf_haltestellengruppen (
    sammelbegriffsnummer integer,
    haltestellennummer integer,
    PRIMARY KEY(sammelbegriffsnummer,haltestellennummer)
);

CREATE TABLE umsteigb (
    haltestellennummer integer,
    umsteigezeit_ic integer,
    umsteigezeit integer
);

CREATE TABLE attribut1_fr (
    code varchar(2) PRIMARY KEY,
    haltestellenzugehorigkeit integer,
    attributsausgabeprioritat integer,
    attibutsausgabefeinsortierung integer,
    text varchar(70)
);

CREATE TABLE attribut1_it (
    code varchar(2) PRIMARY KEY,
    haltestellenzugehorigkeit integer,
    attributsausgabeprioritat integer,
    attibutsausgabefeinsortierung integer,
    text varchar(70)
);

CREATE TABLE attribut1_de (
    code varchar(2) PRIMARY KEY,
    haltestellenzugehorigkeit integer,
    attributsausgabeprioritat integer,
    attibutsausgabefeinsortierung integer,
    text varchar(70)
);

CREATE TABLE attribut1_en (
    code varchar(2) PRIMARY KEY,
    haltestellenzugehorigkeit integer,
    attributsausgabeprioritat integer,
    attibutsausgabefeinsortierung integer,
    text varchar(70)
);

CREATE TABLE attribut2_fr (
    code varchar(2) PRIMARY KEY,
    ausgabe_der_teilstrecke varchar(3),
    einstellig varchar(3)
);

CREATE TABLE attribut2_IT (
    code varchar(2) PRIMARY KEY,
    ausgabe_der_teilstrecke varchar(3),
    einstellig varchar(3)
);

CREATE TABLE attribut2_de (
    code varchar(2) PRIMARY KEY,
    ausgabe_der_teilstrecke varchar(3),
    einstellig varchar(3)
);

CREATE TABLE attribut2_en (
    code varchar(2) PRIMARY KEY,
    ausgabe_der_teilstrecke varchar(3),
    einstellig varchar(3)
);

CREATE TABLE zugart (
    code varchar(3) PRIMARY KEY,
    produktklasse integer,
    tarifgruppe char,
    ausgabesteuerung integer,
    gattungsbezeichnung varchar(10),
    zuschlag integer,
    flag char,
    gattungsbildernamen integer,
    category_franzoesisch varchar(100),
    category_italienisch varchar(100),
    category_deutsch varchar(100),
    category_englisch varchar(100)
);

CREATE TABLE infotext_en (
    infotextnummer integer PRIMARY KEY,
    informationstext varchar(255)
);

CREATE TABLE infotext_fr (
    infotextnummer integer PRIMARY KEY,
    informationstext varchar(255)
);

CREATE TABLE infotext_de (
    infotextnummer integer PRIMARY KEY,
    informationstext varchar(255)
);

CREATE TABLE infotext_it (
    infotextnummer integer PRIMARY KEY,
    informationstext varchar(255)
);

CREATE TABLE betrieb1_de (
    betreibernummer integer PRIMARY KEY,
    kurzname varchar(10),
    langname varchar(10),
    name varchar(100)
);

CREATE TABLE betrieb2_de (
    betreibernummer integer PRIMARY KEY,
    verwaltungen varchar(255)
);

CREATE TABLE betrieb1_en (
    betreibernummer integer PRIMARY KEY,
    kurzname varchar(10),
    langname varchar(10),
    name varchar(100)
);

CREATE TABLE betrieb2_en (
    betreibernummer integer PRIMARY KEY,
    verwaltungen varchar(255)
);

CREATE TABLE betrieb1_fr (
    betreibernummer integer PRIMARY KEY,
    kurzname varchar(10),
    langname varchar(10),
    name varchar(100)
);

CREATE TABLE betrieb2_fr (
    betreibernummer integer PRIMARY KEY,
    verwaltungen varchar(255)
);

CREATE TABLE betrieb1_it (
    betreibernummer integer PRIMARY KEY,
    kurzname varchar(10),
    langname varchar(10),
    name varchar(100)
);

CREATE TABLE betrieb2_it (
    betreibernummer integer PRIMARY KEY,
    verwaltungen varchar(255)
);

CREATE TABLE durchbi (
    fahrtnummer1 integer,
    verwaltungfahrt1 char(6),
    letzterhaltfahrt1 integer,
    fahrtnummer2 integer,
    verwaltungfahrt2 char(6),
    verkehrstagebitfeldnummer integer,
    ersterhaltderfahrt2 integer,
    attributmarkierungdurchbindung char(1),
    kommentar varchar(255)
);

CREATE TABLE zeitvs (
    bahnhofnummer integer PRIMARY KEY,
    zeitverschiebung integer,
    vondatum integer,
    vonzugehorigezeit integer,
    bisdatum integer,
    biszugehorigzeit integer,
    kommentar varchar(255)
);

CREATE TABLE fplan_z (
    fahrtnummer integer,
    verwaltung char(6),
    leer char(3),
    variante smallint,
    taktanzahl smallint,
    takzeit smallint,
    PRIMARY KEY (fahrtnummer, verwaltung, leer, variante)
);

CREATE TABLE fplan_g (
    fahrtnummer integer,
    verwaltung char(6),
    leer char(3),
    variante smallint,
    verkehrsmittel char (3),
    laufwegsindexab integer,
    laufwegsindexbis integer,
    indexab integer,
    indexbis integer,
    PRIMARY KEY (fahrtnummer, verwaltung, leer, variante, verkehrsmittel),
    FOREIGN KEY (fahrtnummer, verwaltung, leer, variante) REFERENCES fplan_z
);

CREATE TABLE fplan_ave (
    fahrtnummer integer,
    verwaltung char(6),
    leer char(3),
    variante smallint,
    laufwegsindexab integer,
    laufwegsindexbis integer,
    verkehrstagenummer smallint,
    indexab integer,
    indexbis integer,
    FOREIGN KEY (fahrtnummer, verwaltung, leer, variante) REFERENCES fplan_z
);

CREATE TABLE fplan_a (
    fahrtnummer integer,
    verwaltung char(6),
    leer char(3),
    variante smallint,
    attributscode char(2) NOT NULL,
    laufwegsindexab integer,
    laufwegsindexbis integer,
    bitfeldnummer smallint NOT NULL,
    indexab integer,
    indexbis integer,
    FOREIGN KEY (fahrtnummer, verwaltung, leer, variante) REFERENCES fplan_z
);

CREATE TABLE fplan_i (
    fahrtnummer integer,
    verwaltung char(6),
    leer char(3),
    variante smallint,
    infotextcode char(2) NOT NULL,
    laufwegsindexab integer,
    laufwegsindexbis integer,
    bitfeldnummer smallint NOT NULL,
    infotextnummer integer NOT NULL,
    indexab integer,
    indexbis integer,
    FOREIGN KEY (fahrtnummer, verwaltung, leer, variante) REFERENCES fplan_z
);

CREATE TABLE fplan_l (
    fahrtnummer integer,
    verwaltung char(6),
    leer char(3),
    variante smallint,
    linienummer char(8) NOT NULL,
    laufwegsindexab integer,
    laufwegsindexbis integer,
    indexab integer,
    indexbis integer,
    FOREIGN KEY (fahrtnummer, verwaltung, leer, variante) REFERENCES fplan_z
);

CREATE TABLE fplan_l (
    fahrtnummer integer,
    verwaltung char(6),
    leer char(3),
    variante smallint,
    kennung char(1),
    richtungscode char(7),
    laufwegsindexab integer,
    laufwegsindexbis integer,
    indexab integer,
    indexbis integer,
    FOREIGN KEY (fahrtnummer, verwaltung, leer, variante) REFERENCES fplan_z
);

CREATE TABLE fplan_gr (
    fahrtnummer integer,
    verwaltung char(6),
    leer char(3),
    variante smallint,
    grenzpunktnummer char(7),
    laufwegsindexletzten integer,
    laufwegsindexersten integer,
    indexletzten integer,
    indexersten integer,
    FOREIGN KEY (fahrtnummer, verwaltung, leer, variante) REFERENCES fplan_z
);

CREATE TABLE fplan_sh (
    fahrtnummer integer,
    verwaltung char(6),
    leer char(3),
    variante smallint,
    laufwegindex integer NOT NULL,
    bitfeldnummer smallint,
    indexfur integer
    FOREIGN KEY (fahrtnummer, verwaltung, leer, variante) REFERENCES fplan_z
);

CREATE TABLE fplan_sh (
    fahrtnummer integer,
    verwaltung char(6),
    leer char(3),
    variante smallint,
    haltesnellennummer integer,
    haltesnellenname varchar(21),
    ankunfstzeit integer,
    abfahrtszeit integer,
    fahrtnummer integer,
    verwaltung char(6),
    x char(1),
    FOREIGN KEY (fahrtnummer, verwaltung, leer, variante) REFERENCES fplan_z
);

CREATE TABLE dirwagen_kw (
    kurswagennummer integer primary key
);

CREATE TABLE dirwagen_kwz (
    kurswagennummer integer,
    zugnummer integer,
    verhaltung char(6),
    bahnhofsnummerab integer,
    bahnhofsname varchar(20),
    bahnhofsnummerbis integer,
    abfahrtzeit1 integer,
    abfahrtzeit2 integer,
    FOREIGN KEY (kurswagennummer) REFERENCES dirwagen_kw
);

CREATE TABLE dirwagen_ave (
    kurswagennummer integer,
    laufwegindexab integer,
    laufwegindexbis integer,
    verkehrstagenummer integer,
    FOREIGN KEY (kurswagennummer) REFERENCES dirwagen_kw
);

CREATE TABLE dirwagen_a (
    kurswagennummer integer,
    attributscode char(2) NOT NULL,
    laufwegsindexab integer,
    laufwegsindexbis integer,
    bitfeldnummer integer NOT NULL,
    indexab integer,
    indexbis integer,
    FOREIGN KEY (kurswagennummer) REFERENCES dirwagen_kw
);


