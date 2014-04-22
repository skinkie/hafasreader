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
    z smallint,
    synonym varchar(50)
);

CREATE TABLE gleis (
    haltestellennummer integer,
    fahrtnummer integer,
    verwaltung varchar(6),
    gleisinformation varchar(8),
    zeit varchar(10)
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
    z smallint,
    synonym varchar(50)
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
    bezeichnung VARCHAR(10),
    fahrplan_periode VARCHAR(10),
    land VARCHAR(10),
    exportdatum VARCHAR(10),
    hrdf_version VARCHAR(10),
    lieferant VARCHAR(10)
);

CREATE TABLE metabhf_ubergangbeziehung (
    haltestellennummer1 integer,
    haltestellennummer2 integer,
    dauer integer,
    PRIMARY KEY(haltestellennummer1,haltestellennummer2)
);

CREATE TABLE metabhf_ubergangbeziehung_a (
    haltestellennummer1 integer,
    haltestellennummer2 integer,
    attributscode char
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
    umsteigezeit integer,
    PRIMARY KEY(haltestellennummer)
);

CREATE TABLE attribute_FR_1 (
    code varchar(2) PRIMARY KEY,
    haltestellenzugehorigkeit integer,
    attributsausgabeprioritat integer,
    attibutsausgabefeinsortierung integer,
    text varchar(70)
);

CREATE TABLE attribute_IT_1 (
    code varchar(2) PRIMARY KEY,
    haltestellenzugehorigkeit integer,
    attributsausgabeprioritat integer,
    attibutsausgabefeinsortierung integer,
    text varchar(70)
);

CREATE TABLE attribute_de_1 (
    code varchar(2) PRIMARY KEY,
    haltestellenzugehorigkeit integer,
    attributsausgabeprioritat integer,
    attibutsausgabefeinsortierung integer,
    text varchar(70)
);

CREATE TABLE attribute_en_1 (
    code varchar(2) PRIMARY KEY,
    haltestellenzugehorigkeit integer,
    attributsausgabeprioritat integer,
    attibutsausgabefeinsortierung integer,
    text varchar(70)
);

CREATE TABLE attribute_FR_2 (
    code varchar(2) PRIMARY KEY,
    ausgabe_der_teilstrecke varchar(3),
    einstellig varchar(3)
);

CREATE TABLE attribute_IT_2 (
    code varchar(2) PRIMARY KEY,
    ausgabe_der_teilstrecke varchar(3),
    einstellig varchar(3)
);

CREATE TABLE attribute_de_2 (
    code varchar(2) PRIMARY KEY,
    ausgabe_der_teilstrecke varchar(3),
    einstellig varchar(3)
);

CREATE TABLE attribute_en_2 (
    code varchar(2) PRIMARY KEY,
    ausgabe_der_teilstrecke varchar(3),
    einstellig varchar(3)
);

CREATE TABLE zugart (
    code varchar(3) PRIMARY KEY,
    produktklasse integer,
    tarifgruppe char,
    ausgabesteuerung integer,
    gattungsbezeichnung char,
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

CREATE TABLE infotext_de (
    infotextnummer integer PRIMARY KEY,
    informationstext varchar(255)
);

CREATE TABLE betrieb1_en (
    betreibernummer integer PRIMARY KEY,
    informationstext varchar(255)
);

CREATE TABLE betrieb2_en (
    betreibernummer integer PRIMARY KEY,
    verwaltungen varchar(255)
);
