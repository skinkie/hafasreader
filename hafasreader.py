import sys
from datetime import date, timedelta
from StringIO import StringIO
from copy import copy
import psycopg2
import codecs
import zipfile
from bitstring import Bits

charset = 'cp437'

out = codecs.getwriter('utf-8')(sys.stdout)

def parse_day(day):
    day = day.split('.')
    return date(int(day[2]), int(day[1]), int(day[0]))

def parse_bahnhof(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    bahnhofen = []
    for line in l_content:
        haltestellennummer = line[:7].strip()
        haltestellenname = line[12:].split('$<')
        name = haltestellenname[0]
        longname = None
        abkurzung = None
        synonym = None
        for x in haltestellenname[1:]:
            if x[:2] == '1>':
                longname = x[3:]
            elif x[:2] == '2>':
                abkurzung = x[3:]
            elif x[:2] == '3>':
                synonym = x[3:]
        bahnhofen.append({'haltestellennummer' : haltestellennummer,
                          'longname'  : longname,
                          'abkurzung' : abkurzung,
                          'name' : name,
                          'synonym' : synonym})
    return bahnhofen

def parse_bfkoord(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    bfkoord = []
    for line in l_content:
        haltestellennummer = line[:7].strip()
        x = float(line[9:18])
        y = float(line[20:29])
        z = line[31:36]
        bfkoord.append({'haltestellennummer' : haltestellennummer,
                        'x'  : x,
                        'y' : y,
                        'z' : z})
    return bfkoord

def parse_eckdaten(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    eckdaten = {}
    eckdaten['fahrplan_start'] = parse_day(l_content[0])
    eckdaten['fahrplan_end'] = parse_day(l_content[1])
    bezeichnung,fahrplan_periode,land,exportdatum,hrdf_version,lieferant = l_content[2].split('$')
    eckdaten['bezeichnung'] = bezeichnung
    eckdaten['fahrplan_periode'] = fahrplan_periode
    eckdaten['land'] = land
    eckdaten['exportdatum'] = exportdatum
    eckdaten['hrdf_version'] = hrdf_version
    eckdaten['lieferant'] = lieferant
    return eckdaten

def parse_bitfeld(zip,filename,eckdaten):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    bitfelds = []
    for line in l_content:
        bitfeldnummer = line[:6]
        bitfeld = {'bitfeldnummer' : bitfeldnummer,'dates' : []}
        bitstring = str(Bits(hex=line[8:]).bin)
        for z in range(0, len(bitstring)):
            if bitstring[z] == '1':
                bitfeld['dates'].append(eckdaten['fahrplan_start']  + timedelta(days=z))
        bitfelds.append(bitfeld)
    return bitfelds


def parse_bfkoord_geo(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    bfkoord_geo = []
    for line in l_content:
        haltestellennummer = int(line[:7].strip())
        x = float(line[9:18])
        y = float(line[20:29])
        z = line[31:36]
        bfkoord_geo.append({'haltestellennummer' : haltestellennummer,
                            'x'  : x,
                            'y' : y,
                            'z' : z})
    return bfkoord_geo

def parse_umsteigb(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    umsteigb = []
    for line in l_content:
        umsteigb.append({'haltestellennummer' : line[:8],
                         'umsteigezeit_ic' : line[8:10],
                         'umsteigezeit' : line[11:13]})
    return umsteigb

def parse_bfprios(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    bfprios = []
    for line in l_content:
        bfprios.append({'haltestellennummer' : line[:8],
                        'umsteigeprioritat' : line[8:10]})
    return bfprios

def parse_vereinig(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    vereinig = []
    for line in l_content:
        vereinig.append({'haltestellennummer1' : line[:8],
                         'haltestellennummer2' : line[8:15],
                         'fahrtnummer1' : line[16:21],
                         'verwaltung1' : line[22:28],
                         'fahrtnummer2' : line[29:34],
                         'verwaltung2' : line[35:41],
                         'kommentar' : line[42:]})
    return vereinig

def parse_infotext(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    infotext = []
    for line in l_content:
        infotext.append({'infotextnummer' : line[:8],
                         'informationstext' : line[8:]})
    return infotext

def parse_kminfo(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    kminfo = []
    for line in l_content:
        kminfo.append({'haltestellennummer' : line[:8],
                       'wert' : line[8:]})
    return kminfo

def parse_umsteigv(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    umsteigv = []
    for line in l_content:
        haltestellennummer = line[:8]
        if haltestellennummer == '@@@@@@@':
            haltestellennummer = None
        umsteigv.append({'haltestellennummer' : haltestellennummer,
                         'verwaltungsbezeichnung1' : line[8:14],
                         'verwaltungsbezeichnung2' : line[15:21],
                         'mindestumsteigezeit' : line[15:21]})
    return umsteigv

def parse_umsteigl(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    umsteigl = []
    for line in l_content:
        haltestellennummer = line[:8]
        if haltestellennummer == '@@@@@@@':
            haltestellennummer = None
        umsteigl.append({'haltestellennummer' : haltestellennummer,
                         'verwaltung1' : line[8:14],
                         'gattung1' : line[15:18],
                         'linie1' : line[19:27],
                         'richtung1' : line[28:29],
                         'verwaltung2' : line[30:36],
                         'gattung2' : line[37:40],
                         'linie2' : line[41:49],
                         'richtung2' : line[50:51],
                         'umsteigezeit' : line[52:55],
                         'garantiert' : line[55:56] == '!'})
    return umsteigl

def parse_umsteigz(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    umsteigz = []
    for line in l_content:
        haltestellennummer = line[:8]
        if haltestellennummer == '@@@@@@@':
            haltestellennummer = None
        umsteigz.append({'haltestellennummer' : haltestellennummer,
                         'fahrtnummer1' : line[8:14],
                         'verwaltung2' : line[14:20],
                         'fahrtnummer2' : line[22:26],
                         'verwaltung2' : line[27:33],
                         'umsteigezeit' : line[34:37],
                         'garantiert' : line[37:38] == '!'})
    return umsteigz

def parse_gleis(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    gleis = []
    for line in l_content:
        gleis.append({'haltestellennummer' : line[:8],
                       'fahrtnummer' : line[8:13],
                       'verwaltung' : line[14:20],
                       'gleisinformation' : line[21:29],
                       'zeit' : line[30:34],
                       'verkehrstageschlussel' : line[35:41]})
    return gleis

def parse_betrieb(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    betrieb1 = []
    betrieb2 = []
    for line in l_content:
        if line[6] == ':':
            betrieb2.append({'betreibernummer' : line[:5],
                             'verwaltungen' : line[8:]})
        else:
            raise Exception("Parse kurzname,longname,name here")
            betrieb1.append({'betreibernummer' : line[:5],
                             'kurzname' : line[8:13],
                             'langname' : line[14:20],
                             'name' : line[21:29],
                             'zeit' : line[30:34],
                             'verkehrstageschlussel' : line[35:41]})
    return betrieb1,betrieb2

def parse_attribut(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    attribut1 = []
    attribut2 = []
    for line in l_content:
        if line[0] == '#':
            attribut2.append({'code' : line[2:4],
                              'ausgabe_der_teilstrecke' : line[5:7], #Or wtf ever this means?/
                              'einstellig' : line[8:10]})
        else:
            attribut1.append({'code' : line[:2],
                              'haltestellenzugehorigkeit' : line[3:4],
                              'attributsausgabeprioritat' : line[5:8],
                              'attibutsausgabefeinsortierung' : line[9:11],
                              'text' : line[12:-1]})
    return attribut1,attribut2

def parse_richtung(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    richtung = []
    for line in l_content:
        richtung.append({'richtingschlussel' : line[:7],
                         'text' : line[9:]})
    return richtung

def parse_metabhf(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    metabhf_ubergangbeziehung = []
    metabhf_ubergangbeziehung_a = []
    metabhf_haltestellengruppen = []
    for i in range(len(l_content)):
        line = l_content[i]
        if len(l_content[i]) > 7 and l_content[i][7] == ':':
            item = {'sammelbegriffsnummer' : line[:7],
                    'haltestellennummers' : []}
            j = 10
            while j < len(line):
                item['haltestellennummers'].append(line[j:j+7])
                j += 9
            metabhf_haltestellengruppen.append(item)
        else:
            item = {'haltestellennummer1' : line[:7],
                    'haltestellennummer2' : line[8:15],
                    'dauer' : line[16:19]}
            metabhf_ubergangbeziehung.append(item)
            j = i
            while j < len(l_content):
                j += 1
                if l_content[j][:2] == '*A':
                    metabhf_ubergangbeziehung_a.append({'haltestellennummer1' : line[:7],
                                                        'haltestellennummer2' : line[8:15],
                                                        'attributscode' : l_content[j][3:5]})
                else:
                    break
    return metabhf_ubergangbeziehung,metabhf_ubergangbeziehung_a,metabhf_haltestellengruppen

def parse_zugart(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    zugart = []
    zugart_category = {}
    zugart_class = {}
    angaben = False
    language = None
    for line in l_content:
        if line == '':
            angaben = True
            continue
        if not angaben:
            item = {'code' : line[:3],
                    'produktklasse' : line[4:6],
                    'tarifgruppe' : line[7:8],
                    'ausgabesteuerung' : line[9:10],
                    'gattungsbezeichnung' : line[11:19].strip(),
                    'zuschlag' : line[20:21].strip(),
                    'flag' : line[22:23],
                    'gattungsbildernamen' : line[24:28].strip()}
            zugart.append(item)
            zugart_category[int(line[30:33])] = item
            zugart_class[int(item['produktklasse'])] = item
        else:
            if line == '<text>':
                continue
            elif line[0] == '<':
                language = line[1:-1].lower()
            elif line[:8] == 'category':
                zugart_category[int(line[8:11])]['category_'+language] = line[12:]
            #elif line[:6] == 'option': What does this do??
                #zugart_class[int(line[6:8])]['option_'+language] = line[9:]
            #elif line[:5] == 'class':
            #    zugart_class[int(line[5:7])]['class_'+language] = line[8:] #I dont think anyone cares for this, it's a superset of category
    return zugart

def parse_durchbi(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    durchbi = []
    for line in l_content:
        item = { 'fahrtnummer1' : line[:5],
                 'verwaltungfahtr1': line[6:12],
                 'letzterhaltderfahrt1': line[13:20],
                 'fahrtnummer2': line[21:26],
                 'verwaltungfahtr1': line[27:33],
                 'verkehrstagebitfeldnummer': line[34:40],
                 'ersterhaltderfahrt2': line[41:48],
                 'attributmarkierungdurchbindung': line[49:51],
                 'kommentar': line[52:] }
        durchbi.append(item)
    return durchbi

def parse_zeitvs(zip,filename):
    l_content = zip.read(filename).decode(charset).split('\r\n')[:-1]
    zeitvs = {}
    for line in l_content:
        bahnhofsnummer = line[:7]

        if (len(line) >= 17 and line[16] != '%'):
            item = { 'bahnhofsnummer' : bahnhofsnummer,
                     'zeitverschiebung': line[8:13],
                     'vondatum': line[14:22],
                     'vonzugehorigezeit': line[23:26],
                     'bisdatum': line[29:36],
                     'biszugehorigezeit': line[37:41],
                     'kommentar': line[42:] }
        else:
            item = zeitv[bahnhofsnummer].copy()
            item['bahnhofsnummer'] = bahnhofsnummer

        zeitvs[bahnhofsnummer] = item

    return zeitvs.values()

def filedict(zip):
    dict = {}
    for name in zip.namelist():
        dict[name] = name
    return dict

def load(path,filename):
    zip = zipfile.ZipFile(path+'/'+filename,'r')
    files = filedict(zip)
    bahnhofen = parse_bahnhof(zip,files['BAHNHOF'])
    bfkoord = parse_bfkoord(zip,files['BFKOORD'])
    bfkoord = parse_bfkoord_geo(zip,files['BFKOORD_GEO'])
    eckdaten = parse_eckdaten(zip,files['ECKDATEN'])
    bitfeld = parse_bitfeld(zip,files['BITFELD'],eckdaten)
    zugart = parse_zugart(zip,files['ZUGART'])
    metabhf_ubergangbeziehung,metabhf_ubergangbeziehung_a,metabhf_haltestellengruppen = parse_metabhf(zip,files['METABHF'])
    umsteigb = parse_umsteigb(zip,files['UMSTEIGB'])
    attribut_de_1,attribut_de_2 = parse_attribut(zip,files['ATTRIBUT_DE'])
    attribut_fr_1,attribut_fr_2 = parse_attribut(zip,files['ATTRIBUT_FR'])
    attribut_it_1,attribut_it_2 = parse_attribut(zip,files['ATTRIBUT_IT'])
    attribut_en_1,attribut_en_2 = parse_attribut(zip,files['ATTRIBUT_EN'])
    bfprios = parse_bfprios(zip,files['BFPRIOS'])
    infotext_en = parse_infotext(zip,files['INFOTEXT_EN'])
    infotext_fr = parse_infotext(zip,files['INFOTEXT_FR'])
    infotext_it = parse_infotext(zip,files['INFOTEXT_IT'])
    infotext_de = parse_infotext(zip,files['INFOTEXT_DE'])
    kminfo = parse_kminfo(zip,files['KMINFO'])
    umsteigv = parse_umsteigv(zip,files['UMSTEIGV'])
    umsteigl = parse_umsteigl(zip,files['UMSTEIGL'])
    umsteigz = parse_umsteigz(zip,files['UMSTEIGZ'])
    if 'VEREINIG' in files:
        vereinig = parse_vereinig(zip,files['VEREINIG'])
    durchbi = parse_durchbi(zip,files['DURCHBI'])
    richtung = parse_richtung(zip,files['RICHTUNG'])
    zeitvs = parse_zeitvs(zip,files['ZEITVS'])
    gleis = parse_gleis(zip,files['GLEIS'])
    betrieb1_en,betrieb2_en = parse_betrieb(zip,files['BETRIEB_EN'])
    betrieb1_de,betrieb2_de = parse_betrieb(zip,files['BETRIEB_DE'])
    betrieb1_it,betrieb2_it= parse_betrieb(zip,files['BETRIEB_IT'])
    betrieb1_fr,betrieb2_fr = parse_betrieb(zip,files['BETRIEB_FR'])
if __name__ == '__main__':
    load(sys.argv[1],sys.argv[2])
