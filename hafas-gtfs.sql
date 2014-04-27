CREATE OR REPLACE FUNCTION
toseconds(time24 text, shift24 integer) RETURNS integer AS $$
SELECT total AS time
FROM
(SELECT
  (cast(split_part($1, ':', 1) as int4) * 3600)      -- hours
+ (cast(split_part($1, ':', 2) as int4) * 60)        -- minutes
+ CASE WHEN $1 similar to '%:%:%' THEN (cast(split_part($1, ':', 3) as int4)) ELSE 0 END -- seconds when applicable
+ (shift24 * 86400) as total --Add 24 hours (in seconds) when shift occured
) as xtotal
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION
to32time(secondssincemidnight integer) RETURNS text AS $$
SELECT lpad(floor((secondssincemidnight / 3600))::text, 2, '0')||':'||lpad(((secondssincemidnight % 3600) / 60)::text, 2,
'0')||':'||lpad((secondssincemidnight % 60)::text, 2, '0') AS time
$$ LANGUAGE SQL;

create table blocks as with recursive chain (fahrtnummer, verwaltungfahrt, verkehrstagebitfeldnummer, parent) AS (
    SELECT fahrtnummer2, verwaltungfahrt2, verkehrstagebitfeldnummer,
    fahrtnummer1||'|'||verwaltungfahrt1||'|'||verkehrstagebitfeldnummer as parent
    FROM durchbi
    union
    SELECT b.fahrtnummer2, b.verwaltungfahrt2, b.verkehrstagebitfeldnummer, parent
    FROM chain AS c JOIN durchbi AS b ON (c.fahrtnummer = b.fahrtnummer1 and c.verwaltungfahrt = b.verwaltungfahrt1 and c.verkehrstagebitfeldnummer = b.verkehrstagebitfeldnummer)
)
SELECT parent AS block_id, fahrtnummer, verwaltungfahrt, verkehrstagebitfeldnummer FROM chain;

create table gtfs_route_type (verkehrsmittel char(3) primary key, route_type smallint);
copy gtfs_route_type from '/home/skinkie/Sources/hafas-fubar/route_type.csv' csv header;

copy (select bahnhof.haltestellennummer as stop_id, name as stop_name, y as stop_lat, x as stop_lon, (a.sammelbegriffsnummer IS NOT NULL)::int4 AS location_type, b.sammelbegriffsnummer as parent_station from bahnhof join bfkoord_geo using (haltestellennummer) left join metabhf_haltestellengruppen as a on (bahnhof.haltestellennummer = a.sammelbegriffsnummer) left join metabhf_haltestellengruppen as b on (bahnhof.haltestellennummer = b.haltestellennummer)) to '/var/tmp/stops.txt' csv header;

-- copy (select haltestellennummer as stop_id, name as stop_name, y as stop_lat, x as stop_lon from bahnhof join bfkoord_geo using (haltestellennummer)) to '/var/tmp/stops.txt' csv header;

copy (select betreibernummer as betreibernummer, langname as agency_name, 'http://sbb.ch/' as agency_url, 'Europe/Zurich' as agency_timezone from betrieb1_en join betrieb2_en using (betreibernummer)) to '/var/tmp/agency.txt' csv header;

copy (select distinct fahrtnummer||'|'||verwaltung||'|'||variante as route_id, verwaltung as agency_id, coalesce(liniennummer, verkehrsmittel) as route_short_name, a.name || ' - ' || b.name as route_long_name, route_type from fplan_z join fplan_g using (id) join fplan_ave using (id) join bahnhof as a on (fplan_g.laufwegsindexab = a.haltestellennummer) join bahnhof as b on (fplan_g.laufwegsindexbis = b.haltestellennummer) join gtfs_route_type using (verkehrsmittel) left join fplan_l using (id) order by route_id) to '/var/tmp/routes.txt' csv header;

copy (select route_id, service_id, trip_id, trip_headsign, direction_id, block_id from (
select fahrtnummer||'|'||verwaltung||'|'||variante as route_id, coalesce(verkehrstagenummer, 0) as service_id, id as trip_id, coalesce(richtung.text, a.name) as trip_headsign, richtung as direction_id, fahrtnummer, verwaltung from fplan_z join fplan_g using (id) join fplan_ave using (id) join bahnhof as a on (fplan_g.laufwegsindexbis = a.haltestellennummer) left join fplan_r using (id) left join richtung on (fplan_r.richtungscode = richtung.richtingschlussel)) as x left join blocks as y on (x.fahrtnummer = y.fahrtnummer and x.verwaltung = y.verwaltungfahrt and x.service_id = y.verkehrstagebitfeldnummer) order by trip_id) to '/var/tmp/trips.txt' csv header;

copy (select id as trip_id, to32time(start_time), to32time((((end_time - start_time) + (60 * takzeit)) * taktanzahl) + start_time) as end_time, (takzeit::integer * 60) as headway_secs, 1 as exact_times from fplan_z join (select id, toseconds(left(lpad(abfahrtszeit::text, 4, '0'), 2)||':'||right(abfahrtszeit::text, 2)||':00', 0) as start_time, toseconds(left(lpad(ankunfstzeit::text, 4, '0'), 2)||':'||right(ankunfstzeit::text, 2)||':00', 0) as end_time from (select id, min(abs(abfahrtszeit)) as abfahrtszeit, max(abs(ankunfstzeit)) as ankunfstzeit from fplan_laufweg group by id) as x) as y using (id) where taktanzahl is not null and taktanzahl > 1) to '/var/tmp/frequencies.txt' csv header;

--copy (select fahrtnummer||'|'||verwaltung||'|'||variante as route_id, coalesce(verkehrstagenummer, 0) as service_id, id as trip_id, coalesce(richtung.text, a.name) as trip_headsign, richtung as direction_id  from fplan_z join fplan_g using (id) join fplan_ave using (id) join bahnhof as a on (fplan_g.laufwegsindexbis = a.haltestellennummer) left join fplan_r using (id) left join richtung on (fplan_r.richtungscode = richtung.richtingschlussel) order by trip_id) to '/var/tmp/trips.txt' csv header;

copy (select id as trip_id, left(lpad(ankunfstzeit::text, 4, '0'), 2)||':'||right(ankunfstzeit::text, 2)||':00' as arrival_time, left(lpad(abfahrtszeit::text, 4, '0'), 2)||':'||right(abfahrtszeit::text, 2)||':00' as departure_time, haltestellennummer as stop_id, row_number() OVER (PARTITION BY id ORDER BY coalesce(abs(ankunfstzeit), abs(abfahrtszeit))) as stop_sequence, case when abs(abfahrtszeit) is not null and abfahrtszeit > 0 then 0 else 1 end as pickup_type, case when abs(ankunfstzeit) is not null and ankunfstzeit > 0 then 0 else 1 end drop_off_type from fplan_laufweg) to '/var/tmp/stop_times.txt' csv header;

copy (select 0 as service_id, 1 as monday, 1 as tuesday, 1 as wednesday, 1 as thursday, 1 as friday, 1 as saturday, 1 as sunday, replace(fahrplan_start::text, '-', '') as start_date, replace(fahrplan_end::text, '-', '') as end_date from eckdaten limit 1) to '/var/tmp/calendar.txt' csv header;

copy (select bitfeldnummer as service_id, replace(servicedate::text, '-', '') as date, 1 as exception_type from bitfeld) to '/var/tmp/calendar_dates.txt' csv header;

copy (select haltestellennummer1 as from_stop_id, haltestellennummer2 as to_stop_id, 2 as transfer_type, dauer * 60 as min_transfer_time from metabhf_ubergangbeziehung) to '/var/tmp/transfers.txt' csv header;

copy (select 'OVapi' as feed_publisher_name, 'http://gtfs.ovapi.nl/' as feed_publisher_url, 'en' as feed_lang, replace(fahrplan_start::text, '-', '') as feed_start_date, replace(fahrplan_end::text, '-', '') as feed_end_date, hrdf_version as feed_version from eckdaten limit 1) to '/var/tmp/feedinfo.txt' csv header;
