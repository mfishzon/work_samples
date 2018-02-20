----------------------------------------------------------------
-- code below 
--
--1. generates vw_building_address MATERIALIZED VIEW to improve search efficiency. source data cONtains ranges for house numbers AND multiple street entrences per building
--
--2. adds additiONal full text search field to the address table to improve search efficiency 
--
--3. generates address_search helper FUNCTION that is executed AS part of the bulding_search WHENever address_string is pASsed AS a parameter. this FUNCTION may be used AS a stANDalONe DAL method.
--
--4. generate building_search FUNCTION
-----------------------------------------------------------------


-- 1. vw_building_address MATERIALIZED VIEW
-- this materilized VIEW cONtains denormolazed bulding/lot/address_lot/address_bulding/address mapping,
-- street address is merged into a single field
-- house number ranges are flattened into a single address record

DROP MATERIALIZED VIEW IF EXISTS vw_building_address;                  
CREATE MATERIALIZED VIEW vw_building_address AS  
      WITH address_mapping AS (
      SELECT DISTINCT 
                   b.bbl,
                   b.bin,
                   b.id AS building_id,
                   b.lot_id,
                   ba.pad_range_id,
                   lh.min_sequence,
                   lh.max_sequence
             FROM building b
              JOIN building_address ba
                ON b.id = ba.building_id 
              JOIN (SELECT building_id,
                           pad_range_id,
                           min(house_number_sequence) AS min_sequence,
                           max(house_number_sequence) AS max_sequence
                      FROM building_address
                     group by  1,2
                    ) AS lh
                ON b.id = lh.building_id
               AND ba.pad_range_id = lh.pad_range_id
            )
             SELECT DISTINCT
                    aa.building_id, 
                    aa.lot_id,
                    aa.bbl,
                    aa.bin,
                    (CASE WHEN lh.house_number_sequence =  hh.house_number_sequence then lh.house_number
                          ELSE lh.house_number || ' -- ' || hh.house_number
                     END) || ' ' || lh.street_name AS street_address,
                    lh.borough_id,
                    lh.zipcode,
                    lh.state,
                    aa.pad_range_id,
                    lh.id AS min_address_id,
                    hh.id AS max_address_id,
                    la.address_id AS lot_address_ID
               FROM address_mapping aa
               JOIN address lh
                 ON aa.pad_range_id = lh.pad_range_id
                AND aa.min_sequence = lh.house_number_sequence
               JOIN address hh
                 ON aa.pad_range_id = hh.pad_range_id
                AND aa.max_sequence = hh.house_number_sequence
               LEFT JOIN lot_address la
                 ON la.lot_id = aa.lot_id
             
;

--MATERIALIZED VIEW CONSTRAINTS
DROP INDEX IF EXISTS vw_building_address__pad_range_id_idx;
CREATE UNIQUE INDEX vw_building_address__pad_range_id_idx ON vw_building_address(pad_range_id);

DROP INDEX IF EXISTS vw_building_address__building_id_idx;
CREATE INDEX vw_building_address__building_id_idx ON vw_building_address(building_id);

DROP INDEX IF EXISTS vw_building_address__bbl_idx;
CREATE INDEX vw_building_address__bbl_idx ON vw_building_address(bbl);

ANALYZE vw_building_address;


---------REFRESH VIEW--------------------------------------
REFRESH MATERIALIZED VIEW CONCURRENTLY vw_building_address;  
------------------------------------------------------------

--2. add address_string fileds to address table------------------------------------

-- address_string
ALTER TABLE public.address ADD COLUMN street_address tsvector;

UPDATE public.address
SET street_address = to_tsvector(coalesce(upper(house_number),'') ||' '|| coalesce(street_name,'') );

-- address_string constraints
CREATE INDEX address__street_address_vector_idx ON public.address
 USING gin(street_address ) ;

CREATE INDEX address__zip_idx ON address(zipcode); -- zip INDEX

CREATE INDEX address__bourough_id_idx ON address(borough_id); --boro INDEX
 
ANALYZE address;

--3. get_address FUNCTION code------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_address(character varying, numeric,smallint);


CREATE OR REPLACE FUNCTION get_address( 
    in_address varchar(200) DEFAULT NULL, 
    in_zip numeric(5,0) DEFAULT NULL,
    in_boro integer DEFAULT NULL
  )
    
  RETURNS table (
    borough_id smallint,
    house_number varchar(20),
    house_number_sequence integer,
    id integer,
    pad_range_id integer,
    street_name varchar(100),
    state character(2),
    zipcode numeric(5,0)
    )
   AS $$

   DECLARE
    in_addres ALIAS FOR $1;
    in_zip    ALIAS for $2;
    in_boro   ALIAS FOR $3;
    address_id integer;
  

   BEGIN
     SELECT replace(in_address,' ','&') INTO in_address;
  
     RETURN QUERY
       SELECT a.borough_id,
              a.house_number,
              a.house_number_sequence,
              a.id,
              a.pad_range_id,
              a.street_name,
              a.state,
              a.zipcode
         FROM address a
      WHERE street_address @@ to_tsquery(in_address) 
        AND (a.borough_id = in_boro or in_boro is null)
        AND (a.zipcode = in_zip  or in_zip is null)
        
     ;
    
   END;
$$ LANGUAGE plpgsql IMMUTABLE;

--address_search test CASEs--------------------------------------------------------------------------
SELECT * FROM get_address('192 ETNA STREET',null,null) -- upper CASE address string 
SELECT * FROM get_address('192 etna street',null,null) -- lower CASE address string 

SELECT * FROM get_address('192 ETNA STREET',null,3) -- string + zip
SELECT * FROM get_address('192 ETNA STREET',null,3) -- string + boro
SELECT * FROM get_address('192 ETNA STREET',11208,3) -- string +boro + zip

SELECT * FROM get_address('0 main street') -- null result

SELECT * FROM get_address('1326 EAST 86 STREET 11236',null) -- building a, entrance a
SELECT * FROM get_address('378 THROOP AVENUE 11221',NULL) -- building a,  entrance b

SELECT * FROM get_address('378 THROOP AVENUE') -- address string WITHout zip/boro

SELECT * FROM get_address('12 GREENE',null) -- partial house number search

SELECT * FROM get_address(null,11208,null) --zip ONly
SELECT * FROM get_address(null,null,3) --boro ONly


--4. building_search FUNCTION----------------------------------
DROP FUNCTION IF EXISTS building_search( bigint, integer, varchar, numeric, integer);

CREATE or replace FUNCTION building_search(
      in_bbl bigint DEFAULT NULL, 
      in_building_id integer DEFAULT NULL,
      in_address varchar(200) DEFAULT NULL, 
      in_zip numeric(5,0) default null,
      in_boro integer DEFAULT NULL
     ) 
  RETURNS TABLE (
    building_id integer,
    lot_id integer,
    bbl bigint,
    bin bigint,
    street_address TEXT,
    borough varchar(15),
    zipcode numeric(5,0),
    state char(2)
    )
   AS $$

   DECLARE
    in_bbl ALIAS FOR $1;
    in_building_id ALIAS for $2;
    in_address ALIAS for $3;
    in_zip ALIAS for $4;
    in_boro ALIAS for $5;
    
    found_address_id integer default NULL;
    found_lot_id integer default null;
    
    BEGIN
      IF ( (in_bbl is not null or in_building_id is not null)  AND in_address is null AND in_zip is null AND in_boro is null )then
        RETURN QUERY
          SELECT v.building_id,
                 v.lot_id,
                 v.bbl,
                 v.bin,
                 v.street_address,
                 b.descriptiON AS borough,
                 v.zipcode,
                 v.state 
            FROM vw_building_address v
            JOIN borough b
             ON b.id = v.borough_id
           WHERE v.building_id = in_building_id
              or v.bbl = in_bbl
         ;
      ELSIF (in_bbl is null AND in_building_id is null AND (in_address is not null or in_zip is not null or in_boro is not null) )THEN
        SELECT id
          into found_address_id  
          FROM get_address(in_address, in_zip, in_boro)
         ;
       
        RETURN QUERY
            SELECT  v.building_id,
                 v.lot_id,
                 v.bbl,
                 v.bin,
                 v.street_address,
                 b.descriptiON AS borough,
                 v.zipcode,
                 v.state 
                FROM vw_building_address v
                JOIN borough b
                  ON b.id = v.borough_id
               WHERE v.pad_range_id in (SELECT pad_range_id FROM get_address(in_address, in_zip, in_boro)) 
                  or v.lot_address_id in (SELECT id FROM get_address(in_address, in_zip, in_boro))
               ;

      END IF;  
 
  
   END;
$$ LANGUAGE plpgsql IMMUTABLE;

--building_search test CASEs-------------------------------
SELECT * FROM building_search('3017860039',null,null,null) -- building_id ONly/optiONal parameters
SELECT * FROM building_search('3017860039') -- building_id ONly/optiONal parameters

SELECT * FROM building_search(null,'408409',null,null) --lot_id ONly;
SELECT * FROM building_search(null,null,'1326 EAST 86 STREET 11236',Null) --address_string + zip
SELECT * FROM building_search(null,null,'1326 EAST 86 STREET 3',Null) --address_string + boro

SELECT * FROM building_search(null,null,'18-08 122 STREET 11356',null) --house_number cONtaining a range of entrances

SELECT * FROM building_search(null,null,'1 AVENUE B 10302',null) -- multiple entrance building
SELECT * FROM building_search(null,null,'10 RICHMOND ROAD 11363',null) -- single entrance building

-- entrances that were CREATEd AS separate building - known source data issue(multiple BBL per building, BBL reASsigned)
SELECT * FROM building_search(null,null,'388 THROOP AVENUE 11221',NULL)
SELECT * FROM building_search(null,null,'386 THROOP AVENUE 11221',NULL)
SELECT * FROM building_search(null,null,'378 THROOP AVENUE 11221',NULL)
SELECT * FROM building_search(null,null,'378A THROOP AVENUE 11221',NULL)

SELECT * FROM building_search(null,null,'761 LAFAYETTE AVENUE 11221',NULL)
SELECT * FROM building_search(null,null,'761 LAFAYETTE AVENUE',NULL)

SELECT * FROM building_search(null,null,' 246 Cornelia Street 11221',NULL)

SELECT * FROM building_search(in_address := '61 ORIENT AVENUE' ) -- named parameters
