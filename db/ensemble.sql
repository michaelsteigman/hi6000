--
-- original pairs, loaded from csv
--
CREATE TABLE ensemble_pairs (
 pair_ID        SERIAL PRIMARY KEY,
 med_string   	TEXT,
 problem_string	TEXT,
 source_file 	TEXT
);

CREATE VIEW orig_bcbstx_pairs AS
     SELECT med_string, problem_string
       FROM ensemble_pairs p
      WHERE source_file = 'bcbstx';
      
CREATE VIEW orig_uth_pairs AS
     SELECT med_string, problem_string
       FROM ensemble_pairs p
      WHERE source_file = 'uth';

CREATE VIEW orig_bwh_pairs AS
     SELECT med_string, problem_string
       FROM ensemble_pairs p
      WHERE source_file = 'bwh';

CREATE VIEW orig_rep_ut_pairs AS
     SELECT med_string, problem_string
       FROM ensemble_pairs p
      WHERE source_file = 'rep_ut';

CREATE VIEW orig_cs_ut_pairs AS
     SELECT med_string, problem_string
       FROM ensemble_pairs p
      WHERE source_file = 'cs_ut';

--
-- views of each pairs list, used to generate output files
-- 
CREATE OR REPLACE VIEW cs_ut_pairs AS
     SELECT COALESCE(m.label, p.med_string) AS med, COALESCE(pr.label, p.problem_string) AS problem
       FROM ensemble_pairs p
  LEFT JOIN ensemble_class_dictionary_flat_meds m ON (LOWER(p.med_string) = LOWER(m.item_string))
  LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (LOWER(p.problem_string) = LOWER(pr.item_string))
      WHERE source_file = 'cs_ut'
      	AND med_string IS NOT NULL
	AND problem_string IS NOT NULL
      ORDER BY pair_id ASC;

CREATE OR REPLACE VIEW rep_ut_pairs AS
     SELECT COALESCE(m.label, p.med_string) AS med, COALESCE(pr.label, p.problem_string) AS problem
       FROM ensemble_pairs p
  LEFT JOIN ensemble_class_dictionary_flat_meds m ON (LOWER(p.med_string) = LOWER(m.item_string))
  LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (LOWER(p.problem_string) = LOWER(pr.item_string))
      WHERE source_file = 'rep_ut'
      	AND med_string IS NOT NULL
	AND problem_string IS NOT NULL
      ORDER BY pair_id ASC;

CREATE OR REPLACE VIEW uth_pairs AS
     SELECT COALESCE(m.label, p.med_string) AS med, COALESCE(pr.label, p.problem_string) AS problem
       FROM ensemble_pairs p
  LEFT JOIN ensemble_class_dictionary_flat_meds m ON (LOWER(p.med_string) = LOWER(m.item_string))
  LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (LOWER(p.problem_string) = LOWER(pr.item_string))
      WHERE source_file = 'uth'
       	AND med_string IS NOT NULL
	AND problem_string IS NOT NULL
      ORDER BY pair_id ASC;

CREATE OR REPLACE VIEW bwh_pairs AS
     SELECT COALESCE(m.label, p.med_string) AS med, COALESCE(pr.label, p.problem_string) AS problem
       FROM ensemble_pairs p
  LEFT JOIN ensemble_class_dictionary_flat_meds m ON (LOWER(p.med_string) = LOWER(m.item_string))
  LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (LOWER(p.problem_string) = LOWER(pr.item_string))
      WHERE source_file = 'bwh'
       	AND med_string IS NOT NULL
	AND problem_string IS NOT NULL
      ORDER BY pair_id ASC;

CREATE OR REPLACE VIEW bcbstx_pairs AS
     SELECT COALESCE(m.label, p.med_string) AS med, COALESCE(pr.label, p.problem_string) AS problem
       FROM ensemble_pairs p
  LEFT JOIN ensemble_class_dictionary_flat_meds m ON (LOWER(p.med_string) = LOWER(m.item_string))
  LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (LOWER(p.problem_string) = LOWER(pr.item_string))
      WHERE source_file = 'bcbstx'
       	AND med_string IS NOT NULL
	AND problem_string IS NOT NULL
      ORDER BY pair_id ASC;

--
-- grouped pairs, for looking at overlap between sources
--
CREATE OR REPLACE VIEW bwh_grouped_pairs AS
     SELECT count(*) as count, med, problem
       FROM bwh_pairs
   GROUP BY med, problem;

CREATE OR REPLACE VIEW bcbstx_grouped_pairs AS
     SELECT count(*) as count, med, problem
       FROM bcbstx_pairs
   GROUP BY med, problem;

CREATE OR REPLACE VIEW uth_grouped_pairs AS
     SELECT count(*) as count, med, problem
       FROM uth_pairs
   GROUP BY med, problem;

CREATE OR REPLACE VIEW rep_ut_grouped_pairs AS
     SELECT count(*) as count, med, problem
       FROM rep_ut_pairs
   GROUP BY med, problem;

CREATE OR REPLACE VIEW cs_ut_grouped_pairs AS
     SELECT count(*) as count, med, problem
       FROM cs_ut_pairs
   GROUP BY med, problem;

-- SQL to output pairs
COPY (SELECT * FROM uth_pairs) TO '/tmp/uth.csv' WITH csv;
COPY (SELECT * FROM bwh_pairs) TO '/tmp/bwh.csv' WITH csv;
COPY (SELECT * FROM bcbstx_pairs) TO '/tmp/bcbstx.csv' WITH csv;
COPY (SELECT * FROM rep_ut_pairs) TO '/tmp/rep_ut.csv' WITH csv;
COPY (SELECT * FROM cs_ut_pairs) TO '/tmp/cs_ut.csv' WITH csv;

-- count the overlap between two sources of pairs
SELECT count(*)
  FROM bwh_grouped_pairs a
  JOIN uth_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem);

SELECT count(*)
  FROM bwh_grouped_pairs a
  JOIN rep_ut_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem);

SELECT count(*)
  FROM bwh_grouped_pairs a
  JOIN cs_ut_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem);

SELECT count(*)
  FROM bwh_grouped_pairs a
  JOIN bcbstx_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem);

SELECT count(*)
  FROM bcbstx_grouped_pairs a
  JOIN cs_ut_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem);

SELECT count(*)
  FROM bcbstx_grouped_pairs a
  JOIN rep_ut_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem);

SELECT count(*)
  FROM bcbstx_grouped_pairs a
  JOIN uth_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem);

SELECT count(*)
  FROM uth_grouped_pairs a
  JOIN cs_ut_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem);

SELECT count(*)
  FROM uth_grouped_pairs a
  JOIN rep_ut_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem);

SELECT count(*)
  FROM uth_grouped_pairs a
  JOIN rep_ut_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem);

SELECT count(*)
  FROM cs_ut_grouped_pairs a
  JOIN rep_ut_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem);
  
-- overlap between all sources
CREATE VIEW all_overlapping_pairs AS
     SELECT a.count + b.count + c.count + d.count + e.count as total, a.med, a.problem
       FROM uth_grouped_pairs a
       JOIN rep_ut_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem)
       JOIN cs_ut_grouped_pairs c ON (b.med = c.med AND b.problem = c.problem)
       JOIN bwh_grouped_pairs d ON (c.med = d.med AND c.problem = d.problem)
       JOIN bcbstx_grouped_pairs e ON (d.med = e.med AND d.problem = e.problem)
   ORDER BY total DESC;

COPY (SELECT * FROM all_overlapping_pairs) TO '/tmp/all.csv' WITH csv;

COPY (   SELECT COALESCE(a.med || '/' || a.problem,
                         b.med || '/' || b.problem,
			 c.med || '/' || c.problem,
			 d.med || '/' || d.problem,
			 e.med || '/' || e.problem, '?'),
   	  	CASE WHEN a.med IS NULL THEN 0 ELSE 1 END AS BWH,
	        CASE WHEN b.med IS NULL THEN 0 ELSE 1 END AS UTH,
	        CASE WHEN c.med IS NULL THEN 0 ELSE 1 END AS REP_UT,
 	        CASE WHEN d.med IS NULL THEN 0 ELSE 1 END AS CS_UT,
                CASE WHEN e.med IS NULL THEN 0 ELSE 1 END AS BCBSTX
           FROM bwh_grouped_pairs a
FULL OUTER JOIN uth_grouped_pairs b ON (a.med = b.med AND a.problem = b.problem)
FULL OUTER JOIN rep_ut_grouped_pairs c ON (b.med = c.med AND b.problem = c.problem)
FULL OUTER JOIN cs_ut_grouped_pairs d ON (c.med = d.med AND c.problem = d.problem)
FULL OUTER JOIN bcbstx_grouped_pairs e ON (d.med = e.med AND d.problem = e.problem)) TO '/tmp/all.csv' WITH CSV HEADER;
