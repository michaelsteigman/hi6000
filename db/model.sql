CREATE TABLE ensemble_pairs (
 pair_ID        SERIAL PRIMARY KEY,
 med_tring   	TEXT,
 problem_tring 	TEXT,
 source_file 	TEXT
);

create table ensemble_class_dictionary (
 item_string         TEXT,
 concept	     TEXT,
 label		     TEXT,
 vocabs 	     TEXT[],
 semantic_types      TEXT[],
 status		     TEXT,
 version 	     TEXT,
 PRIMARY KEY (item_string, concept)
);

-- flatten multiple concepts, concatenating the label
CREATE VIEW ensemble_class_dictionary_flat_problems AS
     SELECT item_string, string_agg(label, ', ') AS label
       FROM ensemble_class_dictionary
   GROUP BY item_string;

-- flatten dupe meds caused by varying case
--
-- this is a quick fix - if we do metamap for meds, should
-- give meds the cat * | lower | sort | uniq treatment
CREATE VIEW ensemble_class_dictionary_flat_meds AS
     SELECT upper(item_string) AS item_string, label
       FROM ensemble_class_dictionary
   GROUP BY upper(item_string), label;

-- specific to project data
-- views of each pairs list
CREATE VIEW cs_ut_pairs AS
     SELECT coalesce(m.label, p.med_string) AS med, coalesce(pr.label, p.problem_string) AS problem
       FROM ensemble_pairs p
  LEFT JOIN ensemble_class_dictionary_flat_meds m ON (lower(p.med_string) = lower(m.item_string))
  LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (lower(p.problem_string) = lower(pr.item_string))
      WHERE source_file = 'cs_ut';

CREATE VIEW rep_ut_pairs AS
     SELECT coalesce(m.label, p.med_string) AS med, coalesce(pr.label, p.problem_string) AS problem
       FROM ensemble_pairs p
  LEFT JOIN ensemble_class_dictionary_flat_meds m ON (lower(p.med_string) = lower(m.item_string))
  LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (lower(p.problem_string) = lower(pr.item_string))
      WHERE source_file = 'rep_ut';

CREATE VIEW uth_pairs AS
     SELECT coalesce(m.label, p.med_string) AS med, coalesce(pr.label, p.problem_string) AS problem
       FROM ensemble_pairs p
  LEFT JOIN ensemble_class_dictionary_flat_meds m ON (lower(p.med_string) = lower(m.item_string))
  LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (lower(p.problem_string) = lower(pr.item_string))
      WHERE source_file = 'uth';

CREATE VIEW bwh_pairs AS
     SELECT coalesce(m.label, p.med_string) AS med, coalesce(pr.label, p.problem_string) AS problem
       FROM ensemble_pairs p
  LEFT JOIN ensemble_class_dictionary_flat_meds m ON (lower(p.med_string) = lower(m.item_string))
  LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (lower(p.problem_string) = lower(pr.item_string))
      WHERE source_file = 'bwh';

CREATE VIEW bcbstx_pairs AS
     SELECT coalesce(m.label, p.med_string) AS med, coalesce(pr.label, p.problem_string) AS problem
       FROM ensemble_pairs p
  LEFT JOIN ensemble_class_dictionary_flat_meds m ON (lower(p.med_string) = lower(m.item_string))
  LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (lower(p.problem_string) = lower(pr.item_string))
      WHERE source_file = 'bcbstx';

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

-- SQL to output pairs
COPY (SELECT COALESCE(m.label, p.med_string) AS med, COALESCE(pr.label, p.problem_string) AS problem
        FROM ensemble_pairs p
   LEFT JOIN ensemble_class_dictionary_flat_meds m ON (lower(p.med_string) = lower(m.item_string))
   LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (lower(p.problem_string) = lower(pr.item_string))
       WHERE source_file = 'uth'
    ORDER BY pair_id ASC) TO '/tmp/uth.csv' WITH csv;

COPY (SELECT COALESCE(m.label, p.med_string) AS med, COALESCE(pr.label, p.problem_string) AS problem
        FROM ensemble_pairs p
   LEFT JOIN ensemble_class_dictionary_flat_meds m ON (lower(p.med_string) = lower(m.item_string))
   LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (lower(p.problem_string) = lower(pr.item_string))
       WHERE source_file = 'bwh'
    ORDER BY pair_id ASC) TO '/tmp/bwh.csv' WITH csv;

COPY (SELECT COALESCE(m.label, p.med_string) AS med, COALESCE(pr.label, p.problem_string) AS problem
        FROM ensemble_pairs p
   LEFT JOIN ensemble_class_dictionary_flat_meds m ON (lower(p.med_string) = lower(m.item_string))
   LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (lower(p.problem_string) = lower(pr.item_string))
       WHERE source_file = 'bcbstx'
    ORDER BY pair_id ASC) TO '/tmp/bcbstx.csv' WITH csv;

COPY (SELECT COALESCE(m.label, p.med_string) AS med, COALESCE(pr.label, p.problem_string) AS problem
        FROM ensemble_pairs p
   LEFT JOIN ensemble_class_dictionary_flat_meds m ON (lower(p.med_string) = lower(m.item_string))
   LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (lower(p.problem_string) = lower(pr.item_string))
       WHERE source_file = 'rep_ut'
    ORDER BY pair_id ASC) TO '/tmp/rep_ut.csv' WITH csv;

COPY (SELECT COALESCE(m.label, p.med_string) AS med, COALESCE(pr.label, p.problem_string) AS problem
        FROM ensemble_pairs p
   LEFT JOIN ensemble_class_dictionary_flat_meds m ON (lower(p.med_string) = lower(m.item_string))
   LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (lower(p.problem_string) = lower(pr.item_string))
       WHERE source_file = 'cs_ut'
    ORDER BY pair_id ASC) TO '/tmp/cs_ut.csv' WITH csv;
    
-- finding overlap
select count(*), b.med, b.problem from bwh_pairs a join uth_pairs u on b.med = u.med and b.problem = u.problem group by b.med, b.problem;

-- count the overlap between two sources of pairs
select count(*) from (select 1 from bwh_pairs a join uth_pairs b on a.med = b.med and a.problem = b.problem group by a.med, a.problem) t;
select count(*) from (select 1 from bwh_pairs a join bcbstx_pairs b on a.med = b.med and a.problem = b.problem group by a.med, a.problem) t;
select count(*) from (select 1 from bwh_pairs a join rep_ut_pairs b on a.med = b.med and a.problem = b.problem group by a.med, a.problem) t;
select count(*) from (select 1 from cs_ut_pairs a join bwh_pairs b on a.med = b.med and a.problem = b.problem group by a.med, a.problem) t;
select count(*) from (select 1 from cs_ut_pairs a join bcbstx_pairs b on a.med = b.med and a.problem = b.problem group by a.med, a.problem) t;
select count(*) from (select 1 from cs_ut_pairs a join rep_ut_pairs b on a.med = b.med and a.problem = b.problem group by a.med, a.problem) t;
select count(*) from (select 1 from cs_ut_pairs a join uth_pairs b on a.med = b.med and a.problem = b.problem group by a.med, a.problem) t;
select count(*) from (select 1 from rep_ut_pairs a join bcbstx_pairs b on a.med = b.med and a.problem = b.problem group by a.med, a.problem) t;
select count(*) from (select 1 from rep_ut_pairs a join uth_pairs b on a.med = b.med and a.problem = b.problem group by a.med, a.problem) t;
select count(*) from (select 1 from bcbstx_pairs a join uth_pairs b on a.med = b.med and a.problem = b.problem group by a.med, a.problem) t;

select count(*)
from uth_pairs a
join rep_ut_pairs b on a.med = b.med and a.problem = b.problem
join cs_ut_pairs c on b.med = c.med and b.problem = c.problem
join bwh_pairs d on c.med = d.med and c.problem = d.problem
join bcbstx_pairs e on d.med = e.med and d.problem = e.problem;
group by 1.med, 1.problem;
